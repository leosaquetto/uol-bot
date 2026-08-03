// scriptable uol - fallback único (ingressos)
// executa somente campanhas de ingresso (até 4 ofertas), para uso quando guard decidir run_fallback.

const GITHUB_TOKEN_FALLBACK = "OCULTO"
const GITHUB_TOKEN_KEYCHAIN_KEY = "uol_bot_github_token"
const REPO_OWNER = "leosaquetto"
const REPO_NAME = "uol-bot"
const TARGET_BRANCH = "main"

const BASE_URL = "https://clube.uol.com.br"
const LIST_URL = `${BASE_URL}/?order=new`
const MAX_INGRESSO_OFFERS = 4
const MAX_RETRIES = 3

const LOCK_TIMEOUT_MS = 8 * 60 * 1000
const LOCK_FILE = "uol_scriptable_ingressos_single.lock.json"
const FALLBACK_AUDIT_FILE = "uol_ios_fallback_audit.jsonl"

const IOS_LEDGER_FILE = "uol_ingressos_sent_ledger.json"
const IOS_LEDGER_TTL_HOURS = 72
const IOS_LEDGER_SOURCE = "ios_fallback_single"

function log(msg) { console.log(`[${new Date().toLocaleTimeString()}] ${msg}`) }
function normalizeLink(url) { return String(url || "").trim() }

async function appendFallbackAudit(event, payload = {}) {
  try {
    const fm = FileManager.iCloud()
    const path = fm.joinPath(fm.documentsDirectory(), FALLBACK_AUDIT_FILE)
    if (fm.fileExists(path)) {
      try { await fm.downloadFileFromiCloud(path) } catch (e) {}
    }
    const previous = fm.fileExists(path) ? String(fm.readString(path) || "") : ""
    const line = JSON.stringify({ event, at_utc: new Date().toISOString(), ...payload })
    fm.writeString(path, `${previous}${previous ? "\n" : ""}${line}`)
  } catch (e) {
    log("⚠️ não foi possível registrar a execução no audit local")
  }
}

function getGithubToken() {
  try {
    const fallback = String(GITHUB_TOKEN_FALLBACK || "").trim()
    if (fallback && fallback !== "OCULTO") {
      if (typeof Keychain !== "undefined") {
        const current = Keychain.contains(GITHUB_TOKEN_KEYCHAIN_KEY) ? String(Keychain.get(GITHUB_TOKEN_KEYCHAIN_KEY) || "").trim() : ""
        if (current !== fallback) Keychain.set(GITHUB_TOKEN_KEYCHAIN_KEY, fallback)
      }
      return fallback
    }
    if (typeof Keychain !== "undefined" && Keychain.contains(GITHUB_TOKEN_KEYCHAIN_KEY)) {
      const fromKeychain = String(Keychain.get(GITHUB_TOKEN_KEYCHAIN_KEY) || "").trim()
      if (fromKeychain) return fromKeychain
    }
  } catch (e) {}
  return ""
}
const GITHUB_TOKEN = getGithubToken()

function toBase64(str) { return Data.fromString(str).toBase64String() }
function githubApiUrl(path) { return `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/contents/${path}` }

async function sleepMs(ms) {
  const seconds = Math.max(0.01, Number(ms || 0) / 1000)
  return await new Promise(resolve => Timer.schedule(seconds, false, () => resolve()))
}

async function withRetries(label, fn, retries = MAX_RETRIES) {
  let lastErr = ""
  for (let i = 1; i <= retries; i++) {
    try {
      const out = await fn(i)
      if (out && out.ok === false) throw new Error(out.error || `${label} falhou`)
      return out
    } catch (e) {
      lastErr = String(e)
      log(`⚠️ ${label} tentativa ${i}/${retries}: ${lastErr}`)
      if (i < retries) await sleepMs(700 * i)
    }
  }
  return { ok: false, error: `${label} esgotou tentativas: ${lastErr}` }
}

async function githubGetJson(path) {
  const req = new Request(githubApiUrl(path))
  req.method = "GET"
  req.headers = { "User-Agent": "Scriptable", "Accept": "application/vnd.github+json", "Authorization": `token ${String(GITHUB_TOKEN || "").trim()}` }
  try {
    const resp = await req.loadJSON()
    if (resp && resp.message === "Not Found") return { ok: true, notFound: true, data: null, sha: null }
    if (!resp || !resp.content) return { ok: false, error: `github sem content: ${JSON.stringify(resp)}` }
    const raw = Data.fromBase64String(String(resp.content).replace(/\n/g, "")).toRawString()
    return { ok: true, notFound: false, data: JSON.parse(raw), sha: resp.sha || null }
  } catch (e) { return { ok: false, error: String(e) } }
}

async function githubPutFile(path, content, message) {
  let lastErr = ""
  for (let attempt = 1; attempt <= 3; attempt++) {
    const existing = await githubGetJson(path)
    const req = new Request(githubApiUrl(path))
    req.method = "PUT"
    req.headers = { "User-Agent": "Scriptable", "Accept": "application/vnd.github+json", "Authorization": `token ${String(GITHUB_TOKEN || "").trim()}`, "Content-Type": "application/json" }
    const body = { message, content: toBase64(content), branch: TARGET_BRANCH }
    if (existing.ok && !existing.notFound && existing.sha) body.sha = existing.sha
    req.body = JSON.stringify(body)
    try {
      const resp = await req.loadJSON()
      if (resp && resp.commit) return { ok: true, data: resp }
      const status = String(resp?.status || "")
      const msg = String(resp?.message || "")
      lastErr = `github sem commit: ${JSON.stringify(resp)}`
      if (status === "409" || msg.includes("expected")) {
        await sleepMs(350 * attempt)
        continue
      }
      return { ok: false, error: lastErr }
    } catch (e) {
      lastErr = String(e)
      if (attempt < 3) {
        await sleepMs(350 * attempt)
        continue
      }
    }
  }
  return { ok: false, error: lastErr || "github put falhou sem detalhe" }
}

function normalizeOfferKey(value) {
  const raw = normalizeLink(value)
  if (!raw) return ""
  let tail = raw
  if (raw.startsWith("http://") || raw.startsWith("https://")) {
    const noHash = raw.split("#")[0]
    const noQuery = noHash.split("?")[0]
    tail = noQuery.replace(/\/$/, "").split("/").pop() || ""
  }

  let decoded = ""
  try { decoded = decodeURIComponent(String(tail || "")) } catch (e) { decoded = String(tail || "") }

  const base = String(decoded)
    .toLowerCase()
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
  if (!base) return ""

  const variants = new Set([base, base.replace(/-de-/g, "-")])
  if (base.includes("joo")) variants.add(base.replace(/joo/g, "joao"))
  if (base.includes("joao")) variants.add(base.replace(/joao/g, "joo"))
  return Array.from(variants).filter(Boolean).sort()[0] || ""
}

function dedupeOffersByLink(items) {
  const seen = new Set()
  const out = []
  for (const it of items) {
    const key = normalizeOfferKey(it.link || it.original_link || it.id || "")
    if (!key || seen.has(key)) continue
    seen.add(key)
    out.push(it)
  }
  return out
}

async function fetchText(url) {
  const req = new Request(url)
  req.method = "GET"
  return await req.loadString()
}

function extractOfferCards(html, maxOffers = 4) {
  const cards = []
  const regex = /<a[^>]+href="([^"]+)"[^>]*>[\s\S]*?<\/a>/gi
  let match
  while ((match = regex.exec(html)) && cards.length < 80) {
    const href = String(match[1] || "")
    if (!href.includes("/ofertas/")) continue
    const block = match[0]
    cards.push({
      link: href.startsWith("http") ? href : `${BASE_URL}${href}`,
      title: (block.match(/title="([^"]+)"/i)?.[1] || block.match(/alt="([^"]+)"/i)?.[1] || "Oferta").trim(),
      category: (block.match(/categoria[^>]*>([^<]+)</i)?.[1] || "").trim(),
    })
  }
  return cards.slice(0, maxOffers)
}

function isIngressoOffer(offer) {
  const bag = [offer.category, offer.title, offer.link].map(v => String(v || "").toLowerCase()).join(" ")
  return bag.includes("ingresso") || bag.includes("campanhasdeingresso")
}

async function fetchOfferDetailData(offer) {
  try {
    const html = await fetchText(offer.link)
    const validity = (html.match(/validade[\s\S]{0,200}<[^>]*>([^<]{3,200})</i)?.[1] || "").trim()
    const description = (html.match(/<meta\s+name="description"\s+content="([^"]+)"/i)?.[1] || "").trim()
    const img = (html.match(/<meta\s+property="og:image"\s+content="([^"]+)"/i)?.[1] || "").trim()
    return { ok: true, validity, description, detail_img_url: img }
  } catch (e) { return { ok: false, error: String(e) } }
}

function getLockFilePath() {
  const fm = FileManager.iCloud()
  const dir = fm.documentsDirectory()
  return { fm, path: fm.joinPath(dir, LOCK_FILE) }
}
async function acquireRunLock() {
  const { fm, path } = getLockFilePath()
  try { if (fm.fileExists(path)) await fm.downloadFileFromiCloud(path) } catch (e) {}
  if (fm.fileExists(path)) {
    try {
      const lockData = JSON.parse(String(fm.readString(path) || "{}"))
      const ageMs = Date.now() - Number(lockData.started_ts || 0)
      if (ageMs >= 0 && ageMs < LOCK_TIMEOUT_MS) return { ok: false, message: "lock ativo fallback iOS" }
    } catch (e) {}
  }
  fm.writeString(path, JSON.stringify({ started_ts: Date.now(), started_at: new Date().toISOString(), part: "single_ingressos" }, null, 2))
  return { ok: true }
}
function releaseRunLock() {
  const { fm, path } = getLockFilePath()
  try { if (fm.fileExists(path)) fm.remove(path) } catch (e) {}
}

function getICloudLedgerPath() {
  const fm = FileManager.iCloud()
  const docs = fm.documentsDirectory()
  return { fm, path: fm.joinPath(docs, IOS_LEDGER_FILE) }
}
async function readIosLedger() {
  const { fm, path } = getICloudLedgerPath()
  try {
    if (!fm.fileExists(path)) return { keys: {} }
    await fm.downloadFileFromiCloud(path)
    const data = JSON.parse(String(fm.readString(path) || "{}"))
    return data && data.keys && typeof data.keys === "object" ? data : { keys: {} }
  } catch (e) { return { keys: {} } }
}
function writeIosLedger(ledger) {
  const { fm, path } = getICloudLedgerPath()
  fm.writeString(path, JSON.stringify({ last_update: new Date().toISOString(), ttl_hours: IOS_LEDGER_TTL_HOURS, keys: ledger.keys || {} }, null, 2))
}
function cleanupLedgerKeys(keysObj, nowMs) {
  const ttlMs = IOS_LEDGER_TTL_HOURS * 60 * 60 * 1000
  const next = {}
  for (const [key, value] of Object.entries(keysObj || {})) {
    const ts = new Date(String(value && value.timestamp || "")).getTime()
    if (Number.isFinite(ts) && (nowMs - ts) <= ttlMs) next[key] = value
  }
  return next
}

async function main() {
  if (!GITHUB_TOKEN) return "erro | token ausente"
  const lock = await acquireRunLock()
  if (!lock.ok) return `abortado | ${lock.message}`

  try {
    await appendFallbackAudit("fallback_executed", { run_fallback: true, runner: "scriptable_single_ingressos" })
    const htmlResp = await withRetries("fetch vitrine", () => fetchText(LIST_URL).then(html => ({ ok: true, html })))
    if (!htmlResp.ok || !htmlResp.html) throw new Error(htmlResp.error || "html indisponível")

    const rawOffers = extractOfferCards(htmlResp.html, 40)
    const ingressoCandidates = rawOffers.filter(isIngressoOffer).slice(0, MAX_INGRESSO_OFFERS)

    const pendingResp = await withRetries("get pending", () => githubGetJson("pending_offers.json"))
    const pendingData = pendingResp.ok && pendingResp.data ? pendingResp.data : { offers: [] }
    const existingPending = Array.isArray(pendingData.offers) ? pendingData.offers : []

    const nowMs = Date.now()
    const ledger = await readIosLedger()
    const cleanedKeys = cleanupLedgerKeys(ledger.keys, nowMs)
    const freshToAppend = []

    for (const offer of ingressoCandidates) {
      const canonicalKey = normalizeOfferKey(offer.link || offer.id || "")
      if (!canonicalKey) continue
      if (cleanedKeys[canonicalKey]) continue

      const detail = await withRetries(`detalhe ${canonicalKey}`, () => fetchOfferDetailData(offer))
      const merged = {
        id: canonicalKey,
        offer_key: canonicalKey,
        link: normalizeLink(offer.link),
        original_link: normalizeLink(offer.link),
        title: String(offer.title || "Oferta").trim(),
        preview_title: String(offer.title || "Oferta").trim(),
        category: String(offer.category || ""),
        validity: detail.ok ? String(detail.validity || "") : "",
        description: detail.ok ? String(detail.description || "") : "",
        img_url: detail.ok ? String(detail.detail_img_url || "") : "",
        img_source: detail.ok ? "detail_img" : "card_img",
        created_at: new Date().toISOString(),
        source: IOS_LEDGER_SOURCE,
      }
      freshToAppend.push(merged)
      cleanedKeys[canonicalKey] = { timestamp: new Date().toISOString(), source: IOS_LEDGER_SOURCE }
    }

    const ingresoOnlyFresh = freshToAppend.filter(isIngressoOffer)
    const mergedPending = dedupeOffersByLink([...existingPending, ...ingresoOnlyFresh])
    const payload = { last_update: new Date().toISOString(), offers: mergedPending }
    const saveResp = await withRetries("upload pending", () => githubPutFile("pending_offers.json", JSON.stringify(payload, null, 2), `scriptable ingresso fallback single ${new Date().toISOString()}`))
    if (!saveResp.ok) throw new Error(saveResp.error || "falha ao salvar pending")

    writeIosLedger({ keys: cleanedKeys })
    return `ok | ingresso_candidates=${ingressoCandidates.length} appended=${ingresoOnlyFresh.length} pending=${mergedPending.length}`
  } catch (e) {
    return `erro | ${String(e && e.message ? e.message : e)}`
  } finally {
    releaseRunLock()
  }
}

const output = await main()
console.log(`final output: ${output}`)
Script.setShortcutOutput(String(output || "ok"))
Script.complete()
