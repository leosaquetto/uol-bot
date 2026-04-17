// scriptable uol - parte 2/3
// lê stage1, busca detalhes, grava detail + stage2

const GITHUB_TOKEN_FALLBACK = "OCULTO"
const GITHUB_TOKEN_KEYCHAIN_KEY = "uol_bot_github_token"
const REPO_OWNER = "leosaquetto"
const REPO_NAME = "uol-bot"
const TARGET_BRANCH = "main"

const BASE_URL = "https://clube.uol.com.br"
const LIST_URL = `${BASE_URL}/?order=new`
const DEFAULT_MAX_DETAIL_FETCHES = 12
const MAX_RUNTIME_SECONDS = 85
const MAX_RETRIES = 3
const PIPELINE_STATE_FILE = "uol_pipeline_state.json"

function log(msg) { console.log(`[${new Date().toLocaleTimeString()}] ${msg}`) }
function pad(n) { return String(n).padStart(2, "0") }
function brDate(d = new Date()) { return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}` }
function brTime(d = new Date()) { return `${pad(d.getHours())}:${pad(d.getMinutes())}` }
function brDateTime(d = new Date()) { return `${brDate(d)} às ${brTime(d)}` }
function normalizeLink(url) { return String(url || "").trim() }
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
async function sleepMs(ms) {
  const seconds = Math.max(0.01, Number(ms || 0) / 1000)
  return await new Promise(resolve => Timer.schedule(seconds, false, () => resolve()))
}
function toBase64(str) { return Data.fromString(str).toBase64String() }
function githubApiUrl(path) { return `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/contents/${path}` }
function normalizeOfferKey(value) {
  const raw = normalizeLink(value)
  if (!raw) return ""
  const tail = raw.startsWith("http://") || raw.startsWith("https://") ? raw.split("?")[0].replace(/\/$/, "").split("/").pop() : raw
  return String(tail || "").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]+/g, "-").replace(/-+/g, "-").replace(/^-+|-+$/g, "")
}
function clampInt(value, fallback, minV = 1, maxV = 30) {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  const i = Math.trunc(n)
  if (i < minV) return minV
  if (i > maxV) return maxV
  return i
}
function resolveDetailLimitFromShortcut() {
  try {
    const p = args.shortcutParameter
    if (typeof p === "number" || typeof p === "string") return clampInt(p, DEFAULT_MAX_DETAIL_FETCHES)
    if (p && typeof p === "object") {
      const fromObj = p.max_detail_fetches ?? p.detail_limit ?? p.maxDetails ?? p.max
      return clampInt(fromObj, DEFAULT_MAX_DETAIL_FETCHES)
    }
  } catch (e) {}
  return DEFAULT_MAX_DETAIL_FETCHES
}

function getIcloudPath() {
  const fm = FileManager.iCloud()
  const dir = fm.documentsDirectory()
  return { fm, path: fm.joinPath(dir, PIPELINE_STATE_FILE) }
}
async function ensureIcloudFile(path, initialData) {
  const { fm } = getIcloudPath()
  if (!fm.fileExists(path)) fm.writeString(path, JSON.stringify(initialData, null, 2))
  try { await fm.downloadFileFromiCloud(path) } catch (e) {}
}
async function loadPipelineState() {
  const { fm, path } = getIcloudPath()
  await ensureIcloudFile(path, { version: 1, last_part: 0 })
  try { return JSON.parse(fm.readString(path)) } catch (e) { return { version: 1, last_part: 0 } }
}
function savePipelineState(state) {
  const { fm, path } = getIcloudPath()
  fm.writeString(path, JSON.stringify(state, null, 2))
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
      if (/bad credentials|status\"?:\"?401|401/i.test(lastErr)) {
        try {
          if (typeof Keychain !== "undefined" && Keychain.contains(GITHUB_TOKEN_KEYCHAIN_KEY)) {
            Keychain.remove(GITHUB_TOKEN_KEYCHAIN_KEY)
          }
        } catch (inner) {}
        return { ok: false, error: `${label} falhou por autenticação GitHub (401). Verifique o token usado pelas 3 partes.` }
      }
      if (i < retries) await sleepMs(800 * i)
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

async function fetchText(url, referer = BASE_URL + "/", timeout = 15) {
  const req = new Request(url)
  req.timeoutInterval = timeout
  req.headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": referer,
    "Cache-Control": "no-cache",
  }
  return await req.loadString()
}
function absolutizeUrl(url) {
  if (!url) return ""
  if (url.startsWith("http://") || url.startsWith("https://")) return url
  if (url.startsWith("//")) return "https:" + url
  if (url.startsWith("/")) return BASE_URL + url
  return `${BASE_URL}/${url}`
}
function cleanText(str) {
  return String(str || "").replace(/<[^>]+>/g, " ").replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/\s+/g, " ").trim()
}
function extractTitleFromDetail(html) {
  return cleanText((html.match(/<h2[^>]*>([\s\S]*?)<\/h2>/i) || html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i) || [])[1] || "")
}
function extractValidityFromDetail(html) {
  for (const regex of [/[Bb]enefício válido de[^.!?\n]*[.!?]?/i, /[Vv]álido até[^.!?\n]*[.!?]?/i, /\d{2}\/\d{2}\/\d{4}[\s\S]{0,80}\d{2}\/\d{2}\/\d{4}/i]) {
    const m = html.match(regex)
    if (m && m[0]) return cleanText(m[0])
  }
  return ""
}
function extractDescriptionFromDetail(html) {
  for (const regex of [/class=["'][^"']*info-beneficio[^"']*["'][^>]*>([\s\S]*?)(?:<script|<footer|class=["'][^"']*box-compartilhar)/i, /id=["']beneficio["'][^>]*>([\s\S]*?)(?:<script|<footer)/i]) {
    const m = html.match(regex)
    if (m && m[1]) {
      const txt = cleanText(m[1])
      if (txt.length >= 20) return txt.slice(0, 4000)
    }
  }
  return ""
}
function isBadOfferImageUrl(url) {
  const src = String(url || "").toLowerCase()
  if (!src) return true
  return (
    src.includes("/parceiros/") ||
    src.includes("loader.gif") ||
    src.includes("/static/images/clubes/uol/categorias/") ||
    src.includes("ingressosexclusivos-hover") ||
    src.includes("ingressos-hover") ||
    src.includes("icone") ||
    src.includes("icon-")
  )
}
function extractDetailImageFromDetail(html) {
  const metaCandidates = [
    (html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i) || [])[1] || "",
    (html.match(/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i) || [])[1] || "",
  ]
  for (const raw of metaCandidates) {
    const src = absolutizeUrl(raw || "")
    if (!isBadOfferImageUrl(src)) return { url: src, source: "meta" }
  }

  const matches = [...html.matchAll(/<img[^>]+(?:data-src|data-original|data-lazy|src)="([^"]+)"/gi)]
  for (const m of matches) {
    const src = absolutizeUrl(m[1] || "")
    if (isBadOfferImageUrl(src)) continue
    if (src.includes("/beneficios/") || src.includes("/campanhasdeingresso/") || src.includes("/teatro") || src.includes("cloudfront")) return { url: src, source: "priority_img" }
  }
  for (const m of matches) {
    const src = absolutizeUrl(m[1] || "")
    if (isBadOfferImageUrl(src)) continue
    return { url: src, source: "fallback_img" }
  }
  return { url: "", source: "none" }
}

async function fetchOfferDetailData(offer) {
  try {
    const html = await fetchText(offer.link, LIST_URL, 15)
    if (!html || html.trim().length < 1000) return { ok: false, title: offer.title, html_length: html ? html.length : 0, validity: "", description: "", detail_img_url: "", error: "html detalhe vazia ou curta" }
    const title = extractTitleFromDetail(html) || offer.title
    const validity = extractValidityFromDetail(html)
    const description = extractDescriptionFromDetail(html)
    const detail_image = extractDetailImageFromDetail(html) || {}
    const detail_img_url = String(detail_image.url || "")
    const detail_img_source = String(detail_image.source || "none")
    return { ok: true, title, html_length: html.length, validity, description, detail_img_url, detail_img_source, error: "" }
  } catch (e) {
    return { ok: false, title: offer.title, html_length: 0, validity: "", description: "", detail_img_url: "", detail_img_source: "error", error: String(e) }
  }
}

async function updateScriptableStatusRuntime({ statusValue, summary, offersSeen, newOffers, pendingCount = 0, lastError = "" }) {
  const resp = await githubGetJson("status_runtime.json")
  let status = { scriptable: {}, scraper: {}, consumer: {}, global: {} }
  if (resp.ok && resp.data && typeof resp.data === "object") status = resp.data
  status.scriptable = {
    last_started_at: brDateTime(startedAtGlobal),
    last_finished_at: brDateTime(new Date()),
    last_success_at: (statusValue === "ok" || statusValue === "sem_novidade" || statusValue === "parcial") ? brDateTime(new Date()) : String(status.scriptable?.last_success_at || ""),
    status: statusValue,
    summary: String(summary || ""),
    offers_seen: Number(offersSeen || 0),
    new_offers: Number(newOffers || 0),
    pending_count: Number(pendingCount || 0),
    last_error: String(lastError || ""),
  }
  return await githubPutFile("status_runtime.json", JSON.stringify(status, null, 2), `scriptable runtime status ${new Date().toISOString()}`)
}

const startedAtGlobal = new Date()

async function main() {
  if (!GITHUB_TOKEN) return "erro | token ausente"
  try {
    const state = await loadPipelineState()
    if (!state || !state.snapshot_id || !state.stage1_path || Number(state.last_part || 0) < 1) return "erro_parte2 | estado da parte1 ausente"

    const stage1Resp = await withRetries("load stage1", () => githubGetJson(state.stage1_path))
    if (!stage1Resp.ok || !stage1Resp.data) throw new Error(stage1Resp.error || "stage1 indisponível")

    const stage1 = stage1Resp.data
    const snapshotId = String(stage1.snapshot_id || state.snapshot_id)
    if (snapshotId !== String(state.snapshot_id)) throw new Error("snapshot_id inconsistente entre estado e stage1")

    const newOffers = Array.isArray(stage1.new_offers) ? stage1.new_offers : []
    const shortcutDetailLimit = resolveDetailLimitFromShortcut()
    const stateDetailLimit = clampInt(state.max_detail_fetches, shortcutDetailLimit)
    const detailLimit = stateDetailLimit
    const offersToTest = newOffers.slice(0, detailLimit)
    const detailMetaPath = `snapshots/detail_${snapshotId}.json`
    const stage2Path = `snapshots/stage2_${snapshotId}.json`

    const detailResults = []
    const pendingToAppend = []
    let okCount = 0

    for (let i = 0; i < offersToTest.length; i++) {
      const elapsedSeconds = (new Date().getTime() - startedAtGlobal.getTime()) / 1000
      if (elapsedSeconds >= MAX_RUNTIME_SECONDS) {
        log(`⏱️ limite de tempo atingido (${Math.trunc(elapsedSeconds)}s), encerrando detalhes em ${i}/${offersToTest.length}`)
        break
      }
      const offer = offersToTest[i]
      const detail = await withRetries(`detalhe ${i + 1}`, () => fetchOfferDetailData(offer))
      const d = detail.ok === false ? { ok: false, title: offer.title, validity: "", description: "", detail_img_url: "", detail_img_source: "failed", html_length: 0, error: detail.error || "falhou" } : detail
      if (d.ok) okCount += 1

      pendingToAppend.push({
        id: normalizeOfferKey(offer.link),
        link: normalizeLink(offer.link),
        original_link: normalizeLink(offer.link),
        title: (d.title || offer.title || "Oferta").trim(),
        preview_title: offer.title || d.title || "Oferta",
        validity: (d.validity || "").trim(),
        description: (d.description || "").trim(),
        category: offer.category || "",
        partner_name: offer.partner_name || "",
        partner_img_url: offer.partner_img_url || "",
        img_url: d.detail_img_url || offer.img_url || "",
        img_source: d.detail_img_source || (offer.img_url ? "card_img" : "none"),
        created_at: new Date().toISOString(),
        snapshot_id: snapshotId,
      })

      detailResults.push({
        index: i + 1,
        link: offer.link,
        card_title: offer.title,
        detail_ok: !!d.ok,
        detail_title: d.title || "",
        detail_html_length: d.html_length || 0,
        validity: d.validity || "",
        description: (d.description || "").slice(0, 4000),
        has_validity: !!d.validity,
        has_description: !!d.description,
        detail_status: d.ok ? ((d.title && d.validity && d.description) ? "complete" : "partial") : "failed",
        detail_img_url: d.detail_img_url || "",
        detail_img_source: d.detail_img_source || "none",
        error: d.error || "",
      })
    }

    const detailMeta = {
      snapshot_id: snapshotId,
      tested_at: new Date().toISOString(),
      tested_count: detailResults.length,
      detail_ok_count: okCount,
      detail_fail_count: detailResults.length - okCount,
      sold_out_detected_count: Array.isArray(stage1.sold_out_updates) ? stage1.sold_out_updates.length : 0,
      offers: detailResults,
    }

    const stage2 = {
      snapshot_id: snapshotId,
      created_at: new Date().toISOString(),
      pending_to_append: pendingToAppend,
      stats: { tested_count: detailResults.length, detail_ok_count: okCount, detail_limit: detailLimit },
    }

    const saves = await Promise.all([
      withRetries("upload detail", () => githubPutFile(detailMetaPath, JSON.stringify(detailMeta, null, 2), `scriptable detail meta ${snapshotId}`)),
      withRetries("upload stage2", () => githubPutFile(stage2Path, JSON.stringify(stage2, null, 2), `scriptable stage2 ${snapshotId}`)),
    ])

    if (saves.some(x => !x.ok)) throw new Error(saves.find(x => !x.ok).error || "falha upload parte2")

    savePipelineState({ ...state, last_part: 2, stage2_path: stage2Path, detail_meta_path: detailMetaPath, updated_at: new Date().toISOString() })

    await updateScriptableStatusRuntime({
      statusValue: "parcial",
      summary: `parte2 ok: ${snapshotId} | detalhes ${okCount}/${pendingToAppend.length} (limite ${detailLimit})`,
      offersSeen: Number(stage1.stats?.total_offers || 0),
      newOffers: Number(stage1.stats?.total_new || 0),
      pendingCount: pendingToAppend.length,
    })

    return `ok_parte2 | snapshot ${snapshotId} | detalhes ${okCount}/${pendingToAppend.length} | limite ${detailLimit}`
  } catch (e) {
    const msg = String(e && e.message ? e.message : e)
    await updateScriptableStatusRuntime({ statusValue: "erro", summary: "parte2 com erro", offersSeen: 0, newOffers: 0, pendingCount: 0, lastError: msg })
    return `erro_parte2 | ${msg}`
  }
}

const output = await main()
console.log(`final output: ${output}`)
Script.setShortcutOutput(String(output || "ok"))
Script.complete()
