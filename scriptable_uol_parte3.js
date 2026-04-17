// scriptable uol - parte 3/3
// consolida pending + sold_out + status final

const GITHUB_TOKEN_FALLBACK = "OCULTO"
const GITHUB_TOKEN_KEYCHAIN_KEY = "uol_bot_github_token"
const REPO_OWNER = "leosaquetto"
const REPO_NAME = "uol-bot"
const TARGET_BRANCH = "main"
const PIPELINE_STATE_FILE = "uol_pipeline_state.json"
const MAX_RETRIES = 3

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

function dedupeOffersByLink(items) {
  const out = []
  const seen = new Set()
  for (const it of items) {
    const link = normalizeLink(it.link || it.original_link || "")
    const key = normalizeOfferKey(link || it.id || "")
    if (!key || seen.has(key)) continue
    seen.add(key)
    out.push(it)
  }
  return out
}

async function mergeAndUploadSoldOutUpdates(newUpdates) {
  if (!Array.isArray(newUpdates) || newUpdates.length === 0) return { ok: true, count: 0 }
  const current = await githubGetJson("sold_out_updates.json")
  let existing = { updated_at: "", updates: [] }
  if (current.ok && current.data) existing = current.data

  const merged = Array.isArray(existing.updates) ? [...existing.updates] : []
  const known = new Set(merged.map(x => `${normalizeOfferKey(x.link)}|${String(x.date || "").trim()}`))

  let added = 0
  for (const item of newUpdates) {
    const key = `${normalizeOfferKey(item.link)}|${String(item.date || "").trim()}`
    if (!key || known.has(key)) continue
    known.add(key)
    merged.push(item)
    added += 1
  }

  const payload = { updated_at: new Date().toISOString(), updates: merged }
  const save = await githubPutFile("sold_out_updates.json", JSON.stringify(payload, null, 2), `scriptable sold out updates ${new Date().toISOString()}`)
  if (!save.ok) return { ok: false, error: save.error, count: 0 }
  return { ok: true, count: added }
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
    if (!state || !state.snapshot_id || !state.stage1_path || !state.stage2_path || Number(state.last_part || 0) < 2) {
      return "erro_parte3 | estado da parte2 ausente"
    }

    const [stage1Resp, stage2Resp, pendingResp] = await Promise.all([
      withRetries("load stage1", () => githubGetJson(state.stage1_path)),
      withRetries("load stage2", () => githubGetJson(state.stage2_path)),
      withRetries("get pending", () => githubGetJson("pending_offers.json")),
    ])

    if (!stage1Resp.ok || !stage1Resp.data) throw new Error(stage1Resp.error || "stage1 ausente")
    if (!stage2Resp.ok || !stage2Resp.data) throw new Error(stage2Resp.error || "stage2 ausente")

    const stage1 = stage1Resp.data
    const stage2 = stage2Resp.data
    const snapshotId = String(stage1.snapshot_id || state.snapshot_id)
    if (String(stage2.snapshot_id || "") !== snapshotId || String(state.snapshot_id || "") !== snapshotId) throw new Error("snapshot_id inconsistente")

    const pendingData = pendingResp.ok && pendingResp.data ? pendingResp.data : { offers: [] }
    const existingPending = Array.isArray(pendingData.offers) ? pendingData.offers : []
    const pendingToAppend = Array.isArray(stage2.pending_to_append) ? stage2.pending_to_append : []

    const mergedPending = dedupeOffersByLink([...existingPending, ...pendingToAppend])
    const pendingPayload = { last_update: new Date().toISOString(), offers: mergedPending }
    const pendingSave = await withRetries("upload pending", () => githubPutFile("pending_offers.json", JSON.stringify(pendingPayload, null, 2), `scriptable pending update ${snapshotId}`))
    if (!pendingSave.ok) throw new Error(pendingSave.error || "falha pending")

    const soldOutUpdates = Array.isArray(stage1.sold_out_updates) ? stage1.sold_out_updates : []
    const soldOutSave = await withRetries("upload sold_out", () => mergeAndUploadSoldOutUpdates(soldOutUpdates))

    const totalOffers = Number(stage1.stats?.total_offers || 0)
    const totalNew = Number(stage1.stats?.total_new || 0)
    const detailsOk = Number(stage2.stats?.detail_ok_count || 0)
    const detailsTotal = Number(stage2.stats?.tested_count || 0)

    const statusValue = (!soldOutSave.ok) ? "parcial" : (totalNew > 0 ? "ok" : "sem_novidade")
    const summary = `pipeline 3 partes ok: ${snapshotId} | vitrine ${totalOffers} | novas ${totalNew} | detalhes ${detailsOk}/${detailsTotal} | pending+ ${pendingToAppend.length}`

    await updateScriptableStatusRuntime({
      statusValue,
      summary,
      offersSeen: totalOffers,
      newOffers: totalNew,
      pendingCount: mergedPending.length,
      lastError: soldOutSave.ok ? "" : String(soldOutSave.error || "falha sold_out"),
    })

    savePipelineState({ ...state, last_part: 3, finished_at: new Date().toISOString() })
    return `ok_parte3 | snapshot ${snapshotId} | pending ${mergedPending.length} | sold_out ${soldOutSave.ok ? soldOutSave.count : 0}`
  } catch (e) {
    const msg = String(e && e.message ? e.message : e)
    await updateScriptableStatusRuntime({ statusValue: "erro", summary: "parte3 com erro", offersSeen: 0, newOffers: 0, pendingCount: 0, lastError: msg })
    return `erro_parte3 | ${msg}`
  }
}

const output = await main()
console.log(`final output: ${output}`)
Script.setShortcutOutput(String(output || "ok"))
Script.complete()
