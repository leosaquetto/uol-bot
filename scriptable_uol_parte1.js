// scriptable uol - parte 1/3
// coleta vitrine + snapshot/meta + estágio para parte 2

const GITHUB_TOKEN = "OCULTO"
const REPO_OWNER = "leosaquetto"
const REPO_NAME = "uol-bot"
const TARGET_BRANCH = "main"

const BASE_URL = "https://clube.uol.com.br"
const LIST_URL = `${BASE_URL}/?order=new`

const MAX_OFFERS_FROM_LIST = 60
const MAX_SEEN_LINKS = 400
const MAX_RETRIES = 3

const SEEN_CACHE_FILE = "uol_seen_links.json"
const TODAY_STATE_FILE = "uol_today_state.json"
const PIPELINE_STATE_FILE = "uol_pipeline_state.json"

function log(msg) { console.log(`[${new Date().toLocaleTimeString()}] ${msg}`) }
function pad(n) { return String(n).padStart(2, "0") }
function brDate(d = new Date()) { return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}` }
function brTime(d = new Date()) { return `${pad(d.getHours())}:${pad(d.getMinutes())}` }
function brDateTime(d = new Date()) { return `${brDate(d)} às ${brTime(d)}` }
function normalizeLink(url) { return String(url || "").trim() }
async function sleepMs(ms) {
  const seconds = Math.max(0.01, Number(ms || 0) / 1000)
  return await new Promise(resolve => Timer.schedule(seconds, false, () => resolve()))
}

function buildSnapshotId() {
  const d = new Date()
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}_${Math.random().toString(36).slice(2, 7)}`
}

function normalizeOfferKey(value) {
  const raw = normalizeLink(value)
  if (!raw) return ""
  const tail = raw.startsWith("http://") || raw.startsWith("https://")
    ? raw.split("?")[0].replace(/\/$/, "").split("/").pop()
    : raw
  return String(tail || "")
    .toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
}

function toBase64(str) { return Data.fromString(str).toBase64String() }
function githubApiUrl(path) { return `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/contents/${path}` }

function getIcloudPaths() {
  const fm = FileManager.iCloud()
  const dir = fm.documentsDirectory()
  return {
    fm,
    seenPath: fm.joinPath(dir, SEEN_CACHE_FILE),
    todayStatePath: fm.joinPath(dir, TODAY_STATE_FILE),
    pipelineStatePath: fm.joinPath(dir, PIPELINE_STATE_FILE),
  }
}

async function ensureIcloudFile(path, initialData) {
  const { fm } = getIcloudPaths()
  if (!fm.fileExists(path)) fm.writeString(path, JSON.stringify(initialData, null, 2))
  try { await fm.downloadFileFromiCloud(path) } catch (e) {}
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
      if (i < retries) await sleepMs(800 * i)
    }
  }
  return { ok: false, error: `${label} esgotou tentativas: ${lastErr}` }
}

async function githubGetJson(path) {
  const req = new Request(githubApiUrl(path))
  req.method = "GET"
  req.headers = {
    "User-Agent": "Scriptable",
    "Accept": "application/vnd.github+json",
    "Authorization": `token ${String(GITHUB_TOKEN || "").trim()}`,
  }
  try {
    const resp = await req.loadJSON()
    if (resp && resp.message === "Not Found") return { ok: true, notFound: true, data: null, sha: null }
    if (!resp || !resp.content) return { ok: false, error: `github sem content: ${JSON.stringify(resp)}` }
    const raw = Data.fromBase64String(String(resp.content).replace(/\n/g, "")).toRawString()
    return { ok: true, notFound: false, data: JSON.parse(raw), sha: resp.sha || null }
  } catch (e) {
    return { ok: false, error: String(e) }
  }
}

async function githubPutFile(path, content, message) {
  const existing = await githubGetJson(path)
  const req = new Request(githubApiUrl(path))
  req.method = "PUT"
  req.headers = {
    "User-Agent": "Scriptable",
    "Accept": "application/vnd.github+json",
    "Authorization": `token ${String(GITHUB_TOKEN || "").trim()}`,
    "Content-Type": "application/json",
  }
  const body = { message, content: toBase64(content), branch: TARGET_BRANCH }
  if (existing.ok && !existing.notFound && existing.sha) body.sha = existing.sha
  req.body = JSON.stringify(body)
  try {
    const resp = await req.loadJSON()
    return (resp && resp.commit) ? { ok: true, data: resp } : { ok: false, error: `github sem commit: ${JSON.stringify(resp)}` }
  } catch (e) {
    return { ok: false, error: String(e) }
  }
}

async function fetchText(url, referer = BASE_URL + "/", timeout = 20) {
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

function extractOfferCards(html, limit = MAX_OFFERS_FROM_LIST) {
  const cards = []
  const matches = html.match(/<div class="col-12 col-sm-4 col-md-3 mb-3 beneficio"[\s\S]*?<!-- Fim div beneficio -->/gi) || []
  for (const block of matches) {
    if (cards.length >= limit) break
    const link = absolutizeUrl((block.match(/<a href="([^"]+)"/i) || [])[1] || "")
    const title = cleanText((block.match(/<p class="titulo mb-0">([\s\S]*?)<\/p>/i) || [])[1] || "")
    if (!link || !title) continue
    cards.push({
      link,
      title,
      category: cleanText((block.match(/data-categoria="([^"]*)"/i) || [])[1] || ""),
      partner_img_url: absolutizeUrl((block.match(/<img[^>]+data-src="([^"]*\/parceiros\/[^"]+)"/i) || [])[1] || ""),
      partner_name: cleanText((block.match(/title="([^"]*)"/i) || [])[1] || ""),
      img_url: absolutizeUrl((block.match(/<div class="col-12 thumb text-center lazy" data-src="([^"]*\/beneficios\/[^"]+)"/i) || [])[1] || ""),
      offer_key: normalizeOfferKey(link),
    })
  }
  return cards
}

async function loadSeenCache() {
  const { fm, seenPath } = getIcloudPaths()
  await ensureIcloudFile(seenPath, { seen: [], updated_at: new Date().toISOString() })
  try {
    const data = JSON.parse(fm.readString(seenPath))
    return { seen: (Array.isArray(data.seen) ? data.seen : []).map(normalizeLink).filter(Boolean) }
  } catch (e) {
    return { seen: [] }
  }
}

function saveSeenCache(seenLinks) {
  const { fm, seenPath } = getIcloudPaths()
  const trimmed = Array.from(new Set((seenLinks || []).map(normalizeLink).filter(Boolean))).slice(-MAX_SEEN_LINKS)
  fm.writeString(seenPath, JSON.stringify({ seen: trimmed, updated_at: new Date().toISOString() }, null, 2))
}

async function loadTodayState() {
  const { fm, todayStatePath } = getIcloudPaths()
  const today = brDate()
  await ensureIcloudFile(todayStatePath, { date: today, active_links: [], sold_out_sent: {} })
  try {
    const data = JSON.parse(fm.readString(todayStatePath))
    if (String(data.date || "") !== today) return { date: today, active_links: [], sold_out_sent: {} }
    return {
      date: today,
      active_links: Array.isArray(data.active_links) ? data.active_links.map(normalizeLink).filter(Boolean) : [],
      sold_out_sent: data.sold_out_sent && typeof data.sold_out_sent === "object" ? data.sold_out_sent : {},
    }
  } catch (e) {
    return { date: today, active_links: [], sold_out_sent: {} }
  }
}

function saveTodayState(state) {
  const { fm, todayStatePath } = getIcloudPaths()
  fm.writeString(todayStatePath, JSON.stringify({
    date: brDate(),
    active_links: Array.from(new Set((state.active_links || []).map(normalizeLink).filter(Boolean))),
    sold_out_sent: state.sold_out_sent && typeof state.sold_out_sent === "object" ? state.sold_out_sent : {},
  }, null, 2))
}

function savePipelineState(state) {
  const { fm, pipelineStatePath } = getIcloudPaths()
  fm.writeString(pipelineStatePath, JSON.stringify(state, null, 2))
}

function collectRepoProcessedKeys(pendingData, latestData, historyData) {
  const processedLinks = new Set()
  const processedKeys = new Set()
  function absorbOffer(offer) {
    if (!offer || typeof offer !== "object") return
    const link = normalizeLink(offer.link || offer.original_link || "")
    if (link) {
      processedLinks.add(link)
      processedKeys.add(normalizeOfferKey(link))
    }
    const id = normalizeOfferKey(offer.id || "")
    if (id) processedKeys.add(id)
  }
  for (const o of (Array.isArray(pendingData?.offers) ? pendingData.offers : [])) absorbOffer(o)
  for (const o of (Array.isArray(latestData?.offers) ? latestData.offers : [])) absorbOffer(o)
  for (const id of (Array.isArray(historyData?.ids) ? historyData.ids : [])) {
    const k = normalizeOfferKey(id)
    if (k) processedKeys.add(k)
  }
  return { processedLinks, processedKeys }
}

function buildSoldOutUpdates(todayState, currentLinks, allowedLinksSet) {
  const currentSet = new Set(currentLinks.map(normalizeLink))
  const previousSet = new Set((todayState.active_links || []).map(normalizeLink))
  const updates = []
  const now = new Date()
  for (const link of previousSet) {
    if (!link || currentSet.has(link) || !allowedLinksSet.has(link)) continue
    const offerKey = normalizeOfferKey(link)
    if (!offerKey || (todayState.sold_out_sent && todayState.sold_out_sent[offerKey])) continue
    updates.push({ link, sold_out_at: brTime(now), date: brDate(now) })
    if (!todayState.sold_out_sent || typeof todayState.sold_out_sent !== "object") todayState.sold_out_sent = {}
    todayState.sold_out_sent[offerKey] = brTime(now)
  }
  todayState.active_links = Array.from(currentSet)
  return { updates, todayState }
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
  const snapshotId = buildSnapshotId()
  const htmlPath = `snapshots/snapshot_${snapshotId}.html`
  const metaPath = `snapshots/snapshot_${snapshotId}.json`
  const stagePath = `snapshots/stage1_${snapshotId}.json`

  try {
    const seenCache = await loadSeenCache()
    const seenSet = new Set(seenCache.seen)
    let todayState = await loadTodayState()

    const [pendingResp, latestResp, historyResp] = await Promise.all([
      withRetries("get pending", () => githubGetJson("pending_offers.json")),
      withRetries("get latest", () => githubGetJson("latest_offers.json")),
      withRetries("get historico", () => githubGetJson("historico_leouol.json")),
    ])

    const pendingData = pendingResp.ok && pendingResp.data ? pendingResp.data : { offers: [] }
    const latestData = latestResp.ok && latestResp.data ? latestResp.data : { offers: [] }
    const historyData = historyResp.ok && historyResp.data ? historyResp.data : { ids: [] }

    const htmlResp = await withRetries("fetch vitrine", () => fetchText(LIST_URL, BASE_URL + "/", 20).then(x => ({ ok: true, html: x })))
    if (!htmlResp.ok || !htmlResp.html) throw new Error(htmlResp.error || "html vazia")

    const html = htmlResp.html
    const allOffers = extractOfferCards(html, MAX_OFFERS_FROM_LIST)
    const currentLinks = allOffers.map(o => normalizeLink(o.link)).filter(Boolean)

    const { processedLinks, processedKeys } = collectRepoProcessedKeys(pendingData, latestData, historyData)
    const allowedLinksSet = new Set([...currentLinks, ...Array.from(processedLinks)])
    const soldOutResult = buildSoldOutUpdates(todayState, currentLinks, allowedLinksSet)
    const soldOutUpdates = soldOutResult.updates
    todayState = soldOutResult.todayState

    const newOffers = allOffers.filter(o => {
      const link = normalizeLink(o.link)
      const key = normalizeOfferKey(link)
      return !!(link && key && !seenSet.has(link) && !processedKeys.has(key))
    })

    const meta = {
      snapshot_id: snapshotId,
      created_at: new Date().toISOString(),
      created_at_br: brDateTime(),
      source_url: LIST_URL,
      html_path: htmlPath,
      html_length: html.length,
      total_offers_found: allOffers.length,
      total_new_offers_found: newOffers.length,
      sold_out_detected_count: soldOutUpdates.length,
      cache_size_before: seenCache.seen.length,
      current_links: currentLinks,
      offers: allOffers,
    }

    const stageData = {
      snapshot_id: snapshotId,
      created_at: new Date().toISOString(),
      new_offers: newOffers,
      sold_out_updates: soldOutUpdates,
      stats: { total_offers: allOffers.length, total_new: newOffers.length },
    }

    const saves = await Promise.all([
      withRetries("upload html", () => githubPutFile(htmlPath, html, `scriptable snapshot html ${snapshotId}`)),
      withRetries("upload meta", () => githubPutFile(metaPath, JSON.stringify(meta, null, 2), `scriptable snapshot meta ${snapshotId}`)),
      withRetries("upload stage1", () => githubPutFile(stagePath, JSON.stringify(stageData, null, 2), `scriptable stage1 ${snapshotId}`)),
    ])

    if (saves.some(x => !x.ok)) throw new Error(saves.find(x => !x.ok).error || "falha upload etapa1")

    const mergedSeen = [...seenCache.seen, ...currentLinks]
    saveSeenCache(mergedSeen)
    saveTodayState(todayState)
    savePipelineState({
      version: 1,
      last_part: 1,
      snapshot_id: snapshotId,
      html_path: htmlPath,
      meta_path: metaPath,
      stage1_path: stagePath,
      stage2_path: "",
      created_at: new Date().toISOString(),
    })

    await updateScriptableStatusRuntime({
      statusValue: newOffers.length > 0 ? "parcial" : "sem_novidade",
      summary: `parte1 ok: ${snapshotId} | vitrine ${allOffers.length} | novas ${newOffers.length}`,
      offersSeen: allOffers.length,
      newOffers: newOffers.length,
      pendingCount: Array.isArray(pendingData.offers) ? pendingData.offers.length : 0,
    })

    return `ok_parte1 | snapshot ${snapshotId} | vitrine ${allOffers.length} | novas ${newOffers.length}`
  } catch (e) {
    const msg = String(e && e.message ? e.message : e)
    await updateScriptableStatusRuntime({ statusValue: "erro", summary: "parte1 com erro", offersSeen: 0, newOffers: 0, pendingCount: 0, lastError: msg })
    return `erro_parte1 | ${msg}`
  }
}

const output = await main()
console.log(`final output: ${output}`)
Script.setShortcutOutput(String(output || "ok"))
Script.complete()
