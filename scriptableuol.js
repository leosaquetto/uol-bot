// scriptable - clube uol
// scraper + widget no mesmo arquivo
// modo widget: renderiza widget
// modo shortcuts/app: roda scraper e devolve output sem ui

const GITHUB_TOKEN = "xxx"
const REPO_OWNER = "leosaquetto"
const REPO_NAME = "uol-bot"

const BASE_URL = "https://clube.uol.com.br"
const LIST_URL = `${BASE_URL}/?order=new`

const HISTORY_FILE = "historico_leouol.json"
const PENDING_FILE = "pending_offers.json"
const DAILY_LOG_FILE = "daily_log.json"
const STATUS_RUNTIME_FILE = "status_runtime.json"

const GITHUB_JSON_URL = "https://raw.githubusercontent.com/leosaquetto/uol-bot/main/latest_offers.json"
const UOL_LOGO_URL = "https://i.imgur.com/UdIgTfI.png"

const fm = FileManager.local()
const cachePath = fm.joinPath(fm.documentsDirectory(), "uol_widget_cache_v3.json")
const CACHE_TIME = 10 * 60 * 1000

function log(msg) {
  const timestamp = new Date().toLocaleTimeString()
  console.log(`[${timestamp}] ${msg}`)
}

function logSeparator() {
  console.log("-".repeat(60))
}

function brDate(d = new Date()) {
  const dd = String(d.getDate()).padStart(2, "0")
  const mm = String(d.getMonth() + 1).padStart(2, "0")
  const yyyy = d.getFullYear()
  return `${dd}/${mm}/${yyyy}`
}

function brTime(d = new Date()) {
  const hh = String(d.getHours()).padStart(2, "0")
  const mm = String(d.getMinutes()).padStart(2, "0")
  return `${hh}:${mm}`
}

function brDateTime(d = new Date()) {
  return `${brDate(d)} às ${brTime(d)}`
}

function cleanText(text) {
  if (!text) return ""
  let cleaned = String(text)

  cleaned = cleaned.replace(/&nbsp;/gi, " ")
  cleaned = cleaned.replace(/&#160;/gi, " ")
  cleaned = cleaned.replace(/[ \t]+/g, " ")
  cleaned = cleaned.replace(/\r\n/g, "\n").replace(/\r/g, "\n")
  cleaned = cleaned.replace(/\n\s*\n+/g, "\n\n")
  cleaned = cleaned.replace(/^ +| +$/gm, "")
  return cleaned.trim()
}

function htmlToText(html) {
  if (!html) return ""
  let text = String(html)

  text = text.replace(/<br\s*\/?>/gi, "\n")
  text = text.replace(/<\/p>/gi, "\n\n")
  text = text.replace(/<\/div>/gi, "\n")
  text = text.replace(/<li[^>]*>/gi, "\n• ")
  text = text.replace(/<\/li>/gi, "")
  text = text.replace(/<[^>]+>/g, " ")

  return cleanText(text)
}

function decodeHtmlEntities(str) {
  if (!str) return ""
  return str
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
}

function getOfferId(link) {
  try {
    const cleanLink = String(link || "").split("?")[0].replace(/\/$/, "")
    return cleanLink.split("/").pop()
  } catch (e) {
    return String(link || "")
  }
}

function normalizeTextKey(value) {
  let raw = String(value || "").trim().toLowerCase()
  if (!raw) return ""

  const map = {
    "á": "a", "à": "a", "ã": "a", "â": "a",
    "é": "e", "ê": "e",
    "í": "i",
    "ó": "o", "ô": "o", "õ": "o",
    "ú": "u",
    "ç": "c"
  }

  Object.keys(map).forEach(k => {
    raw = raw.replaceAll(k, map[k])
  })

  raw = raw.replace(/https?:\/\//g, "")
  raw = raw.replace(/[^a-z0-9]+/g, "-")
  raw = raw.replace(/-+/g, "-")
  raw = raw.replace(/^-+|-+$/g, "")
  return raw
}

function normalizeOfferKey(value) {
  let raw = String(value || "").trim().toLowerCase()
  if (!raw) return ""

  if (raw.startsWith("http://") || raw.startsWith("https://")) {
    raw = getOfferId(raw)
  }

  return normalizeTextKey(raw)
}

function pickDescriptionAnchor(description) {
  if (!description) return ""

  const lines = String(description)
    .split("\n")
    .map(x => cleanText(x))
    .filter(Boolean)

  const blacklistStarts = [
    "beneficio-valido",
    "valido-ate",
    "local",
    "quando",
    "importante",
    "regras-de-resgate",
    "atencao",
    "enviar-cupons-por-e-mail",
    "preencha-os-campos-abaixo",
    "e-mail",
    "mensagem",
    "enviar"
  ]

  const filtered = []
  for (const line of lines) {
    const low = normalizeTextKey(line)
    if (!low) continue
    if (low.length < 12) continue
    if (blacklistStarts.some(x => low.startsWith(x))) continue
    filtered.push(low)
  }

  return filtered.length ? filtered[0].slice(0, 160) : ""
}

function buildDedupeKey(title, validity, description) {
  const titleKey = normalizeTextKey(title || "")
  const validityKey = normalizeTextKey(validity || "")
  const descKey = pickDescriptionAnchor(description || "")
  return [titleKey, validityKey, descKey].filter(Boolean).join("|")
}

function buildLooseDedupeKey(title, description) {
  const titleKey = normalizeTextKey(title || "")
  const descKey = pickDescriptionAnchor(description || "")
  return [titleKey, descKey].filter(Boolean).join("|")
}

function absolutizeUrl(url) {
  if (!url) return ""
  if (url.startsWith("http://") || url.startsWith("https://")) return url
  if (url.startsWith("//")) return "https:" + url
  if (url.startsWith("/")) return BASE_URL + url
  return `${BASE_URL}/${url}`
}

function uniqBy(arr, keyFn) {
  const seen = new Set()
  const out = []
  for (const item of arr) {
    const key = keyFn(item)
    if (!key || seen.has(key)) continue
    seen.add(key)
    out.push(item)
  }
  return out
}

function base64Encode(str) {
  const data = Data.fromString(str)
  return data.toBase64String()
}

function base64DecodeToString(b64) {
  try {
    const clean = String(b64 || "").replace(/\n/g, "")
    const data = Data.fromBase64String(clean)
    if (!data) return null
    return data.toRawString()
  } catch (e) {
    return null
  }
}

function isBadBannerUrl(url) {
  const u = String(url || "").toLowerCase()
  if (!u) return true
  return (
    u.includes("loader.gif") ||
    u.includes("/static/images/loader.gif") ||
    u.includes("/parceiros/") ||
    u.includes("/rodape/") ||
    u.includes("icon-instagram") ||
    u.includes("icon-facebook") ||
    u.includes("icon-twitter") ||
    u.includes("icon-youtube") ||
    u.includes("instagram.png") ||
    u.includes("facebook.png") ||
    u.includes("twitter.png") ||
    u.includes("youtube.png") ||
    u.includes("share-") ||
    u.includes("social") ||
    u.includes("logo-uol") ||
    u.includes("logo_uol")
  )
}

function isLikelyBenefitBanner(url) {
  const u = String(url || "").toLowerCase()
  if (!u || isBadBannerUrl(u)) return false
  return (
    u.includes("/beneficios/") ||
    u.includes("/campanhasdeingresso/") ||
    u.includes("cloudfront.net")
  )
}

function ensureTodayState(state) {
  const today = brDate()
  if (!state || state.date !== today) {
    return {
      date: today,
      message_id: null,
      last_success_check: state?.last_success_check || "",
      last_new_offer_at: state?.last_new_offer_at || "",
      pending_count: 0,
      last_consumer_run: state?.last_consumer_run || "",
      lines: []
    }
  }
  return state
}

function appendDashboardLine(state, source, statusLine) {
  state = ensureTodayState(state)
  const line = `[${brTime()}] ${source}: ${statusLine}`
  state.lines = Array.isArray(state.lines) ? state.lines : []
  state.lines.push(line)
  state.lines = state.lines.slice(-30)
  return state
}

function isShortcutContext() {
  return !!(config.runsInShortcuts || config.runsWithSiri || args.shortcutParameter !== undefined)
}

// github api
function githubContentsUrl(filePath) {
  return `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/contents/${filePath}`
}

async function githubRequest(url, method = "GET", body = null) {
  const req = new Request(url)
  req.method = method

  req.headers = {
    "User-Agent": "Scriptable",
    "Accept": "application/vnd.github+json",
    "Authorization": `token ${String(GITHUB_TOKEN || "").trim()}`
  }

  if (body !== null) {
    req.headers["Content-Type"] = "application/json"
    req.body = JSON.stringify(body)
  }

  try {
    const resp = await req.loadJSON()
    return { ok: true, data: resp }
  } catch (e) {
    return { ok: false, error: String(e) }
  }
}

async function getFileContent(filePath) {
  const result = await githubRequest(githubContentsUrl(filePath), "GET")
  if (!result.ok) {
    return { ok: false, content: null, sha: null, error: result.error }
  }

  const response = result.data || {}

  if (response.message === "Not Found") {
    return { ok: true, content: null, sha: null, notFound: true }
  }

  if (!response.content) {
    return {
      ok: false,
      content: null,
      sha: null,
      error: `resposta sem content para ${filePath}: ${JSON.stringify(response)}`
    }
  }

  const decoded = base64DecodeToString(response.content)
  if (decoded == null) {
    return {
      ok: false,
      content: null,
      sha: null,
      error: `falha ao decodificar base64 de ${filePath}`
    }
  }

  try {
    return {
      ok: true,
      content: JSON.parse(decoded),
      sha: response.sha || null
    }
  } catch (e) {
    return {
      ok: false,
      content: null,
      sha: response.sha || null,
      error: `json inválido em ${filePath}: ${e.message}`
    }
  }
}

async function updateFile(filePath, content, commitMessage) {
  const current = await getFileContent(filePath)

  if (!current.ok && !current.notFound) {
    log(`❌ não consegui ler ${filePath} antes de atualizar`)
    log(`   ${current.error}`)
    return false
  }

  const body = {
    message: commitMessage,
    content: base64Encode(JSON.stringify(content, null, 2))
  }

  if (current.sha) {
    body.sha = current.sha
  }

  const result = await githubRequest(githubContentsUrl(filePath), "PUT", body)

  if (!result.ok) {
    log(`❌ erro http ao atualizar ${filePath}: ${result.error}`)
    return false
  }

  const resp = result.data || {}

  if (resp.commit) {
    log(`✅ ${filePath} atualizado`)
    return true
  }

  log(`❌ github respondeu sem commit ao atualizar ${filePath}`)
  log(`   ${JSON.stringify(resp)}`)
  return false
}

async function loadHistoryStrict() {
  const result = await getFileContent(HISTORY_FILE)
  if (!result.ok) {
    log(`❌ falha ao ler ${HISTORY_FILE}`)
    log(`   ${result.error}`)
    return null
  }

  if (!result.content) {
    return { ids: [], dedupe_keys: [], loose_dedupe_keys: [] }
  }

  if (!Array.isArray(result.content.ids)) {
    log(`❌ formato inválido em ${HISTORY_FILE}`)
    return null
  }

  return {
    ids: Array.isArray(result.content.ids) ? result.content.ids : [],
    dedupe_keys: Array.isArray(result.content.dedupe_keys) ? result.content.dedupe_keys : [],
    loose_dedupe_keys: Array.isArray(result.content.loose_dedupe_keys) ? result.content.loose_dedupe_keys : []
  }
}

async function loadPendingStrict() {
  const result = await getFileContent(PENDING_FILE)
  if (!result.ok) {
    log(`❌ falha ao ler ${PENDING_FILE}`)
    log(`   ${result.error}`)
    return null
  }

  if (!result.content) {
    return { last_update: null, offers: [] }
  }

  if (!Array.isArray(result.content.offers)) {
    log(`❌ formato inválido em ${PENDING_FILE}`)
    return null
  }

  return result.content
}

async function loadDailyLogStrict() {
  const result = await getFileContent(DAILY_LOG_FILE)
  if (!result.ok) {
    log(`❌ falha ao ler ${DAILY_LOG_FILE}`)
    log(`   ${result.error}`)
    return null
  }

  if (!result.content) {
    return {
      date: "",
      message_id: null,
      last_success_check: "",
      last_new_offer_at: "",
      pending_count: 0,
      last_consumer_run: "",
      lines: []
    }
  }

  const content = result.content || {}
  return {
    date: String(content.date || ""),
    message_id: content.message_id || null,
    last_success_check: String(content.last_success_check || ""),
    last_new_offer_at: String(content.last_new_offer_at || ""),
    pending_count: Number(content.pending_count || 0),
    last_consumer_run: String(content.last_consumer_run || ""),
    lines: Array.isArray(content.lines) ? content.lines.map(String) : []
  }
}

async function loadStatusRuntimeStrict() {
  const result = await getFileContent(STATUS_RUNTIME_FILE)
  if (!result.ok) {
    log(`❌ falha ao ler ${STATUS_RUNTIME_FILE}`)
    log(`   ${result.error}`)
    return null
  }

  if (!result.content) {
    return {
      scriptable: {
        last_started_at: "",
        last_finished_at: "",
        status: "",
        summary: "",
        offers_seen: 0,
        new_offers: 0,
        pending_count: 0,
        last_error: ""
      },
      scraper: {
        last_started_at: "",
        last_finished_at: "",
        status: "",
        summary: "",
        offers_seen: 0,
        new_offers: 0,
        pending_count: 0,
        last_error: ""
      },
      consumer: {
        last_started_at: "",
        last_finished_at: "",
        status: "",
        summary: "",
        processed: 0,
        sent: 0,
        failed: 0,
        pending_count: 0,
        last_error: ""
      }
    }
  }

  const c = result.content || {}
  return {
    scriptable: c.scriptable || {
      last_started_at: "",
      last_finished_at: "",
      status: "",
      summary: "",
      offers_seen: 0,
      new_offers: 0,
      pending_count: 0,
      last_error: ""
    },
    scraper: c.scraper || {
      last_started_at: "",
      last_finished_at: "",
      status: "",
      summary: "",
      offers_seen: 0,
      new_offers: 0,
      pending_count: 0,
      last_error: ""
    },
    consumer: c.consumer || {
      last_started_at: "",
      last_finished_at: "",
      status: "",
      summary: "",
      processed: 0,
      sent: 0,
      failed: 0,
      pending_count: 0,
      last_error: ""
    }
  }
}

async function savePending(offers) {
  const payload = {
    last_update: new Date().toISOString(),
    offers
  }

  return await updateFile(
    PENDING_FILE,
    payload,
    `atualiza pending com ${offers.length} ofertas`
  )
}

async function saveDailyLog(state) {
  return await updateFile(
    DAILY_LOG_FILE,
    state,
    `atualiza daily log em ${new Date().toISOString()}`
  )
}

async function saveStatusRuntime(state) {
  return await updateFile(
    STATUS_RUNTIME_FILE,
    state,
    `atualiza status runtime em ${new Date().toISOString()}`
  )
}

function setScriptableStatusStart(state, pendingCount) {
  state.scriptable = {
    last_started_at: brDateTime(),
    last_finished_at: state.scriptable?.last_finished_at || "",
    status: "running",
    summary: "scriptable iniciado",
    offers_seen: 0,
    new_offers: 0,
    pending_count: pendingCount || 0,
    last_error: ""
  }
  return state
}

function setScriptableStatusFinish(state, payload) {
  state.scriptable = {
    last_started_at: state.scriptable?.last_started_at || "",
    last_finished_at: brDateTime(),
    status: payload.status || "",
    summary: payload.summary || "",
    offers_seen: Number(payload.offers_seen || 0),
    new_offers: Number(payload.new_offers || 0),
    pending_count: Number(payload.pending_count || 0),
    last_error: payload.last_error || ""
  }
  return state
}

// parser
function extractAllImageUrls(blockHtml) {
  const urls = []
  const regex = /<img[^>]+(?:data-src|data-original|data-lazy|src)=["']([^"']+)["'][^>]*>/gi
  let match

  while ((match = regex.exec(blockHtml)) !== null) {
    const src = absolutizeUrl(match[1])
    if (src && !src.startsWith("data:image")) {
      urls.push(src)
    }
  }

  return uniqBy(urls, x => x)
}

function chooseImagesFromBlock(blockHtml) {
  const allImgs = []
  const imgRegex = /<img([^>]+)>/gi
  let match

  while ((match = imgRegex.exec(blockHtml)) !== null) {
    const attrs = match[1] || ""

    const srcMatch =
      /data-src=["']([^"']+)["']/i.exec(attrs) ||
      /data-original=["']([^"']+)["']/i.exec(attrs) ||
      /data-lazy=["']([^"']+)["']/i.exec(attrs) ||
      /src=["']([^"']+)["']/i.exec(attrs)

    if (!srcMatch || !srcMatch[1]) continue

    const src = absolutizeUrl(srcMatch[1])
    if (!src || src.startsWith("data:image")) continue

    const titleMatch = /title=["']([^"']+)["']/i.exec(attrs)
    const classMatch = /class=["']([^"']+)["']/i.exec(attrs)
    const widthMatch = /width=["']([^"']+)["']/i.exec(attrs)
    const heightMatch = /height=["']([^"']+)["']/i.exec(attrs)

    allImgs.push({
      src,
      title: titleMatch ? titleMatch[1] : "",
      className: classMatch ? classMatch[1].toLowerCase() : "",
      width: widthMatch ? parseInt(widthMatch[1], 10) || 0 : 0,
      height: heightMatch ? parseInt(heightMatch[1], 10) || 0 : 0
    })
  }

  const uniqImgs = uniqBy(allImgs, x => x.src)

  let partner = uniqImgs.find(img =>
    img.src.includes("/parceiros/") ||
    !!img.title ||
    img.className.includes("logo") ||
    img.className.includes("brand") ||
    img.className.includes("parceiro") ||
    (img.width > 0 && img.width <= 220) ||
    (img.height > 0 && img.height <= 120)
  )

  const bannerCandidates = uniqImgs.filter(img =>
    (!partner || img.src !== partner.src) && isLikelyBenefitBanner(img.src)
  )

  let main = bannerCandidates.length ? bannerCandidates[bannerCandidates.length - 1] : null

  if (!main) {
    const fallbackMain = uniqImgs.find(img => !isBadBannerUrl(img.src) && (!partner || img.src !== partner?.src))
    if (fallbackMain) main = fallbackMain
  }

  if (!partner && uniqImgs.length >= 2) {
    partner = uniqImgs.find(img => !main || img.src !== main.src) || uniqImgs[0]
  }

  return {
    img_url: main ? main.src : "",
    partner_img_url: partner ? partner.src : ""
  }
}

function extractTitleFromBlock(blockHtml) {
  const patterns = [
    /class=["'][^"']*titulo[^"']*["'][^>]*>([\s\S]*?)<\//i,
    /<h3[^>]*>([\s\S]*?)<\/h3>/i,
    /<h2[^>]*>([\s\S]*?)<\/h2>/i,
    /title=["']([^"']+)["']/i
  ]

  for (const regex of patterns) {
    const match = regex.exec(blockHtml)
    if (match && match[1]) {
      return cleanText(decodeHtmlEntities(match[1].replace(/<[^>]+>/g, " ")))
    }
  }

  return ""
}

function findOfferAnchorsInHtml(html) {
  const links = []
  const regex = /<a[^>]+href=["']([^"']+)["'][^>]*>/gi
  let match

  while ((match = regex.exec(html)) !== null) {
    const href = absolutizeUrl(match[1] || "")
    const low = href.toLowerCase()

    if (
      low.includes("/campanhasdeingresso/") ||
      low.includes("/teatrouol/")
    ) {
      links.push(href)
    }
  }

  return uniqBy(links, x => x)
}

function buildOfferBlockAroundLink(html, link) {
  const escaped = String(link || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
  const directRegex = new RegExp(`<a[^>]+href=["']${escaped}["'][^>]*>`, "i")
  const directMatch = directRegex.exec(html)

  let anchorIndex = directMatch ? directMatch.index : -1

  if (anchorIndex < 0) {
    const pathOnly = String(link || "").replace(BASE_URL, "")
    const escapedPath = pathOnly.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    const pathRegex = new RegExp(`<a[^>]+href=["']${escapedPath}["'][^>]*>`, "i")
    const pathMatch = pathRegex.exec(html)
    anchorIndex = pathMatch ? pathMatch.index : -1
  }

  if (anchorIndex < 0) return ""

  const start = Math.max(0, anchorIndex - 2500)
  const end = Math.min(html.length, anchorIndex + 4500)
  return html.slice(start, end)
}

async function scrapeOffersList() {
  log("🌐 buscando ofertas no clube uol...")

  const req = new Request(LIST_URL)
  req.headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": BASE_URL + "/"
  }

  try {
    const html = await req.loadString()
    log(`✅ página baixada: ${html.length} caracteres`)

    const anchorLinks = findOfferAnchorsInHtml(html)
    log(`🔗 links candidatos: ${anchorLinks.length}`)

    const offers = []

    for (const link of anchorLinks) {
      try {
        const blockHtml = buildOfferBlockAroundLink(html, link)
        if (!blockHtml) continue

        let title = extractTitleFromBlock(blockHtml)
        const { img_url, partner_img_url } = chooseImagesFromBlock(blockHtml)

        if (!title) {
          const slug = getOfferId(link)
          title = cleanText(
            decodeHtmlEntities(
              String(slug || "")
                .replace(/[-_]+/g, " ")
                .replace(/\b\w/g, c => c.toUpperCase())
            )
          )
        }

        log(`     main url: ${img_url || "vazia"}`)
        log(`     partner url: ${partner_img_url || "vazia"}`)

        if (!title || !link) continue

        const id = getOfferId(link)

        offers.push({
          id,
          original_link: link,
          preview_title: title,
          title,
          link,
          img_url,
          partner_img_url
        })

        log(`  ✅ extraído: ${title.substring(0, 55)}...`)
      } catch (e) {
        log(`  ⚠️ erro ao extrair card: ${e.message}`)
      }
    }

    const uniqueOffers = uniqBy(offers, o => normalizeOfferKey(o.id || o.link))
    log(`📦 blocos/ofertas aproveitados: ${uniqueOffers.length}`)

    if (uniqueOffers.length <= 1) {
      log("⚠️ parsing suspeito: poucas ofertas capturadas da vitrine")
    }

    return uniqueOffers
  } catch (e) {
    log(`❌ erro ao baixar lista: ${e.message}`)
    return []
  }
}

async function extractOfferDetails(url, previewTitle) {
  const fullUrl = absolutizeUrl(url)
  log(`   🔍 acessando: ${previewTitle.substring(0, 50)}...`)

  const req = new Request(fullUrl)
  req.headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": BASE_URL + "/"
  }

  const html = await req.loadString()

  let pageTitle = previewTitle
  const titlePatterns = [
    /<h2[^>]*>([\s\S]*?)<\/h2>/i,
    /<h1[^>]*>([\s\S]*?)<\/h1>/i
  ]

  for (const regex of titlePatterns) {
    const m = regex.exec(html)
    if (m && m[1]) {
      pageTitle = cleanText(m[1].replace(/<[^>]+>/g, " "))
      break
    }
  }

  let detailImgUrl = ""
  const allImgs = extractAllImageUrls(html)
  const detailCandidates = allImgs.filter(url => isLikelyBenefitBanner(url))
  if (detailCandidates.length) {
    detailImgUrl = detailCandidates[detailCandidates.length - 1]
  } else {
    const fallbackDetail = allImgs.filter(url => !isBadBannerUrl(url))
    if (fallbackDetail.length) {
      detailImgUrl = fallbackDetail[fallbackDetail.length - 1]
    }
  }

  let validity = null
  const validityPatterns = [
    /[Bb]enefício válido de[^.!?\n]*[.!?]?/,
    /[Vv]álido até[^.!?\n]*[.!?]?/,
    /\d{2}\/\d{2}\/\d{4}[\s\S]{0,80}\d{2}\/\d{2}\/\d{4}/
  ]

  for (const pattern of validityPatterns) {
    const m = pattern.exec(html)
    if (m && m[0]) {
      validity = cleanText(m[0].replace(/<[^>]+>/g, " "))
      break
    }
  }

  let description = ""
  const infoPatterns = [
    /class=["'][^"']*info-beneficio[^"']*["'][^>]*>([\s\S]*?)(?:<script|<footer|class=["'][^"']*box-compartilhar)/i,
    /id=["']beneficio["'][^>]*>([\s\S]*?)(?:<script|<footer)/i
  ]

  for (const regex of infoPatterns) {
    const m = regex.exec(html)
    if (m && m[1]) {
      description = htmlToText(m[1])
      if (description.length >= 20) break
    }
  }

  if (!description || description.length < 20) {
    description = "descrição detalhada não disponível."
  }

  description = description.substring(0, 4000)

  return {
    title: pageTitle,
    validity,
    description,
    detailImgUrl
  }
}

function buildExistingDedupeSets(history, pending) {
  const historyKeys = new Set((history.ids || []).map(x => normalizeOfferKey(x)))
  const historyDedupe = new Set((history.dedupe_keys || []).map(x => String(x || "").trim()).filter(Boolean))
  const historyLooseDedupe = new Set((history.loose_dedupe_keys || []).map(x => String(x || "").trim()).filter(Boolean))

  const pendingKeys = new Set((pending.offers || []).map(o => normalizeOfferKey(o.id || o.link)))
  const pendingDedupe = new Set((pending.offers || []).map(o => String(o.dedupe_key || "").trim()).filter(Boolean))
  const pendingLooseDedupe = new Set(
    (pending.offers || [])
      .map(o => String(o.loose_dedupe_key || buildLooseDedupeKey(o.title, o.description || "")).trim())
      .filter(Boolean)
  )

  return {
    historyKeys,
    historyDedupe,
    historyLooseDedupe,
    pendingKeys,
    pendingDedupe,
    pendingLooseDedupe
  }
}

function alreadyKnownByAnyKey(offer, sets) {
  const idKey = normalizeOfferKey(offer.id || offer.link)
  const strictKey = String(offer.dedupe_key || "").trim()
  const looseKey = String(offer.loose_dedupe_key || buildLooseDedupeKey(offer.title, offer.description || "")).trim()

  if (idKey && (sets.historyKeys.has(idKey) || sets.pendingKeys.has(idKey))) return true
  if (strictKey && (sets.historyDedupe.has(strictKey) || sets.pendingDedupe.has(strictKey))) return true
  if (looseKey && (sets.historyLooseDedupe.has(looseKey) || sets.pendingLooseDedupe.has(looseKey))) return true

  return false
}

function mergeOffersDeduped(existingOffers, newOffers) {
  const merged = [...(existingOffers || []), ...(newOffers || [])]

  return uniqBy(merged, o => {
    const strictKey = String(o.dedupe_key || "").trim()
    if (strictKey) return `strict:${strictKey}`

    const looseKey = String(o.loose_dedupe_key || buildLooseDedupeKey(o.title, o.description || "")).trim()
    if (looseKey) return `loose:${looseKey}`

    const idKey = normalizeOfferKey(o.id || o.link)
    if (idKey) return `id:${idKey}`

    return ""
  })
}

async function mainScraper() {
  log("=".repeat(60))
  log("🤖 scriptable - clube uol scraper")
  log(`📅 ${new Date().toLocaleString()}`)
  log("=".repeat(60))

  if (!GITHUB_TOKEN) {
    log("❌ passe o token no parâmetro do widget / script")
    return "erro | token ausente"
  }

  let statusState = null
  let dailyState = null
  let pendingBeforeCount = 0

  try {
    const history = await loadHistoryStrict()
    const pending = await loadPendingStrict()
    const daily = await loadDailyLogStrict()
    const statusRuntime = await loadStatusRuntimeStrict()

    if (!history || !pending || !daily || !statusRuntime) {
      log("❌ abortando por segurança: falha ao ler estado remoto")
      throw new Error("falha ao ler estado remoto")
    }

    pendingBeforeCount = Array.isArray(pending.offers) ? pending.offers.length : 0

    statusState = setScriptableStatusStart(statusRuntime, pendingBeforeCount)
    await saveStatusRuntime(statusState)

    dailyState = ensureTodayState(daily)
    dailyState = appendDashboardLine(dailyState, "scriptable", "▶️ rodada iniciada")
    await saveDailyLog(dailyState)

    log(`📚 histórico atual: ${(history.ids || []).length} ids`)
    log(`🧠 dedupe_keys atuais: ${(history.dedupe_keys || []).length}`)
    log(`📦 pending atual: ${pendingBeforeCount} ofertas`)

    const allOffers = await scrapeOffersList()

    dailyState = await loadDailyLogStrict()
    if (dailyState) {
      dailyState = ensureTodayState(dailyState)
      dailyState.last_success_check = brDateTime()
      dailyState.pending_count = pendingBeforeCount
      await saveDailyLog(dailyState)
    }

    if (allOffers.length === 0) {
      log("📭 nenhuma oferta encontrada")

      dailyState = await loadDailyLogStrict()
      if (dailyState) {
        dailyState = ensureTodayState(dailyState)
        dailyState.pending_count = pendingBeforeCount
        dailyState = appendDashboardLine(dailyState, "scriptable", "💤 sem ofertas novas")
        await saveDailyLog(dailyState)
      }

      statusState = await loadStatusRuntimeStrict()
      if (statusState) {
        statusState = setScriptableStatusFinish(statusState, {
          status: "sem_novidades",
          summary: "nenhuma oferta encontrada",
          offers_seen: 0,
          new_offers: 0,
          pending_count: pendingBeforeCount,
          last_error: ""
        })
        await saveStatusRuntime(statusState)
      }

      return "sem ofertas encontradas"
    }

    const knownSets = buildExistingDedupeSets(history, pending)

    const candidateOffers = allOffers.filter(o => {
      const key = normalizeOfferKey(o.id || o.link)
      return key && !knownSets.historyKeys.has(key) && !knownSets.pendingKeys.has(key)
    })

    if (candidateOffers.length === 0) {
      log("📭 nenhuma oferta nova fora do histórico/pending")

      dailyState = await loadDailyLogStrict()
      if (dailyState) {
        dailyState = ensureTodayState(dailyState)
        dailyState.pending_count = pendingBeforeCount
        dailyState = appendDashboardLine(dailyState, "scriptable", "💤 sem ofertas novas")
        await saveDailyLog(dailyState)
      }

      statusState = await loadStatusRuntimeStrict()
      if (statusState) {
        statusState = setScriptableStatusFinish(statusState, {
          status: "sem_novidades",
          summary: "nenhuma oferta nova fora do histórico/pending",
          offers_seen: allOffers.length,
          new_offers: 0,
          pending_count: pendingBeforeCount,
          last_error: ""
        })
        await saveStatusRuntime(statusState)
      }

      return `sem novidades | vistas: ${allOffers.length}`
    }

    log(`🎉 ${candidateOffers.length} ofertas candidatas detectadas`)
    logSeparator()

    const completeOffers = []
    const shortcutMode = isShortcutContext()

    if (shortcutMode) {
      log("⚡ contexto shortcuts detectado")
      log("📌 prioridade: ainda tentar enriquecer páginas internas para dedupe melhor")
    } else {
      log("📋 extraindo detalhes...")
    }

    for (let i = 0; i < candidateOffers.length; i++) {
      const offer = candidateOffers[i]
      log(`📌 oferta ${i + 1}/${candidateOffers.length}`)
      log(`   id: ${offer.id}`)

      let details = null
      let usedQuickFallback = false

      try {
        details = await extractOfferDetails(offer.link, offer.preview_title)
      } catch (e) {
        usedQuickFallback = true
        log(`   ⚠️ falha ao enriquecer página interna: ${e.message}`)
      }

      const finalTitle = (details && details.title) || offer.title || offer.preview_title || getOfferId(offer.link)
      const finalPartnerImgUrl = absolutizeUrl(offer.partner_img_url || "")

      let finalImgUrl = absolutizeUrl((details && details.detailImgUrl) || "")

      if (
        !finalImgUrl ||
        isBadBannerUrl(finalImgUrl) ||
        finalImgUrl === finalPartnerImgUrl
      ) {
        const fallbackOfferImg = absolutizeUrl(offer.img_url || "")
        if (
          fallbackOfferImg &&
          !isBadBannerUrl(fallbackOfferImg) &&
          fallbackOfferImg !== finalPartnerImgUrl
        ) {
          finalImgUrl = fallbackOfferImg
        }
      }

      if (
        !finalImgUrl ||
        isBadBannerUrl(finalImgUrl) ||
        finalImgUrl === finalPartnerImgUrl
      ) {
        finalImgUrl = ""
      }

      const validity = (details && details.validity) || null
      const description = (details && details.description) || (usedQuickFallback ? "enriquecimento pendente" : "descrição não disponível.")
      const dedupeKey = buildDedupeKey(finalTitle, validity, description)
      const looseDedupeKey = buildLooseDedupeKey(finalTitle, description)

      const normalizedOffer = {
        id: offer.id,
        original_link: offer.original_link || offer.link,
        preview_title: offer.preview_title || finalTitle,
        title: finalTitle,
        link: offer.link,
        img_url: finalImgUrl,
        partner_img_url: finalPartnerImgUrl,
        validity,
        description,
        dedupe_key: dedupeKey,
        loose_dedupe_key: looseDedupeKey,
        scraped_at: new Date().toISOString()
      }

      if (alreadyKnownByAnyKey(normalizedOffer, knownSets)) {
        log("   ⚠️ pulada por dedupe já conhecido")
        continue
      }

      completeOffers.push(normalizedOffer)

      if (normalizedOffer.dedupe_key) knownSets.pendingDedupe.add(normalizedOffer.dedupe_key)
      if (normalizedOffer.loose_dedupe_key) knownSets.pendingLooseDedupe.add(normalizedOffer.loose_dedupe_key)
      const idKey = normalizeOfferKey(normalizedOffer.id || normalizedOffer.link)
      if (idKey) knownSets.pendingKeys.add(idKey)

      log(`   🖼️ banner final: ${finalImgUrl || "vazio"}`)
      log(`   🏷️ logo final: ${finalPartnerImgUrl || "vazio"}`)
      log(`   ✅ título final: ${finalTitle.substring(0, 55)}...`)
    }

    logSeparator()
    log(`✅ ofertas novas úteis após dedupe: ${completeOffers.length}`)

    if (completeOffers.length === 0) {
      dailyState = await loadDailyLogStrict()
      if (dailyState) {
        dailyState = ensureTodayState(dailyState)
        dailyState.pending_count = pendingBeforeCount
        dailyState = appendDashboardLine(dailyState, "scriptable", "💤 sem ofertas novas")
        await saveDailyLog(dailyState)
      }

      statusState = await loadStatusRuntimeStrict()
      if (statusState) {
        statusState = setScriptableStatusFinish(statusState, {
          status: "sem_novidades",
          summary: "dedupe filtrou todas as ofertas",
          offers_seen: allOffers.length,
          new_offers: 0,
          pending_count: pendingBeforeCount,
          last_error: ""
        })
        await saveStatusRuntime(statusState)
      }

      return `sem novidades após dedupe | vistas: ${allOffers.length}`
    }

    const mergedDeduped = mergeOffersDeduped(pending.offers || [], completeOffers)

    const okPending = await savePending(mergedDeduped)
    if (!okPending) {
      dailyState = await loadDailyLogStrict()
      if (dailyState) {
        dailyState = ensureTodayState(dailyState)
        dailyState.pending_count = pendingBeforeCount
        dailyState = appendDashboardLine(dailyState, "scriptable", "⚠️ falha ao salvar pending")
        await saveDailyLog(dailyState)
      }

      statusState = await loadStatusRuntimeStrict()
      if (statusState) {
        statusState = setScriptableStatusFinish(statusState, {
          status: "erro",
          summary: "falha ao salvar pending",
          offers_seen: allOffers.length,
          new_offers: completeOffers.length,
          pending_count: pendingBeforeCount,
          last_error: "falha ao salvar pending_offers.json"
        })
        await saveStatusRuntime(statusState)
      }

      return `erro | falha ao salvar pending | vistas: ${allOffers.length} | novas: ${completeOffers.length}`
    }

    dailyState = await loadDailyLogStrict()
    if (dailyState) {
      dailyState = ensureTodayState(dailyState)
      dailyState.last_new_offer_at = brDateTime()
      dailyState.last_success_check = brDateTime()
      dailyState.pending_count = mergedDeduped.length
      dailyState = appendDashboardLine(dailyState, "scriptable", `✅ novas no pending: ${completeOffers.length}`)
      await saveDailyLog(dailyState)
    }

    statusState = await loadStatusRuntimeStrict()
    if (statusState) {
      statusState = setScriptableStatusFinish(statusState, {
        status: "sucesso",
        summary: `novas no pending: ${completeOffers.length}`,
        offers_seen: allOffers.length,
        new_offers: completeOffers.length,
        pending_count: mergedDeduped.length,
        last_error: ""
      })
      await saveStatusRuntime(statusState)
    }

    log("🚀 github actions vai consumir o pending")

    if (shortcutMode) {
      return `ok rápido | vistas: ${allOffers.length} | novas: ${completeOffers.length} | pending: ${mergedDeduped.length}`
    }

    return `ok | vistas: ${allOffers.length} | novas: ${completeOffers.length} | pending: ${mergedDeduped.length}`
  } catch (e) {
    const errMsg = String(e && e.message ? e.message : e)
    log(`❌ erro geral: ${errMsg}`)

    try {
      let freshStatusState = await loadStatusRuntimeStrict()
      if (freshStatusState) {
        freshStatusState = setScriptableStatusFinish(freshStatusState, {
          status: "erro",
          summary: "erro geral no scriptable",
          offers_seen: 0,
          new_offers: 0,
          pending_count: pendingBeforeCount,
          last_error: errMsg
        })
        await saveStatusRuntime(freshStatusState)
      }
    } catch (_) {}

    try {
      let freshDailyState = await loadDailyLogStrict()
      if (freshDailyState) {
        freshDailyState = ensureTodayState(freshDailyState)
        freshDailyState.pending_count = pendingBeforeCount
        freshDailyState = appendDashboardLine(freshDailyState, "scriptable", `❌ erro: ${errMsg}`)
        await saveDailyLog(freshDailyState)
      }
    } catch (_) {}

    return `erro | ${errMsg}`
  }
}

// widget
function saveCache(data) {
  try {
    fm.writeString(cachePath, JSON.stringify({
      timestamp: Date.now(),
      data: Array.isArray(data) ? data : []
    }))
  } catch (e) {
    console.log("erro salvando cache: " + e)
  }
}

function loadCache() {
  if (!fm.fileExists(cachePath)) return null
  try {
    const parsed = JSON.parse(fm.readString(cachePath))
    if (!parsed || !Array.isArray(parsed.data)) return null
    return parsed
  } catch (e) {
    console.log("erro lendo cache: " + e)
    return null
  }
}

function normalizeOffers(raw) {
  if (!raw) return []

  if (Array.isArray(raw)) {
    return raw.slice(0, 4).map(o => ({
      title: String(o.title || "oferta uol"),
      mainImg: o.mainImg || o.img_url || "",
      logoImg: o.logoImg || o.partner_img_url || ""
    }))
  }

  if (raw && Array.isArray(raw.offers)) {
    return raw.offers.slice(0, 4).map(o => ({
      title: String(o.title || "oferta uol"),
      mainImg: o.img_url || o.mainImg || "",
      logoImg: o.partner_img_url || o.logoImg || ""
    }))
  }

  return []
}

async function fetchWidgetData() {
  const cache = loadCache()

  if (cache && Date.now() - cache.timestamp < CACHE_TIME) {
    return Array.isArray(cache.data) ? cache.data : []
  }

  try {
    const req = new Request(GITHUB_JSON_URL)
    req.timeoutInterval = 5
    req.headers = { "Cache-Control": "no-cache" }

    const text = await req.loadString()

    if (!text || text.trim().startsWith("<")) {
      throw new Error("github retornou html ou vazio em vez de json")
    }

    let json
    try {
      json = JSON.parse(text)
    } catch (e) {
      throw new Error("json inválido: " + e)
    }

    const offers = normalizeOffers(json)

    if (offers.length > 0) {
      saveCache(offers)
      return offers
    }

    return cache && Array.isArray(cache.data) ? cache.data : []
  } catch (e) {
    console.log("erro github: " + e)
    return cache && Array.isArray(cache.data) ? cache.data : []
  }
}

async function loadImage(url) {
  if (!url) return null

  const safeName = url.replace(/[^a-z0-9]/gi, "") + ".jpg"
  const path = fm.joinPath(fm.documentsDirectory(), safeName)

  try {
    if (fm.fileExists(path)) return fm.readImage(path)
  } catch (e) {}

  try {
    const req = new Request(url)
    req.timeoutInterval = 4
    const img = await req.loadImage()
    fm.writeImage(path, img)
    return img
  } catch (e) {
    console.log("erro imagem: " + e)
    return null
  }
}

async function createWidget() {
  const offers = await fetchWidgetData()
  const safeOffers = Array.isArray(offers) ? offers.slice(0, 4) : []

  const urls = new Set([UOL_LOGO_URL])
  safeOffers.forEach(o => {
    if (o.mainImg) urls.add(o.mainImg)
    if (o.logoImg) urls.add(o.logoImg)
  })

  const imgCache = {}
  await Promise.all(Array.from(urls).map(async (u) => {
    imgCache[u] = await loadImage(u)
  }))

  const w = new ListWidget()
  w.backgroundColor = new Color("#4a027e")
  w.setPadding(12, 12, 12, 12)
  w.url = "https://github.com/leosaquetto/uol-bot"
  w.refreshAfterDate = new Date(Date.now() + 10 * 60 * 1000)

  const header = w.addStack()
  header.layoutHorizontally()
  header.centerAlignContent()

  const title = header.addText("clube uol")
  title.textColor = Color.white()
  title.font = Font.boldSystemFont(13)

  header.addSpacer()

  if (imgCache[UOL_LOGO_URL]) {
    const img = header.addImage(imgCache[UOL_LOGO_URL])
    img.imageSize = new Size(45, 12)
  }

  w.addSpacer()

  if (safeOffers.length === 0) {
    const t = w.addText("sem ofertas recentes")
    t.textColor = Color.white()
    t.font = Font.systemFont(12)
    return w
  }

  for (let i = 0; i < 2; i++) {
    const row = w.addStack()
    row.layoutHorizontally()

    for (let j = 0; j < 2; j++) {
      const idx = i * 2 + j
      const item = safeOffers[idx]

      const box = row.addStack()
      box.size = new Size(0, 56)
      box.backgroundColor = new Color("#ffffff", 0.15)
      box.cornerRadius = 10
      box.setPadding(6, 6, 6, 6)

      if (item) {
        box.layoutHorizontally()
        box.centerAlignContent()

        if (item.mainImg && imgCache[item.mainImg]) {
          const img = box.addImage(imgCache[item.mainImg])
          img.imageSize = new Size(44, 44)
          img.cornerRadius = 8
        }

        box.addSpacer(6)

        const col = box.addStack()
        col.layoutVertically()

        if (item.logoImg && imgCache[item.logoImg]) {
          const l = col.addImage(imgCache[item.logoImg])
          l.imageSize = new Size(16, 16)
          l.cornerRadius = 4
          col.addSpacer(2)
        }

        const t = col.addText(String(item.title || "oferta").toUpperCase())
        t.font = Font.boldSystemFont(9)
        t.textColor = Color.white()
        t.lineLimit = 2
        t.minimumScaleFactor = 0.75
      }

      if (j === 0) row.addSpacer(8)
    }

    if (i === 0) w.addSpacer(8)
  }

  return w
}

async function runApp() {
  try {
    return String(await mainScraper() || "ok")
  } catch (e) {
    const message = String(e && e.message ? e.message : e)
    console.error(`top-level error: ${message}`)
    return `erro | ${message}`
  }
}

if (config.runsInWidget) {
  const widget = await createWidget()
  Script.setWidget(widget)
  Script.complete()
} else {
  const output = await runApp()
  console.log(`final output: ${output}`)
  Script.setShortcutOutput(String(output || "ok"))
  Script.complete()
}
