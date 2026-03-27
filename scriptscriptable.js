// scriptable - clube uol scraper
// versão robusta:
// - lê histórico e pending do github com decode base64 correto
// - aborta se não conseguir ler o estado remoto, para não floodar
// - não grava no histórico aqui; histórico fica só com o consumer
// - adiciona apenas ofertas novas ao pending
// - tenta extrair banner principal + logo do parceiro
// - enriquece com título, validade e descrição
// - evita depender de nth-child

// ==============================================
// configurações
// ==============================================
const GITHUB_TOKEN = "COLE_SEU_TOKEN_AQUI"
const REPO_OWNER = "leosaquetto"
const REPO_NAME = "uol-bot"

const BASE_URL = "https://clube.uol.com.br"
const LIST_URL = `${BASE_URL}/?order=new`

const HISTORY_FILE = "historico_leouol.json"
const PENDING_FILE = "pending_offers.json"

// ==============================================
// utilidades
// ==============================================
function log(msg) {
  const timestamp = new Date().toLocaleTimeString()
  console.log(`[${timestamp}] ${msg}`)
}

function logSeparator() {
  console.log("-".repeat(60))
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

function normalizeOfferKey(value) {
  let raw = String(value || "").trim().toLowerCase()
  if (!raw) return ""

  if (raw.startsWith("http://") || raw.startsWith("https://")) {
    raw = getOfferId(raw)
  }

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

  raw = raw.replace(/[^a-z0-9\-_\/]+/g, "-")
  raw = raw.replace(/-+/g, "-")
  raw = raw.replace(/^[-/]+|[-/]+$/g, "")

  return raw
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
    u.endsWith("/loader.gif") ||
    u.includes("/static/images/loader.gif") ||
    u.includes("/parceiros/")
  )
}

// ==============================================
// github api
// ==============================================
function githubContentsUrl(filePath) {
  return `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/contents/${filePath}`
}

async function githubRequest(url, method = "GET", body = null) {
  const req = new Request(url)
  req.method = method
  req.headers = {
    "Authorization": `token ${GITHUB_TOKEN}`,
    "User-Agent": "Scriptable",
    "Accept": "application/vnd.github+json"
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
    return { ids: [] }
  }

  if (!Array.isArray(result.content.ids)) {
    log(`❌ formato inválido em ${HISTORY_FILE}`)
    return null
  }

  return result.content
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

// ==============================================
// parser da lista
// ==============================================
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

  return uniqBy
