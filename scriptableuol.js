// scriptable - clube uol scraper
// versão robusta + deduplicação estrutural:
// - lê histórico e pending do github com decode base64 correto
// - aborta se não conseguir ler o estado remoto, para não floodar
// - não grava no histórico aqui; histórico fica só com o consumer
// - adiciona apenas ofertas novas ao pending
// - tenta extrair banner principal + logo do parceiro
// - enriquece com título, validade e descrição
// - evita depender de nth-child
// - deduplica por offer_key e também por dedupe_key semântica

// ==============================================
// configurações
// ==============================================
const GITHUB_TOKEN = "xxx"
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

  raw = normalizeTextKey(raw)
  return raw
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
    "atenção",
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
    if (blacklistStarts.some(x => low.startsWith(normalizeTextKey(x)))) continue
    filtered.push(low)
  }

  if (!filtered.length) return ""
  return filtered[0].slice(0, 160)
}

function buildDedupeKey(title, validity, description) {
  const titleKey = normalizeTextKey(title || "")
  const validityKey = normalizeTextKey(validity || "")
  const descKey = pickDescriptionAnchor(description || "")
  return [titleKey, validityKey, descKey].filter(Boolean).join("|")
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
    return { ids: [], dedupe_keys: [] }
  }

  if (!Array.isArray(result.content.ids)) {
    log(`❌ formato inválido em ${HISTORY_FILE}`)
    return null
  }

  if (result.content.dedupe_keys && !Array.isArray(result.content.dedupe_keys)) {
    log(`❌ formato inválido em dedupe_keys de ${HISTORY_FILE}`)
    return null
  }

  return {
    ids: Array.isArray(result.content.ids) ? result.content.ids : [],
    dedupe_keys: Array.isArray(result.content.dedupe_keys) ? result.content.dedupe_keys : []
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
    const fallbackMain = uniqImgs.find(img => !isBadBannerUrl(img.src) && (!partner || img.src !== partner.src))
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

function extractLinkFromBlock(blockHtml) {
  const patterns = [
    /<a[^>]*class=["'][^"']*btn[^"']*["'][^>]*href=["']([^"']+)["']/i,
    /<a[^>]*href=["']([^"']+)["'][^>]*>/i
  ]

  for (const regex of patterns) {
    const match = regex.exec(blockHtml)
    if (match && match[1]) {
      return absolutizeUrl(match[1])
    }
  }

  return ""
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

    const rawBlocks = []
    const categoriaRegex = /<div[^>]*data-categoria=['"]Ingressos\s*Exclusivos['"][^>]*>([\s\S]*?)(?=<div[^>]*data-categoria=|$)/gi
    let match

    while ((match = categoriaRegex.exec(html)) !== null) {
      rawBlocks.push(match[1])
    }

    if (rawBlocks.length === 0) {
      log("⚠️ fallback: buscando blocos com menção a ingresso")
      const genericRegex = /<div[^>]*class=["'][^"']*(?:beneficio|item-oferta|oferta)[^"']*["'][^>]*>([\s\S]*?)(?=<div[^>]*class=["'][^"']*(?:beneficio|item-oferta|oferta)[^"']*["']|$)/gi

      while ((match = genericRegex.exec(html)) !== null) {
        const blockHtml = match[1]
        const low = blockHtml.toLowerCase()
        if (
          low.includes("ingresso") ||
          low.includes("ingressos") ||
          low.includes("campanhasdeingresso")
        ) {
          rawBlocks.push(blockHtml)
        }
      }
    }

    log(`📦 blocos candidatos: ${rawBlocks.length}`)

    const offers = []

    for (const blockHtml of rawBlocks) {
      try {
        const title = extractTitleFromBlock(blockHtml)
        const link = extractLinkFromBlock(blockHtml)
        const { img_url, partner_img_url } = chooseImagesFromBlock(blockHtml)

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
        log(`     🖼️ main: ${img_url ? "ok" : "vazio"}`)
        log(`     🏷️ partner: ${partner_img_url ? "ok" : "não detectado"}`)
      } catch (e) {
        log(`  ⚠️ erro ao extrair card: ${e.message}`)
      }
    }

    return uniqBy(offers, o => normalizeOfferKey(o.id || o.link))
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

  try {
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
  } catch (e) {
    log(`   ⚠️ erro na página interna: ${e.message}`)
    return {
      title: previewTitle,
      validity: null,
      description: "descrição não disponível.",
      detailImgUrl: ""
    }
  }
}

function extractHistorySets(history) {
  const ids = Array.isArray(history.ids) ? history.ids : []
  const dedupeKeys = Array.isArray(history.dedupe_keys) ? history.dedupe_keys : []

  const idSet = new Set(ids.map(x => normalizeOfferKey(x)).filter(Boolean))
  const dedupeSet = new Set(dedupeKeys.map(x => String(x || "").trim()).filter(Boolean))

  return { idSet, dedupeSet }
}

function extractPendingSets(pending) {
  const offers = Array.isArray(pending.offers) ? pending.offers : []

  const idSet = new Set()
  const dedupeSet = new Set()

  for (const o of offers) {
    const offerKey = normalizeOfferKey(o.id || o.link)
    if (offerKey) idSet.add(offerKey)

    let dedupeKey = String(o.dedupe_key || "").trim()
    if (!dedupeKey) {
      dedupeKey = buildDedupeKey(
        o.title || o.preview_title || "",
        o.validity || "",
        o.description || ""
      )
    }
    if (dedupeKey) dedupeSet.add(dedupeKey)
  }

  return { idSet, dedupeSet }
}

async function saveCompleteOffers(offers) {
  const history = await loadHistoryStrict()
  const pending = await loadPendingStrict()

  if (!history || !pending) {
    log("❌ abortando: não consegui carregar histórico/pending com segurança")
    return false
  }

  const historySets = extractHistorySets(history)
  const pendingSets = extractPendingSets(pending)

  const freshOffers = []
  const seenOfferKeys = new Set()
  const seenDedupeKeys = new Set()

  for (const o of offers) {
    const offerKey = normalizeOfferKey(o.id || o.link)
    const dedupeKey = String(o.dedupe_key || "").trim()

    if (
      offerKey &&
      (
        historySets.idSet.has(offerKey) ||
        pendingSets.idSet.has(offerKey) ||
        seenOfferKeys.has(offerKey)
      )
    ) {
      continue
    }

    if (
      dedupeKey &&
      (
        historySets.dedupeSet.has(dedupeKey) ||
        pendingSets.dedupeSet.has(dedupeKey) ||
        seenDedupeKeys.has(dedupeKey)
      )
    ) {
      continue
    }

    if (offerKey) seenOfferKeys.add(offerKey)
    if (dedupeKey) seenDedupeKeys.add(dedupeKey)

    freshOffers.push(o)
  }

  if (freshOffers.length === 0) {
    log("📭 nenhuma oferta nova para adicionar ao pending")
    return false
  }

  const merged = [...(pending.offers || []), ...freshOffers]
  const deduped = uniqBy(
    merged,
    o => String(o.dedupe_key || "").trim() || normalizeOfferKey(o.id || o.link)
  )

  const ok = await savePending(deduped)

  if (!ok) {
    log("❌ não consegui salvar o pending no github")
    return false
  }

  log(`✅ ${freshOffers.length} novas ofertas adicionadas ao pending`)
  return true
}

async function main() {
  log("=".repeat(60))
  log("🤖 scriptable - clube uol scraper")
  log(`📅 ${new Date().toLocaleString()}`)
  log("=".repeat(60))

  if (!GITHUB_TOKEN) {
    log("❌ passe o token no parâmetro do widget / script")
    return
  }

  try {
    const history = await loadHistoryStrict()
    const pending = await loadPendingStrict()

    if (!history || !pending) {
      log("❌ abortando por segurança: falha ao ler estado remoto")
      return
    }

    log(`📚 histórico atual: ${(history.ids || []).length} ids`)
    log(`🧠 dedupe_keys atuais: ${(history.dedupe_keys || []).length}`)
    log(`📦 pending atual: ${(pending.offers || []).length} ofertas`)

    const allOffers = await scrapeOffersList()

    if (allOffers.length === 0) {
      log("📭 nenhuma oferta encontrada")
      return
    }

    log("📋 extraindo detalhes...")
    logSeparator()

    const completeOffers = []

    for (let i = 0; i < allOffers.length; i++) {
      const offer = allOffers[i]
      log(`📌 oferta ${i + 1}/${allOffers.length}`)
      log(`   id: ${offer.id}`)

      const details = await extractOfferDetails(offer.link, offer.preview_title)

      const finalTitle = details.title || offer.title || offer.preview_title
      const finalPartnerImgUrl = absolutizeUrl(offer.partner_img_url || "")

      let finalImgUrl = absolutizeUrl(details.detailImgUrl || "")

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

      const dedupeKey = buildDedupeKey(
        finalTitle,
        details.validity,
        details.description
      )

      log(`   🖼️ banner final: ${finalImgUrl || "vazio"}`)
      log(`   🏷️ logo final: ${finalPartnerImgUrl || "vazio"}`)

      completeOffers.push({
        id: offer.id,
        original_link: offer.original_link || offer.link,
        preview_title: offer.preview_title || finalTitle,
        title: finalTitle,
        link: offer.link,
        img_url: finalImgUrl,
        partner_img_url: finalPartnerImgUrl,
        validity: details.validity,
        description: details.description,
        dedupe_key: dedupeKey,
        scraped_at: new Date().toISOString()
      })

      log(`   ✅ título final: ${finalTitle.substring(0, 55)}...`)
      if (finalPartnerImgUrl) {
        log("   🏷️ logo do parceiro detectada")
      }
    }

    logSeparator()
    log(`✅ detalhes extraídos: ${completeOffers.length}`)

    const saved = await saveCompleteOffers(completeOffers)

    if (saved) {
      log("🚀 github actions vai consumir o pending")
    }
  } catch (e) {
    log(`❌ erro geral: ${e.message}`)
  }

  logSeparator()
  log("🏁 fim")
}

await main()

if (config.runsWithSiri || args.shortcutParameter !== undefined) {
  Script.setShortcutOutput("ok")
}

Script.complete()
