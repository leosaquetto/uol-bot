// ------------------------------
// uol widget - github ultra leve v8
// pega as 4 ofertas mais recentes/atuais de verdade
// mantém o layout atual
// compatível com scriptable
// ------------------------------

const SNAPSHOTS_API_URL = "https://api.github.com/repos/leosaquetto/uol-bot/contents/snapshots?ref=main"
const UOL_LOGO_URL = "https://i.imgur.com/UdIgTfI.png"

const fm = FileManager.local()
const cachePath = fm.joinPath(fm.documentsDirectory(), "uol_widget_cache_v8.json")
const CACHE_TIME = 10 * 60 * 1000 // 10 min

const MAX_SNAPSHOT_META_FILES = 10
const MAX_SNAPSHOT_HTML_FILES = 8
const MAX_DETAIL_JSON_FILES = 12

function saveCache(data) {
  try {
    fm.writeString(cachePath, JSON.stringify({ timestamp: Date.now(), data: Array.isArray(data) ? data : [] }))
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

function parseDateSafe(value) {
  if (!value) return 0
  try {
    const t = new Date(value).getTime()
    return Number.isFinite(t) ? t : 0
  } catch (e) {
    return 0
  }
}

function extractSnapshotTsFromName(name) {
  if (!name) return 0
  const m = String(name).match(/snapshot_(\d{8}_\d{6})/i)
  if (!m) return 0
  const raw = m[1]
  const iso = `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T${raw.slice(9, 11)}:${raw.slice(11, 13)}:${raw.slice(13, 15)}Z`
  return parseDateSafe(iso)
}

function absolutizeUrl(url) {
  if (!url) return ""
  if (url.startsWith("http://") || url.startsWith("https://")) return url
  if (url.startsWith("//")) return "https:" + url
  if (url.startsWith("/")) return "https://clube.uol.com.br" + url
  return "https://clube.uol.com.br/" + url
}

function decodeBasicEntities(str) {
  if (!str) return ""
  return String(str)
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
}

function cleanText(str) {
  if (!str) return ""
  return decodeBasicEntities(String(str).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim())
}

function normalizeOfferKey(value) {
  const raw = String(value || "").trim()
  if (!raw) return ""
  const tail = raw.startsWith("http") ? raw.split("?")[0].replace(/\/$/, "").split("/").pop() : raw
  return String(tail || "").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]+/g, "-").replace(/-+/g, "-").replace(/^-+|-+$/g, "")
}

function dedupeOffers(list) {
  const out = []
  const seen = new Set()
  for (const item of list) {
    const key = normalizeOfferKey(item.link) || `${String(item.link || "").trim()}::${String(item.title || "").trim()}`
    if (!item.title || !item.link || seen.has(key)) continue
    seen.add(key)
    out.push(item)
  }
  return out
}

async function fetchJson(url, timeout = 6) {
  const req = new Request(url)
  req.timeoutInterval = timeout
  req.headers = { "Cache-Control": "no-cache", "Accept": "application/vnd.github+json" }
  return await req.loadJSON()
}

async function fetchText(url, timeout = 6) {
  const req = new Request(url)
  req.timeoutInterval = timeout
  req.headers = { "Cache-Control": "no-cache" }
  return await req.loadString()
}

async function listSnapshotFiles() {
  try {
    const items = await fetchJson(SNAPSHOTS_API_URL, 8)
    if (!Array.isArray(items)) return { metaFiles: [], htmlFiles: [], detailFiles: [] }

    const metaFiles = items
      .filter(x => x && x.name && /^snapshot_.*\.json$/i.test(x.name))
      .sort((a, b) => String(b.name).localeCompare(String(a.name)))
      .slice(0, MAX_SNAPSHOT_META_FILES)

    const htmlFiles = items
      .filter(x => x && x.name && /^snapshot_.*\.html$/i.test(x.name))
      .sort((a, b) => String(b.name).localeCompare(String(a.name)))
      .slice(0, MAX_SNAPSHOT_HTML_FILES)

    const detailFiles = items
      .filter(x => x && x.name && /^detail_.*\.json$/i.test(x.name))
      .sort((a, b) => String(b.name).localeCompare(String(a.name)))
      .slice(0, MAX_DETAIL_JSON_FILES)

    return { metaFiles, htmlFiles, detailFiles }
  } catch (e) {
    console.log("erro listando snapshots: " + e)
    return { metaFiles: [], htmlFiles: [], detailFiles: [] }
  }
}

function extractOfferCards(html, snapshotTs = 0, limit = 12) {
  const cards = []
  const cardRegex = /<div class="col-12 col-sm-4 col-md-3 mb-3 beneficio"[\s\S]*?<!-- Fim div beneficio -->/gi
  const matches = html.match(cardRegex) || []

  for (let idx = 0; idx < matches.length; idx++) {
    if (cards.length >= limit) break
    const block = matches[idx]
    const hrefMatch = block.match(/<a href="([^"]+)"/i)
    const titleMatch = block.match(/<p class="titulo mb-0">([\s\S]*?)<\/p>/i)
    const partnerMatch = block.match(/<img[^>]+data-src="([^"]*\/parceiros\/[^\"]+)"[^>]*alt="([^"]*)"[^>]*title="([^"]*)"/i)
    const benefitImgMatch = block.match(/<div class="col-12 thumb text-center lazy" data-src="([^"]*\/beneficios\/[^\"]+)"/i)

    const link = hrefMatch ? absolutizeUrl(hrefMatch[1]) : ""
    const title = titleMatch ? cleanText(titleMatch[1]) : ""
    const partnerImg = partnerMatch ? absolutizeUrl(partnerMatch[1]) : ""
    const partnerAlt = partnerMatch ? cleanText(partnerMatch[2]) : ""
    const partnerTitle = partnerMatch ? cleanText(partnerMatch[3]) : ""
    const benefitImg = benefitImgMatch ? absolutizeUrl(benefitImgMatch[1]) : ""

    if (!link || !title) continue

    cards.push({
      title,
      mainImg: benefitImg,
      logoImg: partnerImg,
      partnerName: partnerTitle || partnerAlt || "",
      link,
      ts: snapshotTs,
      order: idx,
    })
  }

  return cards
}

async function fetchOffersFromLatestMeta(fileList) {
  const metaFiles = Array.isArray(fileList?.metaFiles) ? fileList.metaFiles : []
  for (const file of metaFiles) {
    try {
      if (!file.download_url) continue
      const json = await fetchJson(file.download_url, 8)
      const offers = Array.isArray(json?.offers) ? json.offers : []
      if (!offers.length) continue

      const ts = parseDateSafe(json?.created_at) || extractSnapshotTsFromName(file.name)
      const normalized = offers
        .map((o, idx) => {
          const link = absolutizeUrl(String(o.link || o.original_link || "").trim())
          const title = cleanText(String(o.title || o.preview_title || "").trim())
          if (!link || !title) return null
          return {
            title,
            mainImg: absolutizeUrl(String(o.img_url || o.card_img_url || "").trim()),
            logoImg: absolutizeUrl(String(o.partner_img_url || "").trim()),
            partnerName: cleanText(String(o.partner_name || "").trim()),
            link,
            ts,
            order: idx,
          }
        })
        .filter(Boolean)

      const deduped = dedupeOffers(normalized)
      if (deduped.length) {
        return deduped.slice(0, 4)
      }
    } catch (e) {
      console.log("erro lendo snapshot meta: " + e)
    }
  }
  return []
}

async function fetchOffersFromSnapshotsHtml(fileList) {
  const htmlFiles = Array.isArray(fileList?.htmlFiles) ? fileList.htmlFiles : []
  if (!htmlFiles.length) return []

  const mergedCards = []

  for (const file of htmlFiles) {
    try {
      if (!file.download_url) continue
      const html = await fetchText(file.download_url, 8)
      const snapshotTs = extractSnapshotTsFromName(file.name)
      const cards = extractOfferCards(html, snapshotTs, 12)
      mergedCards.push(...cards)
    } catch (e) {
      console.log("erro lendo snapshot html: " + e)
    }
  }

  const unique = dedupeOffers(mergedCards)
  unique.sort((a, b) => {
    if ((b.ts || 0) !== (a.ts || 0)) return (b.ts || 0) - (a.ts || 0)
    return (a.order || 0) - (b.order || 0)
  })

  return unique.slice(0, 4)
}

async function buildDetailMap(fileList) {
  const detailFiles = Array.isArray(fileList?.detailFiles) ? fileList.detailFiles : []
  const detailMap = {}

  for (const file of detailFiles) {
    try {
      if (!file.download_url) continue
      const json = await fetchJson(file.download_url, 8)
      const testedAt = parseDateSafe(json?.tested_at)
      const offers = Array.isArray(json?.offers) ? json.offers : []

      for (const o of offers) {
        const link = absolutizeUrl(String(o.link || "").trim())
        if (!link) continue

        const current = detailMap[link]
        const candidate = {
          title: String(o.detail_title || o.card_title || "").trim(),
          mainImg: String(o.detail_img_url || o.card_img_url || "").trim(),
          logoImg: String(o.partner_img_url || "").trim(),
          ts: testedAt,
        }

        if (!current || candidate.ts >= current.ts) detailMap[link] = candidate
      }
    } catch (e) {
      console.log("erro lendo detail file: " + e)
    }
  }

  return detailMap
}

async function fetchData() {
  const cache = loadCache()
  if (cache && Date.now() - cache.timestamp < CACHE_TIME) return Array.isArray(cache.data) ? cache.data : []

  try {
    const fileList = await listSnapshotFiles()

    let cards = await fetchOffersFromLatestMeta(fileList)
    if (!cards.length) cards = await fetchOffersFromSnapshotsHtml(fileList)

    if (!cards.length) return cache && Array.isArray(cache.data) ? cache.data : []

    const detailMap = await buildDetailMap(fileList)

    const merged = cards.map(card => {
      const detail = detailMap[card.link] || null
      return {
        title: detail?.title ? detail.title : card.title,
        mainImg: detail?.mainImg ? detail.mainImg : card.mainImg,
        logoImg: detail?.logoImg ? detail.logoImg : card.logoImg,
        link: card.link,
        ts: detail?.ts ? detail.ts : card.ts,
        order: card.order || 0,
      }
    })

    const finalOffers = dedupeOffers(merged)
      .sort((a, b) => {
        if ((b.ts || 0) !== (a.ts || 0)) return (b.ts || 0) - (a.ts || 0)
        return (a.order || 0) - (b.order || 0)
      })
      .slice(0, 4)

    if (finalOffers.length > 0) {
      saveCache(finalOffers)
      return finalOffers
    }

    return cache && Array.isArray(cache.data) ? cache.data : []
  } catch (e) {
    console.log("erro montando widget: " + e)
    return cache && Array.isArray(cache.data) ? cache.data : []
  }
}

async function loadImage(url) {
  if (!url) return null
  const safeName = url.replace(/[^a-z0-9]/gi, "") + ".jpg"
  const path = fm.joinPath(fm.documentsDirectory(), safeName)

  try { if (fm.fileExists(path)) return fm.readImage(path) } catch (e) {}

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
  const offers = await fetchData()
  const safeOffers = Array.isArray(offers) ? offers.slice(0, 4) : []

  const urls = new Set([UOL_LOGO_URL])
  safeOffers.forEach(o => {
    if (o.mainImg) urls.add(o.mainImg)
    if (o.logoImg) urls.add(o.logoImg)
  })

  const imgCache = {}
  await Promise.all(Array.from(urls).map(async (u) => { imgCache[u] = await loadImage(u) }))

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
        box.url = item.link || "https://github.com/leosaquetto/uol-bot"

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

const widget = await createWidget()
if (config.runsInWidget) Script.setWidget(widget)
else await widget.presentMedium()
Script.complete()
