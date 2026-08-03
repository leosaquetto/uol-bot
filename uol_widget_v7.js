// ------------------------------
// uol widget - API pública do Worker v8
// pega as 4 ofertas mais recentes/atuais de verdade
// mantém o layout atual
// compatível com scriptable
// ------------------------------

const OFFERS_API_URL = "https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/offers?limit=4"
const UOL_LOGO_URL = "https://i.imgur.com/UdIgTfI.png"

const fm = FileManager.local()
const cachePath = fm.joinPath(fm.documentsDirectory(), "uol_widget_cache_v8.json")
const CACHE_TIME = 2 * 60 * 1000 // 2 min

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
  req.headers = { "Accept": "application/json" }
  return await req.loadJSON()
}

async function fetchData() {
  const cache = loadCache()
  const canUseFreshCache = !!config.runsInWidget
  if (canUseFreshCache && cache && Date.now() - cache.timestamp < CACHE_TIME) {
    return Array.isArray(cache.data) ? cache.data : []
  }

  try {
    const payload = await fetchJson(OFFERS_API_URL, 6)
    const finalOffers = dedupeOffers((Array.isArray(payload?.offers) ? payload.offers : [])
      .map((offer, order) => ({
        title: String(offer?.title || "").trim(),
        mainImg: String(offer?.imageUrl || "").trim(),
        logoImg: String(offer?.partnerImageUrl || "").trim(),
        link: absolutizeUrl(String(offer?.link || "").trim()),
        ts: parseDateSafe(offer?.sentAt || offer?.observedAt),
        order,
      }))
      .filter(offer => offer.title && offer.link))
      .sort((a, b) => (b.ts || 0) - (a.ts || 0))
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
