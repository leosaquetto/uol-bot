// scriptable - snapshot html + até 2 detalhes novos com cache local no icloud
// objetivo:
// 1) salvar a vitrine no github
// 2) manter cache local de links já vistos no icloud do scriptable
// 3) tentar detalhe só dos links ainda não vistos
// 4) testar no máximo 2 detalhes por rodada
// 5) subir só json leve dos detalhes
// 6) não derrubar o fluxo principal se detalhe falhar

const GITHUB_TOKEN = "xxx"
const REPO_OWNER = "leosaquetto"
const REPO_NAME = "uol-bot"
const TARGET_BRANCH = "main"

const BASE_URL = "https://clube.uol.com.br"
const LIST_URL = `${BASE_URL}/?order=new`

const MAX_DETAIL_FETCHES = 1
const MAX_SEEN_LINKS = 200
const SEEN_CACHE_FILE = "uol_seen_links.json"

function log(msg) {
  const timestamp = new Date().toLocaleTimeString()
  console.log(`[${timestamp}] ${msg}`)
}

function isShortcutContext() {
  return !!(config.runsInShortcuts || config.runsWithSiri || args.shortcutParameter !== undefined)
}

function pad(n) {
  return String(n).padStart(2, "0")
}

function buildSnapshotId() {
  const d = new Date()
  const y = d.getFullYear()
  const m = pad(d.getMonth() + 1)
  const day = pad(d.getDate())
  const hh = pad(d.getHours())
  const mm = pad(d.getMinutes())
  const ss = pad(d.getSeconds())
  const rand = Math.random().toString(36).slice(2, 7)
  return `${y}${m}${day}_${hh}${mm}${ss}_${rand}`
}

function toBase64(str) {
  return Data.fromString(str).toBase64String()
}

function githubApiUrl(path) {
  return `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/contents/${path}`
}

async function githubPutFile(path, content, message) {
  const req = new Request(githubApiUrl(path))
  req.method = "PUT"
  req.headers = {
    "User-Agent": "Scriptable",
    "Accept": "application/vnd.github+json",
    "Authorization": `token ${String(GITHUB_TOKEN || "").trim()}`,
    "Content-Type": "application/json"
  }

  req.body = JSON.stringify({
    message,
    content: toBase64(content),
    branch: TARGET_BRANCH
  })

  try {
    const resp = await req.loadJSON()
    if (resp && resp.commit) {
      return { ok: true, data: resp }
    }
    return { ok: false, error: `github sem commit: ${JSON.stringify(resp)}` }
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
    "Cache-Control": "no-cache"
  }

  return await req.loadString()
}

async function fetchUolHtml() {
  return await fetchText(LIST_URL, BASE_URL + "/", 20)
}

function absolutizeUrl(url) {
  if (!url) return ""
  if (url.startsWith("http://") || url.startsWith("https://")) return url
  if (url.startsWith("//")) return "https:" + url
  if (url.startsWith("/")) return BASE_URL + url
  return `${BASE_URL}/${url}`
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
  return decodeBasicEntities(
    String(str)
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim()
  )
}

function normalizeLink(url) {
  return String(url || "").trim()
}

function getIcloudPaths() {
  const fm = FileManager.iCloud()
  const dir = fm.documentsDirectory()
  const path = fm.joinPath(dir, SEEN_CACHE_FILE)
  return { fm, dir, path }
}

async function ensureSeenCacheFile() {
  const { fm, path } = getIcloudPaths()
  if (!fm.fileExists(path)) {
    const initial = {
      seen: [],
      updated_at: new Date().toISOString()
    }
    fm.writeString(path, JSON.stringify(initial, null, 2))
  }
  try {
    await fm.downloadFileFromiCloud(path)
  } catch (e) {}
}

async function loadSeenCache() {
  await ensureSeenCacheFile()
  const { fm, path } = getIcloudPaths()

  try {
    const raw = fm.readString(path)
    const data = JSON.parse(raw)

    const seen = Array.isArray(data.seen) ? data.seen.map(normalizeLink).filter(Boolean) : []
    return {
      seen,
      updated_at: String(data.updated_at || "")
    }
  } catch (e) {
    return {
      seen: [],
      updated_at: ""
    }
  }
}

function saveSeenCache(seenLinks) {
  const { fm, path } = getIcloudPaths()
  const trimmed = Array.from(new Set(seenLinks.map(normalizeLink).filter(Boolean))).slice(-MAX_SEEN_LINKS)

  const payload = {
    seen: trimmed,
    updated_at: new Date().toISOString()
  }

  fm.writeString(path, JSON.stringify(payload, null, 2))
}

function extractOfferCards(html, limit = 50) {
  const cards = []
  const cardRegex = /<div class="col-12 col-sm-4 col-md-3 mb-3 beneficio"[\s\S]*?<!-- Fim div beneficio -->/gi
  const matches = html.match(cardRegex) || []

  for (const block of matches) {
    if (cards.length >= limit) break

    const categoryMatch = block.match(/data-categoria="([^"]*)"/i)
    const hrefMatch = block.match(/<a href="([^"]+)"/i)
    const titleMatch = block.match(/<p class="titulo mb-0">([\s\S]*?)<\/p>/i)
    const partnerMatch = block.match(/<img[^>]+data-src="([^"]*\/parceiros\/[^"]+)"[^>]*alt="([^"]*)"[^>]*title="([^"]*)"/i)
    const benefitImgMatch = block.match(/<div class="col-12 thumb text-center lazy" data-src="([^"]*\/beneficios\/[^"]+)"/i)

    const link = hrefMatch ? absolutizeUrl(hrefMatch[1]) : ""
    const title = titleMatch ? cleanText(titleMatch[1]) : ""
    const category = categoryMatch ? cleanText(categoryMatch[1]) : ""
    const partnerImg = partnerMatch ? absolutizeUrl(partnerMatch[1]) : ""
    const partnerAlt = partnerMatch ? cleanText(partnerMatch[2]) : ""
    const partnerTitle = partnerMatch ? cleanText(partnerMatch[3]) : ""
    const benefitImg = benefitImgMatch ? absolutizeUrl(benefitImgMatch[1]) : ""

    if (!link || !title) continue

    cards.push({
      link,
      title,
      category,
      partner_img_url: partnerImg,
      partner_name: partnerTitle || partnerAlt || "",
      img_url: benefitImg
    })
  }

  return cards
}

function extractTitleFromDetail(html) {
  const h2 = html.match(/<h2[^>]*>([\s\S]*?)<\/h2>/i)
  if (h2 && h2[1]) return cleanText(h2[1])

  const h1 = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i)
  if (h1 && h1[1]) return cleanText(h1[1])

  return ""
}

function extractValidityFromDetail(html) {
  const regexes = [
    /[Bb]enefício válido de[^.!?\n]*[.!?]?/i,
    /[Vv]álido até[^.!?\n]*[.!?]?/i,
    /\d{2}\/\d{2}\/\d{4}[\s\S]{0,80}\d{2}\/\d{2}\/\d{4}/i
  ]

  for (const regex of regexes) {
    const m = html.match(regex)
    if (m && m[0]) return cleanText(m[0])
  }

  return ""
}

function extractDescriptionFromDetail(html) {
  const regexes = [
    /class=["'][^"']*info-beneficio[^"']*["'][^>]*>([\s\S]*?)(?:<script|<footer|class=["'][^"']*box-compartilhar)/i,
    /id=["']beneficio["'][^>]*>([\s\S]*?)(?:<script|<footer)/i
  ]

  for (const regex of regexes) {
    const m = html.match(regex)
    if (m && m[1]) {
      const txt = cleanText(m[1])
      if (txt.length >= 20) return txt.slice(0, 4000)
    }
  }

  return ""
}

function extractDetailImageFromDetail(html) {
  const matches = [...html.matchAll(/<img[^>]+(?:data-src|data-original|data-lazy|src)="([^"]+)"/gi)]
  for (const m of matches) {
    const src = absolutizeUrl(m[1] || "")
    if (src.includes("/beneficios/") || src.includes("/campanhasdeingresso/")) {
      return src
    }
  }
  return ""
}

async function fetchOfferDetailData(offer) {
  try {
    const html = await fetchText(offer.link, LIST_URL, 15)

    if (!html || html.trim().length < 1000) {
      return {
        ok: false,
        url: offer.link,
        title: offer.title,
        html_length: html ? html.length : 0,
        validity: "",
        description: "",
        detail_img_url: "",
        error: "html detalhe vazia ou curta"
      }
    }

    const detailTitle = extractTitleFromDetail(html) || offer.title
    const validity = extractValidityFromDetail(html)
    const description = extractDescriptionFromDetail(html)
    const detailImg = extractDetailImageFromDetail(html)

    return {
      ok: true,
      url: offer.link,
      title: detailTitle,
      html_length: html.length,
      validity,
      description,
      detail_img_url: detailImg,
      has_validity: !!validity,
      has_description: !!description,
      error: ""
    }
  } catch (e) {
    return {
      ok: false,
      url: offer.link,
      title: offer.title,
      html_length: 0,
      validity: "",
      description: "",
      detail_img_url: "",
      error: String(e)
    }
  }
}

async function main() {
  log("=".repeat(50))
  log("📦 scriptable snapshot + 2 detalhes com cache icloud")
  log("=".repeat(50))

  if (!GITHUB_TOKEN) {
    return "erro | token ausente"
  }

  const snapshotId = buildSnapshotId()
  const htmlPath = `snapshots/snapshot_${snapshotId}.html`
  const metaPath = `snapshots/snapshot_${snapshotId}.json`
  const detailMetaPath = `snapshots/detail_${snapshotId}.json`

  try {
    const seenCache = await loadSeenCache()
    const seenSet = new Set(seenCache.seen)

    log(`🧠 cache local carregado: ${seenCache.seen.length} links`)

    log("🌐 baixando html da vitrine...")
    const html = await fetchUolHtml()

    if (!html || html.trim().length < 1000) {
      return "erro | html vazia ou curta demais"
    }

    log(`✅ html baixada: ${html.length} chars`)

    const allOffers = extractOfferCards(html, 60)
    const newOffers = allOffers.filter(o => !seenSet.has(normalizeLink(o.link)))
    const offersToTest = newOffers.slice(0, MAX_DETAIL_FETCHES)

    log(`🔎 ofertas detectadas na vitrine: ${allOffers.length}`)
    log(`🆕 ofertas novas para detalhe: ${offersToTest.length}`)

    const meta = {
      snapshot_id: snapshotId,
      created_at: new Date().toISOString(),
      source_url: LIST_URL,
      html_path: htmlPath,
      html_length: html.length,
      total_offers_found: allOffers.length,
      total_new_offers_found: newOffers.length,
      tested_detail_count: offersToTest.length,
      cache_size_before: seenCache.seen.length,
      context: isShortcutContext() ? "shortcuts" : (config.runsInApp ? "app" : "unknown")
    }

    log("☁️ enviando html da vitrine para o github...")
    const htmlSave = await githubPutFile(
      htmlPath,
      html,
      `scriptable snapshot html ${snapshotId}`
    )

    if (!htmlSave.ok) {
      log(`❌ falha ao subir html: ${htmlSave.error}`)
      return `erro | falha html | ${snapshotId}`
    }

    log("☁️ enviando meta da vitrine para o github...")
    const metaSave = await githubPutFile(
      metaPath,
      JSON.stringify(meta, null, 2),
      `scriptable snapshot meta ${snapshotId}`
    )

    if (!metaSave.ok) {
      log(`❌ falha ao subir meta: ${metaSave.error}`)
      return `erro | html ok meta falhou | ${snapshotId}`
    }

    const detailResults = []
    let okCount = 0

    for (let i = 0; i < offersToTest.length; i++) {
      const offer = offersToTest[i]
      log(`🌐 detalhe ${i + 1}/${offersToTest.length}: ${offer.title}`)

      const detail = await fetchOfferDetailData(offer)

      if (detail.ok) {
        okCount += 1
        log(`✅ detalhe ok: ${detail.title} | validade=${detail.has_validity ? "sim" : "não"} | descrição=${detail.has_description ? "sim" : "não"}`)
      } else {
        log(`⚠️ detalhe falhou: ${detail.error}`)
      }

      detailResults.push({
        index: i + 1,
        link: offer.link,
        card_title: offer.title,
        category: offer.category,
        partner_name: offer.partner_name,
        partner_img_url: offer.partner_img_url,
        card_img_url: offer.img_url,
        detail_ok: detail.ok,
        detail_title: detail.title || "",
        detail_html_length: detail.html_length || 0,
        validity: detail.validity || "",
        has_validity: !!detail.validity,
        description_preview: (detail.description || "").slice(0, 500),
        has_description: !!detail.description,
        detail_img_url: detail.detail_img_url || "",
        error: detail.error || ""
      })
    }

    const mergedSeen = [...seenCache.seen]
    for (const offer of offersToTest) {
      mergedSeen.push(normalizeLink(offer.link))
    }
    saveSeenCache(mergedSeen)

    const finalCache = await loadSeenCache()
    log(`🧠 cache local salvo: ${finalCache.seen.length} links`)

    const detailMeta = {
      snapshot_id: snapshotId,
      tested_at: new Date().toISOString(),
      tested_count: offersToTest.length,
      detail_ok_count: okCount,
      detail_fail_count: offersToTest.length - okCount,
      cache_size_before: seenCache.seen.length,
      cache_size_after: finalCache.seen.length,
      offers: detailResults
    }

    log("☁️ enviando resumo dos detalhes para o github...")
    const detailMetaSave = await githubPutFile(
      detailMetaPath,
      JSON.stringify(detailMeta, null, 2),
      `scriptable detail meta ${snapshotId}`
    )

    if (!detailMetaSave.ok) {
      log(`⚠️ falha ao subir meta dos detalhes: ${detailMetaSave.error}`)
      return `ok | snapshot ${snapshotId} | vitrine ${html.length} | detalhes ${okCount}/${offersToTest.length} | cache ${finalCache.seen.length} | meta detalhes falhou`
    }

    log("✅ snapshot e detalhes enviados com sucesso")
    return `ok | snapshot ${snapshotId} | vitrine ${html.length} | detalhes ${okCount}/${offersToTest.length} | cache ${finalCache.seen.length}`
  } catch (e) {
    const msg = String(e && e.message ? e.message : e)
    log(`❌ erro geral: ${msg}`)
    return `erro | ${msg}`
  }
}

const output = await main()
console.log(`final output: ${output}`)
Script.setShortcutOutput(String(output || "ok"))
Script.complete()
