#!/usr/bin/env node

/**
 * Scraper de fallback para rodar no Mac via SSH (atalho iOS).
 *
 * Fluxo esperado no iOS:
 * 1) Executa este script via SSH.
 * 2) Se stdout contiver "MAC_OK", encerra o atalho (Mac assume o trabalho).
 * 3) Se der erro/timeout/sem MAC_OK, continua para o fluxo Scriptable no iOS.
 *
 * Observação de arquitetura: sem concorrência entre vias.
 * - MAC_OK => iOS para
 * - MAC_FAIL/erro/timeout => iOS segue fluxo dividido
 */

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const os = require('os');

const TARGET_URL = process.env.UOL_TARGET_URL || 'https://clube.uol.com.br/?order=new';
const EDGE_PROFILE_DIR = process.env.EDGE_PROFILE_DIR || '/Users/leosaquetto/Documents/GrabNumberAutomator/edge-profile';
const DEFAULT_MAX_CARDS = 48;
const MAX_VISIBLE_OFFERS = 48;
const DEFAULT_SOLD_OUT_LOOKBACK_DAYS = 3;
const DEFAULT_SOLD_OUT_MIN_MISSES = 2;
const DEFAULT_DETAIL_PAGE_TIMEOUT_MS = 12000;

function parseMaxCards(value, fallback = DEFAULT_MAX_CARDS) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const i = Math.trunc(n);
  if (i < 1) return fallback;
  if (i > MAX_VISIBLE_OFFERS) return MAX_VISIBLE_OFFERS;
  return i;
}

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const i = Math.trunc(n);
  if (i < 1) return fallback;
  return i;
}

const MAX_CARDS = parseMaxCards(process.env.MAX_CARDS, DEFAULT_MAX_CARDS);
const SOLD_OUT_LOOKBACK_DAYS = parsePositiveInt(process.env.SOLD_OUT_LOOKBACK_DAYS, DEFAULT_SOLD_OUT_LOOKBACK_DAYS);
const SOLD_OUT_MIN_MISSES = parsePositiveInt(process.env.SOLD_OUT_MIN_MISSES, DEFAULT_SOLD_OUT_MIN_MISSES);
const DETAIL_PAGE_TIMEOUT_MS = parsePositiveInt(process.env.DETAIL_PAGE_TIMEOUT_MS, DEFAULT_DETAIL_PAGE_TIMEOUT_MS);

const GITHUB_TOKEN = String(process.env.GITHUB_TOKEN || '').trim();
const GITHUB_REPO_OWNER = process.env.GITHUB_REPO_OWNER || 'leosaquetto';
const GITHUB_REPO_NAME = process.env.GITHUB_REPO_NAME || 'uol-bot';
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || 'main';
const GITHUB_TARGET_PATH = process.env.GITHUB_TARGET_PATH || 'snapshots/mac-uol-offers.json';
const GITHUB_LATEST_OFFERS_PATH = process.env.GITHUB_LATEST_OFFERS_PATH || 'latest_offers.json';
const GITHUB_SOLD_OUT_UPDATES_PATH = process.env.GITHUB_SOLD_OUT_UPDATES_PATH || 'sold_out_updates.json';
const REQUIRE_GITHUB_UPLOAD = String(process.env.REQUIRE_GITHUB_UPLOAD || '1') === '1';

const homeDir = os.homedir();
const icloudBase = path.join(homeDir, 'Library', 'Mobile Documents', 'com~apple~CloudDocs');
const outFile = process.env.OUT_FILE || path.join(icloudBase, 'Shortcuts', 'ClubeUol', 'mac-uol-offers.json');
const stateFile = process.env.MAC_SOLD_OUT_STATE_FILE || path.join(path.dirname(outFile), 'mac_sold_out_state.json');

function ensureDir(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function cleanText(s) {
  return String(s || '').replace(/\s+/g, ' ').trim();
}

function normalizeLink(url) {
  return String(url || '').trim();
}

function normalizeOfferKey(value) {
  const raw = normalizeLink(value);
  if (!raw) return '';
  const tail = raw.startsWith('http://') || raw.startsWith('https://')
    ? raw.split('?')[0].replace(/\/$/, '').split('/').pop()
    : raw;
  return String(tail || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function pad(n) {
  return String(n).padStart(2, '0');
}

function brDate(date = new Date()) {
  return `${pad(date.getDate())}/${pad(date.getMonth() + 1)}/${date.getFullYear()}`;
}

function brTime(date = new Date()) {
  return `${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function toISOStringSafe(value) {
  if (!value) return '';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '';
  return d.toISOString();
}

function isWithinLookback(offer, now, lookbackDays) {
  const sentAtCandidates = [
    offer.sent_at,
    offer.channel_sent_at,
    offer.created_at,
    offer.posted_at,
    offer.published_at,
    offer.timestamp,
  ];

  let sentAt = '';
  for (const candidate of sentAtCandidates) {
    const iso = toISOStringSafe(candidate);
    if (iso) {
      sentAt = iso;
      break;
    }
  }
  if (!sentAt) return false;

  const sentDate = new Date(sentAt);
  const ageMs = now.getTime() - sentDate.getTime();
  if (ageMs < 0) return false;

  return ageMs <= lookbackDays * 24 * 60 * 60 * 1000;
}

function isEligibleForSoldOut(offer, now, lookbackDays) {
  if (!offer || typeof offer !== 'object') return false;
  if (!normalizeLink(offer.link)) return false;
  if (offer.sold_out_at) return false;
  if (!offer.channel_message_id) return false;
  return isWithinLookback(offer, now, lookbackDays);
}

function githubApiUrl(targetPath) {
  return `https://api.github.com/repos/${GITHUB_REPO_OWNER}/${GITHUB_REPO_NAME}/contents/${targetPath}`;
}

async function githubGetContent(targetPath) {
  if (!GITHUB_TOKEN) {
    throw new Error('GITHUB_TOKEN ausente para leitura no GitHub');
  }

  const resp = await fetch(githubApiUrl(targetPath), {
    method: 'GET',
    headers: {
      Authorization: `token ${GITHUB_TOKEN}`,
      Accept: 'application/vnd.github+json',
      'User-Agent': 'mac-uol-scraper'
    }
  });

  if (resp.status === 404) return { exists: false, sha: null, data: null };
  if (!resp.ok) {
    throw new Error(`github get ${targetPath} ${resp.status} ${await resp.text()}`);
  }

  const json = await resp.json();
  if (!json || !json.content) return { exists: true, sha: json?.sha || null, data: null };

  const raw = Buffer.from(String(json.content).replace(/\n/g, ''), 'base64').toString('utf8');
  return { exists: true, sha: json.sha || null, data: JSON.parse(raw) };
}

async function githubGetShaIfExists(targetPath) {
  const content = await githubGetContent(targetPath);
  return content.sha || null;
}

async function githubPutFile(targetPath, jsonText) {
  if (!GITHUB_TOKEN) {
    throw new Error('GITHUB_TOKEN ausente para upload no GitHub');
  }

  const sha = await githubGetShaIfExists(targetPath);
  const body = {
    message: `mac uol scraper update ${targetPath} ${new Date().toISOString()}`,
    content: Buffer.from(jsonText, 'utf8').toString('base64'),
    branch: GITHUB_BRANCH
  };

  if (sha) body.sha = sha;

  const resp = await fetch(githubApiUrl(targetPath), {
    method: 'PUT',
    headers: {
      Authorization: `token ${GITHUB_TOKEN}`,
      Accept: 'application/vnd.github+json',
      'Content-Type': 'application/json',
      'User-Agent': 'mac-uol-scraper'
    },
    body: JSON.stringify(body)
  });

  if (!resp.ok) {
    throw new Error(`github put ${targetPath} ${resp.status} ${await resp.text()}`);
  }
}

function loadLocalSoldOutState() {
  try {
    const raw = fs.readFileSync(stateFile, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return { links: {} };
    const links = parsed.links && typeof parsed.links === 'object' ? parsed.links : {};
    return { links };
  } catch (_) {
    return { links: {} };
  }
}

function saveLocalSoldOutState(state) {
  const payload = {
    updated_at: new Date().toISOString(),
    lookback_days: SOLD_OUT_LOOKBACK_DAYS,
    min_misses: SOLD_OUT_MIN_MISSES,
    links: state && state.links && typeof state.links === 'object' ? state.links : {}
  };
  ensureDir(stateFile);
  fs.writeFileSync(stateFile, JSON.stringify(payload, null, 2), 'utf8');
}

function buildSoldOutUpdates({ activeLinksSet, latestOffers, previousState, now }) {
  const nextState = { links: {} };
  const soldOutDetected = [];

  for (const offer of latestOffers) {
    if (!isEligibleForSoldOut(offer, now, SOLD_OUT_LOOKBACK_DAYS)) continue;

    const link = normalizeLink(offer.link);
    if (!link) continue;

    const prev = previousState.links[link] || {};
    const isMissing = !activeLinksSet.has(link);

    if (!isMissing) continue;

    const absenceCount = Number(prev.absence_count || 0) + 1;
    const firstMissingAt = prev.first_missing_at || now.toISOString();

    nextState.links[link] = {
      absence_count: absenceCount,
      first_missing_at: firstMissingAt,
      last_checked_at: now.toISOString(),
    };

    if (absenceCount >= SOLD_OUT_MIN_MISSES) {
      soldOutDetected.push({
        link,
        sold_out_at: brTime(now),
        date: brDate(now),
      });
      delete nextState.links[link];
    }
  }

  return { nextState, soldOutDetected };
}

function mergeSoldOutUpdates(existingPayload, newUpdates) {
  const base = existingPayload && typeof existingPayload === 'object'
    ? existingPayload
    : { updated_at: '', updates: [] };

  const merged = Array.isArray(base.updates) ? [...base.updates] : [];
  const known = new Set(
    merged
      .filter((x) => x && typeof x === 'object')
      .map((x) => `${normalizeLink(x.link)}|${String(x.date || '').trim()}`)
  );

  let added = 0;
  for (const item of newUpdates) {
    const key = `${normalizeLink(item.link)}|${String(item.date || '').trim()}`;
    if (!item.link || !item.date || known.has(key)) continue;
    known.add(key);
    merged.push({
      link: normalizeLink(item.link),
      sold_out_at: String(item.sold_out_at || '').trim(),
      date: String(item.date || '').trim(),
    });
    added += 1;
  }

  return {
    payload: {
      updated_at: new Date().toISOString(),
      updates: merged,
    },
    added,
  };
}

function isBadOfferImageUrl(url) {
  const src = String(url || '').toLowerCase();
  if (!src) return true;
  return (
    src.includes('/parceiros/') ||
    src.includes('loader.gif') ||
    src.includes('/static/images/clubes/uol/categorias/') ||
    src.includes('ingressosexclusivos-hover') ||
    src.includes('ingressos-hover') ||
    src.includes('icone') ||
    src.includes('icon-')
  );
}

function evaluateDetailQuality(detail) {
  if (!detail || !detail.ok) return 'failed';
  const hasTitle = cleanText(detail.title || '').length >= 4;
  const hasValidity = cleanText(detail.validity || '').length >= 8;
  const hasDescription = cleanText(detail.description || '').length >= 60;
  const hasImage = !!normalizeLink(detail.detail_img_url || '');

  if (hasTitle && hasValidity && hasDescription && hasImage) return 'complete';
  if ((hasValidity && hasDescription) || (hasDescription && hasImage) || (hasValidity && hasImage)) return 'partial';
  if (hasTitle || hasDescription || hasImage || hasValidity) return 'weak';
  return 'failed';
}

function shouldRetryDetail(errorText) {
  const text = String(errorText || '');
  return /(timeout|navigation|net::|ERR_|Target closed|Execution context was destroyed)/i.test(text);
}

async function collectOfferCards(page) {
  await page.goto(TARGET_URL, { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForTimeout(2000);

  const cards = await page.$$eval('div.beneficio', (nodes, limit) => {
    const clean = (s) => String(s || '').replace(/\s+/g, ' ').trim();
    return nodes.slice(0, limit).map((node) => {
      const linkEl = node.querySelector('a[href]');
      const titleEl = node.querySelector('p.titulo');
      const category = node.getAttribute('data-categoria') || '';
      const imgLazy = node.querySelector('div.thumb.lazy');
      const partnerImg = node.querySelector('img[data-src*="/parceiros/"]');
      const partnerName = node.querySelector('img[title]');

      return {
        link: linkEl ? linkEl.href : '',
        title: clean(titleEl ? titleEl.textContent : ''),
        category: clean(category),
        img_url: imgLazy ? (imgLazy.getAttribute('data-src') || '') : '',
        partner_img_url: partnerImg ? (partnerImg.getAttribute('data-src') || '') : '',
        partner_name: clean(partnerName ? partnerName.getAttribute('title') : ''),
        scraped_at: new Date().toISOString()
      };
    }).filter((x) => x.link && x.title);
  }, MAX_CARDS);

  return cards;
}

async function fetchOfferDetailData(page, offer) {
  const startedAt = Date.now();
  try {
    await page.goto(offer.link, { waitUntil: 'domcontentloaded', timeout: DETAIL_PAGE_TIMEOUT_MS });
    await page.waitForTimeout(700);

    const detail = await page.evaluate(() => {
      const clean = (s) => String(s || '').replace(/\s+/g, ' ').trim();
      const bySel = (sel) => {
        const el = document.querySelector(sel);
        return clean(el ? el.textContent : '');
      };

      const title =
        bySel('h2') ||
        bySel('h1') ||
        bySel('.titulo') ||
        clean(document.title || '');

      const bodyText = clean(document.body ? document.body.innerText : '');
      let validity = '';
      const validityPatterns = [
        /[Bb]enefício válido de[^.!?\n]*[.!?]?/,
        /[Vv]álido até[^.!?\n]*[.!?]?/,
        /\d{2}\/\d{2}\/\d{4}[\s\S]{0,80}\d{2}\/\d{2}\/\d{4}/
      ];
      for (const pattern of validityPatterns) {
        const m = bodyText.match(pattern);
        if (m && m[0]) {
          validity = clean(m[0]);
          break;
        }
      }

      const descriptionSelectors = [
        '.info-beneficio',
        '#beneficio .info-beneficio',
        '#beneficio',
        '.descricao-beneficio',
        '.box-descricao-beneficio',
        '.descricao',
        '.content-beneficio',
        'section.beneficio',
        'main'
      ];
      let description = '';
      for (const sel of descriptionSelectors) {
        const txt = bySel(sel);
        if (txt && txt.length >= 20) {
          description = txt.slice(0, 5000);
          break;
        }
      }

      const metaOg = document.querySelector('meta[property="og:image"]')?.getAttribute('content') || '';
      const metaOgSecure = document.querySelector('meta[property="og:image:secure_url"]')?.getAttribute('content') || '';
      const metaTw = document.querySelector('meta[name="twitter:image"]')?.getAttribute('content') || '';

      const imageCandidates = [];
      if (metaOgSecure) imageCandidates.push({ src: metaOgSecure, source: 'meta_og_secure' });
      if (metaOg) imageCandidates.push({ src: metaOg, source: 'meta_og' });
      if (metaTw) imageCandidates.push({ src: metaTw, source: 'meta_twitter' });

      const bgThumb = Array.from(document.querySelectorAll('[data-src*="/beneficios/"], [style*="/beneficios/"]'));
      for (const node of bgThumb) {
        const src = node.getAttribute('data-src') || '';
        if (src) imageCandidates.push({ src, source: 'benefit_data_src' });

        const style = node.getAttribute('style') || '';
        const match = style.match(/url\((['"]?)(.*?)\1\)/i);
        if (match && match[2]) imageCandidates.push({ src: match[2], source: 'benefit_style_url' });
      }

      const imgs = Array.from(document.querySelectorAll('img'));
      for (const img of imgs) {
        const src = img.getAttribute('data-src') || img.getAttribute('data-original') || img.getAttribute('src') || '';
        if (!src) continue;
        imageCandidates.push({ src, source: 'img' });
      }

      return {
        title,
        validity,
        description,
        imageCandidates,
        html_length: (document.documentElement?.outerHTML || '').length,
      };
    });

    let detailImgUrl = '';
    let detailImgSource = 'none';

    for (const candidate of detail.imageCandidates || []) {
      const src = candidate && candidate.src ? new URL(candidate.src, offer.link).href : '';
      if (!src || isBadOfferImageUrl(src)) continue;
      if (src.includes('/beneficios/') || src.includes('cloudfront') || src.includes('/campanhas')) {
        detailImgUrl = src;
        detailImgSource = candidate.source || 'img';
        break;
      }
    }

    if (!detailImgUrl) {
      for (const candidate of detail.imageCandidates || []) {
        const src = candidate && candidate.src ? new URL(candidate.src, offer.link).href : '';
        if (!src || isBadOfferImageUrl(src)) continue;
        detailImgUrl = src;
        detailImgSource = candidate.source || 'img_fallback';
        break;
      }
    }

    return {
      ok: true,
      title: cleanText(detail.title) || offer.title,
      validity: cleanText(detail.validity),
      description: cleanText(detail.description),
      detail_img_url: detailImgUrl,
      detail_img_source: detailImgSource,
      html_length: Number(detail.html_length || 0),
      elapsed_ms: Date.now() - startedAt,
      error: '',
    };
  } catch (err) {
    return {
      ok: false,
      title: offer.title,
      validity: '',
      description: '',
      detail_img_url: '',
      detail_img_source: 'error',
      html_length: 0,
      elapsed_ms: Date.now() - startedAt,
      error: cleanText(err && err.message ? err.message : String(err)),
    };
  }
}

async function enrichOffers(context, cards) {
  const detailPage = await context.newPage();
  const enriched = [];
  let detailOkCount = 0;
  const qualityCounts = { complete: 0, partial: 0, weak: 0, failed: 0 };

  for (let i = 0; i < cards.length; i++) {
    const card = cards[i];
    let attempts = 1;
    let detail = await fetchOfferDetailData(detailPage, card);
    if (!detail.ok && shouldRetryDetail(detail.error)) {
      attempts = 2;
      await detailPage.waitForTimeout(250);
      const retryDetail = await fetchOfferDetailData(detailPage, card);
      if (retryDetail.ok || !detail.ok) detail = retryDetail;
    }
    if (detail.ok) detailOkCount += 1;
    const detailQuality = evaluateDetailQuality(detail);
    qualityCounts[detailQuality] = Number(qualityCounts[detailQuality] || 0) + 1;

    enriched.push({
      id: normalizeOfferKey(card.link),
      link: normalizeLink(card.link),
      original_link: normalizeLink(card.link),
      title: (detail.title || card.title || 'Oferta').trim(),
      preview_title: (card.title || detail.title || 'Oferta').trim(),
      validity: (detail.validity || '').trim(),
      description: (detail.description || '').trim(),
      category: card.category || '',
      partner_name: card.partner_name || '',
      partner_img_url: card.partner_img_url || '',
      img_url: detail.detail_img_url || card.img_url || '',
      card_img_url: card.img_url || '',
      detail_img_url: detail.detail_img_url || '',
      img_source: detail.detail_img_source || (card.img_url ? 'card_img' : 'none'),
      detail_ok: !!detail.ok,
      detail_quality: detailQuality,
      detail_error: detail.error || '',
      detail_attempts: attempts,
      detail_html_length: Number(detail.html_length || 0),
      detail_elapsed_ms: Number(detail.elapsed_ms || 0),
      scraped_at: new Date().toISOString(),
    });
  }

  await detailPage.close();
  return { enriched, detailOkCount, qualityCounts };
}

(async () => {
  const runStartedAt = Date.now();
  let browser;
  try {
    browser = await chromium.launchPersistentContext(EDGE_PROFILE_DIR, {
      channel: 'msedge',
      headless: true,
      viewport: { width: 1280, height: 720 }
    });

    const page = browser.pages()[0] || await browser.newPage();
    const cards = await collectOfferCards(page);
    const enrichment = await enrichOffers(browser, cards);
    const offers = enrichment.enriched;
    const activeLinksSet = new Set(offers.map((o) => normalizeLink(o.link)).filter(Boolean));

    const runDurationMs = Date.now() - runStartedAt;
    const detailFailCount = offers.length - enrichment.detailOkCount;
    const payload = {
      ok: true,
      source: 'mac-playwright',
      target_url: TARGET_URL,
      max_cards_per_round: MAX_CARDS,
      detail_page_timeout_ms: DETAIL_PAGE_TIMEOUT_MS,
      collected_cards_count: cards.length,
      enriched_offers_count: offers.length,
      detail_ok_count: enrichment.detailOkCount,
      detail_fail_count: detailFailCount,
      detail_quality_counts: enrichment.qualityCounts,
      run_duration_ms: runDurationMs,
      run_duration_seconds: Number((runDurationMs / 1000).toFixed(2)),
      avg_detail_ms_per_offer: offers.length > 0 ? Math.round(runDurationMs / offers.length) : 0,
      generated_at: new Date().toISOString(),
      host: os.hostname(),
      offers,
    };

    const payloadText = JSON.stringify(payload, null, 2);

    ensureDir(outFile);
    fs.writeFileSync(outFile, payloadText, 'utf8');

    let githubUpload = 'skipped';
    let soldOutUpload = 'skipped';
    let soldOutAdded = 0;

    if (GITHUB_TOKEN) {
      await githubPutFile(GITHUB_TARGET_PATH, payloadText);
      githubUpload = 'ok';

      const [latestResp, soldOutResp] = await Promise.all([
        githubGetContent(GITHUB_LATEST_OFFERS_PATH),
        githubGetContent(GITHUB_SOLD_OUT_UPDATES_PATH),
      ]);

      const latestOffers = Array.isArray(latestResp?.data?.offers) ? latestResp.data.offers : [];
      const previousState = loadLocalSoldOutState();
      const now = new Date();
      const detection = buildSoldOutUpdates({
        activeLinksSet,
        latestOffers,
        previousState,
        now,
      });

      saveLocalSoldOutState(detection.nextState);

      if (detection.soldOutDetected.length > 0) {
        const merged = mergeSoldOutUpdates(soldOutResp.data, detection.soldOutDetected);
        if (merged.added > 0) {
          await githubPutFile(GITHUB_SOLD_OUT_UPDATES_PATH, JSON.stringify(merged.payload, null, 2));
          soldOutAdded = merged.added;
          soldOutUpload = 'ok';
        } else {
          soldOutUpload = 'noop';
        }
      } else {
        soldOutUpload = 'noop';
      }
    } else if (REQUIRE_GITHUB_UPLOAD) {
      throw new Error('GITHUB_TOKEN ausente e REQUIRE_GITHUB_UPLOAD=1');
    }

    console.log(`MAC_OK cards=${cards.length} enriched=${offers.length} detail_ok=${enrichment.detailOkCount} detail_fail=${detailFailCount} duration_ms=${runDurationMs} out=${outFile} github=${githubUpload} sold_out=${soldOutUpload} sold_out_added=${soldOutAdded} repo_path=${GITHUB_TARGET_PATH}`);
    process.exit(0);
  } catch (err) {
    console.error(`MAC_FAIL ${cleanText(err && err.message ? err.message : String(err))}`);
    process.exit(2);
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch (_) {}
    }
  }
})();
