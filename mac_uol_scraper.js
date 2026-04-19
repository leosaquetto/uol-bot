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
const DEFAULT_MAX_CARDS = 60;
function parseMaxCards(value, fallback = DEFAULT_MAX_CARDS) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const i = Math.trunc(n);
  if (i < 1) return fallback;
  if (i > 200) return 200;
  return i;
}
const MAX_CARDS = parseMaxCards(process.env.MAX_CARDS, DEFAULT_MAX_CARDS);

const GITHUB_TOKEN = String(process.env.GITHUB_TOKEN || '').trim();
const GITHUB_REPO_OWNER = process.env.GITHUB_REPO_OWNER || 'leosaquetto';
const GITHUB_REPO_NAME = process.env.GITHUB_REPO_NAME || 'uol-bot';
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || 'main';
const GITHUB_TARGET_PATH = process.env.GITHUB_TARGET_PATH || 'snapshots/mac-uol-offers.json';
const REQUIRE_GITHUB_UPLOAD = String(process.env.REQUIRE_GITHUB_UPLOAD || '1') === '1';

const homeDir = os.homedir();
const icloudBase = path.join(homeDir, 'Library', 'Mobile Documents', 'com~apple~CloudDocs');
const outFile = process.env.OUT_FILE || path.join(icloudBase, 'Shortcuts', 'ClubeUol', 'mac-uol-offers.json');

function ensureDir(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function cleanText(s) {
  return String(s || '').replace(/\s+/g, ' ').trim();
}

function githubApiUrl(targetPath) {
  return `https://api.github.com/repos/${GITHUB_REPO_OWNER}/${GITHUB_REPO_NAME}/contents/${targetPath}`;
}

async function githubGetShaIfExists(targetPath) {
  const resp = await fetch(githubApiUrl(targetPath), {
    method: 'GET',
    headers: {
      Authorization: `token ${GITHUB_TOKEN}`,
      Accept: 'application/vnd.github+json',
      'User-Agent': 'mac-uol-scraper'
    }
  });

  if (resp.status === 404) return null;
  if (!resp.ok) {
    throw new Error(`github get ${resp.status} ${await resp.text()}`);
  }

  const data = await resp.json();
  return data.sha || null;
}

async function githubPutFile(targetPath, jsonText) {
  if (!GITHUB_TOKEN) {
    throw new Error('GITHUB_TOKEN ausente para upload no GitHub');
  }

  const sha = await githubGetShaIfExists(targetPath);
  const body = {
    message: `mac uol scraper snapshot ${new Date().toISOString()}`,
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
    throw new Error(`github put ${resp.status} ${await resp.text()}`);
  }
}

async function collectOffers(page) {
  await page.goto(TARGET_URL, { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForTimeout(2500);

  const cards = await page.$$eval('div.beneficio', (nodes, limit) => {
    const clean = (s) => String(s || '').replace(/\s+/g, ' ').trim();
    return nodes.slice(0, limit).map((node) => {
      const linkEl = node.querySelector('a[href]');
      const titleEl = node.querySelector('p.titulo');
      const category = node.getAttribute('data-categoria') || '';
      const imgLazy = node.querySelector('div.thumb.lazy');
      const partnerImg = node.querySelector('img[data-src*="/parceiros/"]');

      return {
        link: linkEl ? linkEl.href : '',
        title: clean(titleEl ? titleEl.textContent : ''),
        category: clean(category),
        img_url: imgLazy ? (imgLazy.getAttribute('data-src') || '') : '',
        partner_img_url: partnerImg ? (partnerImg.getAttribute('data-src') || '') : '',
        scraped_at: new Date().toISOString()
      };
    }).filter((x) => x.link && x.title);
  }, MAX_CARDS);

  return cards;
}

(async () => {
  let browser;
  try {
    browser = await chromium.launchPersistentContext(EDGE_PROFILE_DIR, {
      channel: 'msedge',
      headless: true,
      viewport: { width: 1280, height: 720 }
    });

    const page = browser.pages()[0] || await browser.newPage();
    const offers = await collectOffers(page);

    const payload = {
      ok: true,
      source: 'mac-playwright',
      target_url: TARGET_URL,
      total: offers.length,
      generated_at: new Date().toISOString(),
      host: os.hostname(),
      offers
    };

    const payloadText = JSON.stringify(payload, null, 2);

    ensureDir(outFile);
    fs.writeFileSync(outFile, payloadText, 'utf8');

    let githubUpload = 'skipped';
    if (GITHUB_TOKEN) {
      await githubPutFile(GITHUB_TARGET_PATH, payloadText);
      githubUpload = 'ok';
    } else if (REQUIRE_GITHUB_UPLOAD) {
      throw new Error('GITHUB_TOKEN ausente e REQUIRE_GITHUB_UPLOAD=1');
    }

    console.log(`MAC_OK total=${offers.length} out=${outFile} github=${githubUpload} repo_path=${GITHUB_TARGET_PATH}`);
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
