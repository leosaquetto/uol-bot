// Private child of instagram_browser.py. No credentials or page text are logged.
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

const TARGET = 'https://www.instagram.com/stories/clubeuol/';
const MAX_BYTES = 4 * 1024 * 1024;

export function hasTargetStoryData(text) {
  try {
    const pending = [JSON.parse(text)];
    let visited = 0;
    while (pending.length && ++visited <= 100000) {
      const node = pending.pop();
      if (!node || typeof node !== 'object') continue;
      if (node.user?.username === 'clubeuol' && Array.isArray(node.items)) return true;
      if (pending.length + Object.keys(node).length > 100000) return false;
      pending.push(...Object.values(node));
    }
  } catch {}
  return false;
}

export function installStoryObserver() {
  const state = { scripts: [], bytes: 0, count: 0, error: '' };
  Object.defineProperty(window, '__uolStoryCapture', { value: state, configurable: false });
  const relevant = value => {
    try {
      const url = new URL(value, location.href);
      return url.origin === 'https://www.instagram.com' &&
        /^\/(graphql\/query|api\/v1\/feed\/reels_media)\/?$/.test(url.pathname);
    } catch { return false; }
  };
  const keep = text => {
    if (typeof text !== 'string' || state.error) return;
    if (text.length > 4 * 1024 * 1024 || ++state.count > 16) { state.error = 'browser_body_limit'; return; }
    state.bytes += new TextEncoder().encode(text).length;
    if (state.bytes > 4 * 1024 * 1024) { state.error = 'browser_body_limit'; return; }
    // Instagram may prefix JSON responses with this anti-JSON-hijacking guard.
    // Keep the raw JSON text so large numeric Story IDs are never reserialized.
    text = text.replace(/^\s*for\s*\(\s*;\s*;\s*\)\s*;\s*/, '');
    if (/"(?:reels_media|xdt_api__v1__feed__reels_media)"\s*:/.test(text)) state.scripts.push(text);
  };
  const originalFetch = window.fetch;
  window.fetch = function (...args) {
    const result = originalFetch.apply(this, args);
    result.then(response => {
      if (!relevant(response.url) || response.status !== 200 || state.error) return;
      const clone = response.clone();
      (async () => {
        const reader = clone.body?.getReader();
        if (!reader) return;
        const decoder = new TextDecoder();
        let text = '', bytes = 0;
        try {
          while (true) {
            const part = await reader.read();
            if (part.done) break;
            bytes += part.value.byteLength;
            if (bytes + state.bytes > 4 * 1024 * 1024) {
              state.error = 'browser_body_limit';
              await reader.cancel();
              return;
            }
            text += decoder.decode(part.value, { stream: true });
          }
          keep(text + decoder.decode());
        } finally { reader.releaseLock(); }
      })().catch(() => {});
    }).catch(() => {});
    return result;
  };
  const send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function (...args) {
    this.addEventListener('load', () => {
      if (!relevant(this.responseURL) || this.status !== 200 || state.error) return;
      if (this.responseType === '' || this.responseType === 'text') keep(this.responseText);
    }, { once: true });
    return send.apply(this, args);
  };
}

export function targetPage(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && url.hostname === 'www.instagram.com' && !url.port &&
      !url.username && !url.password && /^\/stories\/clubeuol\/(?:\d+\/)?$/.test(url.pathname);
  } catch { return false; }
}

export function pageSnapshot() {
  const url = new URL(location.href);
  if (url.origin !== 'https://www.instagram.com' || !/^\/stories\/clubeuol\/(?:\d+\/)?$/.test(url.pathname))
    return { error: /^\/(accounts\/login|challenge|checkpoint|auth_platform)(\/|$)/.test(url.pathname) ? 'login_page' : 'unexpected_page' };
  if (document.querySelector('form[action^="/accounts/login"], input[name="password"]')) return { error: 'login_page' };
  let bytes = 0;
  const scripts = [];
  for (const node of document.querySelectorAll('script[type="application/json"]')) {
    const value = node.textContent || '';
    bytes += new TextEncoder().encode(value).length;
    if (bytes > 4 * 1024 * 1024 || scripts.length >= 512) return { error: 'browser_body_limit' };
    scripts.push(value);
  }
  const observed = window.__uolStoryCapture;
  if (observed?.error) return { error: 'browser_body_limit' };
  for (const value of observed?.scripts || []) {
    if (typeof value !== 'string') continue;
    bytes += new TextEncoder().encode(value).length;
    if (bytes > 4 * 1024 * 1024 || scripts.length >= 512) return { error: 'browser_body_limit' };
    scripts.push(value);
  }
  // Data availability is tested without parsing large numeric IDs in JavaScript.
  const hasTargetData = scripts.some(value => {
    try {
      const pending = [JSON.parse(value)];
      let visited = 0;
      while (pending.length && ++visited < 100000) {
        const node = pending.pop();
        if (!node || typeof node !== 'object') continue;
        if (node.user?.username === 'clubeuol' && Array.isArray(node.items)) return true;
        if (pending.length + Object.keys(node).length > 100000) return false;
        pending.push(...Object.values(node));
      }
    } catch {}
    return false;
  });
  return { scripts, hasTargetData };
}

export function clickStoryGate() {
  const url = new URL(location.href);
  if (url.origin !== 'https://www.instagram.com' || !/^\/stories\/clubeuol\/(?:\d+\/)?$/.test(url.pathname)) return 'unexpected_page';
  if (document.querySelector('form[action^="/accounts/login"], input[name="password"]')) return 'login_page';
  const matches = [...document.querySelectorAll('button, [role="button"]')].filter(node => {
    if (!['ver story', 'view story'].includes((node.innerText || node.textContent || '').trim().toLowerCase())) return false;
    const rect = node.getBoundingClientRect(), style = getComputedStyle(node);
    return !node.disabled && node.getAttribute('aria-disabled') !== 'true' && rect.width > 0 && rect.height > 0 &&
      style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
  });
  if (matches.length !== 1) return matches.length ? 'story_gate_ambiguous' : 'story_gate_not_found';
  matches[0].click();
  return 'clicked';
}

function retryAfter(value) {
  if (/^\d+$/.test(value || '')) return Math.min(Number(value), 365 * 86400);
  const date = Date.parse(value || '');
  return Number.isFinite(date) ? Math.min(365 * 86400, Math.max(0, Math.ceil((date - Date.now()) / 1000))) : 3600;
}

export async function collectBrowser(input, chromium) {
  let browser, context, requests = 0, gateClicked = false, terminal, watchdog;
  const outcome = (status, reason, extra = {}) => ({ status, reason, requests, gateClicked, ...extra });
  try {
    browser = await chromium.launch({ headless: true, chromiumSandbox: true,
      executablePath: process.env.UOL_INSTAGRAM_CHROMIUM_PATH || '/usr/bin/google-chrome',
      args: ['--disable-dev-shm-usage'] });
    watchdog = setTimeout(() => {
      terminal = outcome('unknown', 'browser_timeout');
      browser.close().catch(() => {});
    }, 35000);
    context = await browser.newContext({ userAgent: input.userAgent, locale: 'pt-BR',
      viewport: { width: 1280, height: 900 }, serviceWorkers: 'block' });
    await context.addInitScript(installStoryObserver);
    await context.addCookies(input.cookies.map(cookie => ({ name: cookie.name, value: cookie.value,
      domain: cookie.domain, path: cookie.path, secure: cookie.secure, httpOnly: cookie.httpOnly,
      ...(cookie.expires ? { expires: cookie.expires } : {}) })));
    const page = await context.newPage();
    await context.route('**/*', route => {
      const request = route.request(), url = new URL(request.url());
      const allowed = url.protocol === 'https:' && /(^|\.)(instagram\.com|cdninstagram\.com|fbcdn\.net)$/.test(url.hostname);
      if (terminal || !allowed || ['image', 'media', 'font'].includes(request.resourceType())) return route.abort();
      return route.continue();
    });
    page.on('response', response => {
      const url = new URL(response.url()), type = response.request().resourceType();
      const source = url.hostname === 'www.instagram.com' &&
        (type === 'document' || /^\/(graphql\/query|api\/v1\/feed\/reels_media)\/?$/.test(url.pathname));
      if (!source || terminal) return;
      if (response.status() === 429) {
        terminal = outcome('rate_limited', 'http_429', { retryAfterSeconds: retryAfter(response.headers()['retry-after']) });
        return;
      }
      if ([401, 403].includes(response.status())) {
        terminal = outcome('auth_required', 'http_auth_required');
        return;
      }
    });
    requests = 1;
    const navigation = await page.goto(TARGET, { waitUntil: 'domcontentloaded', timeout: 20000 });
    if (terminal) return terminal;
    if (navigation && navigation.status() !== 200) return outcome('unknown', 'http_status');
    if (!targetPage(page.url())) return outcome('auth_required', 'login_page');
    await page.waitForTimeout(700);
    if (terminal) return terminal;
    let snapshot = await page.evaluate(pageSnapshot);
    if (snapshot.error) return outcome(snapshot.error === 'login_page' ? 'auth_required' : 'unknown', snapshot.error);
    if (!snapshot.hasTargetData) {
      if (terminal) return terminal;
      const clicked = await page.evaluate(clickStoryGate);
      if (clicked === 'login_page') return outcome('auth_required', clicked);
      if (clicked === 'story_gate_ambiguous' || clicked === 'unexpected_page') return outcome('unknown', clicked);
      if (clicked === 'clicked') {
        gateClicked = true;
        requests = 2;
      }
      // Wait for the single page/click already issued; never issue a new query.
      for (let attempt = 0; attempt < 12 && !snapshot.hasTargetData; attempt++) {
        await page.waitForTimeout(500);
        if (terminal) return { ...terminal, requests, gateClicked };
        snapshot = await page.evaluate(pageSnapshot);
        if (snapshot.error) return outcome(snapshot.error === 'login_page' ? 'auth_required' : 'unknown', snapshot.error);
      }
    }
    if (terminal) return terminal;
    const scripts = snapshot.scripts;
    if (scripts.length > 512 || scripts.reduce((sum, text) => sum + Buffer.byteLength(text), 0) > MAX_BYTES)
      return outcome('unknown', 'browser_body_limit');
    return outcome('loaded', '', { scripts });
  } catch (error) {
    if (terminal) return terminal;
    return outcome('unknown', error?.name === 'TimeoutError' ? 'browser_timeout' : 'browser_failed');
  } finally {
    clearTimeout(watchdog);
    // Closing the browser closes its contexts too. A stalled context teardown
    // must not prevent browser termination; Python retains the hard deadline.
    await browser?.close().catch(() => {});
  }
}

async function main() {
  let raw = '';
  for await (const chunk of process.stdin) {
    raw += chunk;
    if (Buffer.byteLength(raw) > 256 * 1024) throw new Error('input_limit');
  }
  const input = JSON.parse(raw);
  if (typeof input.userAgent !== 'string' || !Array.isArray(input.cookies)) throw new Error('invalid_input');
  let chromium;
  try {
    const require = createRequire(new URL('../saquetto-drops/package.json', import.meta.url));
    ({ chromium } = require('playwright-core'));
  } catch {
    process.stdout.write(JSON.stringify({ status: 'unknown', reason: 'browser_unavailable', requests: 0 }));
    return;
  }
  process.stdout.write(JSON.stringify(await collectBrowser(input, chromium)));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(() => {
    process.stdout.write(JSON.stringify({ status: 'unknown', reason: 'browser_failed', requests: 0 }));
  });
}
