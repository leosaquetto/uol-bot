// Own Instagram WebView session. No device token, cookies or viewer data cross into/out of the page.
const HunterInstagramBrowser = (() => {
  const ORIGIN = 'https://www.instagram.com';
  const STORY_PATH = '/stories/clubeuol/';
  const fail = (reason, status = 'unknown', extra = {}) => Object.assign(new Error(reason), { status, reason, ...extra });

  // Read only data loaded by the official Stories viewer. Temporary response hooks
  // observe gate hydration; they issue no requests and export only scoped Stories.
  async function pageCapture(parseJSON, waitMs = 10000) {
    const done = value => completion(JSON.stringify(value));
    const invalid = reason => { throw new Error(reason); };
    const scalar = v => typeof v === 'string' && v.length <= 8192 || typeof v === 'number' && Number.isFinite(v) || typeof v === 'boolean' || v === null;
    const fields = (value, names) => {
      if (!value || typeof value !== 'object' || Array.isArray(value)) invalid('browser_story_data_invalid');
      const output = {};
      for (const name of names) if (name in value) { if (!scalar(value[name])) invalid('browser_story_data_invalid'); output[name] = value[name]; }
      return output;
    };
    const owner = value => fields(value, ['id', 'pk', 'username']);
    const array = (value, map) => { if (!Array.isArray(value) || value.length > 100) invalid('structure_limit'); return value.map(map); };
    const item = value => {
      const out = fields(value, ['id','pk','taken_at','expiring_at','original_width','original_height']);
      for (const key of ['user','owner']) if (key in value) out[key] = owner(value[key]);
      out.image_versions2 = { candidates: array(value.image_versions2?.candidates, c => fields(c, ['width','height','url'])) };
      out.story_link_stickers = array(value.story_link_stickers || [], s => ({ story_link: fields(s.story_link || {}, ['url']) }));
      return out;
    };
    const reel = value => ({ ...fields(value, ['id']), user: owner(value.user), items: array(value.items, item) });
    const started = Date.now();
    let gateClicked = false, active = true, observed = false, networkBytes = 0, responses = 0;
    let networkError = null;
    const networkReels = [], restore = [], readers = new Set(), listeners = new Set();
    const byteLength = text => unescape(encodeURIComponent(text)).length;
    const project = (text, direct = false) => {
      if (!text.includes('xdt_api__v1__feed__reels_media') && !(direct && text.includes('reels_media'))) return [];
      let value; try { value = parseJSON(text.replace(/^for \(;;\);\s*/, '')); } catch (_) { invalid('browser_story_data_invalid'); }
      const reels = [];
      let nodes = 0;
      function add(payload) {
        if (!payload || !Array.isArray(payload.reels_media) || payload.reels_media.length > 100) invalid('browser_story_data_invalid');
        for (const entry of payload.reels_media) if (entry?.user?.username === 'clubeuol') reels.push(reel(entry));
        if (reels.length > 100) invalid('structure_limit');
      }
      function walk(entry, depth) {
        if (++nodes > 100000 || depth > 50) invalid('structure_limit');
        if (!entry || typeof entry !== 'object') return;
        if (Object.prototype.hasOwnProperty.call(entry, 'xdt_api__v1__feed__reels_media')) add(entry.xdt_api__v1__feed__reels_media);
        for (const child of Object.values(entry)) walk(child, depth + 1);
      }
      if (direct && value && Object.prototype.hasOwnProperty.call(value, 'reels_media')) add(value);
      walk(value, 0);
      return reels;
    };
    const safeError = error => ({ error: ['browser_story_data_invalid','structure_limit','body_limit'].includes(error?.message) ? error.message : 'browser_script_error' });
    const endpoint = value => {
      try {
        const url = new URL(typeof value === 'string' ? value : value?.url, location.origin);
        if (url.origin !== 'https://www.instagram.com') return null;
        return /^\/graphql\/query\/?$/.test(url.pathname) ? 'graphql' : /^\/api\/v1\/feed\/reels_media\/?$/.test(url.pathname) ? 'reels' : null;
      } catch (_) { return null; }
    };
    function statusFailure(code, retryHeader) {
      if (code === 429) {
        let retryAfter = 0;
        if (typeof retryHeader === 'string' && retryHeader.length <= 128) {
          const value = retryHeader.trim();
          const delay = /^\d+(?:\.\d+)?$/.test(value) ? Number(value) * 1000 : Date.parse(value) - Date.now();
          if (Number.isFinite(delay) && delay > 0 && delay <= Number.MAX_SAFE_INTEGER) retryAfter = delay;
        }
        return { error: 'http_429', statusCode: 429, ...(retryAfter ? { retryAfter } : {}) };
      }
      if (code === 401 || code === 403) return { error: 'http_auth_required', statusCode: code };
      if (Number.isInteger(code) && code >= 400 && code <= 599) return { error: 'http_status', statusCode: code };
      return null;
    }
    function acceptText(text, direct) {
      if (!active || networkError) return;
      networkReels.push(...project(text, direct));
      if (networkReels.length > 100) invalid('structure_limit');
    }
    function reserveResponse() {
      if (!active || networkError) return false;
      if (++responses > 16) { networkError = { error: 'structure_limit' }; return false; }
      return true;
    }
    function observe() {
      if (observed) return;
      observed = true;
      const originalFetch = globalThis.fetch;
      if (typeof originalFetch === 'function') {
        const wrappedFetch = function (...args) {
          const kind = endpoint(args[0]);
          const inspect = kind && reserveResponse();
          const pending = originalFetch.apply(this, args);
          if (inspect) Promise.resolve(pending).then(async response => {
            if (!active || networkError || !endpoint(response.url)) return;
            const failure = statusFailure(response.status, response.headers?.get('retry-after'));
            if (failure) { networkError = failure; return; }
            if (response.status !== 200) return;
            // Consume only a clone, through a bounded stream. Never buffer an unbounded body.
            const copy = response.clone();
            if (!copy.body?.getReader || typeof TextDecoder !== 'function') return;
            const reader = copy.body.getReader(), decoder = new TextDecoder();
            readers.add(reader);
            let text = '';
            try {
              while (active && !networkError) {
                const chunk = await reader.read();
                if (!active || networkError) return;
                if (chunk.done) { text += decoder.decode(); acceptText(text, kind === 'reels'); return; }
                networkBytes += chunk.value.byteLength;
                if (networkBytes > 4194304) invalid('body_limit');
                text += decoder.decode(chunk.value, { stream: true });
              }
            } finally { readers.delete(reader); Promise.resolve(reader.cancel()).catch(() => {}); }
          }).catch(error => { if (active && !networkError) networkError = safeError(error); });
          return pending;
        };
        globalThis.fetch = wrappedFetch;
        restore.push(() => { if (globalThis.fetch === wrappedFetch) globalThis.fetch = originalFetch; });
      }
      const proto = globalThis.XMLHttpRequest?.prototype;
      if (proto && typeof proto.open === 'function' && typeof proto.send === 'function') {
        const originalOpen = proto.open, originalSend = proto.send, requests = new WeakMap();
        const wrappedOpen = function (...args) {
          requests.set(this, endpoint(args[1]));
          return originalOpen.apply(this, args);
        };
        const wrappedSend = function (...args) {
          const kind = requests.get(this);
          if (kind && reserveResponse()) {
            const xhr = this;
            const entry = { xhr, listener: null };
            entry.listener = () => {
              xhr.removeEventListener('loadend', entry.listener); listeners.delete(entry);
              if (!active || networkError || !endpoint(xhr.responseURL)) return;
              try {
                const failure = statusFailure(xhr.status, xhr.getResponseHeader('retry-after'));
                if (failure) { networkError = failure; return; }
                // responseType=json has already rounded large IDs; never reuse it.
                if (xhr.status !== 200 || (xhr.responseType && xhr.responseType !== 'text')) return;
                const text = xhr.responseText;
                if (typeof text !== 'string') return;
                networkBytes += byteLength(text);
                if (networkBytes > 4194304) invalid('body_limit');
                acceptText(text, kind === 'reels');
              } catch (error) { networkError = safeError(error); }
            };
            listeners.add(entry); xhr.addEventListener('loadend', entry.listener);
          }
          return originalSend.apply(this, args);
        };
        proto.open = wrappedOpen; proto.send = wrappedSend;
        restore.push(() => {
          if (proto.open === wrappedOpen) proto.open = originalOpen;
          if (proto.send === wrappedSend) proto.send = originalSend;
        });
      }
    }
    try {
      for (;;) {
        if (location.origin !== 'https://www.instagram.com' || /\/(?:accounts\/login|challenge|checkpoint|consent)(?:\/|$)/.test(location.pathname) || document.querySelector('input[name="username"],input[name="password"]')) {
          done({ error: 'login_required' }); return;
        }
        // WebKit versions without responseStatus provide no HTTP evidence. Never infer 429 from text.
        const code = typeof performance !== 'undefined' ? performance.getEntriesByType?.('navigation')?.[0]?.responseStatus : undefined;
        if (code === 429) { done({ error: 'http_429', statusCode: 429 }); return; }
        if (code === 401 || code === 403) { done({ error: 'http_auth_required', statusCode: code }); return; }
        if (Number.isInteger(code) && code >= 400 && code <= 599) { done({ error: 'http_status', statusCode: code }); return; }
        if (!/^\/stories\/clubeuol\/(?:[1-9][0-9]{0,23}\/?)?$/.test(location.pathname)) {
          done({ error: 'browser_story_page_invalid' }); return;
        }
        const scripts = Array.from(document.querySelectorAll('script[type="application/json"]'));
        if (scripts.length > 256) invalid('structure_limit');
        let size = 0;
        const reels = [];
        for (const script of scripts) {
          const text = script.textContent || '';
          size += byteLength(text);
          if (size > 4194304) invalid('body_limit');
          reels.push(...project(text));
          if (reels.length > 100) invalid('structure_limit');
        }
        if (networkError) { done(networkError); return; }
        reels.push(...networkReels);
        if (reels.length) {
          // Host validates numeric identity again, using this same scoped reel.
          done({ profile: reels[0].user, reels: { reels_media: reels } }); return;
        }
        // Install before the gate click, so its async hydration is observed.
        observe();
        // Instagram may hold viewer hydration behind its explicit view button.
        // One exact, visible button only; never challenge/login/consent controls.
        if (!gateClicked) {
          const buttons = Array.from(document.querySelectorAll('button,[role="button"]'));
          const gates = buttons.filter(button => ['ver story', 'view story', 'see story'].includes((button.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase()) &&
            !button.disabled && button.getAttribute('aria-disabled') !== 'true' && button.getClientRects().length > 0 &&
            (typeof getComputedStyle !== 'function' || !['hidden', 'collapse'].includes(getComputedStyle(button).visibility) && getComputedStyle(button).display !== 'none'));
          if (gates.length === 1) { gateClicked = true; gates[0].click(); }
        }
        if (Date.now() - started >= waitMs) { done({ error: 'browser_story_data_missing' }); return; }
        await new Promise(resolve => setTimeout(resolve, 250));
      }
    } catch (error) {
      done(safeError(error));
    } finally {
      active = false;
      for (const undo of restore) undo();
      for (const { xhr, listener } of listeners) xhr.removeEventListener('loadend', listener);
      for (const reader of readers) Promise.resolve(reader.cancel()).catch(() => {});
      networkReels.length = 0; listeners.clear(); readers.clear();
    }
  }

  function create(env) {
    let view = null, disable = () => {}, inFlight = false;
    const bounded = (promise, milliseconds, rejection = 'browser_load_failed') => new Promise((resolve, reject) => {
      let settled = false;
      const timer = env.timer(milliseconds, () => { if (!settled) { settled = true; disable(); view = null; reject(fail('browser_timeout')); } });
      Promise.resolve(promise).then(value => { if (!settled) { settled = true; timer.invalidate(); resolve(value); } }, error => {
        if (!settled) {
          settled = true; timer.invalidate();
          const unsupported = rejection === 'browser_evaluation_failed' && /result (?:of an? |type (?:is )?)?unsupported type|result.*unsupported type/i.test(String(error?.message || ''));
          reject(fail(unsupported ? 'browser_bridge_unsupported' : rejection));
        }
      });
    });
    function getView() {
      if (view) return view;
      view = env.webView(); let active = true;
      view.shouldAllowRequest = request => active && typeof request?.url === 'string' &&
        (request.url === 'about:blank' || /^https:\/\/(?:[a-z0-9-]+\.)*(?:instagram\.com|cdninstagram\.com|fbcdn\.net)\//i.test(request.url));
      disable = () => { active = false; };
      return view;
    }
    async function connect() {
      if (inFlight) return { status: 'busy' };
      inFlight = true;
      try {
        const currentView = getView();
        await bounded(currentView.loadURL(ORIGIN + '/accounts/login/'), 25000);
        await currentView.present(true);
        const raw = await bounded(currentView.evaluateJavaScript('JSON.stringify({origin:location.origin,path:location.pathname,login:!!document.querySelector(\'input[name="username"],input[name="password"]\')})'), 5000, 'browser_evaluation_failed');
        const current = JSON.parse(raw);
        if (current.origin !== ORIGIN || current.login || /\/(?:accounts\/login|challenge|checkpoint|consent)(?:\/|$)/.test(current.path)) return { status: 'cancelled', reason: 'login_incomplete' };
        return { status: 'ready' }; // Only capture proves that Stories can be read.
      } catch (error) {
        return { status: 'unknown', stage: 'login', reason: ['browser_timeout','browser_load_failed','browser_evaluation_failed','browser_bridge_unsupported'].includes(error?.reason) ? error.reason : 'browser_login_failed' };
      } finally { inFlight = false; }
    }
    async function capture() {
      if (inFlight) throw fail('browser_busy');
      inFlight = true;
      try {
        const currentView = getView();
        await bounded(currentView.loadURL(ORIGIN + STORY_PATH), 25000);
        const code = '(' + pageCapture.toString() + ')((() => { const error = () => new Error(); return ' + env.losslessJSON.toString() + '; })()).catch(() => completion(JSON.stringify({error:"browser_script_error"}))); "";';
        const raw = await bounded(currentView.evaluateJavaScript(code, true), 15000, 'browser_evaluation_failed');
        if (typeof raw !== 'string' || raw.length > 4194304) throw fail('browser_response_invalid');
        let result; try { result = JSON.parse(raw); } catch (_) { throw fail('browser_response_invalid'); }
        if (!result || typeof result !== 'object' || Array.isArray(result)) throw fail('browser_response_invalid');
        if (result.error) {
          const reasons = ['login_required','http_auth_required','http_429','http_status','body_limit','structure_limit','browser_story_page_invalid','browser_story_data_invalid','browser_story_data_missing','browser_script_error'];
          const reason = reasons.includes(result.error) ? result.error : 'browser_response_invalid';
          throw fail(reason, ['login_required','http_auth_required'].includes(reason) ? 'auth_required' : reason === 'http_429' ? 'rate_limited' : 'unknown',
            { ...(Number.isInteger(result.statusCode) && result.statusCode >= 100 && result.statusCode <= 599 ? { statusCode: result.statusCode } : {}),
              ...(reason === 'http_429' && Number.isFinite(result.retryAfter) && result.retryAfter > 0 && result.retryAfter <= Number.MAX_SAFE_INTEGER ? { retryAfter: result.retryAfter } : {}) });
        }
        return result;
      } finally { inFlight = false; }
    }
    return { capture, connect };
  }
  return { create, pageCapture };
})();
if (typeof module !== 'undefined' && module.exports) module.exports = HunterInstagramBrowser;
