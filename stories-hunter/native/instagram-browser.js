// Own Instagram WebView session. No device token, cookies or viewer data cross into/out of the page.
const HunterInstagramBrowser = (() => {
  const ORIGIN = 'https://www.instagram.com';
  const STORY_PATH = '/stories/clubeuol/';
  const fail = (reason, status = 'unknown', extra = {}) => Object.assign(new Error(reason), { status, reason, ...extra });

  // Read only the JSON already loaded by the official Stories viewer. No fetch,
  // private API, React internals or exported document/session data.
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
    let gateClicked = false;
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
        let size = 0, nodes = 0;
        const reels = [];
        function walk(value, depth) {
          if (++nodes > 100000 || depth > 50) invalid('structure_limit');
          if (!value || typeof value !== 'object') return;
          if (Object.prototype.hasOwnProperty.call(value, 'xdt_api__v1__feed__reels_media')) {
            const payload = value.xdt_api__v1__feed__reels_media;
            if (!payload || !Array.isArray(payload.reels_media) || payload.reels_media.length > 100) invalid('browser_story_data_invalid');
            for (const entry of payload.reels_media) if (entry?.user?.username === 'clubeuol') reels.push(reel(entry));
            if (reels.length > 100) invalid('structure_limit');
          }
          for (const child of Object.values(value)) walk(child, depth + 1);
        }
        for (const script of scripts) {
          const text = script.textContent || '';
          size += unescape(encodeURIComponent(text)).length;
          if (size > 4194304) invalid('body_limit');
          if (!text.includes('xdt_api__v1__feed__reels_media')) continue;
          let value; try { value = parseJSON(text); } catch (_) { invalid('browser_story_data_invalid'); }
          walk(value, 0);
        }
        if (reels.length) {
          // Host validates numeric identity again, using this same scoped reel.
          done({ profile: reels[0].user, reels: { reels_media: reels } }); return;
        }
        // Instagram may hold viewer hydration behind its explicit view button.
        // One exact, visible button only; never challenge/login/consent controls.
        if (!gateClicked) {
          const buttons = Array.from(document.querySelectorAll('button,[role="button"]'));
          const gates = buttons.filter(button => ['ver story', 'view story', 'see story'].includes((button.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase()) &&
            !button.disabled && button.getAttribute('aria-disabled') !== 'true' && button.getClientRects().length > 0);
          if (gates.length === 1) { gateClicked = true; gates[0].click(); }
        }
        if (Date.now() - started >= waitMs) { done({ error: 'browser_story_data_missing' }); return; }
        await new Promise(resolve => setTimeout(resolve, 250));
      }
    } catch (error) {
      const reason = ['browser_story_data_invalid','structure_limit','body_limit'].includes(error?.message) ? error.message : 'browser_script_error';
      done({ error: reason });
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
            Number.isInteger(result.statusCode) && result.statusCode >= 100 && result.statusCode <= 599 ? { statusCode: result.statusCode } : {});
        }
        return result;
      } finally { inFlight = false; }
    }
    return { capture, connect };
  }
  return { create, pageCapture };
})();
if (typeof module !== 'undefined' && module.exports) module.exports = HunterInstagramBrowser;
