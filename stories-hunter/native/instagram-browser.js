// Own Instagram WebView session. Never receives the device token or imports cookies.
const HunterInstagramBrowser = (() => {
  const ORIGIN = 'https://www.instagram.com';
  const PROFILE = '/api/v1/users/web_profile_info/?username=clubeuol';
  const allowedPath = path => path === PROFILE || /^\/api\/v1\/feed\/reels_media\/\?reel_ids=[1-9][0-9]{0,23}$/.test(path);
  const fail = reason => Object.assign(new Error(reason), { status: 'unknown', reason });

  // Runs inside the actual Instagram page. Return only public, allowlisted
  // fields required by the existing parser, never HTML, cookies or viewer data.
  async function pageRequest(path, maxBytes, parseJSON) {
    let statusCode;
    const done = value => completion(JSON.stringify({ ...(statusCode ? { statusCode } : {}), ...value }));
    if (location.origin !== 'https://www.instagram.com' || /\/(?:accounts\/login|challenge|checkpoint|consent)(?:\/|$)/.test(location.pathname)) {
      done({ error: 'login_required' }); return;
    }
    if (typeof AbortController !== 'function' || typeof TextDecoder !== 'function') {
      done({ error: 'browser_runtime_unsupported' }); return;
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 20000);
    try {
      let response;
      try { response = await fetch(path, { method: 'GET', credentials: 'same-origin', redirect: 'manual',
        headers: { Accept: 'application/json', 'X-IG-App-ID': '936619743392459', 'X-ASBD-ID': '359341' }, signal: controller.signal }); }
      catch (_) { done({ error: controller.signal.aborted ? 'request_timeout' : 'browser_network_error' }); return; }
      if (Number.isInteger(response.status) && response.status >= 100 && response.status <= 599) statusCode = response.status;
      if (response.type === 'opaqueredirect') { done({ error: 'login_required' }); return; }
      const result = { statusCode: response.status, url: response.url, contentType: response.headers.get('content-type') || '',
        retryAfter: (response.headers.get('retry-after') || '').slice(0, 128), body: '' };
      if (response.status !== 200) { done(result); return; }
      if (!/^application\/(?:json|[\w.-]+\+json)(?:;|$)/i.test(result.contentType)) { done({ error: 'non_json_response' }); return; }
      if (!response.body || typeof response.body.getReader !== 'function') { done({ error: 'browser_runtime_unsupported' }); return; }
      let text = '';
      // Bound the downloaded API response; page assets are not collector requests.
      const reader = response.body.getReader(), decoder = new TextDecoder(); let count = 0;
      for (;;) {
        const chunk = await reader.read(); if (chunk.done) break;
        count += chunk.value.byteLength;
        if (count > maxBytes) { await reader.cancel(); done({ error: 'body_limit' }); return; }
        text += decoder.decode(chunk.value, { stream: true });
      }
      text += decoder.decode();
      const value = parseJSON(text);
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error();
      if (['login_required','challenge_required','checkpoint_required','checkpoint_challenge_required','consent_required'].includes(value.message) ||
          ['login_required','challenge_required','checkpoint_required'].some(k => value[k] === true) || value.is_logged_in === false || value.isLoggedIn === false) {
        done({ error: 'login_required' }); return;
      }
      const scalar = v => typeof v === 'string' && v.length <= 8192 || typeof v === 'number' && Number.isFinite(v) || typeof v === 'boolean' || v === null;
      const fields = (value, names) => {
        if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error();
        const output = {};
        for (const name of names) if (name in value) { if (!scalar(value[name])) throw new Error(); output[name] = value[name]; }
        return output;
      };
      const owner = value => fields(value, ['id','pk','username']);
      const array = (value, map) => { if (!Array.isArray(value) || value.length > 100) throw new Error(); return value.map(map); };
      const item = value => {
        const out = fields(value, ['id','pk','taken_at','expiring_at','original_width','original_height']);
        for (const key of ['user','owner']) if (key in value) out[key] = owner(value[key]);
        out.image_versions2 = { candidates: array(value.image_versions2?.candidates, c => fields(c, ['width','height','url'])) };
        out.story_link_stickers = array(value.story_link_stickers || [], s => ({ story_link: fields(s.story_link || {}, ['url']) }));
        return out;
      };
      const reel = value => ({ ...fields(value, ['id']), user: owner(value.user), items: array(value.items, item) });
      let projected;
      if (path.includes('web_profile_info')) projected = { data: { user: owner(value.data?.user) } };
      else {
        projected = fields(value, ['status']);
        if ('reels' in value) {
          if (!value.reels || typeof value.reels !== 'object' || Array.isArray(value.reels) || Object.keys(value.reels).length > 100) throw new Error();
          const id = path.split('=')[1]; projected.reels = {};
          if (Object.prototype.hasOwnProperty.call(value.reels, id)) projected.reels[id] = reel(value.reels[id]);
        }
        if ('reels_media' in value) projected.reels_media = array(value.reels_media, reel);
      }
      result.body = JSON.stringify(projected); done(result);
    } catch (_) { done({ error: controller.signal.aborted ? 'request_timeout' : 'browser_response_invalid' }); }
    finally { clearTimeout(timer); }
  }

  function create(env) {
    let view = null, loaded = false, inFlight = false;
    const bounded = (promise, milliseconds, onTimeout, rejection = 'browser_load_failed') => new Promise((resolve, reject) => {
      let settled = false;
      const timer = env.timer(milliseconds, () => { if (!settled) { settled = true; onTimeout(); reject(fail('browser_timeout')); } });
      Promise.resolve(promise).then(value => { if (!settled) { settled = true; timer.invalidate(); resolve(value); } }, error => {
        if (!settled) {
          settled = true; timer.invalidate();
          const unsupported = rejection === 'browser_evaluation_failed' && /result (?:of an? |type (?:is )?)?unsupported type|result.*unsupported type/i.test(String(error?.message || ''));
          reject(fail(unsupported ? 'browser_bridge_unsupported' : rejection));
        }
      });
    });
    function makeView() {
      const next = env.webView(); let active = true;
      next.shouldAllowRequest = request => active && typeof request?.url === 'string' &&
        (request.url === 'about:blank' || /^https:\/\/(?:[a-z0-9-]+\.)*(?:instagram\.com|cdninstagram\.com|fbcdn\.net)\//i.test(request.url));
      return { view: next, disable: () => { active = false; } };
    }
    async function load(path) {
      const next = makeView(); view = next.view; loaded = false;
      await bounded(view.loadURL(ORIGIN + path), 25000, next.disable);
      loaded = true;
    }
    async function connect() {
      if (inFlight) return { status: 'busy' };
      inFlight = true;
      try {
        await load('/accounts/login/');
        await view.present(true); // User closes after official login/verification.
        const location = await bounded(view.evaluateJavaScript('JSON.stringify({origin:location.origin,path:location.pathname})'), 5000, () => { loaded = false; }, 'browser_evaluation_failed');
        const current = JSON.parse(location);
        if (current.origin !== ORIGIN || /\/(?:accounts\/login|challenge|checkpoint|consent)(?:\/|$)/.test(current.path)) return { status: 'cancelled', reason: 'login_incomplete' };
        return { status: 'ready' }; // A successful API read, not closing the sheet, proves authentication.
      } catch (error) {
        return { status: 'unknown', stage: 'login', reason: ['browser_timeout','browser_load_failed','browser_evaluation_failed','browser_bridge_unsupported'].includes(error?.reason) ? error.reason : 'browser_login_failed' };
      }
      finally { inFlight = false; }
    }
    async function request(path) {
      if (!allowedPath(path)) throw fail('browser_path_invalid');
      if (inFlight) throw fail('browser_busy');
      inFlight = true;
      try {
        if (!loaded) await load('/clubeuol/');
        const parse = env.losslessJSON.toString();
        // WKWebView must not serialize the Promise from this async invocation.
        // The only capture result crosses the bridge through completion().
        const code = '(' + pageRequest.toString() + ')(' + JSON.stringify(path) + ',4194304,(() => { const error = () => new Error(); return ' + parse + '; })()).catch(() => completion(JSON.stringify({error:"browser_script_error"}))); "";';
        const raw = await bounded(view.evaluateJavaScript(code, true), 25000, () => { loaded = false; }, 'browser_evaluation_failed');
        if (typeof raw !== 'string' || raw.length > 4194304) throw fail('browser_response_invalid');
        let result;
        try { result = JSON.parse(raw); } catch (_) { throw fail('browser_response_invalid'); }
        if (!result || typeof result !== 'object' || Array.isArray(result)) throw fail('browser_response_invalid');
        if (result?.error) {
          const reasons = ['login_required','non_json_response','body_limit','request_timeout','browser_network_error','browser_response_invalid','browser_script_error','browser_runtime_unsupported'];
          const reason = reasons.includes(result.error) ? result.error : 'browser_response_invalid';
          throw Object.assign(new Error(reason), { status: reason === 'login_required' ? 'auth_required' : 'unknown', reason,
            ...(Number.isInteger(result.statusCode) && result.statusCode >= 100 && result.statusCode <= 599 ? { statusCode: result.statusCode } : {}) });
        }
        return result;
      } finally { inFlight = false; }
    }
    return { request, connect };
  }
  return { create, pageRequest, allowedPath };
})();
if (typeof module !== 'undefined' && module.exports) module.exports = HunterInstagramBrowser;
