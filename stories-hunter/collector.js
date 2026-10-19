// Embedded in the native Scriptable host, never in the WebView. All network and
// storage dependencies are injectable so the contract can be verified offline.
const HunterCollector = (() => {
  const ORIGIN = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev';
  const PORTAL = 'https://clube.uol.com.br/campanhasdeingresso';
  const IG = 'https://www.instagram.com';
  const KEYS = { device: 'uol.hunter.device.v1', token: 'uol.hunter.token.v1', instagram: 'uol.hunter.instagram.v1' };
  const LIMITS = { portal: 120000, instagram: 300000, requests: 30, body: 4 * 1024 * 1024, batch: 65536 };
  const plain = v => !!v && typeof v === 'object' && !Array.isArray(v);
  const bytes = s => unescape(encodeURIComponent(s)).length;
  const stable = v => JSON.stringify(v, (k, x) => plain(x) ? Object.keys(x).sort().reduce((o, key) => { o[key] = x[key]; return o; }, {}) : x);
  const error = (reason, status = 'unknown', retryAfter = 0) => Object.assign(new Error(reason), { reason, status, retryAfter });
  const header = (headers, name) => Object.entries(headers || {}).find(([k]) => k.toLowerCase() === name.toLowerCase())?.[1] || '';
  const id = v => typeof v === 'string' && /^[1-9][0-9]{0,23}$/.test(v) ? v : Number.isSafeInteger(v) && v > 0 ? String(v) : null;
  const clean = (v, max = 500) => {
    const text = typeof v === 'string' ? v.replace(/<[^>]*>/g, '').replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/\s+/g, ' ').trim() : '';
    let result = '', length = 0;
    for (const char of text) { const n = bytes(char); if (length + n > max) break; length += n; result += char; }
    return result;
  };
  const entities = s => String(s || '').replace(/&amp;/gi, '&').replace(/&quot;/gi, '"').replace(/&#39;|&apos;/gi, "'").replace(/&lt;/gi, '<').replace(/&gt;/gi, '>').replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Math.min(Number(n), 0x10ffff)));

  function campaign(value) {
    if (typeof value !== 'string' || value.length > 8192 || /[\s\\#\u0000-\u001f]/.test(value)) return null;
    for (let depth = 0; depth < 3; depth++) {
      if (!value.startsWith('https://l.instagram.com/')) break;
      const m = value.match(/^https:\/\/l\.instagram\.com\/\?([^#]*)$/);
      if (!m) return null;
      const entries = m[1].split('&').filter(p => p.split('=')[0] === 'u');
      if (entries.length !== 1) return null;
      try { value = decodeURIComponent(entries[0].slice(2).replace(/\+/g, ' ')); } catch { return null; }
    }
    const match = value.match(/^(?:https:\/\/clube\.uol\.com\.br)?(\/campanhasdeingresso\/p[A-Za-z0-9]{2,5}-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*)\/?(?:\?[^#\s\\]*)?$/);
    return match ? 'https://clube.uol.com.br' + match[1] : null;
  }

  function media(value, instagram = true) {
    if (typeof value !== 'string' || value.length > 4096 || /[\s\\#\u0000-\u001f]/.test(value)) return '';
    const m = value.match(/^https:\/\/([a-z0-9.-]+)(\/[^?]*)(?:\?[^#]*)?$/i);
    if (!m || /(?:^|\/)\.\.?\//.test(m[2]) || /\/(?:api|webhooks|login|resgatar|resgate)(?:\/|$)/i.test(m[2])) return '';
    try {
      if ((value.split('?')[1] || '').split('&').some(part => /^(?:access_token|token|cookie|session|sessionid|authorization|password)$/i.test(decodeURIComponent(part.split('=')[0])))) return '';
    } catch (_) { return ''; }
    if (instagram) return /^[a-z0-9.-]+\.(?:fbcdn\.net|cdninstagram\.com)$/i.test(m[1]) && /\.(?:jpe?g|webp)$/i.test(m[2]) ? value : '';
    return /^(?:(?:d310rdc9v8uuq|ddrxgn8ucibei)\.cloudfront\.net|(?:[a-z0-9-]+\.)*(?:uol\.com\.br|imguol\.com\.br))$/i.test(m[1]) ? value : '';
  }

  // JSON.parse alone rounds Instagram's numeric IDs. Scan outside strings and
  // quote unsafe integer tokens before parsing, preserving the original digits.
  function losslessJSON(text) {
    let out = '', i = 0;
    while (i < text.length) {
      if (text[i] === '"') {
        const start = i++;
        while (i < text.length) { if (text[i] === '\\') i += 2; else if (text[i++] === '"') break; }
        out += text.slice(start, i); continue;
      }
      if (text[i] === '-' || /[0-9]/.test(text[i])) {
        const m = text.slice(i).match(/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/);
        if (!m) throw error('malformed_json');
        const n = m[0]; out += /^-?\d+$/.test(n) && !Number.isSafeInteger(Number(n)) ? JSON.stringify(n) : n; i += n.length;
      } else out += text[i++];
    }
    return JSON.parse(out);
  }

  const authPayload = value => plain(value) && (['login_required', 'challenge_required', 'checkpoint_required', 'checkpoint_challenge_required', 'consent_required'].includes(value.message) || ['login_required', 'challenge_required', 'checkpoint_required'].some(k => value[k] === true) || value.is_logged_in === false || value.isLoggedIn === false);
  function ownerMatches(owner, profile) {
    if (!plain(owner) || owner.username !== 'clubeuol') return false;
    const ids = ['id', 'pk'].filter(k => k in owner).map(k => id(owner[k]));
    return ids.length > 0 && ids.every(v => v === profile.id);
  }
  function profileIdentity(value) {
    const user = value?.data?.user;
    const profile = { username: 'clubeuol', id: id(user?.id ?? user?.pk) };
    if (!profile.id || !ownerMatches(user, profile)) throw error('profile_identity_mismatch');
    return profile;
  }
  function parseReels(value, profile, now) {
    if (!profile || profile.username !== 'clubeuol' || !id(profile.id) || !plain(value)) throw error('no_validated_story_structure');
    if (authPayload(value)) throw error('login_payload', 'auth_required');
    if (value.status && value.status !== 'ok') throw error('api_error');
    const reels = [];
    if ('reels' in value) {
      if (!plain(value.reels) || Object.keys(value.reels).length > 100) throw error('malformed_story_payload');
      if (profile.id in value.reels) reels.push(value.reels[profile.id]);
    }
    if ('reels_media' in value) {
      if (!Array.isArray(value.reels_media) || value.reels_media.length > 100) throw error('malformed_story_payload');
      for (const reel of value.reels_media) {
        if (!plain(reel)) throw error('malformed_story_payload');
        if (ownerMatches(reel.user, profile)) reels.push(reel);
        else if (reel.user?.username === 'clubeuol' || ['id', 'pk'].some(k => id(reel.user?.[k]) === profile.id)) throw error('owner_mismatch');
      }
    }
    if (!reels.length) throw error('no_validated_story_structure');
    const seen = new Map(); let count = 0;
    for (const reel of reels) {
      if (!ownerMatches(reel?.user, profile) || ('id' in reel && id(reel.id) !== profile.id)) throw error('owner_mismatch');
      if (!Array.isArray(reel.items) || (count += reel.items.length) > 100) throw error('structure_limit');
      for (const item of reel.items) {
        if (!plain(item) || ('user' in item && !ownerMatches(item.user, profile)) || ('owner' in item && !ownerMatches(item.owner, profile))) throw error('owner_mismatch');
        const ids = [];
        if ('pk' in item) ids.push(id(item.pk));
        if ('id' in item) { const p = String(item.id).split('_'); if (p.length > 2 || (p.length === 2 && p[1] !== profile.id)) throw error('malformed_story_payload'); ids.push(id(p[0])); }
        const storyId = ids[0]; const taken = item.taken_at, expiry = item.expiring_at ?? taken + 86400;
        if (!storyId || !/^\d{10,24}$/.test(storyId) || !ids.every(x => x === storyId) || !Number.isInteger(taken) || !Number.isInteger(expiry) || taken <= 0 || taken * 1000 > now + 60000 || expiry <= taken || expiry - taken > 86400) throw error('malformed_story_payload');
        const stickers = item.story_link_stickers || [], candidates = item.image_versions2?.candidates;
        if (!Array.isArray(stickers) || stickers.length > 100 || !Array.isArray(candidates) || candidates.length > 100) throw error('malformed_story_payload');
        let valid = candidates.filter(c => Number.isInteger(c?.width) && Number.isInteger(c?.height) && Math.min(c.width, c.height) > 0 && c.width + c.height <= 10000 && Math.max(c.width, c.height) <= 20 * Math.min(c.width, c.height) && media(c.url));
        const w = item.original_width, h = item.original_height;
        if (Number.isInteger(w) && Number.isInteger(h) && w > 0 && h > 0) valid = valid.filter(c => Math.abs(c.width * h - c.height * w) * 100 <= 2 * c.height * w);
        else if (valid.some(c => c.height > c.width)) valid = valid.filter(c => c.height > c.width);
        valid.sort((a, b) => b.height - a.height || b.width - a.width);
        const image = valid[0];
        const links = [...new Set(stickers.map(s => campaign(s?.story_link?.url)).filter(Boolean))].sort();
        const canonical = { storyId, publishedAt: new Date(taken * 1000).toISOString(), expiresAt: new Date(expiry * 1000).toISOString(), links, image };
        if (seen.has(storyId) && stable(seen.get(storyId)) !== stable(canonical)) throw error('conflicting_story_payload');
        seen.set(storyId, canonical);
      }
    }
    return [...seen.values()].filter(s => Date.parse(s.expiresAt) > now && s.image).flatMap(s => s.links.map(link => ({ storyId: s.storyId, link, imageUrl: s.image.url, imageWidth: s.image.width, imageHeight: s.image.height, publishedAt: s.publishedAt, expiresAt: s.expiresAt })));
  }

  function parsePortal(html) {
    const offers = new Map();
    const blocks = html.match(/<div\b[^>]*class=["'][^"']*\bbeneficio\b[^"']*["'][\s\S]*?<!--\s*Fim div beneficio\s*-->/gi) || [];
    if (blocks.length > 500) throw error('structure_limit');
    if (!blocks.length) throw error('no_validated_portal_structure');
    for (const block of blocks) {
      const link = campaign(entities(block.match(/href=["']([^"']*\/campanhasdeingresso\/[^"']+)["']/i)?.[1]));
      if (!link) continue;
      const title = clean(entities(block.match(/<p\b[^>]*class=["'][^"']*\btitulo\b[^"']*["'][^>]*>([\s\S]*?)<\/p>/i)?.[1]), 280);
      if (!title) continue;
      const imageUrl = media(entities(block.match(/(?:data-src|src)=["']([^"']+)["']/i)?.[1]), false);
      const partner = clean(entities(block.match(/<p\b[^>]*class=["'][^"']*\bparceiro\b[^"']*["'][^>]*>([\s\S]*?)<\/p>/i)?.[1]), 120);
      offers.set(link, { link, title, ...(imageUrl ? { imageUrl } : {}), ...(partner ? { partner } : {}) });
    }
    if (!offers.size) throw error('no_validated_portal_structure');
    return [...offers.values()];
  }

  function normalizeSession(value) {
    if (!plain(value) || typeof value.user_agent !== 'string' || !value.user_agent || value.user_agent.length > 2048 || /[\r\n\0]/.test(value.user_agent)) throw error('session_invalid', 'auth_required');
    const cookies = new Map();
    if (value.cookies !== undefined && (!Array.isArray(value.cookies) || value.cookies.length > 100)) throw error('session_invalid', 'auth_required');
    for (const c of value.cookies || []) {
      if (!plain(c)) continue;
      const domain = String(c.domain || '.instagram.com').replace(/^\./, '');
      if (domain !== 'instagram.com' && !domain.endsWith('.instagram.com')) continue;
      if (typeof c.name === 'string' && typeof c.value === 'string' && /^[A-Za-z0-9_]+$/.test(c.name) && c.value.length <= 16384 && !/[;\r\n\0]/.test(c.value)) cookies.set(c.name, c.value);
    }
    if (typeof value.cookie_header === 'string' && value.cookie_header.length <= 65536 && !/[\r\n\0]/.test(value.cookie_header)) {
      for (const part of value.cookie_header.split(';')) { const m = part.trim().match(/^([A-Za-z0-9_]+)=(.*)$/); if (m) cookies.set(m[1], m[2]); }
    }
    if (!cookies.get('sessionid')) throw error('session_invalid', 'auth_required');
    const language = value.headers?.['accept-language'] || value.headers?.['Accept-Language'];
    return { user_agent: value.user_agent, headers: typeof language === 'string' && language.length < 1000 && !/[\r\n\0]/.test(language) ? { 'accept-language': language } : {}, cookies: [...cookies].map(([name, val]) => ({ name, value: val, domain: '.instagram.com', path: '/' })), cookie_header: '' };
  }

  function create(env) {
    const fm = env.fm, keychain = env.keychain, now = env.now || Date.now, uuid = env.uuid;
    const root = fm.joinPath(fm.documentsDirectory(), 'uol-hunter-collector-v1');
    if (!fm.fileExists(root)) fm.createDirectory(root, true);
    const file = name => fm.joinPath(root, name);
    const list = prefix => fm.listContents(root).filter(n => n.startsWith(prefix) && n.endsWith('.json'));
    const read = name => { try { return JSON.parse(fm.readString(file(name))); } catch { return null; } };
    function writeNew(name, value) {
      if (fm.fileExists(file(name))) return;
      const temp = file('.tmp-' + uuid()); fm.writeString(temp, JSON.stringify(value));
      if (!fm.fileExists(file(name))) { try { fm.move(temp, file(name)); } catch (e) { if (!fm.fileExists(file(name))) throw e; } }
      if (fm.fileExists(temp)) fm.remove(temp);
    }
    const records = prefix => list(prefix).map(name => ({ name, value: read(name) })).filter(r => r.value);
    const state = source => records('state-' + source + '-').map(r => r.value).sort((a, b) => b.at - a.at || (b.sequence || 0) - (a.sequence || 0))[0] || {};
    const saveState = (source, value) => writeNew('state-' + source + '-' + uuid() + '.json', { ...value, at: now(), sequence: (state(source).sequence || 0) + 1 });
    function importBootstrap(value) {
      if (!plain(value)) return;
      const token = value.token, deviceId = value.deviceId;
      if (typeof deviceId === 'string' && /^[a-z0-9-]{1,32}$/.test(deviceId) && typeof token === 'string' && token.startsWith('h1.' + deviceId + '.') && /^h1\.[a-z0-9-]{1,32}\.[a-f0-9]{64}$/.test(token)) {
        if (!keychain.contains(KEYS.token) && (!keychain.contains(KEYS.device) || keychain.get(KEYS.device) === deviceId)) { keychain.set(KEYS.token, token); if (!keychain.contains(KEYS.device)) keychain.set(KEYS.device, deviceId); }
      }
    }
    const token = () => keychain.contains(KEYS.token) ? keychain.get(KEYS.token) : '';
    function eligible(source) {
      const s = state(source), time = now();
      if (s.suspended) return { status: 'auth_required', reason: 'authentication_suspended' };
      if (s.retryAt > time) return { status: 'backoff' };
      const attempts = records('request-' + source + '-').map(r => r.value);
      const last = attempts.reduce((max, a) => Math.max(max, a.at), 0);
      if (last && time - last < LIMITS[source]) return { status: 'cooldown' };
      if (attempts.filter(a => new Date(a.at).toISOString().slice(0, 10) === new Date(time).toISOString().slice(0, 10)).length >= LIMITS.requests) return { status: 'daily_budget' };
      return null;
    }
    function reserve(source) {
      const day = new Date(now()).toISOString().slice(0, 10);
      if (records('request-' + source + '-').filter(r => new Date(r.value.at).toISOString().slice(0, 10) === day).length >= LIMITS.requests) throw error('daily_budget', 'daily_budget');
      writeNew('request-' + source + '-' + uuid() + '.json', { at: now() });
    }
    async function request(url, options = {}) {
      const req = env.request(url); req.timeoutInterval = 20; req.onRedirect = () => null;
      req.headers = options.headers || {}; req.method = options.method || 'GET';
      if (options.body) req.body = options.body;
      let text;
      try { text = await req.loadString(); } catch { throw error('network_error'); }
      const response = req.response || {}, code = response.statusCode, retry = String(header(response.headers, 'Retry-After'));
      const retryMs = /^\d+$/.test(retry) ? Number(retry) * 1000 : Math.max(0, Date.parse(retry) - now()) || 0;
      if (code === 429) throw error('http_429', 'rate_limited', Math.max(options.instagram ? 21600000 : 120000, retryMs));
      if ([401, 403].includes(code)) throw error('http_auth_required', 'auth_required');
      if (code >= 300 && code < 400) throw error('redirect_not_allowed', /login|challenge|checkpoint/.test(header(response.headers, 'Location')) ? 'auth_required' : 'unknown');
      if (code !== 200 && code !== 202) throw error('http_status');
      if (response.url && response.url !== url) throw error('unexpected_response_url', /login|challenge|checkpoint/.test(response.url) ? 'auth_required' : 'unknown');
      // Scriptable exposes no streaming body API. This is a post-read bound;
      // timeoutInterval bounds idle time, not the total transfer duration.
      if (typeof text !== 'string' || bytes(text) > (options.maxBytes || LIMITS.body)) throw error('body_limit');
      if (options.json && !/^application\/(?:json|[\w.-]+\+json)(?:;|$)/i.test(String(header(response.headers, 'Content-Type') || response.mimeType))) throw error('non_json_response');
      const value = options.json ? losslessJSON(text) : text;
      if (options.instagram && authPayload(value)) throw error('login_payload', 'auth_required');
      return value;
    }
    function enqueue(source, payload) {
      const fingerprint = stable({ source, payload });
      if (records('event-').some(r => r.value.fingerprint === fingerprint)) return false;
      if (records('event-').filter(r => !fm.fileExists(file('receipt-' + r.value.event.eventId + '.json'))).length >= 256) throw error('local_outbox_full');
      const event = { eventId: uuid(), source, observedAt: new Date(now()).toISOString(), payload };
      writeNew('event-' + event.eventId + '.json', { fingerprint, event });
      return true;
    }
    const browserState = 'instagramWebView';
    const browserReasons = ['http_429','http_auth_required','login_required','login_payload','daily_budget','non_json_response','body_limit','request_timeout',
      'browser_timeout','browser_load_failed','browser_network_error','browser_response_invalid','browser_busy','profile_identity_mismatch','owner_mismatch',
      'browser_evaluation_failed','browser_bridge_unsupported','browser_script_error','browser_runtime_unsupported','browser_login_failed','login_incomplete',
      'http_status','unexpected_response_url','no_validated_story_structure','api_error','malformed_story_payload','structure_limit','conflicting_story_payload','local_outbox_full'];
    const nextDay = () => new Date(new Date(now()).setUTCHours(24, 0, 0, 0)).toISOString();
    function browserSummary(status, extra = {}) {
      const s = state(browserState);
      return { status, found: 0, transport: 'webview',
        ...(s.lastAttemptAt ? { lastAttemptAt: s.lastAttemptAt } : {}),
        ...(s.lastSuccessAt ? { lastSuccessAt: s.lastSuccessAt } : {}), ...extra };
    }
    function browserBlocked(connect = false) {
      const s = state(browserState);
      if (s.retryAt > now()) return browserSummary('rate_limited', { reason: 'http_429', nextAttemptAt: new Date(s.retryAt).toISOString() });
      if (s.suspended && !connect) return browserSummary('auth_required', { reason: 'login_required' });
      const day = new Date(now()).toISOString().slice(0, 10);
      if (records('request-' + browserState + '-').filter(r => new Date(r.value.at).toISOString().slice(0, 10) === day).length >= LIMITS.requests)
        return browserSummary('daily_budget', { reason: 'daily_budget', nextAttemptAt: nextDay() });
      return null;
    }
    async function browserRequest(path) {
      reserve(browserState);
      saveState(browserState, { ...state(browserState), lastAttemptAt: new Date(now()).toISOString() });
      const response = await env.instagramBrowser.request(path);
      if (!plain(response) || !Number.isInteger(response.statusCode)) throw error('browser_response_invalid');
      const responseError = (reason, status, retry) => Object.assign(error(reason, status, retry),
        response.statusCode >= 100 && response.statusCode <= 599 ? { statusCode: response.statusCode } : {});
      const retry = String(response.retryAfter || ''), delay = /^\d+$/.test(retry) ? Number(retry) * 1000 : Math.max(0, Date.parse(retry) - now()) || 0;
      if (response.statusCode === 429) throw responseError('http_429', 'rate_limited', Math.max(21600000, Math.min(Number.isFinite(delay) ? delay : 21600000, 8640000000000000 - now())));
      if ([401,403].includes(response.statusCode)) throw responseError('http_auth_required', 'auth_required');
      if (response.statusCode !== 200) throw responseError('http_status');
      if (response.url !== IG + path) throw responseError('unexpected_response_url');
      if (typeof response.body !== 'string' || bytes(response.body) > LIMITS.body) throw responseError('body_limit');
      if (!/^application\/(?:json|[\w.-]+\+json)(?:;|$)/i.test(response.contentType || '')) throw responseError('non_json_response');
      const value = losslessJSON(response.body);
      if (authPayload(value)) throw error('login_payload', 'auth_required');
      return value;
    }
    async function collectInstagram(connect = false) {
      const blocked = browserBlocked(connect); if (blocked) return blocked;
      if (!env.instagramBrowser || typeof env.instagramBrowser.request !== 'function') return browserSummary('unavailable', { reason: 'browser_unavailable' });
      let stage = 'profile';
      try {
        const current = state(browserState); let profile = current.profile;
        if (!profile || profile.username !== 'clubeuol' || !id(profile.id) || !Number.isFinite(current.profileAt) || current.profileAt > now() || now() - current.profileAt >= 86400000) {
          profile = profileIdentity(await browserRequest('/api/v1/users/web_profile_info/?username=clubeuol'));
          saveState(browserState, { ...state(browserState), profile, profileAt: now() });
        }
        stage = 'stories';
        const payloads = parseReels(await browserRequest('/api/v1/feed/reels_media/?reel_ids=' + profile.id), profile, now());
        let found = 0; for (const payload of payloads) if (enqueue('instagram', payload)) found++;
        const status = payloads.length ? 'found' : 'empty';
        saveState(browserState, { ...state(browserState), suspended: false, retryAt: 0, reason: '', status, lastSuccessAt: new Date(now()).toISOString() });
        return browserSummary(status, { found });
      } catch (e) {
        const status = ['auth_required','rate_limited','daily_budget'].includes(e.status) ? e.status : 'unknown';
        const reason = browserReasons.includes(e.reason) ? e.reason : 'browser_response_invalid';
        const diagnostic = { stage, ...(Number.isInteger(e.statusCode) && e.statusCode >= 100 && e.statusCode <= 599 ? { statusCode: e.statusCode } : {}) };
        const before = state(browserState), retryAt = status === 'rate_limited' ? now() + Math.max(21600000, e.retryAfter || 0) : before.retryAt || 0;
        saveState(browserState, { ...before, suspended: status === 'auth_required' || !!before.suspended, retryAt, status, reason, ...diagnostic });
        return browserSummary(status, { reason, ...diagnostic, ...(status === 'rate_limited' ? { nextAttemptAt: new Date(retryAt).toISOString() } :
          status === 'daily_budget' ? { nextAttemptAt: nextDay() } : {}) });
      }
    }
    async function collect(source) {
      if (source === 'instagram') return collectInstagram();
      const blocked = eligible(source); if (blocked) return { ...blocked, found: 0 };
      try {
        reserve(source);
        const payloads = parsePortal(await request(PORTAL, { maxBytes: LIMITS.body, headers: { Accept: 'text/html' } }));
        let found = 0; for (const payload of payloads) if (enqueue(source, payload)) found++;
        saveState(source, { ...state(source), suspended: false, retryAt: 0, status: payloads.length ? 'found' : 'empty' });
        return { status: payloads.length ? 'found' : 'empty', found };
      } catch (e) {
        const s = state(source); saveState(source, { ...s, suspended: e.status === 'auth_required', retryAt: now() + (e.retryAfter || (e.status === 'daily_budget' ? 0 : LIMITS[source])), status: e.status || 'unknown', reason: e.reason || 'invalid_response' });
        return { status: e.status || 'unknown', reason: e.reason || 'invalid_response', found: 0 };
      }
    }
    async function flush() {
      const auth = token(); if (!auth) return;
      const s = state('intake'); if (s.suspended || s.retryAt > now()) return;
      const events = records('event-').map(r => r.value.event).filter(e => !fm.fileExists(file('receipt-' + e.eventId + '.json'))).sort((a, b) => a.observedAt.localeCompare(b.observedAt));
      const observations = [];
      for (const e of events) { if (observations.length >= 8 || bytes(JSON.stringify({ observations: [...observations, e] })) > LIMITS.batch) break; observations.push(e); }
      if (!observations.length) return;
      try {
        const result = await request(ORIGIN + '/hunter-intake', { method: 'POST', headers: { Authorization: 'Bearer ' + auth, 'Content-Type': 'application/json' }, body: JSON.stringify({ observations }), json: true, maxBytes: 65536 });
        if (!result || result.ok !== true || result.accepted !== true || result.persisted !== true || !Array.isArray(result.receipts) || result.receipts.length > 8) throw error('receipt_invalid');
        for (const receipt of result.receipts) {
          if (receipt?.persisted === true && observations.some(e => e.eventId === receipt.eventId) && ['queued', 'processing', 'registered', 'delivered', 'expired', 'blocked', 'needs_reconciliation'].includes(receipt.status)) writeNew('receipt-' + receipt.eventId + '.json', { eventId: receipt.eventId, persisted: true, status: receipt.status, receivedAt: now() });
        }
      } catch (e) { saveState('intake', { suspended: e.status === 'auth_required', retryAt: now() + (e.retryAfter || 120000), status: e.status || 'unknown' }); }
    }
    async function refreshReceipts() {
      const auth = token(); if (!auth || state('intake').retryAt > now() || state('intake').suspended) return;
      // Portal registration hands ownership to the normal delivery pipeline;
      // it is final for this intake receipt, but is never reported as delivered.
      const terminal = new Set(['registered', 'delivered', 'expired', 'blocked']);
      const pending = records('receipt-').map(r => r.value).filter(r => !terminal.has(r.status) && !fm.fileExists(file('terminal-' + r.eventId + '.json')))
        .map(r => { const s = state('receipt-' + r.eventId); return { ...r, status: s.receiptStatus || r.status, lastCheck: s.at || 0 }; })
        .filter(r => now() - r.lastCheck >= (r.status === 'needs_reconciliation' ? 900000 : 120000)).sort((a, b) => a.lastCheck - b.lastCheck || a.receivedAt - b.receivedAt).slice(0, 2);
      for (const previous of pending) {
        try {
          saveState('receipt-' + previous.eventId, { receiptStatus: previous.status });
          const result = await request(ORIGIN + '/hunter-intake-status?eventId=' + encodeURIComponent(previous.eventId), { headers: { Authorization: 'Bearer ' + auth }, json: true, maxBytes: 65536 });
          if (result?.eventId === previous.eventId && result.persisted === true) {
            if (terminal.has(result.status)) writeNew('terminal-' + previous.eventId + '.json', { eventId: previous.eventId, status: result.status, at: now() });
            else if (['queued', 'processing', 'needs_reconciliation'].includes(result.status)) saveState('receipt-' + previous.eventId, { receiptStatus: result.status });
          }
        } catch (e) {
          if (e.status === 'auth_required' || e.status === 'rate_limited') saveState('intake', { suspended: e.status === 'auth_required', retryAt: now() + (e.retryAfter || 120000), status: e.status });
          break;
        }
      }
    }
    function summary(status, sources = {}) {
      const events = records('event-'), receipts = records('receipt-');
      return { status, discovered: Object.values(sources).reduce((n, s) => n + (s.found || 0), 0), received: receipts.length, delivered: receipts.filter(r => r.value.status === 'delivered' || read('terminal-' + r.value.eventId + '.json')?.status === 'delivered').length, pending: Math.max(0, events.length - receipts.length), sources };
    }
    async function withLease(operation) {
      if (!token()) return summary('not_configured');
      const leaseId = uuid(), leaseName = 'lease-' + leaseId + '.json';
      writeNew(leaseName, { id: leaseId, at: now(), expires: now() + 180000 });
      // FileManager has no exclusive-open primitive. Append-only events and
      // receipts guarantee overlapping invocations never erase one another.
      // A short election additionally prevents normal overlapping source reads.
      if (env.pause) await env.pause(100);
      const contenders = records('lease-').map(r => r.value).filter(v => v.expires > now()).sort((a, b) => a.at - b.at || a.id.localeCompare(b.id));
      if (contenders[0]?.id !== leaseId) { if (read(leaseName)?.id === leaseId) fm.remove(file(leaseName)); return summary('busy'); }
      try {
        return await operation();
      } finally { if (read(leaseName)?.id === leaseId) fm.remove(file(leaseName)); }
    }
    async function run() {
      return withLease(async () => {
        await refreshReceipts(); await flush();
        const sources = { portal: await collect('portal'), instagram: await collect('instagram') };
        await flush(); return summary('complete', sources);
      });
    }
    let connecting = false;
    async function runInstagram({ connect = false } = {}) {
      if (!token()) return summary('not_configured');
      if (connecting) return summary('busy');
      if (connect) {
        const blocked = browserBlocked(true);
        if (blocked) return summary('complete', { instagram: blocked });
        if (!env.instagramBrowser?.connect) return summary('complete', { instagram: browserSummary('unavailable', { reason: 'browser_unavailable' }) });
        connecting = true;
        try {
          const result = await env.instagramBrowser.connect();
          if (result?.status !== 'ready') return summary('complete', { instagram: browserSummary(result?.status === 'cancelled' ? 'cancelled' : 'unknown',
            { stage: 'login', reason: browserReasons.includes(result?.reason) ? result.reason : 'login_incomplete' }) });
        } finally { connecting = false; }
      }
      return withLease(async () => {
        const sources = { instagram: await collectInstagram(connect) };
        await flush(); return summary('complete', sources);
      });
    }

    return { importBootstrap, run, enqueue, flush, summary, collect, refreshReceipts, runInstagram };
  }
  return { create, parsePortal, parseReels, losslessJSON, campaign, media, profileIdentity, normalizeSession, KEYS, LIMITS };
})();
if (typeof module !== 'undefined') module.exports = HunterCollector;
