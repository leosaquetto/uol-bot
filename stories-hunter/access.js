// Native Scriptable only. Safe summaries may enter the WebView; credentials and
// configuration stay in Keychain and never appear in a returned value.
const HunterAccess = (() => {
  const ORIGIN = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev';
  const KEYS = { device: 'uol.hunter.device.v1', token: 'uol.hunter.token.v1', instagram: 'uol.hunter.instagram.v1', access: 'uol.hunter.access.v1' };
  const TTL = { approved: 21600000, pending: 120000, revoked: 1800000, expired: 1800000 };
  const STATES = { approved: 'approved', pending: 'access_pending', revoked: 'access_revoked', expired: 'access_expired' };
  const ACCOUNTS = ['leosaquetto', 'gaoriel'];
  const COOKIE_NAMES = ['SESS', 'JS_SESS', 'DNA', 'PHPSESSID', 'AWSALB', 'AWSALBCORS'];
  const COOKIE_DOMAINS = ['clube.uol.com.br', 'api.uol.com.br', 'conta.uol.com.br', 'uol.com.br'];
  const COOKIE_FIELDS = ['name', 'value', 'domain', 'path', 'hostOnly', 'secure', 'httpOnly', 'expires', 'expirationDate', 'sameSite', 'session'];
  const plain = value => !!value && typeof value === 'object' && !Array.isArray(value);
  const device = value => typeof value === 'string' && /^[a-z0-9-]{1,32}$/.test(value);
  const label = value => typeof value === 'string' && value.trim().length > 0 && value.length <= 60 && !/[<>\u0000-\u001f\u007f]/.test(value);
  const code = value => typeof value === 'string' && /^[A-Za-z0-9 -]{1,64}$/.test(value);
  const fail = reason => Object.assign(new Error(reason), { reason });
  function accountList(value) {
    const accounts = value === undefined ? [] : value;
    if (!Array.isArray(accounts) || accounts.length > 2 || accounts.some(account => !ACCOUNTS.includes(account)) || new Set(accounts).size !== accounts.length) throw fail('accounts_invalid');
    return accounts.slice();
  }

  function create(env) {
    const keychain = env.keychain, now = env.now || Date.now;
    let statusFlight = null, requestFlight = null;
    const get = key => keychain.contains(key) ? keychain.get(key) : '';
    const unavailable = reason => ({ status: 'unavailable', reason: reason || 'access_unavailable' });

    function credentials() {
      const token = get(KEYS.token), deviceId = get(KEYS.device);
      const match = typeof token === 'string' && token.match(/^h1\.([a-z0-9-]{1,32})\.[a-f0-9]{64}$/);
      if (!match) return null;
      if (deviceId && deviceId !== match[1]) throw fail('device_mismatch');
      if (!deviceId) keychain.set(KEYS.device, match[1]);
      return { token, deviceId: match[1] };
    }

    function createCredentials() {
      const existing = credentials();
      if (existing) return existing;
      if (get(KEYS.token) || get(KEYS.device)) throw fail('device_credentials_invalid');
      const random = () => {
        const value = env.uuid();
        if (typeof value !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) throw fail('random_unavailable');
        return value.replace(/-/g, '').toLowerCase();
      };
      const first = random(), second = random();
      const deviceId = 'phone-' + first.slice(0, 24);
      const token = 'h1.' + deviceId + '.' + first + second;
      // Persist the immutable identity before making any request. A lost reply
      // or process exit must use exactly these same credentials next time.
      keychain.set(KEYS.token, token);
      keychain.set(KEYS.device, deviceId);
      return { token, deviceId };
    }

    function safeState(value, deviceId) {
      if (!plain(value) || value.ok !== true || value.deviceId !== deviceId || !Object.prototype.hasOwnProperty.call(STATES, value.state) ||
          !label(value.label) || !code(value.code) || !['member', 'owner'].includes(value.role)) throw fail('access_response_invalid');
      let accounts;
      try { accounts = accountList(value.accounts); } catch (_) { throw fail('access_response_invalid'); }
      return { deviceId, state: value.state, label: value.label, code: value.code, role: value.role, accounts };
    }
    function cached(deviceId) {
      try {
        const value = JSON.parse(get(KEYS.access));
        const safe = safeState({ ...value, ok: true }, deviceId);
        return Number.isSafeInteger(value.checkedAt) && value.checkedAt > 0 && value.checkedAt <= now()
          ? { ...safe, checkedAt: value.checkedAt } : null;
      } catch (_) { return null; }
    }
    const save = value => keychain.set(KEYS.access, JSON.stringify({ ...value, checkedAt: now() }));
    const summary = value => ({ status: STATES[value.state], state: value.state, deviceId: value.deviceId, label: value.label, code: value.code, role: value.role, accounts: value.accounts.slice() });

    async function request(path, auth, body) {
      const url = ORIGIN + path;
      const req = env.request(url);
      req.method = body ? 'POST' : 'GET';
      req.timeoutInterval = 20;
      req.onRedirect = () => null;
      req.headers = { Authorization: 'Bearer ' + auth.token, Accept: 'application/json' };
      if (body) { req.headers['Content-Type'] = 'application/json'; req.body = JSON.stringify(body); }
      let text;
      try { text = await req.loadString(); } catch (_) { throw fail('access_network_error'); }
      const response = req.response || {};
      if (response.statusCode === 404 && path === '/hunter-access/status') throw fail('activation_required');
      if (response.statusCode === 401 || response.statusCode === 403) throw fail('access_denied');
      if (![200, 201, 202].includes(response.statusCode) || (response.url && response.url !== url)) throw fail('access_http_error');
      const type = Object.entries(response.headers || {}).find(([name]) => name.toLowerCase() === 'content-type')?.[1] || response.mimeType || '';
      if (!/^application\/(?:json|[\w.-]+\+json)(?:;|$)/i.test(type)) throw fail('access_response_invalid');
      // Scriptable has no streaming reader; this bound applies after loadString.
      if (typeof text !== 'string' || unescape(encodeURIComponent(text)).length > 65536) throw fail('access_response_invalid');
      try { const value = JSON.parse(text); if (!plain(value)) throw new Error(); return value; }
      catch (_) { throw fail('access_response_invalid'); }
    }

    async function loadStatus(refresh) {
      let auth;
      try { auth = credentials(); } catch (error) { return unavailable(error.reason); }
      if (!auth) return { status: 'not_configured', reason: 'activation_required' };
      const old = cached(auth.deviceId);
      try {
        if (!refresh && old && now() - old.checkedAt < TTL[old.state]) return summary(old);
        const value = safeState(await request('/hunter-access/status', auth), auth.deviceId);
        save(value);
        return summary(value);
      } catch (error) {
        if (error.reason === 'access_network_error' && old && old.state === 'approved' && now() - old.checkedAt < TTL.approved) {
          return { ...summary(old), cached: true };
        }
        if (['access_response_invalid', 'access_denied', 'activation_required'].includes(error.reason)) keychain.set(KEYS.access, '{}');
        if (error.reason === 'activation_required') return { status: 'not_configured', reason: 'activation_required', deviceId: auth.deviceId };
        return unavailable(error.reason);
      }
    }
    function status({ refresh = false } = {}) {
      if (!statusFlight) statusFlight = loadStatus(refresh === true).finally(() => { statusFlight = null; });
      return statusFlight;
    }

    async function submit(labelValue) {
      if (!label(labelValue)) return unavailable('device_label_invalid');
      try {
        const auth = createCredentials();
        const previous = cached(auth.deviceId);
        if (previous && previous.state !== 'expired' && now() - previous.checkedAt < TTL[previous.state]) return summary(previous);
        const known = previous ? previous.label : null;
        // Keep the first submitted label too, so retry after lost acceptance is
        // idempotent even when the user edits the field before trying again.
        const requestKey = KEYS.access + '.request';
        let savedLabel;
        try { const saved = JSON.parse(get(requestKey)); if (saved.deviceId === auth.deviceId && label(saved.label)) savedLabel = saved.label; } catch (_) {}
        const submitted = savedLabel || known || labelValue.trim();
        if (!savedLabel) keychain.set(requestKey, JSON.stringify({ deviceId: auth.deviceId, label: submitted }));
        const response = await request('/hunter-access/request', auth, { label: submitted });
        if (response.ok !== true) throw fail('access_response_invalid');
        if (response.state) {
          const value = safeState(response, auth.deviceId);
          save(value);
          return summary(value);
        }
        return await loadStatus(true);
      } catch (error) { return unavailable(error.reason); }
    }
    function requestAccess(labelValue) {
      if (!requestFlight) requestFlight = submit(labelValue).finally(() => { requestFlight = null; });
      return requestFlight;
    }

    async function ownerCredentials() {
      const access = await status();
      if (access.status !== 'approved' || access.role !== 'owner') throw fail('owner_required');
      return credentials();
    }
    async function devices() {
      try {
        const response = await request('/hunter-access/devices', await ownerCredentials());
        if (response.ok !== true || !Array.isArray(response.devices) || response.devices.length > 128) throw fail('access_response_invalid');
        const list = response.devices.map(value => {
          if (!device(value?.deviceId)) throw fail('access_response_invalid');
          const safe = safeState({ ...value, ok: true }, value.deviceId);
          const requestedAt = typeof value.requestedAt === 'string' && /^\d{4}-\d\d-\d\dT/.test(value.requestedAt) && Number.isFinite(Date.parse(value.requestedAt))
            ? value.requestedAt : Number.isSafeInteger(value.requestedAt) && value.requestedAt >= 0 ? value.requestedAt : null;
          if (requestedAt === null) throw fail('access_response_invalid');
          return { ...safe, requestedAt };
        });
        return { ok: true, devices: list };
      } catch (error) { return { ok: false, reason: error.reason || 'access_unavailable' }; }
    }
    async function decide(deviceId, decision, confirmationCode, accounts) {
      if (!device(deviceId) || !['approve', 'revoke'].includes(decision) || !code(confirmationCode)) return { ok: false, reason: 'decision_invalid' };
      try {
        if (accounts !== undefined && decision !== 'approve') return { ok: false, reason: 'decision_invalid' };
        let grants;
        try { if (accounts !== undefined) grants = accountList(accounts); } catch (_) { return { ok: false, reason: 'decision_invalid' }; }
        const response = await request('/hunter-access/decision', await ownerCredentials(), { deviceId, decision, code: confirmationCode, ...(grants ? { accounts: grants } : {}) });
        if (response.ok !== true) throw fail('access_response_invalid');
        const safe = safeState(response, deviceId);
        return { ok: true, ...summary(safe) };
      } catch (error) { return { ok: false, reason: error.reason || 'access_unavailable' }; }
    }
    function accountCookies(value) {
      if (!Array.isArray(value) || !value.length || value.length > 128) throw fail('account_config_invalid');
      const seen = new Set();
      return value.map(cookie => {
        if (!plain(cookie) || Object.keys(cookie).some(key => !COOKIE_FIELDS.includes(key)) || !COOKIE_NAMES.includes(cookie.name) || typeof cookie.value !== 'string' || cookie.value.length > 16384 || /[\u0000-\u0020\u007f;]/.test(cookie.value) || typeof cookie.domain !== 'string' || !COOKIE_DOMAINS.includes(cookie.domain.replace(/^\./, '').toLowerCase()) || typeof cookie.path !== 'string' || !cookie.path.startsWith('/') || cookie.path.length > 1024 || /[\u0000-\u0020\u007f;]/.test(cookie.path)) throw fail('account_config_invalid');
        if (['hostOnly', 'secure', 'httpOnly', 'session'].some(key => cookie[key] !== undefined && typeof cookie[key] !== 'boolean')) throw fail('account_config_invalid');
        if (['expires', 'expirationDate'].some(key => cookie[key] !== undefined && (typeof cookie[key] !== 'number' || !Number.isFinite(cookie[key]) || cookie[key] < -1))) throw fail('account_config_invalid');
        if (cookie.expires !== undefined && cookie.expirationDate !== undefined && cookie.expires !== cookie.expirationDate) throw fail('account_config_invalid');
        if (cookie.sameSite !== undefined && (typeof cookie.sameSite !== 'string' || !['strict', 'lax', 'none', 'no_restriction', 'unspecified'].includes(cookie.sameSite.toLowerCase()))) throw fail('account_config_invalid');
        const identity = cookie.name + '\n' + cookie.domain.replace(/^\./, '').toLowerCase() + '\n' + cookie.path;
        if (seen.has(identity)) throw fail('account_config_invalid');
        seen.add(identity);
        return Object.fromEntries(Object.entries(cookie));
      });
    }
    async function accountConfig(account) {
      if (!ACCOUNTS.includes(account)) return { ok: false, reason: 'account_invalid' };
      try {
        const access = await status({ refresh: true });
        if (access.status !== 'approved' || !access.accounts.includes(account)) throw fail('account_access_denied');
        const auth = credentials();
        if (!auth) throw fail('account_access_denied');
        const response = await request('/hunter-access/account-config?account=' + account, auth);
        if (Object.keys(response).some(key => !['ok', 'deviceId', 'account', 'identityDigest', 'cookies'].includes(key)) || response.ok !== true || response.deviceId !== auth.deviceId || response.account !== account || typeof response.identityDigest !== 'string' || !/^[a-f0-9]{64}$/i.test(response.identityDigest)) throw fail('account_config_invalid');
        const cookies = accountCookies(response.cookies);
        // This private result is consumed by the native redemption transport.
        // It is deliberately never saved into, or returned by, access status.
        return { ok: true, deviceId: auth.deviceId, account, identityDigest: response.identityDigest.toLowerCase(), cookies };
      } catch (error) { return { ok: false, reason: error.reason || 'access_unavailable' }; }
    }
    return { status, requestAccess, devices, decide, accountConfig };
  }
  return { create, KEYS, TTL };
})();
if (typeof module !== 'undefined') module.exports = HunterAccess;
