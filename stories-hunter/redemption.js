// Native Scriptable transport. Account cookies and the mutation journal stay in
// Keychain; only verified history and sanitized attempt summaries reach the UI.
const HunterRedemption = (() => {
  const CLUB = 'https://clube.uol.com.br';
  const HISTORY = CLUB + '/perfil/beneficios';
  const ACCOUNTS = ['leosaquetto', 'gaoriel'];
  const HOSTS = ['clube.uol.com.br', 'api.uol.com.br', 'conta.uol.com.br'];
  const DOMAINS = HOSTS.concat('uol.com.br');
  const COOKIE_NAMES = ['SESS', 'JS_SESS', 'DNA', 'PHPSESSID', 'AWSALB', 'AWSALBCORS'];
  const REDIRECTS = [301, 302, 303, 307, 308];
  const COOKIE_KEY = 'uol.hunter.cookies.v1.';
  const ATTEMPT_KEY = 'uol.hunter.redemption.v1.';
  const ACTIVE = ['reserved', 'uncertain'];
  const STATES = ACTIVE.concat(['confirmed', 'quota_used', 'auth_required', 'sold_out']);
  const norm = value => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/\s+/g, ' ').trim();
  const failure = reason => Object.assign(new Error(reason), { reason });
  const object = value => value && typeof value === 'object' && !Array.isArray(value);
  const domainMatches = (host, domain) => host === domain || host.endsWith('.' + domain);
  const pathMatches = (path, cookiePath) => path === cookiePath || (path.startsWith(cookiePath) && (cookiePath.endsWith('/') || path[cookiePath.length] === '/'));
  const header = (headers, key) => Object.entries(headers || {}).find(([name]) => name.toLowerCase() === key.toLowerCase())?.[1] || '';

  // Scriptable runs JavaScriptCore without the browser URL/URLSearchParams
  // globals. This deliberately small parser accepts HTTPS DNS URLs only and
  // leaves encoded query values unchanged until the read allowlist checks them.
  function parseUrl(input, base = CLUB) {
    if (typeof input !== 'string' || /[\u0000-\u0020\u007f\\]/.test(input)) throw failure('url_blocked');
    let absolute = input;
    if (!/^https:\/\//i.test(absolute)) {
      if (/^[a-z][a-z0-9+.-]*:/i.test(input) || input.startsWith('//')) throw failure('url_blocked');
      const parent = parseUrl(base);
      absolute = parent.origin + (input.startsWith('/') ? input : input.startsWith('?') ? parent.pathname + input : parent.pathname.slice(0, parent.pathname.lastIndexOf('/') + 1) + input);
    }
    const match = /^https:\/\/([^/?#]+)([^?#]*)(\?[^#]*)?(#.*)?$/i.exec(absolute);
    if (!match || !/^(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,63}$/i.test(match[1])) throw failure('url_blocked');
    const hostname = match[1].toLowerCase(), pathname = match[2] || '/', search = match[3] || '', hash = match[4] || '';
    if (!pathname.startsWith('/')) throw failure('url_blocked');
    const pairs = search ? search.slice(1).split('&').map(part => {
      const at = part.indexOf('=');
      const component = value => { try { return decodeURIComponent(value.replace(/\+/g, ' ')); } catch (_) { throw failure('url_blocked'); } };
      return [component(at < 0 ? part : part.slice(0, at)), component(at < 0 ? '' : part.slice(at + 1))];
    }) : [];
    pairs.keys = () => pairs.map(pair => pair[0]);
    pairs.get = key => pairs.find(pair => pair[0] === key)?.[1] ?? null;
    const origin = 'https://' + hostname;
    return { protocol: 'https:', hostname, port: '', username: '', password: '', origin, pathname, search, hash, href: origin + pathname + search + hash, searchParams: pairs };
  }

  function decode(value) {
    let result = value;
    for (let i = 0; i < 8; i++) {
      if (!result.includes('%')) return result;
      let next;
      try { next = decodeURIComponent(result); } catch (_) { throw failure('url_blocked'); }
      if (next === result) return result;
      result = next;
    }
    if (result.includes('%')) throw failure('url_blocked');
    return result;
  }
  function readUrl(input, base = CLUB, depth = 0) {
    if (depth > 8 || typeof input !== 'string' || input.length > 16384) throw failure('url_blocked');
    const clear = decode(input);
    if (/[\u0000-\u0020\u007f\\]/.test(input) || /[\u0000-\u001f\u007f\\]/.test(clear) || /resgatar|logout|favoritos|\/contato(?:[/?#]|$)/i.test(clear) || /(?:^|\/)\.{1,2}(?:\/|[?#]|$)/.test(clear)) throw failure('url_blocked');
    let url;
    try { url = parseUrl(input, base); } catch (_) { throw failure('url_blocked'); }
    if (url.protocol !== 'https:' || !HOSTS.includes(url.hostname) || url.port || url.username || url.password || url.hash || /%/.test(url.pathname)) throw failure('url_blocked');
    const only = allowed => {
      const keys = [...url.searchParams.keys()];
      return keys.every(key => allowed.includes(key)) && new Set(keys).size === keys.length;
    };
    let allowed = false;
    if (url.origin === CLUB) {
      if (url.pathname === '/') allowed = !url.search || (only(['categoria']) && url.searchParams.get('categoria') === 'ingressosexclusivos');
      else if (/^\/perfil\/beneficios(?:\/[A-Za-z0-9_-]+)?$/.test(url.pathname) || /^\/campanhasdeingresso\/[A-Za-z0-9][A-Za-z0-9_-]*$/.test(url.pathname)) allowed = !url.search;
      else if (url.pathname === '/auth/uol/login') allowed = only(['redirect_uri']);
      else if (url.pathname === '/auth/uol') allowed = only(['code', 'state', 'error', 'error_description']);
    } else if (url.origin === 'https://api.uol.com.br' && url.pathname === '/oauth/auth') allowed = only(['response_type', 'client_id', 'redirect_uri', 'state', 't', 'scope']);
    else if (url.origin === 'https://conta.uol.com.br' && url.pathname === '/login') allowed = only(['t', 'dest', 'request_id']);
    if (!allowed) throw failure('url_blocked');
    for (const [key, value] of url.searchParams) {
      const target = decode(value);
      if (/resgatar|logout|favoritos|\/contato(?:[/?#]|$)/i.test(target)) throw failure('url_blocked');
      if (['dest', 'redirect_uri'].includes(key) || /^(?:https?:|\/)/i.test(target)) {
        if (!/^(?:https:\/\/|\/)/i.test(target)) throw failure('url_blocked');
        readUrl(target, url.origin, depth + 1);
      }
    }
    return url;
  }
  function offerUrl(input) {
    if (typeof input !== 'string' || !input.startsWith(CLUB + '/')) throw failure('offer_url_invalid');
    const url = readUrl(input);
    if (url.origin !== CLUB || !/^\/campanhasdeingresso\/[A-Za-z0-9][A-Za-z0-9_-]*$/.test(url.pathname)) throw failure('offer_url_invalid');
    return url.href;
  }
  function publicUrl(input) {
    try { const url = parseUrl(input); return /^https:\/\//i.test(input) ? url.href : null; } catch (_) { return null; }
  }

  class Jar {
    constructor(cookies, now) {
      this.items = []; this.now = now;
      if (!Array.isArray(cookies) || cookies.length > 128) throw failure('cookies_invalid');
      cookies.forEach(cookie => this.put(cookie));
    }
    put(cookie) {
      if (!object(cookie) || !COOKIE_NAMES.includes(cookie.name)) return;
      const originalDomain = cookie.domain ?? 'clube.uol.com.br';
      const domain = typeof originalDomain === 'string' ? originalDomain.replace(/^\./, '').toLowerCase() : '';
      const path = cookie.path ?? '/';
      const expires = Number(cookie.expires ?? cookie.expirationDate ?? -1);
      if (!DOMAINS.includes(domain) || typeof cookie.value !== 'string' || cookie.value.length > 16384 || /[\u0000-\u0020\u007f;]/.test(cookie.value) || typeof path !== 'string' || !path.startsWith('/') || /[\u0000-\u0020\u007f;]/.test(path) || !Number.isFinite(expires)) throw failure('cookies_invalid');
      if (['hostOnly', 'secure', 'httpOnly'].some(key => cookie[key] !== undefined && typeof cookie[key] !== 'boolean')) throw failure('cookies_invalid');
      const saved = { name: cookie.name, value: cookie.value, domain, path, hostOnly: cookie.hostOnly ?? !originalDomain.startsWith('.'), secure: cookie.secure !== false, httpOnly: cookie.httpOnly === true, expires,
        ...(typeof cookie.sameSite === 'string' ? { sameSite: cookie.sameSite } : {}), ...(typeof cookie.session === 'boolean' ? { session: cookie.session } : {}) };
      this.items = this.items.filter(item => !(item.name === saved.name && item.domain === domain && item.path === path));
      if (expires < 0 || expires * 1000 > this.now()) this.items.push(saved);
    }
    absorb(headers, url) {
      const combined = header(headers, 'set-cookie');
      const lines = Array.isArray(combined) ? combined : typeof combined === 'string' ? combined.split(/,(?=\s*[^;,=\s]+=[^;]*)/) : [];
      for (const line of lines) {
        const segments = line.split(';').map(part => part.trim());
        const equal = segments[0].indexOf('=');
        if (equal < 1 || !COOKIE_NAMES.includes(segments[0].slice(0, equal))) continue;
        const slash = url.pathname.lastIndexOf('/');
        const cookie = { name: segments[0].slice(0, equal), value: segments[0].slice(equal + 1), domain: url.hostname, path: slash <= 0 ? '/' : url.pathname.slice(0, slash), hostOnly: true, secure: false, expires: -1 };
        let invalid = false, maxAge;
        for (const segment of segments.slice(1)) {
          const at = segment.indexOf('='); const key = (at < 0 ? segment : segment.slice(0, at)).toLowerCase(); const value = at < 0 ? '' : segment.slice(at + 1);
          if (key === 'domain') { cookie.domain = value.replace(/^\./, '').toLowerCase(); cookie.hostOnly = false; invalid = !DOMAINS.includes(cookie.domain) || !domainMatches(url.hostname, cookie.domain); }
          else if (key === 'path' && value.startsWith('/')) cookie.path = value;
          else if (key === 'secure') cookie.secure = true;
          else if (key === 'httponly') cookie.httpOnly = true;
          else if (key === 'max-age' && /^-?\d+$/.test(value)) maxAge = Number(value);
          else if (key === 'expires' && Number.isFinite(Date.parse(value))) cookie.expires = Date.parse(value) / 1000;
        }
        if (invalid) continue;
        if (maxAge !== undefined) cookie.expires = maxAge <= 0 ? 1 : this.now() / 1000 + maxAge;
        this.put(cookie);
      }
    }
    snapshot() { this.items = this.items.filter(cookie => cookie.expires < 0 || cookie.expires * 1000 > this.now()); return this.items.map(cookie => ({ ...cookie })); }
    header(url) {
      return this.snapshot().filter(cookie => (cookie.hostOnly ? url.hostname === cookie.domain : domainMatches(url.hostname, cookie.domain)) && pathMatches(url.pathname, cookie.path) && (!cookie.secure || url.protocol === 'https:')).sort((a, b) => b.path.length - a.path.length).map(cookie => cookie.name + '=' + cookie.value).join('; ');
    }
  }

  function create(env) {
    const keychain = env.keychain, now = env.now || Date.now;
    const get = key => keychain.contains(key) ? keychain.get(key) : '';
    const account = user => { if (!ACCOUNTS.includes(user)) throw failure('account_invalid'); return user; };
    async function authorize(user) {
      if (typeof env.authorizeAccount !== 'function') return;
      let allowed = false;
      try { allowed = (await env.authorizeAccount(user)) === true; } catch (_) { /* Missing approval fails closed, without exposing the verifier's error. */ }
      if (!allowed) throw failure('account_access_required');
    }
    function expected(user) {
      const value = typeof env.expectedName === 'function' ? env.expectedName(user) : env.expectedName?.[user];
      if (typeof value !== 'string' || !value.trim() || value.length > 100) throw failure('account_identity_unconfigured');
      return value;
    }
    function attempt(user) {
      const raw = get(ATTEMPT_KEY + user);
      if (!raw) return null;
      let value;
      try { value = JSON.parse(raw); } catch (_) { throw failure('attempt_journal_invalid'); }
      if (!object(value) || value.account !== user || !STATES.includes(value.status) || typeof value.attemptId !== 'string' || !/^[a-f0-9-]{36}$/i.test(value.attemptId) || !Array.isArray(value.baseline) || value.baseline.some(id => typeof id !== 'string') || !Number.isSafeInteger(value.observedAt) || offerUrl(value.offerUrl) !== value.offerUrl || !value.title) throw failure('attempt_journal_invalid');
      return value;
    }
    function saveAttempt(value) {
      // A slow network result or a second history read cannot undo a receipt
      // another in-flight call has already proved, nor restore an older attempt.
      const raw = get(ATTEMPT_KEY + value.account);
      if (raw) {
        let previous;
        try { previous = JSON.parse(raw); } catch (_) { throw failure('attempt_journal_invalid'); }
        if (previous.attemptId === value.attemptId && previous.status === 'confirmed') return previous;
        if (previous.attemptId !== value.attemptId && value.status !== 'reserved') return previous;
      }
      // Keep each attempt under its immutable ID as well as the active pointer.
      // A failed pointer write occurs before dispatch, and cannot authorize send.
      keychain.set(ATTEMPT_KEY + value.account + '.' + value.attemptId, JSON.stringify(value));
      keychain.set(ATTEMPT_KEY + value.account, JSON.stringify(value));
      return value;
    }
    const summary = value => ({ status: value.status, account: value.account, attemptId: value.attemptId, observedAt: value.observedAt, ...(value.reason ? { reason: value.reason } : {}), ...(value.receipt ? { receipt: value.receipt } : {}) });

    async function transport(user) {
      const key = COOKIE_KEY + user;
      if (!keychain.contains(key)) {
        const supplied = await env.loadCookies(user);
        const imported = new Jar(supplied, now).snapshot();
        if (!keychain.contains(key)) keychain.set(key, JSON.stringify(imported));
      }
      let jar;
      try { jar = new Jar(JSON.parse(get(key)), now); } catch (_) { throw failure('cookies_invalid'); }
      const persist = async () => {
        const cookies = jar.snapshot();
        keychain.set(key, JSON.stringify(cookies));
        if (typeof env.saveCookies === 'function') await env.saveCookies(user, cookies);
      };
      async function request(url) {
        const req = env.request(url.href);
        req.method = 'GET'; req.timeoutInterval = 15; req.onRedirect = () => null;
        // An explicit empty Cookie header prevents accidental ambient account use.
        req.headers = { Cookie: jar.header(url), Accept: 'text/html,application/xhtml+xml', 'Cache-Control': 'no-cache' };
        let html;
        try { html = await req.loadString(); } catch (_) { throw failure('network_error'); }
        const response = req.response || {};
        if (response.url && response.url !== url.href) throw failure('redirect_not_controlled');
        if (typeof html !== 'string' || Number(header(response.headers, 'content-length')) > 1048576 || unescape(encodeURIComponent(html)).length > 1048576) throw failure('response_too_large');
        jar.absorb(response.headers, url); await persist();
        if (/captcha|challenge/i.test(header(response.headers, 'x-amzn-waf-action')) || /<title[^>]*>\s*Human Verification|verify you are human|verifique que voc[eê] [eé] humano/i.test(html)) throw failure('challenge_required');
        return { status: response.statusCode, html, url: url.href, location: header(response.headers, 'location') };
      }
      async function read(input) {
        let url = readUrl(input);
        for (let i = 0; i <= 8; i++) {
          const result = await request(url);
          if (!REDIRECTS.includes(result.status)) return result;
          if (!result.location) throw failure('redirect_invalid');
          url = readUrl(result.location, url.href);
          if (i === 8) throw failure('redirect_limit');
        }
      }
      return { read, request };
    }
    const authResponse = result => result.status === 401 || result.status === 403 || parseUrl(result.url).origin === 'https://conta.uol.com.br' || /<input\b[^>]*type=["']password["']|id=["']dynamic-config["']|\bosirisUai\b/i.test(result.html);
    async function verifiedHistory(user, client) {
      const customIdentity = typeof env.verifyIdentity === 'function';
      const name = customIdentity ? null : expected(user);
      async function verify(parsed) {
        if (!parsed?.authenticated) return;
        let verified = false;
        try { verified = customIdentity ? (await env.verifyIdentity(user, parsed)) === true : norm(parsed.signedInName) === norm(name); } catch (_) { /* An unavailable verifier cannot authorize this account. */ }
        if (!verified) throw failure('identity_mismatch');
      }
      let response = await client.read(HISTORY);
      if (response.status !== 200 && !authResponse(response)) throw failure('history_unavailable');
      let parsed = response.status === 200 && response.url === HISTORY && !authResponse(response) ? await env.parseHistory(response.html) : null;
      await verify(parsed);
      if (!parsed?.authenticated) {
        const restored = await client.read(CLUB + '/auth/uol/login');
        if (restored.status !== 200 || authResponse(restored) || parseUrl(restored.url).origin !== CLUB || parseUrl(restored.url).pathname.startsWith('/auth/')) throw failure('auth_required');
        response = await client.read(HISTORY);
        parsed = response.status === 200 && response.url === HISTORY && !authResponse(response) ? await env.parseHistory(response.html) : null;
        await verify(parsed);
      }
      if (!parsed?.authenticated || !parsed.historyScopeFound) throw failure('auth_required');
      if (parsed.hasPagination !== false || !Array.isArray(parsed.entries) || parsed.entries.length > 1000) throw failure('history_incomplete');
      const records = [];
      for (const value of parsed.entries) {
        if (!object(value) || typeof value.id !== 'string' || !/^[A-Za-z0-9_-]{1,200}$/.test(value.id) || typeof value.title !== 'string' || value.title.length > 1000) throw failure('history_invalid');
        const url = readUrl(value.url);
        if (url.href !== CLUB + '/perfil/beneficios/' + value.id) throw failure('history_invalid');
        let source = null;
        if (value.offerUrl) source = offerUrl(value.offerUrl);
        records.push({ id: value.id, url: url.href, title: value.title, category: value.category === 'campanhasdeingresso' ? value.category : null, imageUrl: publicUrl(value.imageUrl), offerUrl: source, redemptionDate: typeof value.redemptionDate === 'string' && value.redemptionDate.length <= 160 ? value.redemptionDate : null });
      }
      if (new Set(records.map(record => record.id)).size !== records.length) throw failure('history_invalid');
      return records;
    }
    function reconcile(value, records) {
      if (!value || !ACTIVE.includes(value.status)) return value;
      const latest = attempt(value.account);
      if (latest && (latest.attemptId !== value.attemptId || !ACTIVE.includes(latest.status))) return latest;
      if (latest) value = latest;
      const candidates = records.filter(record => !value.baseline.includes(record.id) && (record.offerUrl === value.offerUrl || (norm(record.title) === norm(value.title) && value.imageUrl && record.imageUrl === value.imageUrl)));
      if (candidates.length === 1) {
        value = { ...value, status: 'confirmed', reason: undefined, receipt: { ...candidates[0], observedAt: now() } };
        value = saveAttempt(value);
      } else if (value.status === 'reserved') {
        value = saveAttempt({ ...value, status: 'uncertain', reason: 'receipt_missing' });
      }
      return value;
    }
    function safeError(error, user) {
      const reason = ['auth_required', 'identity_mismatch', 'account_access_required', 'account_identity_unconfigured', 'history_unavailable', 'history_incomplete', 'history_invalid', 'cookies_invalid', 'account_invalid', 'url_blocked', 'offer_url_invalid', 'offer_unverified', 'network_error', 'challenge_required', 'response_too_large', 'redirect_limit', 'redirect_invalid', 'redirect_not_controlled', 'attempt_journal_invalid'].includes(error?.reason) ? error.reason : 'request_unavailable';
      return { status: ['auth_required', 'identity_mismatch', 'account_access_required'].includes(reason) ? reason : 'unavailable', account: ACCOUNTS.includes(user) ? user : null, reason };
    }
    async function history(user) {
      try {
        account(user); await authorize(user); const client = await transport(user);
        const records = await verifiedHistory(user, client);
        const current = reconcile(attempt(user), records);
        return { status: 'authenticated', account: user, records, checkedAt: now(), ...(current ? { attempt: summary(current) } : {}) };
      } catch (error) { return safeError(error, user); }
    }
    async function redeem(user, input) {
      let claimed = null;
      try {
        account(user); await authorize(user); const url = offerUrl(input); const before = attempt(user);
        const client = await transport(user);
        const baseline = await verifiedHistory(user, client);
        // Interrupted dispatches only reconcile. Neither an absent voucher nor
        // another offer can authorize repeating the mutation for this account.
        if (before && ACTIVE.includes(before.status)) return summary(reconcile(before, baseline));
        const response = await client.read(url);
        if (response.status !== 200 || response.url !== url || authResponse(response)) throw failure('offer_unverified');
        const offer = await env.parseOffer(response.html, url);
        if (offer?.soldOut === true) return { status: 'sold_out', account: user };
        if (offer?.requiresLogin === true) return { status: 'auth_required', account: user };
        const proof = offer?.identityEvidence;
        if (offer?.url !== url || !offer.available || !proof?.scoped || proof.titleCount !== 1 || proof.descriptionCount !== 1 || !proof.canonicalMatches || proof.actionCount !== 1 || !proof.actionMatches || offer.redeemUrl !== url + '/resgatar' || typeof offer.title !== 'string' || !offer.title.trim() || offer.title.length > 1000) throw failure('offer_unverified');
        // This final synchronous check and durable write are adjacent: concurrent
        // preflights cannot both acquire the account's mutation reservation.
        const latest = attempt(user);
        if (latest && ACTIVE.includes(latest.status)) return summary(reconcile(latest, baseline));
        if (latest && latest.attemptId !== before?.attemptId) return summary(latest);
        const attemptId = env.uuid();
        if (typeof attemptId !== 'string' || !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i.test(attemptId)) throw failure('random_unavailable');
        claimed = { account: user, attemptId, status: 'reserved', observedAt: now(), offerUrl: url, title: offer.title, imageUrl: publicUrl(offer.imageUrl), baseline: baseline.map(record => record.id) };
        saveAttempt(claimed);
        const result = await client.request(parseUrl(url + '/resgatar'));
        const text = norm(result.html.replace(/<[^>]*>/g, ' '));
        let destination = null;
        try { const parsed = parseUrl(result.location, url); if (parsed.origin === CLUB && !parsed.username && !parsed.password && !parsed.port) destination = parsed; } catch (_) {}
        let status = 'uncertain', reason = 'receipt_missing';
        if ((result.status === 200 || REDIRECTS.includes(result.status)) && ((destination && /(?:^|[/?=&-])max-redeem(?:$|[/?=&-])/.test(destination.pathname + destination.search)) || /limite de resgates excedido|atingiu o limite de resgates|limite mensal|ja utilizou (?:o|seu|este) (?:beneficio|resgate|limite)/.test(text))) { status = 'quota_used'; reason = 'monthly_quota_confirmed'; }
        else if (result.status === 401 || (destination && destination.pathname === '/auth/uol/login') || ((result.status === 200 || REDIRECTS.includes(result.status)) && /<input\b[^>]*type=["']password["']|id=["']dynamic-config["']|\bosirisUai\b/i.test(result.html))) { status = 'auth_required'; reason = 'authentication_required'; }
        else if (result.status === 200 && /(?:ingressos|beneficio|oferta|resgates)\s+(?:estao\s+|esta\s+)?(?:esgotad[oa]s?|indisponive[li]s?|encerrad[oa]s?)/.test(text)) { status = 'sold_out'; reason = 'sold_out_confirmed'; }
        claimed = saveAttempt({ ...claimed, status, reason });
        if (status === 'uncertain') {
          try { claimed = reconcile(claimed, await verifiedHistory(user, client)); } catch (_) { /* Keep uncertainty; a failed read never authorizes a resend. */ }
        }
        return summary(claimed);
      } catch (error) {
        if (claimed) {
          claimed = { ...claimed, status: 'uncertain', reason: 'response_unconfirmed' };
          try { claimed = saveAttempt(claimed); } catch (_) { /* The original durable reservation still blocks dispatch. */ }
          return summary(claimed);
        }
        return safeError(error, user);
      }
    }
    return { history, redeem };
  }
  return { create, COOKIE_KEY, ATTEMPT_KEY };
})();
if (typeof module !== 'undefined' && module.exports) module.exports = HunterRedemption;
