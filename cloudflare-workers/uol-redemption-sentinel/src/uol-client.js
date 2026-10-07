const CLUB = 'https://clube.uol.com.br';
const CATALOG = `${CLUB}/?categoria=ingressosexclusivos`;
const HISTORY = `${CLUB}/perfil/beneficios`;
const LOGIN = `${CLUB}/auth/uol/login`;
const MAX_BODY_BYTES = 1024 * 1024;
const TIMEOUT_MS = 15_000;
const MAX_REDIRECTS = 8;
const COOKIE_NAMES = new Set(['SESS', 'JS_SESS', 'DNA', 'PHPSESSID', 'AWSALB', 'AWSALBCORS']);
const HOSTS = new Set(['clube.uol.com.br', 'api.uol.com.br', 'conta.uol.com.br']);
const COOKIE_DOMAINS = new Set([...HOSTS, 'uol.com.br']);
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
export const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36';

export class UolClientError extends Error {
  constructor(code) {
    super(code);
    this.name = 'UolClientError';
    this.code = code;
  }
}

function fail(code) { throw new UolClientError(code); }

function decoded(value) {
  let result = value;
  for (let i = 0; i < 8; i += 1) {
    if (!result.includes('%')) return result;
    let next;
    try { next = decodeURIComponent(result); } catch { fail('URL_BLOCKED'); }
    if (next === result) return result;
    result = next;
  }
  if (result.includes('%')) fail('URL_BLOCKED');
  return result;
}

function onlyKeys(url, keys) {
  const found = [...url.searchParams.keys()];
  return found.every((key) => keys.includes(key)) && new Set(found).size === found.length;
}

/** Every redirect and nested destination is checked before it can reach fetch. */
export function validateReadUrl(input, base = CLUB, depth = 0) {
  if (depth > 8 || typeof input !== 'string' || input.length > 16_384) fail('URL_BLOCKED');
  const plain = decoded(input);
  if (/[\u0000-\u0020\u007f\\]/.test(input) || /[\u0000-\u001f\u007f\\]/.test(plain)
    || /resgatar|logout|favoritos|\/contato(?:[/?#]|$)/i.test(plain)
    || /(?:^|\/)\.{1,2}(?:\/|[?#]|$)/.test(plain)) fail('URL_BLOCKED');
  let url;
  try { url = new URL(input, base); } catch { fail('URL_BLOCKED'); }
  if (url.protocol !== 'https:' || !HOSTS.has(url.hostname) || url.port || url.username
    || url.password || url.hash || /%/.test(url.pathname)) fail('URL_BLOCKED');
  let allowed = false;
  if (url.origin === CLUB) {
    if (url.pathname === '/') {
      allowed = !url.search || (onlyKeys(url, ['categoria']) && url.searchParams.get('categoria') === 'ingressosexclusivos');
    } else if (/^\/perfil\/beneficios(?:\/[A-Za-z0-9_-]+)?$/.test(url.pathname)
      || /^\/campanhasdeingresso\/[A-Za-z0-9][A-Za-z0-9_-]*$/.test(url.pathname)) {
      allowed = !url.search;
    } else if (url.pathname === '/auth/uol/login') {
      allowed = onlyKeys(url, ['redirect_uri']);
    } else if (url.pathname === '/auth/uol') {
      allowed = onlyKeys(url, ['code', 'state', 'error', 'error_description']);
    }
  } else if (url.origin === 'https://api.uol.com.br' && url.pathname === '/oauth/auth') {
    allowed = onlyKeys(url, ['response_type', 'client_id', 'redirect_uri', 'state', 't', 'scope']);
  } else if (url.origin === 'https://conta.uol.com.br' && url.pathname === '/login') {
    allowed = onlyKeys(url, ['t', 'dest', 'request_id']);
  }
  if (!allowed) fail('URL_BLOCKED');
  for (const [key, value] of url.searchParams) {
    const clear = decoded(value);
    if (/resgatar|logout|favoritos|\/contato(?:[/?#]|$)/i.test(clear)) fail('URL_BLOCKED');
    if (['dest', 'redirect_uri'].includes(key) || /^(?:https?:|\/)/i.test(clear)) {
      if (!clear || !/^(?:https:\/\/|\/)/i.test(clear)) fail('URL_BLOCKED');
      validateReadUrl(clear, url.origin, depth + 1);
    }
  }
  return url;
}

function domainMatches(host, domain) { return host === domain || host.endsWith(`.${domain}`); }
function pathMatches(path, cookiePath) {
  return path === cookiePath || (path.startsWith(cookiePath) && (cookiePath.endsWith('/') || path[cookiePath.length] === '/'));
}

class CookieJar {
  constructor(cookies, now) {
    this.now = now;
    this.items = [];
    if (!Array.isArray(cookies)) fail('COOKIE_FORMAT');
    for (const cookie of cookies) this.put(cookie);
  }

  put(cookie) {
    if (!cookie || !COOKIE_NAMES.has(cookie.name)) return;
    const originalDomain = cookie.domain ?? 'clube.uol.com.br';
    const domain = typeof originalDomain === 'string' ? originalDomain.replace(/^\./, '').toLowerCase() : '';
    const path = cookie.path ?? '/';
    if (!COOKIE_DOMAINS.has(domain) || typeof cookie.value !== 'string'
      || /[\u0000-\u0020\u007f;]/.test(cookie.value) || typeof path !== 'string'
      || !path.startsWith('/') || /[\u0000-\u0020\u007f;]/.test(path)) fail('COOKIE_FORMAT');
    const rawExpiry = cookie.expires ?? cookie.expirationDate ?? -1;
    const expires = Number(rawExpiry);
    if (!Number.isFinite(expires)) fail('COOKIE_FORMAT');
    const stored = {
      name: cookie.name, value: cookie.value, domain, path,
      hostOnly: cookie.hostOnly ?? !originalDomain.startsWith('.'),
      secure: cookie.secure !== false,
      expires,
    };
    this.items = this.items.filter((item) => !(item.name === stored.name && item.domain === domain && item.path === path));
    if (expires >= 0 && expires * 1000 <= this.now()) return;
    this.items.push(stored);
  }

  absorb(headers, responseUrl) {
    let lines = [];
    if (typeof headers.getSetCookie === 'function') lines = headers.getSetCookie();
    else if (typeof headers.getAll === 'function') lines = headers.getAll('Set-Cookie');
    else {
      const combined = headers.get('set-cookie');
      if (combined) lines = combined.split(/,(?=\s*[^;,=\s]+=[^;]*)/);
    }
    const host = responseUrl.hostname;
    for (const line of lines) {
      const segments = line.split(';').map((part) => part.trim());
      const equal = segments[0].indexOf('=');
      if (equal < 1) continue;
      const name = segments[0].slice(0, equal);
      if (!COOKIE_NAMES.has(name)) continue;
      const lastSlash = responseUrl.pathname.lastIndexOf('/');
      const cookie = {
        name, value: segments[0].slice(equal + 1), domain: host, hostOnly: true,
        path: lastSlash <= 0 ? '/' : responseUrl.pathname.slice(0, lastSlash),
        secure: false, expires: -1,
      };
      let maxAge;
      let invalidDomain = false;
      for (const segment of segments.slice(1)) {
        const split = segment.indexOf('=');
        const key = (split < 0 ? segment : segment.slice(0, split)).toLowerCase();
        const value = split < 0 ? '' : segment.slice(split + 1);
        if (key === 'domain') {
          const domain = value.replace(/^\./, '').toLowerCase();
          if (!COOKIE_DOMAINS.has(domain) || !domainMatches(host, domain)) invalidDomain = true;
          cookie.domain = domain;
          cookie.hostOnly = false;
        } else if (key === 'path' && value.startsWith('/')) cookie.path = value;
        else if (key === 'secure') cookie.secure = true;
        else if (key === 'max-age' && /^-?\d+$/.test(value)) maxAge = Number(value);
        else if (key === 'expires' && Number.isFinite(Date.parse(value))) cookie.expires = Date.parse(value) / 1000;
      }
      if (invalidDomain) continue;
      if (maxAge !== undefined) cookie.expires = maxAge <= 0 ? 1 : this.now() / 1000 + maxAge;
      this.put(cookie);
    }
  }

  header(url) {
    this.items = this.items.filter((cookie) => cookie.expires < 0 || cookie.expires * 1000 > this.now());
    return this.items.filter((cookie) => (cookie.hostOnly ? url.hostname === cookie.domain : domainMatches(url.hostname, cookie.domain))
      && pathMatches(url.pathname, cookie.path) && (!cookie.secure || url.protocol === 'https:'))
      .sort((a, b) => b.path.length - a.path.length).map((cookie) => `${cookie.name}=${cookie.value}`).join('; ');
  }

  snapshot() {
    this.items = this.items.filter((cookie) => cookie.expires < 0 || cookie.expires * 1000 > this.now());
    return this.items.map((cookie) => ({ ...cookie }));
  }
}

async function boundedText(response) {
  if (Number(response.headers.get('content-length')) > MAX_BODY_BYTES) {
    await response.body?.cancel().catch(() => {});
    fail('RESPONSE_TOO_LARGE');
  }
  if (!response.body) return '';
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let bytes = 0;
  let text = '';
  try {
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      bytes += part.value.byteLength;
      if (bytes > MAX_BODY_BYTES) {
        await reader.cancel().catch(() => {});
        fail('RESPONSE_TOO_LARGE');
      }
      text += decoder.decode(part.value, { stream: true });
    }
    return text + decoder.decode();
  } finally { reader.releaseLock(); }
}

function isChallenge(response) {
  return /captcha|challenge/i.test(response.wafAction ?? '')
    || /<title[^>]*>\s*Human Verification|verify you are human|verifique que voc[eê] [eé] humano/i.test(response.html);
}

function needsAuthentication(result) {
  return [401, 403].includes(result.status) || new URL(result.url).origin === 'https://conta.uol.com.br'
    || /<input\b[^>]*type=["']password["']|id=["']dynamic-config["']|\bosirisUai\b/i.test(result.html);
}

function historyNeedsAuthentication(result) {
  return needsAuthentication(result) || result.url !== HISTORY
    || (result.status === 200 && (!/Meus(?:\s|<[^>]+>)+Resgates/i.test(result.html)
      || !/Ol(?:á|a|&aacute;|&#225;|&#x[eE]1;),[\s\S]{1,300}!/i.test(result.html)));
}

export class UolClient {
  #jar;
  #fetch;
  #userAgent;
  #attempted = false;

  constructor({ cookies = [], fetchImpl = globalThis.fetch, userAgent = DEFAULT_USER_AGENT, now = Date.now } = {}) {
    if (typeof fetchImpl !== 'function' || typeof now !== 'function' || typeof userAgent !== 'string'
      || /[\r\n]/.test(userAgent)) fail('CLIENT_CONFIGURATION');
    this.#jar = new CookieJar(cookies, now);
    // Native Workers fetch requires its global receiver, unlike our test fakes.
    this.#fetch = fetchImpl.bind(globalThis);
    this.#userAgent = userAgent;
  }

  /** Contains secrets. Persist privately; never serialize in logs or public status. */
  get cookies() { return this.#jar.snapshot(); }
  snapshotCookies() { return this.#jar.snapshot(); }

  async #request(url) {
    const controller = new AbortController();
    let stage = 'headers';
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new UolClientError('REQUEST_TIMEOUT'));
      }, TIMEOUT_MS);
    });
    try {
      return await Promise.race([(async () => {
        const headers = new Headers({ 'User-Agent': this.#userAgent, Accept: 'text/html,application/xhtml+xml', 'Cache-Control': 'no-cache' });
        const cookie = this.#jar.header(url);
        if (cookie) headers.set('Cookie', cookie);
        stage = 'fetch';
        const response = await this.#fetch(url.href, {
          method: 'GET', headers, redirect: 'manual', signal: controller.signal,
        });
        stage = 'cookies';
        this.#jar.absorb(response.headers, url);
        stage = 'body';
        const html = await boundedText(response);
        return {
          html, url: url.href, status: response.status,
          location: response.headers.get('location'),
          wafAction: response.headers.get('x-amzn-waf-action'),
        };
      })(), timeout]);
    } catch (error) {
      if (error instanceof UolClientError) throw error;
      if (controller.signal.aborted || error?.name === 'AbortError') fail('REQUEST_TIMEOUT');
      const failure = new UolClientError('REQUEST_FAILED');
      failure.stage = stage;
      failure.causeCode = /illegal invocation/i.test(error?.message || '') ? 'illegal_invocation' : 'runtime_error';
      throw failure;
    } finally { clearTimeout(timer); }
  }

  async #read(input) {
    let url = validateReadUrl(input);
    for (let redirects = 0; redirects <= MAX_REDIRECTS; redirects += 1) {
      const response = await this.#request(url);
      if (isChallenge(response)) fail('CHALLENGE_REQUIRED');
      if (!REDIRECT_STATUSES.has(response.status)) return { html: response.html, url: response.url, status: response.status };
      if (!response.location) fail('REDIRECT_INVALID');
      url = validateReadUrl(response.location, url.href);
      if (redirects === MAX_REDIRECTS) fail('REDIRECT_LIMIT');
    }
    fail('REDIRECT_LIMIT');
  }

  getCatalog() { return this.#read(CATALOG); }

  getOffer(input) {
    const url = validateReadUrl(input);
    if (url.origin !== CLUB || !/^\/campanhasdeingresso\/[A-Za-z0-9][A-Za-z0-9_-]*$/.test(url.pathname)) fail('URL_BLOCKED');
    return this.#read(url.href);
  }

  async restoreSession() {
    const result = await this.#read(LOGIN);
    if (needsAuthentication(result)) {
      if (/["']captcha["']\s*:\s*true/.test(result.html)) fail('CHALLENGE_REQUIRED');
      fail('AUTH_REQUIRED');
    }
    if (result.status !== 200 || new URL(result.url).origin !== CLUB
      || new URL(result.url).pathname.startsWith('/auth/')) fail('AUTH_REQUIRED');
    return result;
  }

  async getHistory() {
    let result = await this.#read(HISTORY);
    if (historyNeedsAuthentication(result)) {
      await this.restoreSession();
      result = await this.#read(HISTORY);
    }
    if (historyNeedsAuthentication(result) || result.status !== 200) fail('AUTH_REQUIRED');
    return result;
  }

  async redeemOnce(input, { permit } = {}) {
    if (permit !== true) fail('REDEMPTION_NOT_PERMITTED');
    if (this.#attempted) fail('REDEMPTION_ALREADY_ATTEMPTED');
    const offer = validateReadUrl(input);
    if (offer.origin !== CLUB || !/^\/campanhasdeingresso\/[A-Za-z0-9][A-Za-z0-9_-]*$/.test(offer.pathname)) fail('URL_BLOCKED');
    const target = new URL(`${offer.pathname}/resgatar`, CLUB);
    this.#attempted = true;
    const response = await this.#request(target);
    if (isChallenge(response)) fail('CHALLENGE_REQUIRED');
    const result = { status: response.status, html: response.html };
    if (response.location) {
      const location = validateReadUrl(response.location, target.href);
      // The caller reconciles history separately. Authentication codes never leave this response.
      result.location = `${location.origin}${location.pathname}`;
    }
    return result;
  }
}
