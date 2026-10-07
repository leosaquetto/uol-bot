import test from 'node:test';
import assert from 'node:assert/strict';
import { UolClient, validateReadUrl } from '../src/uol-client.js';

const CLUB = 'https://clube.uol.com.br';
const OFFER = `${CLUB}/campanhasdeingresso/pNG-2-ingressos-08-10-casa-natura-musical-sp`;
const HISTORY = `${CLUB}/perfil/beneficios`;
const HISTORY_HTML = '<html>Olá, LEONARDO!<div id="beneficios">Meus Resgates</div></html>';
const code = (expected) => (error) => error.code === expected && error.message === expected;

function scripted(steps) {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    assert.equal(options.method, 'GET');
    assert.equal(options.redirect, 'manual');
    assert.ok(options.signal instanceof AbortSignal);
    assert.match(options.headers.get('User-Agent'), /Mozilla/);
    assert.ok(steps.length, 'unexpected network request');
    const next = steps.shift();
    if (typeof next === 'function') return next(url, options);
    return new Response(next.body ?? '', { status: next.status ?? 200, headers: next.headers });
  };
  return { calls, fetchImpl };
}

test('read allowlist admits only known fixed pages and safe offer paths', () => {
  for (const url of [CLUB, `${CLUB}/?categoria=ingressosexclusivos`, HISTORY, `${HISTORY}/123`, OFFER,
    `${CLUB}/auth/uol/login`, `${CLUB}/auth/uol?code=opaque&state=opaque`,
    'https://api.uol.com.br/oauth/auth?redirect_uri=https%3A%2F%2Fclube.uol.com.br%2Fauth%2Fuol',
    'https://conta.uol.com.br/login?t=clubeuol&dest=https%3A%2F%2Fclube.uol.com.br%2F']) {
    assert.ok(validateReadUrl(url));
  }
});

test('read allowlist blocks encoded, nested, unknown and confusing destinations before fetch', async () => {
  const net = scripted([]);
  const client = new UolClient({ fetchImpl: net.fetchImpl });
  for (const url of [
    `${OFFER}/resgatar`, `${OFFER}/%72esgatar`, `${OFFER}/%2572esgatar`,
    `${CLUB}/auth/uol/login?redirect_uri=${encodeURIComponent(`${OFFER}/resgatar`)}`,
    `${CLUB}/auth/uol/login?redirect_uri=${encodeURIComponent(encodeURIComponent(`${OFFER}/resgatar`))}`,
    `${CLUB}/auth/uol/login?redirect_uri=https%3A%2F%2Fevil.example%2F`,
    `${CLUB}.evil.example/`, 'https://clube.uol.com.br@evil.example/',
    'https://evil.example@clube.uol.com.br/', `${CLUB}:444/`, `http://clube.uol.com.br/`,
    `${CLUB}/logout`, `${CLUB}/favoritos/update`, `${CLUB}/contato/`,
    `${CLUB}/perfil/beneficios/../beneficios`, `${CLUB}/perfil/%62eneficios`,
    `${OFFER}?next=safe`, `${OFFER}#resgatar`, `${CLUB}/?categoria=outro`,
    `${CLUB}/?categoria=ingressosexclusivos&categoria=ingressosexclusivos`,
    `${CLUB}/%`, `${CLUB}/auth/uol/login?redirect_uri=javascript%3Aalert(1)`,
    `${CLUB}/auth/uol/login?redirect_uri=//evil.example/`,
  ]) {
    assert.throws(() => validateReadUrl(url), code('URL_BLOCKED'));
    await assert.rejects(async () => client.getOffer(url), code('URL_BLOCKED'));
  }
  assert.equal(net.calls.length, 0);
});

test('catalog and offer GETs return bounded page content and response URL', async () => {
  const net = scripted([{ body: '<html>catalog</html>' }, { body: '<html>offer</html>' }]);
  const client = new UolClient({ fetchImpl: net.fetchImpl });
  assert.deepEqual(await client.getCatalog(), { html: '<html>catalog</html>', status: 200, url: `${CLUB}/?categoria=ingressosexclusivos` });
  assert.deepEqual(await client.getOffer(OFFER), { html: '<html>offer</html>', status: 200, url: OFFER });
});

test('network requests preserve the native fetch global receiver', async () => {
  const client = new UolClient({ fetchImpl: async function () {
    assert.equal(this, globalThis);
    return new Response('catalog');
  } });
  assert.equal((await client.getCatalog()).status, 200);
});

test('normal SSO follows safe redirects, scopes cookies, and saves refreshed session', async () => {
  const oauth = 'https://api.uol.com.br/oauth/auth?redirect_uri=https%3A%2F%2Fclube.uol.com.br%2Fauth%2Fuol&state=opaque';
  const net = scripted([
    { status: 302, headers: { Location: oauth, 'Set-Cookie': 'PHPSESSID=club-new; Path=/; Secure; HttpOnly' } },
    { status: 302, headers: { Location: `${CLUB}/auth/uol?code=secret-auth-code&state=opaque` } },
    { status: 302, headers: { Location: '/', 'Set-Cookie': 'SESS=sso-new; Domain=.uol.com.br; Path=/; Max-Age=3600; Secure' } },
    { body: '<html>Olá, LEONARDO!</html>' },
  ]);
  const client = new UolClient({
    fetchImpl: net.fetchImpl, now: () => 1_000_000,
    cookies: [
      { name: 'SESS', value: 'sso-old', domain: '.uol.com.br', path: '/', expires: 2000, secure: true },
      { name: 'PHPSESSID', value: 'club-old', domain: 'clube.uol.com.br', path: '/', expires: -1, secure: true },
      { name: 'DNA', value: 'restricted', domain: 'conta.uol.com.br', path: '/', expires: -1, secure: true },
    ],
  });
  assert.equal((await client.restoreSession()).status, 200);
  assert.equal(net.calls[1].options.headers.get('Cookie'), 'SESS=sso-old');
  assert.match(net.calls[2].options.headers.get('Cookie'), /PHPSESSID=club-new/);
  assert.doesNotMatch(net.calls[2].options.headers.get('Cookie'), /restricted/);
  assert.match(net.calls[3].options.headers.get('Cookie'), /SESS=sso-new/);
  assert.equal(client.cookies.find((item) => item.name === 'SESS').expires, 4600);
  const copy = client.snapshotCookies();
  copy[0].value = 'modified';
  assert.notEqual(client.cookies[0].value, 'modified');
});

test('history restores authentication only once then retries the safe history GET', async () => {
  const net = scripted([
    { status: 401 },
    { status: 302, headers: { Location: '/' } },
    { body: '<html>Olá, LEONARDO!</html>' },
    { body: HISTORY_HTML },
  ]);
  const client = new UolClient({ fetchImpl: net.fetchImpl });
  assert.equal((await client.getHistory()).url, HISTORY);
  assert.deepEqual(net.calls.map((call) => call.url), [HISTORY, `${CLUB}/auth/uol/login`, `${CLUB}/`, HISTORY]);
});

test('history never loops if restored session is still unauthorized', async () => {
  const net = scripted([
    { status: 401 }, { status: 302, headers: { Location: '/' } }, { body: '<html>Home</html>' }, { status: 401 },
  ]);
  const client = new UolClient({ fetchImpl: net.fetchImpl });
  await assert.rejects(client.getHistory(), code('AUTH_REQUIRED'));
  assert.equal(net.calls.length, 4);
});

test('anonymous history redirected to home restores SSO then retries authenticated history once', async () => {
  const net = scripted([
    { status: 302, headers: { Location: '/' } },
    { body: '<html>ENTRAR</html>' },
    { status: 302, headers: { Location: 'https://api.uol.com.br/oauth/auth?redirect_uri=https%3A%2F%2Fclube.uol.com.br%2Fauth%2Fuol' } },
    { status: 302, headers: { Location: `${CLUB}/auth/uol?code=opaque&state=opaque` } },
    { status: 302, headers: { Location: '/' } },
    { body: '<html>Olá, LEONARDO!</html>' },
    { body: HISTORY_HTML },
  ]);
  const client = new UolClient({ fetchImpl: net.fetchImpl });
  assert.equal((await client.getHistory()).url, HISTORY);
  assert.equal(net.calls.length, 7);
  assert.equal(net.calls.filter(call => call.url === HISTORY).length, 2);
  assert.equal(net.calls.filter(call => call.url === `${CLUB}/auth/uol/login`).length, 1);
});

test('history missing authenticated page markers restores once and fails closed', async () => {
  for (const html of ['<h1>Meus Resgates</h1>', '<html>Olá, LEONARDO!</html>', '<html>ENTRAR</html>']) {
    const net = scripted([
      { body: html }, { status: 302, headers: { Location: '/' } },
      { body: '<html>Olá, LEONARDO!</html>' }, { body: html },
    ]);
    await assert.rejects(new UolClient({ fetchImpl: net.fetchImpl }).getHistory(), code('AUTH_REQUIRED'));
    assert.equal(net.calls.length, 4);
  }
});

test('a read redirect to redemption is never followed, including encoded nested auth redirect', async () => {
  for (const destination of [`${OFFER}/resgatar`, `${OFFER}/%2572esgatar`,
    `${CLUB}/auth/uol/login?redirect_uri=${encodeURIComponent(`${OFFER}/resgatar`)}`]) {
    const net = scripted([{ status: 302, headers: { Location: destination } }]);
    const client = new UolClient({ fetchImpl: net.fetchImpl });
    await assert.rejects(client.getOffer(OFFER), code('URL_BLOCKED'));
    assert.equal(net.calls.length, 1);
  }
});

test('redirect loops stop at a strict maximum, and missing destinations fail safely', async () => {
  const loop = scripted(Array.from({ length: 9 }, () => ({ status: 302, headers: { Location: '/' } })));
  await assert.rejects(new UolClient({ fetchImpl: loop.fetchImpl }).getCatalog(), code('REDIRECT_LIMIT'));
  assert.equal(loop.calls.length, 9);
  const missing = scripted([{ status: 302 }]);
  await assert.rejects(new UolClient({ fetchImpl: missing.fetchImpl }).getCatalog(), code('REDIRECT_INVALID'));
});

test('cookie expiration, paths, deletion and malicious domain attributes are respected', async () => {
  let clock = 1_000_000;
  const net = scripted([
    { headers: { 'Set-Cookie': 'SESS=evil; Domain=evil.example; Path=/; Secure' } },
    { headers: { 'Set-Cookie': 'PHPSESSID=deleted; Path=/; Max-Age=0; Expires=Wed, 21 Oct 2030 07:28:00 GMT' } },
    { body: HISTORY_HTML },
    { body: HISTORY_HTML },
  ]);
  const client = new UolClient({ fetchImpl: net.fetchImpl, now: () => clock, cookies: [
    { name: 'SESS', value: 'expired', domain: '.uol.com.br', expires: 500 },
    { name: 'PHPSESSID', value: 'initial', domain: 'clube.uol.com.br', expires: -1 },
    { name: 'DNA', value: 'history-only', domain: 'clube.uol.com.br', path: '/perfil/beneficios', expires: 1001 },
  ] });
  await client.getCatalog();
  await client.getOffer(OFFER);
  await client.getHistory();
  assert.equal(net.calls[0].options.headers.get('Cookie'), 'PHPSESSID=initial');
  assert.equal(net.calls[2].options.headers.get('Cookie'), 'DNA=history-only');
  clock = 1_002_000;
  await client.getHistory();
  assert.equal(net.calls[3].options.headers.get('Cookie'), null);
  assert.deepEqual(client.cookies, []);
});

test('cookies cannot escape their host or inject headers and path prefixes require slash boundaries', async () => {
  assert.throws(() => new UolClient({ cookies: [{ name: 'SESS', value: 'x\r\nInjected: yes', domain: '.uol.com.br' }] }), code('COOKIE_FORMAT'));
  assert.throws(() => new UolClient({ cookies: [{ name: 'SESS', value: 'x', domain: '.example.com' }] }), code('COOKIE_FORMAT'));
  const net = scripted([{ body: HISTORY_HTML }]);
  const client = new UolClient({ fetchImpl: net.fetchImpl, cookies: [
    { name: 'DNA', value: 'no-prefix-leak', domain: 'clube.uol.com.br', path: '/perfil/beneficio' },
  ] });
  await client.getHistory();
  assert.equal(net.calls[0].options.headers.get('Cookie'), null);
});

test('a cookie without Path uses the containing directory and expires at epoch zero', async () => {
  const net = scripted([
    { status: 302, headers: { Location: '/auth/uol?code=opaque', 'Set-Cookie': 'PHPSESSID=auth-only; Secure' } },
    { status: 302, headers: { Location: '/', 'Set-Cookie': 'PHPSESSID=deleted; Path=/auth/uol; Expires=Thu, 01 Jan 1970 00:00:00 GMT' } },
    { body: 'Home' },
  ]);
  const client = new UolClient({ fetchImpl: net.fetchImpl });
  await client.restoreSession();
  assert.equal(net.calls[1].options.headers.get('Cookie'), 'PHPSESSID=auth-only');
  assert.equal(net.calls[2].options.headers.get('Cookie'), null);
  assert.deepEqual(client.cookies, []);
});

test('WAF pages and login requiring authentication fail with safe codes', async () => {
  for (const response of [
    { status: 405, body: '<title>Human Verification</title>' },
    { headers: { 'x-amzn-waf-action': 'captcha' }, body: '<html></html>' },
  ]) {
    const net = scripted([response]);
    await assert.rejects(new UolClient({ fetchImpl: net.fetchImpl }).getCatalog(), code('CHALLENGE_REQUIRED'));
  }
  const login = scripted([
    { status: 302, headers: { Location: 'https://conta.uol.com.br/login?t=clubeuol' } },
    { body: '<script id="dynamic-config">var config={"captcha":true}</script>' },
  ]);
  await assert.rejects(new UolClient({ fetchImpl: login.fetchImpl }).restoreSession(), code('CHALLENGE_REQUIRED'));
});

test('body limits apply to actual streaming bytes and declared size', async () => {
  const stream = scripted([() => new Response(new ReadableStream({ start(controller) {
    controller.enqueue(new Uint8Array(1024 * 1024));
    controller.enqueue(new Uint8Array(1));
    controller.close();
  } }))]);
  await assert.rejects(new UolClient({ fetchImpl: stream.fetchImpl }).getCatalog(), code('RESPONSE_TOO_LARGE'));
  const header = scripted([{ headers: { 'Content-Length': String(1024 * 1024 + 1) }, body: 'small' }]);
  await assert.rejects(new UolClient({ fetchImpl: header.fetchImpl }).getCatalog(), code('RESPONSE_TOO_LARGE'));
});

test('redemption requires exact explicit permit before any request', async () => {
  const net = scripted([]);
  const client = new UolClient({ fetchImpl: net.fetchImpl });
  for (const permit of [undefined, false, 'true', 1, {}]) {
    await assert.rejects(client.redeemOnce(OFFER, { permit }), code('REDEMPTION_NOT_PERMITTED'));
  }
  assert.equal(net.calls.length, 0);
});

test('redemption sends exactly one GET, does not follow redirects, and refuses a second attempt', async () => {
  const net = scripted([{ status: 302, headers: { Location: '/perfil/beneficios/123' } }]);
  const client = new UolClient({ fetchImpl: net.fetchImpl });
  assert.deepEqual(await client.redeemOnce(OFFER, { permit: true }), { status: 302, html: '', location: `${HISTORY}/123` });
  assert.equal(net.calls[0].url, `${OFFER}/resgatar`);
  await assert.rejects(client.redeemOnce(OFFER, { permit: true }), code('REDEMPTION_ALREADY_ATTEMPTED'));
  assert.equal(net.calls.length, 1);
});

test('redemption blocks duplicate redemption redirects and strips auth parameters from returned locations', async () => {
  const duplicate = scripted([{ status: 302, headers: { Location: `${OFFER}/resgatar` } }]);
  const client = new UolClient({ fetchImpl: duplicate.fetchImpl });
  await assert.rejects(client.redeemOnce(OFFER, { permit: true }), code('URL_BLOCKED'));
  assert.equal(duplicate.calls.length, 1);
  const auth = scripted([{ status: 302, headers: { Location: '/auth/uol?code=very-secret&state=private-state' } }]);
  const result = await new UolClient({ fetchImpl: auth.fetchImpl }).redeemOnce(OFFER, { permit: true });
  assert.equal(result.location, `${CLUB}/auth/uol`);
  assert.doesNotMatch(JSON.stringify(result), /very-secret|private-state/);
});

test('a redemption timeout or network failure is never retried and secrets do not enter errors', async () => {
  for (const thrown of [Object.assign(new Error('secret-url-and-cookie'), { name: 'AbortError' }), new Error('secret-url-and-cookie')]) {
    const net = scripted([() => { throw thrown; }]);
    const client = new UolClient({ fetchImpl: net.fetchImpl });
    await assert.rejects(client.redeemOnce(OFFER, { permit: true }), code(thrown.name === 'AbortError' ? 'REQUEST_TIMEOUT' : 'REQUEST_FAILED'));
    await assert.rejects(client.redeemOnce(OFFER, { permit: true }), code('REDEMPTION_ALREADY_ATTEMPTED'));
    assert.equal(net.calls.length, 1);
  }
});
