const { test } = require('node:test');
const assert = require('node:assert/strict');
const R = require('./redemption.js');

const CLUB = 'https://clube.uol.com.br';
const START = CLUB + '/auth/uol/login';
const HISTORY = CLUB + '/perfil/beneficios';
const STATE = 'synthetic_state_0123456789';
const CALLBACK = CLUB + '/auth/uol?code=synthetic-code&state=' + STATE;
const OAUTH = 'https://api.uol.com.br/oauth/auth?response_type=code&client_id=synthetic-client&redirect_uri=' + encodeURIComponent(CLUB + '/auth/uol') + '&state=' + STATE;
const NOW = Date.parse('2026-10-18T12:00:00Z');
const NAMES = { leosaquetto: 'Leonardo', gaoriel: 'Gabriel' };

function harness(options = {}) {
  const keys = new Map(), calls = [], events = [], writes = [], loginCalls = [];
  for (const user of Object.keys(NAMES)) {
    keys.set(R.COOKIE_KEY + user, JSON.stringify([{ name: 'SESS', value: 'old-' + user, domain: 'clube.uol.com.br', path: '/', secure: true, httpOnly: true, expires: -1 }]));
    const attempt = JSON.stringify({ account: user, attemptId: '00000000-0000-4000-8000-000000000001', status: 'uncertain', observedAt: NOW,
      baseline: [], offerUrl: CLUB + '/campanhasdeingresso/synthetic-offer', title: 'Synthetic offer' });
    keys.set(R.ATTEMPT_KEY + user, attempt);
    keys.set(R.ATTEMPT_KEY + user + '.00000000-0000-4000-8000-000000000001', attempt);
  }
  const original = new Map(keys);
  let loginFinished = false;
  const env = {
    keychain: { contains: key => keys.has(key), get: key => keys.get(key), set(key, value) { writes.push({ key, value }); events.push('write:' + key); keys.set(key, value); } },
    now: () => NOW,
    expectedName: NAMES,
    authorizeAccount(user) { events.push('authorize:' + user); return !(options.revokeAfterLogin && loginFinished); },
    loadCookies() { assert.fail('reconnect must start with an empty candidate jar'); },
    saveCookies(user) { events.push('save:' + user); },
    verifyIdentity(user, parsed) { const valid = parsed.signedInName === NAMES[user]; events.push('identity:' + valid); return valid; },
    parseHistory(text) { assertOriginal(); return JSON.parse(text); },
    async login(input) {
      assertOriginal();
      loginCalls.push(input); events.push('login');
      assert.equal(input.url, OAUTH);
      assert.equal(typeof input.isCallback, 'function');
      loginFinished = true;
      return Object.hasOwn(options, 'callback') ? options.callback : CALLBACK;
    },
    request(url) {
      const req = { async loadString() {
        assertOriginal();
        assert.equal(req.method, 'GET');
        assert.equal(req.onRedirect(), null);
        calls.push({ url, cookie: req.headers.Cookie }); events.push('request:' + url);
        let statusCode = 200, headers = {}, text = '';
        if (url === START) {
          assert.equal(req.headers.Cookie, '');
          statusCode = 302;
          headers = { Location: OAUTH, 'Set-Cookie': 'PHPSESSID=candidate-state; Path=/; Secure; HttpOnly' };
        } else if (url === CALLBACK) {
          assert.match(req.headers.Cookie, /PHPSESSID=candidate-state/);
          if (options.callbackResponseLost) throw Error('synthetic lost callback response');
          statusCode = 302;
          headers = { Location: options.callbackSelfRedirect ? CALLBACK : CLUB + '/', 'Set-Cookie': 'SESS=candidate-session; Path=/; Secure; HttpOnly' };
        } else if (url === CLUB + '/') {
          text = '<html>synthetic landing</html>';
        } else if (url === HISTORY) {
          assert.match(req.headers.Cookie, /SESS=candidate-session/);
          if (options.historyUnavailable) statusCode = 503;
          else text = JSON.stringify({ authenticated: true, historyScopeFound: true, hasPagination: false, signedInName: options.name || NAMES.leosaquetto, entries: [] });
        } else assert.fail('unexpected native request: ' + url);
        req.response = { statusCode, headers, url };
        return text;
      } };
      return req;
    },
  };
  function assertOriginal() {
    for (const [key, value] of original) assert.equal(keys.get(key), value, 'candidate persisted before proof: ' + key);
  }
  return { client: R.create(env), keys, original, calls, writes, events, loginCalls, assertOriginal };
}

function assertUnrelatedIntact(h) {
  for (const [key, value] of h.original) {
    if (key !== R.COOKIE_KEY + 'leosaquetto') assert.equal(h.keys.get(key), value, key);
  }
  assert.equal(h.calls.some(call => call.url.includes('/resgatar')), false);
}

test('reconnect proves the expected account before persisting HttpOnly cookies without redemption', async () => {
  const h = harness();
  const result = await h.client.reconnect('leosaquetto');
  assert.equal(result.status, 'authenticated');
  assert.equal(result.account, 'leosaquetto');
  assert.equal(h.loginCalls.length, 1);
  assert.equal(h.calls.filter(call => call.url === CALLBACK).length, 1);
  const cookies = JSON.parse(h.keys.get(R.COOKIE_KEY + 'leosaquetto'));
  assert.ok(cookies.some(cookie => cookie.name === 'SESS' && cookie.value === 'candidate-session' && cookie.httpOnly === true));
  assert.equal(cookies.some(cookie => cookie.value.startsWith('old-')), false);
  assert.ok(h.events.indexOf('identity:true') < h.events.indexOf('write:' + R.COOKIE_KEY + 'leosaquetto'));
  const authorizations = h.events.flatMap((event, index) => event === 'authorize:leosaquetto' ? [index] : []);
  assert.ok(authorizations.length >= 2);
  assert.ok(authorizations[0] < h.events.indexOf('login'));
  assert.ok(authorizations.at(-1) > h.events.indexOf('login'));
  assertUnrelatedIntact(h);
});

test('cancelled official login preserves both account jars and the attempt journal', async () => {
  const h = harness({ callback: null });
  const result = await h.client.reconnect('leosaquetto');
  assert.equal(result.status, 'cancelled');
  h.assertOriginal();
  assert.deepEqual(h.calls.map(call => call.url), [START]);
  assert.equal(h.writes.length, 0);
  assertUnrelatedIntact(h);
});

test('wrong state, duplicate code and external callback fail before native callback dispatch', async () => {
  for (const callback of [CALLBACK.replace(STATE, 'different_state_0123456789'), CALLBACK + '&code=duplicate', CALLBACK.replace(CLUB, 'https://example.test')]) {
    const h = harness({ callback });
    const result = await h.client.reconnect('leosaquetto');
    assert.equal(result.status, 'unavailable');
    h.assertOriginal();
    assert.deepEqual(h.calls.map(call => call.url), [START]);
    assert.equal(h.writes.length, 0);
    assertUnrelatedIntact(h);
  }
});

test('a different signed-in account cannot replace the selected account jar', async () => {
  const h = harness({ name: NAMES.gaoriel });
  const result = await h.client.reconnect('leosaquetto');
  assert.equal(result.status, 'identity_mismatch');
  h.assertOriginal();
  assert.equal(h.writes.length, 0);
  assertUnrelatedIntact(h);
});

test('unavailable history preserves the previous cookies after the candidate callback', async () => {
  const h = harness({ historyUnavailable: true });
  const result = await h.client.reconnect('leosaquetto');
  assert.equal(result.status, 'unavailable');
  h.assertOriginal();
  assert.equal(h.writes.length, 0);
  assertUnrelatedIntact(h);
});

test('access revoked after official login cannot commit the candidate cookies', async () => {
  const h = harness({ revokeAfterLogin: true });
  const result = await h.client.reconnect('leosaquetto');
  assert.equal(result.status, 'account_access_required');
  assert.equal(h.loginCalls.length, 1);
  assert.ok(h.events.filter(event => event === 'authorize:leosaquetto').length >= 2);
  h.assertOriginal();
  assert.equal(h.writes.length, 0);
  assertUnrelatedIntact(h);
});

test('a lost callback response never retries the one-time authorization code', async () => {
  const h = harness({ callbackResponseLost: true });
  const result = await h.client.reconnect('leosaquetto');
  assert.equal(result.status, 'unavailable');
  assert.equal(h.calls.filter(call => call.url === CALLBACK).length, 1);
  assert.equal(h.calls.some(call => call.url === HISTORY), false);
  h.assertOriginal();
  assert.equal(h.writes.length, 0);
  assertUnrelatedIntact(h);
});

test('a callback redirect cannot dispatch the same one-time authorization code again', async () => {
  const h = harness({ callbackSelfRedirect: true });
  const result = await h.client.reconnect('leosaquetto');
  assert.equal(result.status, 'unavailable');
  assert.equal(h.calls.filter(call => call.url === CALLBACK).length, 1);
  h.assertOriginal();
  assert.equal(h.writes.length, 0);
  assertUnrelatedIntact(h);
});
