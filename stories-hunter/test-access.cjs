const { test } = require('node:test');
const assert = require('node:assert/strict');
const A = require('./access.js');
const C = require('./collector.js');
const ORIGIN = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev';
const NOW = Date.parse('2026-10-18T12:00:00Z');
const TOKEN = 'h1.existing-phone.' + 'a'.repeat(64);
const session = { user_agent: 'synthetic-browser', cookies: [{ name: 'sessionid', value: 'private-test-session', domain: '.instagram.com' }] };
function harness({ existing = false, instagram = false } = {}) {
  let time = NOW, counter = 0, responder;
  const keys = new Map(), calls = [];
  if (existing) { keys.set(A.KEYS.device, 'existing-phone'); keys.set(A.KEYS.token, TOKEN); }
  if (instagram) keys.set(A.KEYS.instagram, JSON.stringify(session));
  const env = { keychain: { contains: key => keys.has(key), get: key => keys.get(key), set: (key, value) => keys.set(key, value) },
    now: () => time, uuid: () => '00000000-0000-4000-8000-' + String(++counter).padStart(12, '0'), normalizeSession: C.normalizeSession,
    request: url => { const req = { async loadString() {
      calls.push({ url, method: req.method, headers: req.headers, body: req.body, timeout: req.timeoutInterval, redirect: req.onRedirect() });
      const result = await responder(url, req);
      req.response = { statusCode: result.code || 200, headers: { 'Content-Type': result.type || 'application/json' }, url: result.url || url };
      return result.text !== undefined ? result.text : JSON.stringify(result.value);
    } }; return req; } };
  const state = (state = 'pending', extra = {}) => ({ ok: true, state, deviceId: keys.get(A.KEYS.device), label: 'iPhone de teste', code: 'ABCD1234', role: 'member', ...extra });
  responder = url => url.endsWith('/config') ? { value: { ok: true, deviceId: keys.get(A.KEYS.device), role: 'member', instagramSession: session } } : { value: state() };
  return { env, keys, calls, state, client: A.create(env), advance: ms => { time += ms; }, setResponder: callback => { responder = callback; } };
}
function privateFree(value) {
  const text = JSON.stringify(value);
  assert.doesNotMatch(text, /h1\.|private-test-session|user_agent|cookies|Authorization|instagramSession/);
}

test('unconfigured status makes no request and creates no credentials', async () => {
  const h = harness();
  assert.deepEqual(await h.client.status(), { status: 'not_configured', reason: 'activation_required' });
  assert.equal(h.calls.length, 0); assert.equal(h.keys.size, 0);
});

test('request persists immutable device credentials and label before POST, then resumes a lost acceptance', async () => {
  const h = harness(); let accepted;
  h.setResponder((url, req) => {
    assert.equal(url, ORIGIN + '/hunter-access/request');
    assert.match(h.keys.get(A.KEYS.token), /^h1\.phone-[a-f0-9]{24}\.[a-f0-9]{64}$/);
    assert.equal(req.headers.Authorization, 'Bearer ' + h.keys.get(A.KEYS.token));
    accepted = { body: req.body, token: h.keys.get(A.KEYS.token), deviceId: h.keys.get(A.KEYS.device) };
    throw new Error('response lost with private data');
  });
  assert.equal((await h.client.requestAccess('iPhone de teste')).status, 'unavailable');
  h.setResponder((url, req) => {
    assert.equal(req.body, accepted.body); assert.equal(h.keys.get(A.KEYS.token), accepted.token);
    return { code: 202, value: h.state() };
  });
  const result = await A.create(h.env).requestAccess('rótulo editado');
  assert.equal(result.status, 'access_pending'); assert.equal(result.deviceId, accepted.deviceId);
  assert.equal(h.calls.length, 2); assert.equal(h.calls[0].timeout, 20); assert.equal(h.calls[0].redirect, null);
  privateFree(result);
});

test('existing valid h1 identity is reused and concurrent requests converge', async () => {
  const h = harness({ existing: true });
  const [first, second] = await Promise.all([h.client.requestAccess('iPhone'), h.client.requestAccess('Outro')]);
  assert.deepEqual(first, second); assert.equal(h.calls.length, 1); assert.equal(h.keys.get(A.KEYS.token), TOKEN);
  await h.client.requestAccess('Tentativa repetida'); assert.equal(h.calls.length, 1, 'accepted pending request is cached');
});

test('pending status is cached for at least 120 seconds and never fetches Instagram configuration', async () => {
  const h = harness({ existing: true });
  assert.equal((await h.client.status()).status, 'access_pending');
  h.advance(119999); await A.create(h.env).status(); assert.equal(h.calls.length, 1);
  h.advance(1); await A.create(h.env).status(); assert.equal(h.calls.length, 2);
  assert.ok(h.calls.every(call => call.url.endsWith('/status')));
  assert.equal(h.keys.has(A.KEYS.instagram), false);
});

test('approval validates and stores private configuration once, returns only a safe summary', async () => {
  const h = harness({ existing: true });
  h.setResponder(url => ({ value: url.endsWith('/config')
    ? { ok: true, deviceId: 'existing-phone', role: 'member', instagramSession: session }
    : h.state('approved') }));
  const result = await h.client.status();
  assert.equal(result.status, 'approved'); assert.equal(h.calls.length, 2);
  assert.equal(JSON.parse(h.keys.get(A.KEYS.instagram)).cookies[0].value, 'private-test-session');
  privateFree(result);
  await A.create(h.env).status(); assert.equal(h.calls.length, 2, 'fresh approved cache survives restart');
});

test('approval never replaces an existing Instagram session', async () => {
  const h = harness({ existing: true, instagram: true }); const original = h.keys.get(A.KEYS.instagram);
  h.setResponder(() => ({ value: h.state('approved') }));
  assert.equal((await h.client.status()).status, 'approved');
  assert.equal(h.calls.length, 1); assert.equal(h.keys.get(A.KEYS.instagram), original);
});

test('revocation is cached for 30 minutes, never downloads configuration, and preserves local session', async () => {
  const h = harness({ existing: true, instagram: true }); const original = h.keys.get(A.KEYS.instagram);
  h.setResponder(() => ({ value: h.state('revoked') }));
  assert.equal((await h.client.status()).status, 'access_revoked');
  h.advance(1799999); await A.create(h.env).status(); assert.equal(h.calls.length, 1);
  h.advance(1); await A.create(h.env).status(); assert.equal(h.calls.length, 2);
  assert.equal(h.keys.get(A.KEYS.instagram), original);
});

test('explicit refresh observes revocation before the approval TTL and expired requests stay denied', async () => {
  const h = harness({ existing: true, instagram: true }); h.setResponder(() => ({ value: h.state('approved') }));
  await h.client.status(); h.setResponder(() => ({ value: h.state('revoked') }));
  assert.equal((await h.client.status({ refresh: true })).status, 'access_revoked');
  assert.equal((await A.create(h.env).status()).status, 'access_revoked');
  h.setResponder(() => ({ value: h.state('expired') }));
  assert.equal((await h.client.status({ refresh: true })).status, 'access_expired');
  assert.ok(h.calls.every(call => !call.url.endsWith('/config')));
});

test('expired approval requests can be renewed with the same credentials before the expired cache TTL', async () => {
  const h = harness({ existing: true });
  h.setResponder(() => ({ value: h.state('expired') }));
  assert.equal((await h.client.status()).status, 'access_expired');
  h.setResponder((url, req) => {
    assert.equal(url, ORIGIN + '/hunter-access/request');
    assert.equal(req.headers.Authorization, 'Bearer ' + TOKEN);
    return { code: 202, value: h.state('pending') };
  });
  assert.equal((await A.create(h.env).requestAccess('Novo nome')).status, 'access_pending');
  assert.equal(h.keys.get(A.KEYS.device), 'existing-phone');
  assert.equal(h.keys.get(A.KEYS.token), TOKEN);
  assert.deepEqual(JSON.parse(h.calls.at(-1).body), { label: 'iPhone de teste' });
});

test('approved status retries a lost configuration response without creating a new identity or partial session', async () => {
  const h = harness({ existing: true }); let statusCalls = 0, configCalls = 0;
  h.setResponder(url => {
    if (url.endsWith('/status')) { statusCalls++; return { value: h.state('approved') }; }
    configCalls++;
    if (configCalls === 1) throw new Error('private-test-session response lost');
    return { value: { ok: true, deviceId: 'existing-phone', role: 'member', instagramSession: session } };
  });
  const first = await h.client.status();
  assert.equal(first.status, 'unavailable'); privateFree(first);
  assert.equal(h.keys.has(A.KEYS.instagram), false);
  const result = await A.create(h.env).status();
  assert.equal(result.status, 'approved'); privateFree(result);
  assert.equal(statusCalls, 1, 'saved approval can resume only the missing configuration');
  assert.equal(configCalls, 2);
  assert.equal(h.keys.get(A.KEYS.token), TOKEN);
  assert.equal(JSON.parse(h.keys.get(A.KEYS.instagram)).cookies[0].value, 'private-test-session');
});

test('approved cache survives network loss only inside its six-hour lifetime; stale approval fails closed', async () => {
  const h = harness({ existing: true, instagram: true }); h.setResponder(() => ({ value: h.state('approved') }));
  await h.client.status(); h.setResponder(() => { throw new Error('network'); });
  h.advance(1000); assert.equal((await h.client.status({ refresh: true })).status, 'approved');
  h.advance(A.TTL.approved); const result = await A.create(h.env).status();
  assert.equal(result.status, 'unavailable'); privateFree(result);
});

test('invalid state and device mismatch fail closed even after a fresh approval', async () => {
  const h = harness({ existing: true, instagram: true }); h.setResponder(() => ({ value: h.state('approved') })); await h.client.status();
  h.setResponder(() => ({ value: h.state('administrator') }));
  assert.equal((await h.client.status({ refresh: true })).status, 'unavailable');
  assert.equal((await A.create(h.env).status()).status, 'unavailable');
  h.keys.set(A.KEYS.device, 'different-phone'); const calls = h.calls.length;
  assert.equal((await h.client.status()).reason, 'device_mismatch'); assert.equal(h.calls.length, calls);
});

test('malformed or unsafe configuration cannot grant access or persist a session', async () => {
  for (const config of [{ ok: true, deviceId: 'other', role: 'member', instagramSession: session },
    { ok: true, deviceId: 'existing-phone', role: 'owner', instagramSession: session },
    { ok: true, deviceId: 'existing-phone', role: 'member', instagramSession: { user_agent: 'x', cookies: [] } }]) {
    const h = harness({ existing: true });
    h.setResponder(url => ({ value: url.endsWith('/config') ? config : h.state('approved') }));
    const result = await h.client.status(); assert.equal(result.status, 'unavailable');
    assert.equal(h.keys.has(A.KEYS.instagram), false); privateFree(result);
  }
});

test('HTTP, redirect, content type and size failures return sanitized failures', async () => {
  for (const response of [{ code: 403, value: { secret: 'private-test-session' } }, { code: 302, value: {} },
    { url: 'https://example.test/redirect', value: {} }, { type: 'text/html', text: '<html>private-test-session</html>' },
    { text: JSON.stringify({ secret: 'x'.repeat(65536) }) }, { text: '{not-json' }]) {
    const h = harness({ existing: true }); h.setResponder(() => response);
    const result = await h.client.status(); assert.equal(result.status, 'unavailable'); privateFree(result);
  }
  const h = harness({ existing: true }); h.setResponder(() => ({ code: 404, value: {} }));
  assert.equal((await h.client.status()).status, 'not_configured');
});

test('owner devices and decisions strip private fields; member cannot call owner routes', async () => {
  const h = harness({ existing: true, instagram: true });
  h.setResponder(url => {
    if (url.endsWith('/devices')) return { value: { ok: true, devices: [
      { ...h.state('pending'), deviceId: 'friend', requestedAt: NOW, token: TOKEN, session },
      { ...h.state('approved'), deviceId: 'legacy', requestedAt: 0, token: TOKEN, session },
    ] } };
    if (url.endsWith('/decision')) return { value: { ...h.state('approved'), deviceId: 'friend', token: TOKEN, session } };
    return { value: h.state('approved', { role: 'owner' }) };
  });
  const devices = await h.client.devices(); assert.equal(devices.ok, true); assert.equal(devices.devices[0].deviceId, 'friend'); privateFree(devices);
  assert.equal(devices.devices[1].requestedAt, 0, 'static approved devices remain manageable without a fabricated timestamp');
  const result = await h.client.decide('friend', 'approve', 'ABCD1234'); assert.equal(result.ok, true); privateFree(result);
  assert.deepEqual(JSON.parse(h.calls.at(-1).body), { deviceId: 'friend', decision: 'approve', code: 'ABCD1234' });
  const member = harness({ existing: true, instagram: true }); member.setResponder(() => ({ value: member.state('approved') }));
  assert.equal((await member.client.devices()).reason, 'owner_required'); assert.equal(member.calls.length, 1);
});

test('bad labels, invalid existing credentials and non-UUID random data do not create requests', async () => {
  const h = harness();
  assert.equal((await h.client.requestAccess('<bad>')).reason, 'device_label_invalid');
  assert.equal((await h.client.requestAccess('a'.repeat(61))).reason, 'device_label_invalid');
  h.env.uuid = () => 'not-a-secure-uuid';
  assert.equal((await A.create(h.env).requestAccess('iPhone')).reason, 'random_unavailable');
  h.keys.set(A.KEYS.token, 'malformed');
  assert.equal((await A.create(h.env).requestAccess('iPhone')).reason, 'device_credentials_invalid');
  assert.equal(h.calls.length, 0);
});

const accountConfig = (h, account = 'leosaquetto', extra = {}) => ({ ok: true, deviceId: 'existing-phone', account, identityDigest: 'a'.repeat(64), cookies: [
  { name: 'SESS', value: 'private-uol-session', domain: '.uol.com.br', path: '/', hostOnly: false, secure: true, httpOnly: true, expires: -1, sameSite: 'None' },
], ...extra });

test('technical approval defaults to no personal grants and never downloads account cookies', async () => {
  const h = harness({ existing: true, instagram: true }); h.setResponder(() => ({ value: h.state('approved') }));
  const state = await h.client.status(); assert.deepEqual(state.accounts, []); privateFree(state);
  const denied = await h.client.accountConfig('leosaquetto');
  assert.deepEqual(denied, { ok: false, reason: 'account_access_denied' });
  assert.ok(h.calls.every(call => call.url.endsWith('/status')));
  assert.equal((await h.client.accountConfig('other')).reason, 'account_invalid');
  assert.equal(h.calls.length, 2);
});

test('explicit account grant downloads private configuration only for the native caller', async () => {
  const h = harness({ existing: true, instagram: true });
  h.setResponder(url => ({ value: url.includes('/account-config?') ? accountConfig(h) : h.state('approved', { accounts: ['leosaquetto'] }) }));
  const result = await h.client.accountConfig('leosaquetto');
  assert.equal(result.ok, true); assert.equal(result.account, 'leosaquetto'); assert.equal(result.identityDigest, 'a'.repeat(64));
  assert.equal(result.cookies[0].value, 'private-uol-session'); assert.equal(result.cookies[0].httpOnly, true);
  assert.equal(h.calls[1].url, ORIGIN + '/hunter-access/account-config?account=leosaquetto');
  assert.equal(h.calls[1].headers.Authorization, 'Bearer ' + TOKEN);
  assert.equal(h.keys.size, 4, 'only device/token/technical session/access summary are persisted');
  const saved = JSON.parse(h.keys.get(A.KEYS.access)); assert.deepEqual(saved.accounts, ['leosaquetto']); privateFree(saved);
  assert.ok(!JSON.stringify([...h.keys.values()]).includes('private-uol-session'));
  const state = await h.client.status(); privateFree(state); assert.deepEqual(state.accounts, ['leosaquetto']);
  result.cookies[0].value = 'mutated'; assert.equal((await h.client.status()).accounts[0], 'leosaquetto');
});

test('invalid account grant lists fail closed without using existing approved cache', async () => {
  for (const accounts of [null, ['other'], ['leosaquetto', 'leosaquetto'], ['leosaquetto', 'gaoriel', 'other'], 'leosaquetto']) {
    const h = harness({ existing: true, instagram: true }); h.setResponder(() => ({ value: h.state('approved', { accounts }) }));
    assert.equal((await h.client.status()).status, 'unavailable');
    assert.equal((await h.client.accountConfig('leosaquetto')).ok, false);
    assert.ok(h.calls.every(call => call.url.endsWith('/status')));
  }
});

test('wrong account, device, digest, extra fields and unsafe cookies never leave account-config', async () => {
  for (const change of [
    { account: 'gaoriel' }, { deviceId: 'different-phone' }, { identityDigest: 'missing' }, { identityDigest: null }, { secret: 'private-uol-session' },
    { cookies: [] }, { cookies: [{ name: 'password', value: 'private-uol-session', domain: '.uol.com.br', path: '/' }] },
    { cookies: [{ name: 'SESS', value: 'private-uol-session', domain: '.evil.example', path: '/' }] },
    { cookies: [{ name: 'SESS', value: 'private-uol-session', domain: '.uol.com.br', path: '/\r\nHeader' }] },
    { cookies: [{ name: 'SESS', value: 'private-uol-session; injected=true', domain: '.uol.com.br', path: '/' }] },
    { cookies: [{ name: 'SESS', value: 'private-uol-session', domain: '.uol.com.br', path: '/', storeId: 'arbitrary' }] },
    { cookies: [{ name: 'SESS', value: 'private-uol-session', domain: '.uol.com.br', path: '/', secure: 'false' }] },
    { cookies: [{ name: 'SESS', value: 'private-uol-session', domain: '.uol.com.br', path: '/', expires: -1, expirationDate: NOW / 1000 }] },
  ]) {
    const h = harness({ existing: true, instagram: true });
    h.setResponder(url => ({ value: url.includes('/account-config?') ? accountConfig(h, 'leosaquetto', change) : h.state('approved', { accounts: ['leosaquetto'] }) }));
    const result = await h.client.accountConfig('leosaquetto');
    assert.equal(result.ok, false); assert.equal(result.reason, 'account_config_invalid');
    assert.ok(!JSON.stringify(result).includes('private-uol-session'));
    assert.ok(!JSON.stringify([...h.keys.values()]).includes('private-uol-session'));
  }
});

test('personal configuration refresh observes revoked grants before downloading and rejects forbidden responses', async () => {
  const h = harness({ existing: true, instagram: true });
  h.setResponder(() => ({ value: h.state('approved', { accounts: ['leosaquetto'] }) })); await h.client.status();
  h.setResponder(() => ({ value: h.state('approved', { accounts: [] }) }));
  assert.equal((await h.client.accountConfig('leosaquetto')).reason, 'account_access_denied');
  assert.ok(h.calls.every(call => call.url.endsWith('/status')));
  for (const response of [{ code: 403, value: {} }, { type: 'text/html', text: 'private-uol-session' }, { text: JSON.stringify({ cookies: 'x'.repeat(65536) }) }]) {
    const denied = harness({ existing: true, instagram: true });
    denied.setResponder(url => url.includes('/account-config?') ? response : { value: denied.state('approved', { accounts: ['leosaquetto'] }) });
    const result = await denied.client.accountConfig('leosaquetto'); assert.equal(result.ok, false); assert.ok(!JSON.stringify(result).includes('private-uol-session'));
  }
});

test('owner decisions send personal grants only when explicitly approving', async () => {
  const h = harness({ existing: true, instagram: true });
  h.setResponder(url => ({ value: url.endsWith('/decision') ? { ...h.state('approved', { accounts: ['leosaquetto', 'gaoriel'] }), deviceId: 'friend' } : h.state('approved', { role: 'owner' }) }));
  const result = await h.client.decide('friend', 'approve', 'ABCD1234', ['leosaquetto', 'gaoriel']);
  assert.equal(result.ok, true); assert.deepEqual(result.accounts, ['leosaquetto', 'gaoriel']); privateFree(result);
  assert.deepEqual(JSON.parse(h.calls.at(-1).body), { deviceId: 'friend', decision: 'approve', code: 'ABCD1234', accounts: ['leosaquetto', 'gaoriel'] });
  await h.client.decide('friend', 'approve', 'ABCD1234', []); assert.deepEqual(JSON.parse(h.calls.at(-1).body).accounts, []);
  const count = h.calls.length;
  for (const [decision, accounts] of [['revoke', ['leosaquetto']], ['approve', ['other']], ['approve', ['gaoriel', 'gaoriel']], ['approve', null]]) {
    assert.equal((await h.client.decide('friend', decision, 'ABCD1234', accounts)).reason, 'decision_invalid');
  }
  assert.equal(h.calls.length, count);
});
