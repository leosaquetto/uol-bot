import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { APP_IDS, createAccess, initialState, prepareRequest, responseFor } from './access.mjs';

const crypto = {
  async hash(value) { return createHash('sha256').update(value).digest('hex'); },
  async equal(a, b) { return timingSafeEqual(createHash('sha256').update(a).digest(), createHash('sha256').update(b).digest()); },
  randomHex(bytes) { return randomBytes(bytes).toString('hex'); },
};
const identity = name => ({ name, deviceId: randomUUID(), token: randomBytes(32).toString('hex') });
function fixture() {
  let state = initialState(crypto);
  let now = 1792324800000;
  const bootstrapToken = randomBytes(32).toString('hex');
  const store = { read: () => structuredClone(state), transact(fn) {
    const next = structuredClone(state);
    const outcome = fn(next);
    state = next;
    return outcome;
  } };
  const access = createAccess({ store, crypto, bootstrapToken, clock: () => now });
  const call = (path, body, bearer = '', ip = '192.0.2.1') => access.handle({ path, body, bearer, ip });
  const owner = identity('Leo');
  const bootstrap = () => call('/v1/bootstrap', owner, bootstrapToken);
  const enroll = user => call('/v1/enroll', user);
  const approve = (user, code, appIds = ['clube-uol'], status = 'approved') => call('/v1/decision', { id: user.deviceId, code, appIds, status }, owner.token);
  return { access, store, call, owner, bootstrapToken, bootstrap, enroll, approve, advance: duration => { now += duration; } };
}

test('bootstrap requires secret; exactly one immutable admin and credential retry', async () => {
  const f = fixture();
  assert.equal((await f.call('/v1/bootstrap', f.owner, 'wrong')).status, 401);
  const first = await f.bootstrap();
  assert.equal(first.status, 201);
  assert.deepEqual(first.body.profile.appIds, APP_IDS);
  assert.equal((await f.bootstrap()).status, 200);
  assert.equal((await f.call('/v1/bootstrap', identity('Impostor'), f.bootstrapToken)).body.error, 'bootstrap_closed');
  assert.equal((await f.call('/v1/decision', { id: f.owner.deviceId, code: first.body.profile.code, status: 'revoked', appIds: [] }, f.owner.token)).body.error, 'admin_immutable');
});

test('enroll remains pending; grants, authorization and revocation enforced per app', async () => {
  const f = fixture(); await f.bootstrap();
  const user = identity('User');
  const enrolled = await f.enroll(user);
  assert.equal(enrolled.status, 201);
  assert.equal(enrolled.body.profile.status, 'pending');
  assert.equal((await f.call('/v1/access/clube-uol', undefined, user.token)).status, 403);
  assert.equal((await f.call('/v1/users', undefined, user.token)).status, 403);
  assert.equal((await f.approve(user, 'DEADBEEF')).status, 409);
  assert.equal((await f.approve(user, enrolled.body.profile.code)).status, 200);
  assert.deepEqual((await f.call('/v1/access/clube-uol', undefined, user.token)).body, { allowed: true });
  assert.equal((await f.call('/v1/access/pilltracker', undefined, user.token)).status, 403);
  assert.equal((await f.approve(user, enrolled.body.profile.code, [], 'revoked')).status, 200);
  assert.equal((await f.call('/v1/access/clube-uol', undefined, user.token)).status, 403);
  assert.deepEqual((await f.call('/v1/me', undefined, user.token)).body.profile.appIds, []);
});

test('re-enrollment cannot overwrite name, grants, approval or revocation', async () => {
  const f = fixture(); await f.bootstrap();
  const user = identity('Original');
  const enrolled = await f.enroll(user);
  await f.approve(user, enrolled.body.profile.code);
  let replay = await f.enroll({ ...user, name: 'Spoofed Leo' });
  assert.equal(replay.body.profile.status, 'approved');
  assert.equal(replay.body.profile.name, 'Original');
  await f.approve(user, enrolled.body.profile.code, [], 'revoked');
  replay = await f.enroll(user);
  assert.equal(replay.body.profile.status, 'revoked');
  assert.deepEqual(replay.body.profile.appIds, []);
});

test('identity and token collision reject takeover; role injection rejected', async () => {
  const f = fixture(); await f.bootstrap();
  const user = identity('User'); await f.enroll(user);
  assert.equal((await f.enroll({ ...user, token: identity('Other').token })).status, 409);
  assert.equal((await f.enroll({ ...identity('Other'), token: user.token })).status, 409);
  assert.equal((await f.enroll({ ...identity('Other'), role: 'admin' })).status, 400);
  assert.equal((await f.call('/v1/me', undefined, f.owner.deviceId)).status, 401);
  assert.equal((await f.call('/v1/me', undefined, identity('Other').token)).status, 401);
});

test('users, me and enrollment responses expose no token, token hash, salt or IP', async () => {
  const f = fixture(); await f.bootstrap();
  const user = identity('User'); const enrollment = await f.enroll(user);
  const list = await f.call('/v1/users', undefined, f.owner.token);
  const me = await f.call('/v1/me', undefined, user.token);
  const responseText = JSON.stringify([list, me, enrollment]);
  const saved = JSON.stringify(f.store.read());
  for (const value of [f.owner.token, user.token, f.bootstrapToken, await crypto.hash(user.token), '192.0.2.1', f.store.read().ipSalt]) assert.ok(!responseText.includes(value));
  for (const value of [f.owner.token, user.token, f.bootstrapToken, '192.0.2.1']) assert.ok(!saved.includes(value));
  assert.ok(saved.includes(await crypto.hash(user.token)));
});

test('expired pending enrollment can explicitly renew, retaining no grants and requiring new confirmation', async () => {
  const f = fixture(); await f.bootstrap();
  const user = identity('User'); const enrolled = await f.enroll(user);
  f.advance(7 * 86400000);
  assert.equal((await f.call('/v1/me', undefined, user.token)).body.profile.status, 'expired');
  assert.equal((await f.approve(user, enrolled.body.profile.code)).body.error, 'enrollment_expired');
  const renewed = await f.enroll(user);
  assert.equal(renewed.body.profile.status, 'pending');
  assert.deepEqual(renewed.body.profile.appIds, []);
  assert.notEqual(renewed.body.profile.code, enrolled.body.profile.code);
  assert.equal((await f.approve(user, enrolled.body.profile.code)).body.error, 'confirmation_mismatch');
  assert.deepEqual((await f.enroll(user)).body.profile, renewed.body.profile);
  assert.equal((await f.call('/v1/access/clube-uol', undefined, user.token)).status, 403);
  assert.equal((await f.approve(user, renewed.body.profile.code)).status, 200);
});

test('expired pending renewal respects rate limit and pending capacity', async () => {
  const f = fixture();
  const user = identity('Expired'); await f.enroll(user); f.advance(7 * 86400000);
  for (let i = 0; i < 10; i++) await f.enroll(identity('New'));
  assert.equal((await f.enroll(user)).body.error, 'enrollment_limited');
  for (let i = 10; i < 32; i++) await f.call('/v1/enroll', identity('New'), '', '198.51.100.' + i);
  assert.equal((await f.call('/v1/enroll', user, '', '203.0.113.1')).body.error, 'enrollment_capacity');
  assert.equal((await f.call('/v1/me', undefined, user.token)).body.profile.status, 'expired');
});

test('enrollment limited to ten per IP per hour; retry remains idempotent', async () => {
  const f = fixture(); const first = identity('First'); await f.enroll(first);
  for (let i = 1; i < 10; i++) assert.equal((await f.enroll(identity('User'))).status, 201);
  assert.equal((await f.enroll(identity('Overflow'))).status, 429);
  assert.equal((await f.enroll(first)).status, 200);
  f.advance(3600000);
  assert.equal((await f.enroll(identity('Next hour'))).status, 201);
});

test('global enrollment cap and pending capacity are bounded', async () => {
  const f = fixture(); await f.bootstrap();
  for (let i = 0; i < 40; i++) {
    const user = identity('User');
    const enrolled = await f.call('/v1/enroll', user, '', '192.0.2.' + i);
    assert.equal(enrolled.status, 201);
    await f.approve(user, enrolled.body.profile.code);
  }
  assert.equal((await f.call('/v1/enroll', identity('Overflow'), '', '198.51.100.1')).body.error, 'enrollment_limited');
  f.advance(86400000);
  for (let i = 0; i < 32; i++) assert.equal((await f.call('/v1/enroll', identity('Pending'), '', '198.51.100.' + i)).status, 201);
  assert.equal((await f.call('/v1/enroll', identity('Overflow'), '', '203.0.113.1')).body.error, 'enrollment_capacity');
});

test('128 total users cap prevents unbounded stored data', async () => {
  const f = fixture(); await f.bootstrap();
  f.store.transact(state => {
    const sample = state.users[0];
    for (let i = 1; i < 128; i++) state.users.push({ ...sample, id: randomUUID(), tokenHash: i.toString(16).padStart(64, '0'), role: 'member' });
  });
  assert.equal((await f.enroll(identity('Overflow'))).body.error, 'enrollment_capacity');
});

test('full capacity recycles only oldest expired pending slot, preserving manual revocation', async () => {
  const f = fixture(); await f.bootstrap();
  const expired = identity('Old expired');
  await f.enroll(expired);
  f.advance(7 * 86400000);
  await f.call('/v1/me', undefined, expired.token);
  let revokedId;
  f.store.transact(state => {
    const sample = state.users[0];
    for (let i = 2; i < 128; i++) state.users.push({ ...sample, id: randomUUID(), tokenHash: i.toString(16).padStart(64, '0'), role: 'member', status: 'revoked', appIds: [] });
    revokedId = state.users.at(-1).id;
  });
  const newUser = identity('New');
  assert.equal((await f.enroll(newUser)).status, 201);
  assert.equal(f.store.read().users.length, 128);
  assert.ok(!f.store.read().users.some(user => user.id === expired.deviceId));
  assert.ok(f.store.read().users.some(user => user.id === revokedId && user.status === 'revoked'));
  assert.equal((await f.call('/v1/me', undefined, expired.token)).status, 401);
  assert.equal((await f.enroll(identity('Still full'))).body.error, 'enrollment_capacity');
});

test('invalid enrollment and decision data reject without grants', async () => {
  const f = fixture(); await f.bootstrap();
  for (const name of ['', ' '.repeat(4), 'X'.repeat(61), 'Leo\u0000', 'Leo\u202e']) assert.equal((await f.enroll(identity(name))).status, 400);
  assert.equal((await f.enroll({ ...identity('User'), deviceId: '../../admin' })).status, 400);
  const user = identity('User'); const enrolled = await f.enroll(user);
  for (const appIds of [['unknown'], ['clube-uol', 'clube-uol'], 'clube-uol']) assert.equal((await f.approve(user, enrolled.body.profile.code, appIds)).status, 400);
  assert.equal((await f.approve(user, enrolled.body.profile.code, [], 'admin')).status, 400);
  assert.equal((await f.call('/v1/access/unknown', undefined, f.owner.token)).status, 404);
});

test('member cannot spoof an admin with request body or foreign device ID', async () => {
  const f = fixture(); await f.bootstrap();
  const user = identity('Leo'); const enrolled = await f.enroll(user);
  const body = { id: user.deviceId, code: enrolled.body.profile.code, appIds: APP_IDS, status: 'approved' };
  assert.equal((await f.call('/v1/decision', body, user.token)).body.error, 'admin_required');
  assert.equal((await f.call('/v1/decision', { ...body, role: 'admin' }, user.token)).status, 400);
  assert.equal((await f.enroll({ ...user, deviceId: f.owner.deviceId })).status, 409);
});

test('simultaneous bootstrap and enrollment requests keep single admin and token ownership', async () => {
  const f = fixture();
  const results = await Promise.all([f.bootstrap(), f.call('/v1/bootstrap', identity('Other'), f.bootstrapToken)]);
  assert.deepEqual(results.map(value => value.status).sort(), [201, 409]);
  const user = identity('User');
  const registrations = await Promise.all([f.enroll(user), f.enroll(user)]);
  assert.deepEqual(registrations.map(value => value.status).sort(), [200, 201]);
  assert.equal(f.store.read().users.length, 2);
});

test('storage failure returns closed unavailable response', async () => {
  const f = fixture();
  const access = createAccess({ crypto, bootstrapToken: f.bootstrapToken, store: { read: f.store.read, transact() { throw new Error('private store detail'); } } });
  const result = await access.handle({ path: '/v1/enroll', body: identity('User'), ip: '192.0.2.1' });
  assert.deepEqual(result, { status: 503, body: { error: 'access_unavailable' } });
  assert.equal(f.store.read().users.length, 0);
});

test('HTTP parser bounds chunked bodies, validates transport and rejects browser calls', async () => {
  const request = (path, options = {}) => new Request('https://example.test' + path, options);
  const post = body => ({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
  assert.equal((await prepareRequest(request('/v1/enroll', post(JSON.stringify(identity('User')))))).path, '/v1/enroll');
  await assert.rejects(prepareRequest(request('/v1/enroll', post(' '.repeat(4097)))), { code: 'payload_too_large' });
  await assert.rejects(prepareRequest(request('/v1/enroll', post('{'))), { code: 'invalid_json' });
  await assert.rejects(prepareRequest(new Request('http://example.test/health')), { code: 'https_required' });
  await assert.rejects(prepareRequest(request('/health', { headers: { Origin: 'https://attacker.test' } })), { code: 'native_client_required' });
  await assert.rejects(prepareRequest(request('/health?token=secret')), { code: 'invalid_request' });
  await assert.rejects(prepareRequest(request('/v1/users', { method: 'POST' })), { code: 'method_not_allowed' });
  await assert.rejects(prepareRequest(request('/missing')), { code: 'not_found' });
  await assert.rejects(prepareRequest(request('/v1/enroll', { method: 'POST', body: '{}' })), { code: 'json_required' });
  const response = responseFor({ status: 200, body: { allowed: true } });
  assert.equal(response.headers.get('Cache-Control'), 'no-store');
  assert.equal(response.headers.get('Access-Control-Allow-Origin'), null);
  assert.equal(response.headers.get('Set-Cookie'), null);
});
