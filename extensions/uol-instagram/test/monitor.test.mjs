import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { StoryMonitor, readJsonLimited, LIMITS, WORKER_URL } from '../src/monitor.js';

const START = Date.parse('2026-10-16T12:00:00.000Z');
const MINUTE = 60_000;
const DAY = 86_400_000;
const TOKEN = 'offline-test-token-not-a-credential';
const PROFILE_ID = '1234567890';
const LINK = 'https://clube.uol.com.br/campanhasdeingresso/pA12-evento-uol';
const IMAGE = 'https://scontent.cdninstagram.com/story.jpg?signature=offline-test';
const TARGETS = ['main', 'canal2', 'discord', 'beeper'];

function memoryStorage(seed = {}) {
  let values = structuredClone(seed);
  return {
    async get(keys) { return Object.fromEntries(keys.filter(key => key in values).map(key => [key, structuredClone(values[key])])); },
    async set(update) { values = { ...values, ...structuredClone(update) }; },
    snapshot() { return structuredClone(values); },
  };
}

function json(body, status = 200, headers = {}) {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json', ...headers } });
}

function reels(username = 'clubeuol', items = []) {
  return { reels: { [PROFILE_ID]: { user: { pk: PROFILE_ID, username }, items } } };
}

function sourceStory() {
  return { pk: '1234567890123456789', taken_at: START / 1000 - 60,
    image_versions2: { candidates: [{ url: IMAGE, width: 1080, height: 1920 }] },
    story_link_stickers: [{ story_link: { url: LINK } }] };
}

function delivery(overrides = {}) {
  const payload = { storyId: '1234567890123456789', publishedAt: new Date(START - MINUTE).toISOString(),
    expiresAt: new Date(START + 60 * MINUTE).toISOString(), imageUrl: IMAGE, imageWidth: 1080, imageHeight: 1920, link: LINK };
  const key = `instagram:${payload.storyId}:${createHash('sha256').update(payload.link).digest('hex')}`;
  return { key, payload, status: 'pending', attempts: 0, retryAt: 0, updatedAt: new Date(START).toISOString(), ...overrides };
}

function receipt(entry, changes = {}) {
  const value = { ok: true, storyId: entry.payload.storyId, link: entry.payload.link, canonicalKey: entry.key, status: 'delivered',
    targets: Object.fromEntries(TARGETS.map(name => [name, { status: 'confirmed', messageId: `message-${name}`, imageConfirmed: true }])) };
  return { ...value, ...changes };
}

async function harness({ fetchImpl, profiles = ['clubeuol'], storage = memoryStorage(), time = { value: START } } = {}) {
  const calls = [];
  const fetchMock = async (url, options = {}) => {
    const call = { url: String(url), ...options };
    calls.push(call);
    if (!fetchImpl) throw new Error('Unexpected offline request');
    return fetchImpl(call, calls.length);
  };
  const monitor = new StoryMonitor({ storage, fetchImpl: fetchMock, now: () => time.value });
  await monitor.init();
  if (!monitor.config) await monitor.configure({ token: TOKEN, profiles, intervalMinutes: 5 });
  for (const username of profiles) monitor.state.profileIds[username] = { id: PROFILE_ID, verifiedAt: time.value };
  await monitor.save();
  return { monitor, calls, storage, time, fetchMock };
}

test('overlapping runs serialize; cadence and charged budget survive restart', async () => {
  let release, started;
  const requested = new Promise(resolve => { started = resolve; });
  const pending = new Promise(resolve => { release = resolve; });
  const fixture = await harness({ fetchImpl: async (_call, count) => {
    if (count === 1) { started(); return pending; }
    return json(reels());
  } });
  const { monitor, storage, time, calls, fetchMock } = fixture;
  const first = monitor.run();
  await requested;
  assert.equal(storage.snapshot().monitor.budget.used, 1, 'charge must be durable before request completion');
  assert.equal(storage.snapshot().monitor.nextPollAt, START + 5 * MINUTE);
  await monitor.run();
  assert.equal(calls.length, 1, 'overlap must not start another source request');
  release(json(reels()));
  await first;
  assert.equal(monitor.busy, false);
  assert.equal(monitor.state.sourceStatus, 'empty');
  assert.equal(calls[0].credentials, 'include');
  assert.equal(calls[0].redirect, 'error');
  const restarted = new StoryMonitor({ storage, fetchImpl: fetchMock, now: () => time.value });
  await restarted.init();
  await restarted.run();
  assert.equal(calls.length, 1, 'restart must respect persisted cadence');
  time.value += 5 * MINUTE;
  await restarted.run();
  assert.equal(calls.length, 2);
  assert.equal(storage.snapshot().monitor.budget.used, 2);
});

test('daily budget stops requests and resets on a new Sao Paulo day', async () => {
  const { monitor, storage, time, calls } = await harness({ fetchImpl: () => json(reels()) });
  monitor.state.budget = { day: '2026-10-16', used: LIMITS.requestsPerDay };
  await monitor.save();
  await monitor.run();
  assert.equal(calls.length, 0);
  assert.equal(monitor.state.lastError, 'daily_budget');
  assert.equal(storage.snapshot().monitor.budget.used, LIMITS.requestsPerDay);
  time.value += DAY;
  await monitor.run();
  assert.equal(calls.length, 1);
  assert.equal(monitor.state.budget.day, '2026-10-17');
  assert.equal(monitor.state.budget.used, 1);
});

test('HTTP and structured auth failures pause durably until resume', async t => {
  for (const [name, failure] of [
    ['HTTP 401', () => json({}, 401)],
    ['HTTP 403', () => json({}, 403)],
    ['login payload', () => json({ message: 'login_required' })],
    ['challenge payload', () => json({ challenge: { api_path: '/challenge/' } })],
  ]) await t.test(name, async () => {
    const { monitor, storage, time, calls, fetchMock } = await harness({ fetchImpl: (_call, count) => count === 1 ? failure() : json(reels()) });
    await monitor.run();
    assert.equal(monitor.state.sourceStatus, 'auth_required');
    assert.equal(monitor.state.pausedReason, 'auth_required');
    time.value += 60 * MINUTE;
    const restarted = new StoryMonitor({ storage, fetchImpl: fetchMock, now: () => time.value });
    await restarted.init();
    await restarted.run();
    assert.equal(calls.length, 1);
    await restarted.resume();
    await restarted.run();
    assert.equal(calls.length, 2);
    assert.equal(restarted.state.pausedReason, '');
    assert.equal(restarted.state.sourceStatus, 'empty');
  });
});

test('Instagram 429 cancels its body and honors the greater Retry-After delay', async () => {
  let cancelled = false;
  const body = new ReadableStream({ cancel() { cancelled = true; } });
  const { monitor, time, calls } = await harness({ fetchImpl: () => new Response(body, { status: 429, headers: { 'Retry-After': '28800' } }) });
  await monitor.run();
  assert.equal(cancelled, true);
  assert.equal(monitor.state.sourceStatus, 'rate_limited');
  assert.equal(monitor.state.nextPollAt, START + 8 * 60 * MINUTE);
  assert.equal(monitor.state.pausedReason, '');
  time.value += 7 * 60 * MINUTE;
  await monitor.run();
  assert.equal(calls.length, 1);
});

test('stream size limit cancels unread content and releases reader lock', async () => {
  let cancelled = false;
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('{"value":'));
      controller.enqueue(new TextEncoder().encode('"too large"}'));
    },
    cancel() { cancelled = true; },
  });
  const response = new Response(stream);
  await assert.rejects(readJsonLimited(response, 10), { reason: 'body_limit' });
  assert.equal(cancelled, true);
  assert.equal(stream.locked, false);
  let headerCancelled = false;
  const declaredLarge = new Response(new ReadableStream({ cancel() { headerCancelled = true; } }), { headers: { 'Content-Length': '100' } });
  await assert.rejects(readJsonLimited(declaredLarge, 10), { reason: 'body_limit' });
  assert.equal(headerCancelled, true);
});

test('test profile stories are observable but never sent to Worker intake', async () => {
  const { monitor, calls } = await harness({ profiles: ['testaccount'], fetchImpl: call => {
    assert.ok(call.url.startsWith('https://www.instagram.com/'));
    return json(reels('testaccount', [sourceStory()]));
  } });
  await monitor.run();
  assert.equal(monitor.state.sourceStatus, 'found');
  assert.deepEqual(monitor.state.lastResults, [{ username: 'testaccount', status: 'found', count: 1, newCount: 0 }]);
  assert.equal(calls.length, 1);
  assert.deepEqual(monitor.state.outbox, {});
});

test('lost POST and interrupted sending recover by receipt GET only; missing receipt stays uncertain', async () => {
  const entry = delivery();
  let durableSending;
  const fixture = await harness({ fetchImpl: () => {
    durableSending = fixture.storage.snapshot().monitor;
    assert.equal(durableSending.outbox[entry.key].status, 'sending');
    throw new TypeError('Offline simulated lost response');
  } });
  fixture.monitor.state.nextPollAt = START + DAY;
  fixture.monitor.state.outbox[entry.key] = entry;
  await fixture.monitor.run();
  assert.equal(fixture.calls.length, 1);
  assert.equal(fixture.calls[0].method, 'POST');
  assert.equal(entry.status, 'uncertain');
  assert.equal(entry.attempts, 1);
  assert.equal(entry.retryAt, START + 5 * MINUTE);

  // Restore the last durable state before the POST response, as an actual crash would.
  await fixture.storage.set({ monitor: durableSending });
  const recoveryCalls = [];
  const restarted = new StoryMonitor({ storage: fixture.storage, now: () => fixture.time.value, fetchImpl: async (url, options) => {
    recoveryCalls.push({ url: String(url), ...options });
    return json({ ok: true, stories: recoveryCalls.length < 3 ? [] : [receipt(entry)] });
  } });
  await restarted.init();
  const recovered = restarted.state.outbox[entry.key];
  assert.equal(recovered.status, 'uncertain');
  assert.equal(recovered.error, 'interrupted_attempt');
  await restarted.run();
  assert.equal(recovered.status, 'uncertain');
  assert.equal(restarted.state.workerError, 'receipt_unconfirmed');
  assert.equal(recovered.retryAt, START + 30 * MINUTE);
  fixture.time.value += 30 * MINUTE;
  await restarted.run();
  assert.equal(recovered.status, 'uncertain');
  fixture.time.value += 30 * MINUTE;
  await restarted.run();
  assert.equal(recovered.status, 'delivered');
  assert.equal(restarted.state.workerError, '', 'validated recovery must clear the stale missing-receipt error');
  assert.equal(recovered.attempts, 1);
  assert.equal(recoveryCalls.length, 3);
  assert.ok(recoveryCalls.every(call => call.method === 'GET' && call.url.startsWith(`${WORKER_URL}/instagram-story-status?`)));
  assert.ok(recoveryCalls.every(call => !Object.hasOwn(call, 'body')));
});

test('accepted recoverable receipt retries identical payload only when nextAttemptAt is due', async () => {
  const entry = delivery();
  const accepted = receipt(entry, { status: 'partial' });
  accepted.targets.main = { status: 'failed_safe', nextAttemptAt: new Date(START + 20 * MINUTE).toISOString() };
  const { monitor, calls, time, storage } = await harness({ fetchImpl: (_call, count) => json(count === 1 ? accepted : receipt(entry)) });
  monitor.state.nextPollAt = START + DAY;
  monitor.state.outbox[entry.key] = entry;
  await monitor.run();
  assert.equal(entry.status, 'accepted');
  assert.equal(entry.retryAt, START + 20 * MINUTE);
  assert.equal(storage.snapshot().monitor.outbox[entry.key].retryAt, entry.retryAt);
  await monitor.run();
  time.value += 19 * MINUTE;
  await monitor.run();
  assert.equal(calls.length, 1);
  time.value += MINUTE;
  await monitor.run();
  assert.equal(calls.length, 2);
  assert.equal(calls[0].body, calls[1].body);
  assert.equal(calls[0].body, JSON.stringify(entry.payload));
  assert.ok(calls.every(call => call.method === 'POST' && call.url === `${WORKER_URL}/ingest-instagram-story` && call.credentials === 'omit'));
  assert.equal(entry.status, 'delivered');
  assert.equal(entry.attempts, 2);
});

test('accepted ambiguous destination recovers through spaced receipt GETs without another POST', async t => {
  for (const status of ['in_flight', 'unknown']) await t.test(status, async () => {
    const entry = delivery({ status: 'accepted', attempts: 1 });
    const ambiguous = receipt(entry, { status: 'partial' });
    ambiguous.targets.main = { status };
    const { monitor, calls, time, storage } = await harness({ fetchImpl: (_call, count) =>
      json({ ok: true, stories: [count === 1 ? ambiguous : receipt(entry)] }) });
    monitor.state.nextPollAt = START + DAY;
    monitor.state.outbox[entry.key] = entry;
    monitor.recordReceipt(entry, ambiguous);
    await monitor.save();
    assert.equal(entry.retryAt, START + 30 * MINUTE);
    await monitor.run();
    assert.equal(calls.length, 0);
    time.value += 30 * MINUTE;
    await monitor.run();
    assert.equal(entry.status, 'accepted');
    assert.equal(entry.retryAt, START + 60 * MINUTE);
    assert.equal(storage.snapshot().monitor.outbox[entry.key].retryAt, entry.retryAt);
    time.value += 29 * MINUTE;
    await monitor.run();
    assert.equal(calls.length, 1);
    time.value += MINUTE;
    await monitor.run();
    assert.equal(entry.status, 'delivered');
    assert.equal(entry.attempts, 1);
    assert.equal(calls.length, 2);
    assert.ok(calls.every(call => call.method === 'GET' && call.url.startsWith(`${WORKER_URL}/instagram-story-status?`)));
    assert.ok(calls.every(call => !Object.hasOwn(call, 'body')));
  });
});

test('public state excludes credentials, image URLs, bytes, and raw receipt metadata', async () => {
  const { monitor } = await harness();
  const entry = delivery({ status: 'accepted' });
  entry.payload.imageBase64 = 'private-image-bytes';
  entry.receipt = receipt(entry);
  entry.receipt.privateMetadata = 'private-receipt';
  entry.receipt.targets.main.imageUrl = IMAGE;
  entry.receipt.targets.main.token = TOKEN;
  monitor.state.outbox[entry.key] = entry;
  monitor.state.lastResults = [{ username: 'clubeuol', status: 'found', count: 1, newCount: 0 }];
  const exposed = monitor.publicState();
  const serialized = JSON.stringify(exposed);
  for (const secret of [TOKEN, IMAGE, 'imageBase64', 'private-image-bytes', 'private-receipt', 'privateMetadata']) {
    assert.equal(serialized.includes(secret), false);
  }
  assert.equal(exposed.deliveries[0].storyId, entry.payload.storyId);
  assert.equal(exposed.deliveries[0].targets.main.messageId, 'message-main');
  assert.equal(Object.hasOwn(exposed, 'settings'), false);
});

test('retention removes only old delivered entries and preserves every unresolved status', async () => {
  const { monitor, storage, calls } = await harness();
  monitor.state.nextPollAt = START + DAY;
  for (const status of ['delivered', 'uncertain', 'accepted', 'pending', 'blocked', 'expired']) {
    const entry = delivery({ key: status, status, retryAt: START + DAY });
    entry.payload.expiresAt = new Date(START - 8 * DAY).toISOString();
    monitor.state.outbox[status] = entry;
  }
  const recent = delivery({ key: 'recent-delivered', status: 'delivered' });
  recent.payload.expiresAt = new Date(START - 6 * DAY).toISOString();
  monitor.state.outbox[recent.key] = recent;
  await monitor.run();
  assert.deepEqual(Object.keys(monitor.state.outbox).sort(), ['accepted', 'blocked', 'expired', 'pending', 'recent-delivered', 'uncertain']);
  assert.deepEqual(Object.keys(storage.snapshot().monitor.outbox).sort(), Object.keys(monitor.state.outbox).sort());
  assert.equal(calls.length, 0);
});
