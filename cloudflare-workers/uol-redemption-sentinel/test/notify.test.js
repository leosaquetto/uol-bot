import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { Ledger } from '../src/ledger.js';
import { buildBlockedMessage, buildSuccessMessage, sendNtfy } from '../src/notify.js';

const campaign = { artist: 'Zayn', eventDate: '2026-10-10', venue: 'Nubank Parque SP', quantity: 2 };
const topicUrl = 'https://ntfy.sh/test-topic-not-real';
const success = () => buildSuccessMessage({ accountLabel: 'Leo', campaign, voucherUrl: 'https://private.invalid/SECRET-VOUCHER', confirmedAt: Date.parse('2026-10-07T18:00:00Z') });
const receipt = (overrides = {}) => Response.json({ id: 'receipt123', time: 1_791_388_800, topic: 'test-topic-not-real', event: 'message', ...overrides });

test('success message states exact event and links only to general history', () => {
  const payload = success();
  assert.equal(payload.title, 'Resgate confirmado — Zayn');
  assert.match(payload.message, /Leo: 2 ingressos/);
  assert.match(payload.message, /10\/10\/2026 · Nubank Parque SP/);
  assert.match(payload.message, /15:00/);
  assert.equal(payload.click, 'https://clube.uol.com.br/perfil/beneficios');
  assert.ok(!JSON.stringify(payload).includes('SECRET-VOUCHER'));
});

test('blockage messages use known codes and never upstream raw errors', () => {
  assert.match(buildBlockedMessage({ accountLabel: 'Leo', campaign, code: 'identity_mismatch' }).message, /identidade/);
  const payload = buildBlockedMessage({ accountLabel: 'Leo', campaign, reason: 'SESS=very-secret; password=hidden' });
  assert.match(payload.message, /parou por segurança/);
  assert.ok(!JSON.stringify(payload).includes('very-secret'));
  assert.ok(!JSON.stringify(payload).includes('password'));
});

test('ntfy JSON publication uses configured topic and accepts only valid receipt', async () => {
  let calls = 0;
  const result = await sendNtfy({ ...success(), topic: 'injected', click: 'https://private.invalid/voucher' }, {
    topicUrl, token: 'test-access-token',
    fetchImpl: async (url, init) => {
      calls += 1;
      assert.equal(url, 'https://ntfy.sh');
      assert.equal(init.method, 'POST');
      assert.equal(init.redirect, 'error');
      assert.equal(init.headers.Authorization, 'Bearer test-access-token');
      const body = JSON.parse(init.body);
      assert.equal(body.topic, 'test-topic-not-real');
      assert.equal(body.click, 'https://clube.uol.com.br/perfil/beneficios');
      assert.ok(init.signal instanceof AbortSignal);
      return receipt();
    },
  });
  assert.equal(calls, 1);
  assert.deepEqual(result, { ok: true, id: 'receipt123', time: 1_791_388_800 });
  assert.ok(!JSON.stringify(result).includes('test-access-token'));
});

test('wrong topic, malformed receipts, HTTP failure, and network errors do not confirm delivery', async () => {
  const responses = [
    () => receipt({ topic: 'wrong' }),
    () => receipt({ event: 'open' }),
    () => receipt({ id: '' }),
    () => receipt({ time: 0 }),
    () => new Response('OK', { status: 200 }),
    () => new Response('secret-error-body', { status: 429 }),
    () => { throw new Error('SESS=private-cookie'); },
  ];
  for (const response of responses) {
    const result = await sendNtfy(success(), { topicUrl, fetchImpl: async () => response() });
    assert.equal(result.ok, false);
    assert.ok(!JSON.stringify(result).includes('secret-error-body'));
    assert.ok(!JSON.stringify(result).includes('private-cookie'));
  }
});

test('invalid destination is rejected before network and cannot redirect secrets elsewhere', async () => {
  for (const invalid of ['http://ntfy.sh/topic', 'https://ntfy.sh.evil.invalid/topic', 'https://ntfy.sh/topic?token=secret', 'https://user:pass@ntfy.sh/topic', 'https://ntfy.sh/a/b', 'https://ntfy.sh/']) {
    let calls = 0;
    const result = await sendNtfy(success(), { topicUrl: invalid, fetchImpl: async () => { calls += 1; return receipt(); } });
    assert.equal(result.ok, false);
    assert.equal(calls, 0);
  }
});

test('failed notification retries independently and never grants a second redemption', async () => {
  const db = new DatabaseSync(':memory:');
  const ledger = new Ledger({ sql: { exec(query, ...bindings) {
    const rows = db.prepare(query).all(...bindings);
    return { toArray: () => rows };
  } } });
  const reservation = { month: '2026-10', campaignId: 'zayn', offerUrl: 'https://clube.uol.com.br/campanhasdeingresso/zayn', baseline: [], createdAt: 1_000 };
  let redemptionRequests = 0;
  let notificationRequests = 0;
  try {
    if (ledger.reserveAttempt(reservation)) redemptionRequests += 1;
    ledger.updateAttempt(reservation.month, { status: 'confirmed', confirmedAt: 2_000 });
    ledger.enqueueNotification('success:zayn', success(), 2_000);
    for (const now of [2_000, 62_000]) {
      for (const notification of ledger.pendingNotifications(now)) {
        const result = await sendNtfy(notification.payload, {
          topicUrl, fetchImpl: async () => { notificationRequests += 1; return notificationRequests === 1 ? new Response('', { status: 503 }) : receipt(); },
        });
        if (result.ok) ledger.markNotificationSent(notification.key, now);
        else ledger.failNotification(notification.key, now);
      }
      if (ledger.reserveAttempt(reservation)) redemptionRequests += 1;
    }
    assert.equal(redemptionRequests, 1);
    assert.equal(notificationRequests, 2);
    assert.deepEqual(ledger.pendingNotifications(1_000_000), []);
    assert.equal(ledger.getAttempt(reservation.month).status, 'confirmed');
  } finally { db.close(); }
});
