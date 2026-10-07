import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { Ledger } from '../src/ledger.js';

function openLedger(path = ':memory:') {
  const db = new DatabaseSync(path);
  const ledger = new Ledger({ sql: { exec(query, ...bindings) {
    const rows = db.prepare(query).all(...bindings);
    return { toArray: () => rows };
  } } });
  return { ledger, close: () => db.close() };
}

const attempt = {
  month: '2026-10', campaignId: 'zayn-2026-10-10',
  offerUrl: 'https://clube.uol.com.br/campanhasdeingresso/zayn',
  baseline: ['old-voucher'], createdAt: 1_791_388_800_000,
};

test('one monthly reservation wins across simultaneous callers and remains after restart', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'uol-ledger-test-'));
  const path = join(dir, 'ledger.sqlite');
  let instance = openLedger(path);
  try {
    const results = await Promise.all(Array.from({ length: 20 }, (_, index) => Promise.resolve().then(() => (
      instance.ledger.reserveAttempt({ ...attempt, campaignId: `campaign-${index}` })
    ))));
    assert.equal(results.filter(Boolean).length, 1);
    assert.equal(instance.ledger.getAttempt('2026-10').status, 'reserved');
    instance.close();
    instance = openLedger(path);
    assert.deepEqual(instance.ledger.getAttempt('2026-10').baseline, ['old-voucher']);
    assert.equal(instance.ledger.reserveAttempt(attempt), null);
  } finally {
    instance.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('reservation is present before external work; errors never release the month', async () => {
  const { ledger, close } = openLedger();
  try {
    assert.ok(ledger.reserveAttempt(attempt));
    const externalWork = async () => {
      assert.equal(ledger.getAttempt(attempt.month).status, 'reserved');
      throw new Error('simulated network loss');
    };
    await assert.rejects(externalWork(), /network loss/);
    for (const status of ['uncertain', 'failed', 'confirmed', 'reserved']) {
      ledger.updateAttempt(attempt.month, { status, updatedAt: attempt.createdAt + 1 });
      assert.equal(ledger.reserveAttempt({ ...attempt, campaignId: 'another-show' }), null);
    }
    assert.throws(() => ledger.updateAttempt(attempt.month, { month: '2026-11' }), /immutable/);
    assert.throws(() => ledger.updateAttempt(attempt.month, { baseline: [] }), /immutable/);
    assert.throws(() => ledger.updateAttempt(attempt.month, { offerUrl: 'another' }), /immutable/);
  } finally { close(); }
});

test('different months and separate account databases can reserve independently', () => {
  const one = openLedger();
  const two = openLedger();
  try {
    assert.ok(one.ledger.reserveAttempt(attempt));
    assert.ok(one.ledger.reserveAttempt({ ...attempt, month: '2026-11' }));
    assert.ok(two.ledger.reserveAttempt(attempt));
    assert.equal(one.ledger.getAttempt('2026-12'), null);
    assert.throws(() => one.ledger.reserveAttempt({ ...attempt, month: '2026-13' }), /invalid_month/);
    one.ledger.setState('identity', { verified: true });
    assert.deepEqual(one.ledger.getState('identity'), { verified: true });
    assert.equal(two.ledger.getState('identity'), null);
  } finally { one.close(); two.close(); }
});

test('patches preserve attempt identity and previous confirmation metadata', () => {
  const { ledger, close } = openLedger();
  try {
    ledger.reserveAttempt(attempt);
    ledger.updateAttempt(attempt.month, { status: 'confirmed', confirmedAt: attempt.createdAt + 10 });
    const updated = ledger.updateAttempt(attempt.month, { reason: 'history_verified' });
    assert.equal(updated.status, 'confirmed');
    assert.equal(updated.confirmedAt, attempt.createdAt + 10);
    assert.equal(updated.campaignId, attempt.campaignId);
    assert.equal(updated.offerUrl, attempt.offerUrl);
    assert.equal(updated.createdAt, attempt.createdAt);
    assert.deepEqual(updated.baseline, attempt.baseline);
  } finally { close(); }
});

test('outbox survives restart, deduplicates payload, persists backoff, and never reopens sent rows', () => {
  const dir = mkdtempSync(join(tmpdir(), 'uol-outbox-test-'));
  const path = join(dir, 'ledger.sqlite');
  let instance = openLedger(path);
  let now = attempt.createdAt;
  try {
    instance.ledger.enqueueNotification('success:zayn', { title: 'Success', message: 'original' }, now);
    assert.equal(instance.ledger.enqueueNotification('success:zayn', { message: 'replacement' }, now), null);
    assert.equal(instance.ledger.pendingNotifications(now)[0].payload.message, 'original');
    const delays = [60_000, 120_000, 240_000, 480_000, 900_000, 900_000];
    for (const [index, delay] of delays.entries()) {
      const failed = instance.ledger.failNotification('success:zayn', now);
      assert.equal(failed.failures, index + 1);
      assert.equal(failed.nextAttemptAt, now + delay);
      instance.close();
      instance = openLedger(path);
      assert.deepEqual(instance.ledger.pendingNotifications(now + delay - 1), []);
      assert.equal(instance.ledger.nextNotificationAt(), now + delay);
      now += delay;
      assert.equal(instance.ledger.pendingNotifications(now).length, 1);
    }
    assert.equal(instance.ledger.markNotificationSent('success:zayn', now).sentAt, now);
    assert.equal(instance.ledger.failNotification('success:zayn', now), null);
    assert.equal(instance.ledger.enqueueNotification('success:zayn', { message: 'again' }, now), null);
    assert.deepEqual(instance.ledger.pendingNotifications(now + 1_000_000), []);
    assert.equal(instance.ledger.nextNotificationAt(), null);
  } finally {
    instance.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('pending notification ordering and bounded batches do not lose work', () => {
  const { ledger, close } = openLedger();
  try {
    ledger.enqueueNotification('later', {}, 500);
    ledger.enqueueNotification('earlier', {}, 100);
    assert.deepEqual(ledger.pendingNotifications(500, 1).map((row) => row.key), ['earlier']);
    ledger.markNotificationSent('earlier', 500);
    assert.deepEqual(ledger.pendingNotifications(500, 1).map((row) => row.key), ['later']);
    assert.throws(() => ledger.pendingNotifications(500, 0), /invalid_notification_limit/);
  } finally { close(); }
});
