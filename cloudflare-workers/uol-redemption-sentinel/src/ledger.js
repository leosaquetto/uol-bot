const ATTEMPT_PATCH_FIELDS = new Set([
  'status', 'updatedAt', 'confirmedAt', 'voucherUrl', 'reason',
]);

function json(value) {
  const result = JSON.stringify(value);
  if (result === undefined) throw new Error('invalid_json_value');
  return result;
}

function nonempty(value, field, max = 2048) {
  if (typeof value !== 'string' || !value.trim() || value.length > max) {
    throw new Error(`invalid_${field}`);
  }
  return value;
}

function timestamp(value) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error('invalid_timestamp');
  return value;
}

function monthKey(value) {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(value)) throw new Error('invalid_month');
  return value;
}

function attemptFromRow(row) {
  if (!row) return null;
  return {
    month: row.month,
    campaignId: row.campaign_id,
    offerUrl: row.offer_url,
    baseline: JSON.parse(row.baseline_json),
    createdAt: row.created_at,
    ...JSON.parse(row.details_json),
    status: row.status,
  };
}

function notificationFromRow(row) {
  return {
    key: row.notification_key,
    payload: JSON.parse(row.payload_json),
    createdAt: row.created_at,
    failures: row.failures,
    nextAttemptAt: row.next_attempt_at,
    sentAt: row.sent_at,
    status: row.status,
  };
}

/** One instance per account. All methods are synchronous; no cursor crosses an await. */
export class Ledger {
  constructor(storage) {
    if (typeof storage?.sql?.exec !== 'function') throw new Error('sql_storage_required');
    this.sql = storage.sql;
    this.init();
  }

  init() {
    this.sql.exec(`CREATE TABLE IF NOT EXISTS sentinel_state (
      state_key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL
    )`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS redemption_attempt (
      month TEXT PRIMARY KEY,
      campaign_id TEXT NOT NULL,
      offer_url TEXT NOT NULL,
      baseline_json TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      status TEXT NOT NULL,
      details_json TEXT NOT NULL DEFAULT '{}'
    )`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS notification_outbox (
      notification_key TEXT PRIMARY KEY,
      payload_json TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      failures INTEGER NOT NULL DEFAULT 0,
      next_attempt_at INTEGER NOT NULL,
      sent_at INTEGER
    )`);
  }

  getState(key) {
    const [row] = this.sql.exec('SELECT value_json FROM sentinel_state WHERE state_key = ?', nonempty(key, 'state_key', 256)).toArray();
    return row ? JSON.parse(row.value_json) : null;
  }

  setState(key, value) {
    this.sql.exec(`INSERT INTO sentinel_state (state_key, value_json) VALUES (?, ?)
      ON CONFLICT(state_key) DO UPDATE SET value_json = excluded.value_json`, nonempty(key, 'state_key', 256), json(value));
  }

  /** A returned row grants the sole attempt. Any existing month, whatever its status, refuses it. */
  reserveAttempt({ month, campaignId, offerUrl, baseline, createdAt }) {
    const [row] = this.sql.exec(`INSERT INTO redemption_attempt
      (month, campaign_id, offer_url, baseline_json, created_at, status)
      VALUES (?, ?, ?, ?, ?, 'reserved')
      ON CONFLICT(month) DO NOTHING RETURNING *`,
    monthKey(month), nonempty(campaignId, 'campaign_id', 200), nonempty(offerUrl, 'offer_url'), json(baseline), timestamp(createdAt)).toArray();
    return attemptFromRow(row);
  }

  getAttempt(month) {
    const [row] = this.sql.exec('SELECT * FROM redemption_attempt WHERE month = ?', monthKey(month)).toArray();
    return attemptFromRow(row);
  }

  updateAttempt(month, patch) {
    if (!patch || typeof patch !== 'object' || Array.isArray(patch)) throw new Error('invalid_attempt_patch');
    if (Object.keys(patch).some((key) => !ATTEMPT_PATCH_FIELDS.has(key))) throw new Error('immutable_attempt_field');
    const current = this.getAttempt(month);
    if (!current) return null;
    const { status = current.status, ...details } = patch;
    nonempty(status, 'attempt_status', 64);
    if (details.updatedAt !== undefined) timestamp(details.updatedAt);
    if (details.confirmedAt !== undefined) timestamp(details.confirmedAt);
    const persistedDetails = Object.fromEntries(Object.entries(current).filter(([key]) => ATTEMPT_PATCH_FIELDS.has(key) && key !== 'status'));
    const [row] = this.sql.exec(`UPDATE redemption_attempt SET status = ?, details_json = ?
      WHERE month = ? RETURNING *`, status, json({ ...persistedDetails, ...details }), monthKey(month)).toArray();
    return attemptFromRow(row);
  }

  enqueueNotification(key, payload, now) {
    const [row] = this.sql.exec(`INSERT INTO notification_outbox
      (notification_key, payload_json, created_at, next_attempt_at) VALUES (?, ?, ?, ?)
      ON CONFLICT(notification_key) DO NOTHING RETURNING *`, nonempty(key, 'notification_key', 256), json(payload), timestamp(now), now).toArray();
    return row ? notificationFromRow(row) : null;
  }

  pendingNotifications(now, limit = 20) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new Error('invalid_notification_limit');
    return this.sql.exec(`SELECT * FROM notification_outbox
      WHERE status = 'pending' AND next_attempt_at <= ?
      ORDER BY next_attempt_at, notification_key LIMIT ?`, timestamp(now), limit).toArray().map(notificationFromRow);
  }

  nextNotificationAt() {
    const [row] = this.sql.exec("SELECT MIN(next_attempt_at) AS next_at FROM notification_outbox WHERE status = 'pending'").toArray();
    return row?.next_at ?? null;
  }

  markNotificationSent(key, now) {
    const [row] = this.sql.exec(`UPDATE notification_outbox SET status = 'sent', sent_at = ?
      WHERE notification_key = ? AND status = 'pending' RETURNING *`, timestamp(now), nonempty(key, 'notification_key', 256)).toArray();
    return row ? notificationFromRow(row) : null;
  }

  failNotification(key, now) {
    const [row] = this.sql.exec(`UPDATE notification_outbox SET
      next_attempt_at = ? + CASE WHEN failures >= 4 THEN 900000 ELSE 60000 * (1 << failures) END,
      failures = failures + 1
      WHERE notification_key = ? AND status = 'pending' RETURNING *`, timestamp(now), nonempty(key, 'notification_key', 256)).toArray();
    return row ? notificationFromRow(row) : null;
  }
}
