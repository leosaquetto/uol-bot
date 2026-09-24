import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, chmodSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

export const digest = value => createHash('sha256').update(value).digest('hex');

export function openStore(path, { recover = true } = {}) {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const db = new DatabaseSync(path);
  chmodSync(path, 0o600);
  db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
    CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS push_events(
      id TEXT PRIMARY KEY, received_at TEXT NOT NULL, payload TEXT NOT NULL,
      state TEXT NOT NULL DEFAULT 'pending', code TEXT, attempts INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS jobs(
      id TEXT PRIMARY KEY, dedup_key TEXT UNIQUE NOT NULL, request_hash TEXT NOT NULL,
      destination TEXT NOT NULL, payload TEXT NOT NULL, priority INTEGER NOT NULL,
      state TEXT NOT NULL DEFAULT 'queued', message_id TEXT, attempts INTEGER NOT NULL DEFAULT 0,
      available_at INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      code TEXT, confirmation TEXT);
    CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(state,priority,created_at);
    CREATE TABLE IF NOT EXISTS auth(kind TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL,
      PRIMARY KEY(kind,id));`);
  if (recover) db.exec(`
    UPDATE jobs SET state='unknown', code='interrupted_dispatch' WHERE state='dispatching';
    UPDATE push_events SET state='pending' WHERE state='processing';`);
  const transaction = fn => {
    db.exec('BEGIN IMMEDIATE');
    try { const result = fn(); db.exec('COMMIT'); return result; }
    catch (e) { db.exec('ROLLBACK'); throw e; }
  };
  return {
    db, transaction,
    getSetting(key) { return db.prepare('SELECT value FROM settings WHERE key=?').get(key)?.value; },
    setSetting(key, value) { db.prepare('INSERT OR REPLACE INTO settings VALUES(?,?)').run(key, String(value)); },
    recordPush(event, now = new Date()) {
      const payload = JSON.stringify(event);
      // Canonical metadata ordering makes a CDP replay stable across restarts.
      const id = digest(JSON.stringify([event.origin,event.service,event.timestamp,event.instanceId,event.eventName,
        [...(event.eventMetadata || [])].map(m=>[m.key,m.value]).sort((a,b)=>a[0].localeCompare(b[0]))]));
      const result = db.prepare('INSERT OR IGNORE INTO push_events(id,received_at,payload) VALUES(?,?,?)').run(id, now.toISOString(), payload);
      return { id, added: result.changes === 1 };
    },
    nextEvent() { return db.prepare("SELECT * FROM push_events WHERE state='pending' ORDER BY received_at LIMIT 1").get(); },
    updateEvent(id, state, code = null) {
      db.prepare('UPDATE push_events SET state=?,code=?,attempts=attempts+? WHERE id=?').run(state,code,state==='processing'?1:0,id);
    },
    enqueue({ key, destination, payload, priority = 10 }, now = Date.now()) {
      const serialized = JSON.stringify(payload);
      const hash = digest(JSON.stringify({ destination, payload }));
      return transaction(() => {
        const old = db.prepare('SELECT * FROM jobs WHERE dedup_key=?').get(key);
        if (old) {
          if (old.request_hash !== hash) throw new Error('idempotency_conflict');
          return old;
        }
        const id = randomUUID();
        db.prepare('INSERT INTO jobs(id,dedup_key,request_hash,destination,payload,priority,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)')
          .run(id,key,hash,destination,serialized,priority,now,now);
        return this.job(id);
      });
    },
    job(id) { return db.prepare('SELECT * FROM jobs WHERE id=?').get(id); },
    jobByKey(key) { return db.prepare('SELECT * FROM jobs WHERE dedup_key=?').get(key); },
    claim(now = Date.now()) {
      return transaction(() => {
        const row = db.prepare("SELECT * FROM jobs WHERE state='queued' AND available_at<=? ORDER BY priority,created_at LIMIT 1").get(now);
        if (!row) return null;
        const messageId = randomUUID().replaceAll('-', '').toUpperCase();
        db.prepare("UPDATE jobs SET state='dispatching',message_id=?,attempts=attempts+1,updated_at=? WHERE id=?")
          .run(messageId,now,row.id);
        return this.job(row.id);
      });
    },
    updateJob(id, state, code = null, confirmation = null, availableAt = 0) {
      db.prepare('UPDATE jobs SET state=?,code=?,confirmation=?,available_at=?,updated_at=? WHERE id=?')
        .run(state,code,confirmation,availableAt,Date.now(),id);
    },
    receipt(messageId, jid, state, level) {
      const rows = db.prepare("SELECT id,destination FROM jobs WHERE message_id=? AND state IN ('dispatching','unknown','accepted','confirmed')").all(messageId);
      for (const row of rows) {
        if (row.destination !== jid) continue;
        const old = this.job(row.id);
        if (old.state === 'confirmed' && state !== 'confirmed') continue;
        this.updateJob(row.id,state,null,level);
      }
    },
    snapshot() {
      return {
        events: db.prepare('SELECT state,COUNT(*) AS count FROM push_events GROUP BY state').all(),
        jobs: db.prepare('SELECT state,COUNT(*) AS count FROM jobs GROUP BY state').all(),
        activatedAt: this.getSetting('activated_at') || null,
      };
    },
    close() { db.close(); },
  };
}
