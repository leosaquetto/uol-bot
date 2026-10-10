import { DatabaseSync } from 'node:sqlite';
import { existsSync, lstatSync, openSync, closeSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { privateDirectory } from './private.mjs';

export function openPushStore(path) {
  privateDirectory(dirname(path));
  if (existsSync(path)) {
    const s = lstatSync(path);
    if (!s.isFile() || s.isSymbolicLink() || (s.mode & 0o777) !== 0o600 || s.uid !== process.getuid())
      throw new Error('private_database_required');
  } else closeSync(openSync(path, 'wx', 0o600));
  const db = new DatabaseSync(path);
  db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
    CREATE TABLE IF NOT EXISTS push_events(id TEXT PRIMARY KEY, received_at INTEGER NOT NULL,
      packet TEXT NOT NULL, packet_hash TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', reason TEXT);
    CREATE TABLE IF NOT EXISTS signals(seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL,
      received_at INTEGER NOT NULL, profile TEXT NOT NULL, story_id TEXT, evidence TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS receiver_state(id INTEGER PRIMARY KEY CHECK(id=1), status TEXT NOT NULL,
      observed_at INTEGER NOT NULL);`);
  const transaction = fn => {
    db.exec('BEGIN IMMEDIATE');
    try { const result=fn(); db.exec('COMMIT'); return result; }
    catch(e) { db.exec('ROLLBACK'); throw e; }
  };
  return {
    db,
    persist(packet, now=Date.now()) {
      const id = createHash('sha256').update(packet.channelID+'\n'+packet.version).digest('hex');
      const serialized = JSON.stringify(packet), hash=createHash('sha256').update(JSON.stringify([
        packet.channelID,packet.version,packet.data,packet.headers?.encoding,
        packet.headers?.encryption,packet.headers?.crypto_key])).digest('hex');
      return transaction(() => {
        const old=db.prepare('SELECT packet_hash FROM push_events WHERE id=?').get(id);
        if (old && old.packet_hash !== hash) throw new Error('push_version_conflict');
        if (!old && db.prepare('SELECT count(*) AS n FROM push_events').get().n >= 5000)
          throw new Error('push_retention_limit');
        const saved=db.prepare('INSERT OR IGNORE INTO push_events(id,received_at,packet,packet_hash) VALUES(?,?,?,?)')
          .run(id,now,serialized,hash);
        return { id, added: saved.changes === 1 };
      });
    },
    pending() { return db.prepare("SELECT id,packet,received_at FROM push_events WHERE state='pending' ORDER BY received_at LIMIT 50").all(); },
    finish(event, signal, reason) {
      transaction(() => {
        if (signal) db.prepare('INSERT OR IGNORE INTO signals(event_id,received_at,profile,story_id,evidence) VALUES(?,?,?,?,?)')
          .run(event.id,event.received_at,signal.profile,signal.storyId,signal.evidence);
        db.prepare('UPDATE push_events SET state=?,reason=? WHERE id=?').run(signal?'signalled':'ignored',reason,event.id);
      });
    },
    health(status, now=Date.now()) {
      db.prepare('INSERT INTO receiver_state VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,observed_at=excluded.observed_at')
        .run(status,now);
    },
    close() { db.close(); },
  };
}
