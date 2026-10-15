import { DatabaseSync } from 'node:sqlite';
import { existsSync, lstatSync, openSync, closeSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { privateDirectory } from './private.mjs';

export const PUSH_STORE_LIMITS = Object.freeze({pending:5000, payloadBytes:32*1024*1024,
  events:100000, packetBytes:256*1024, retentionMs:7*24*60*60*1000, batch:50});
// Only aggregate enums are diagnostic data: never add packet fields or identities here.
export const PUSH_DIAGNOSTIC_KEYS = Object.freeze([
  'receiver_start','connect_attempt','connection_open','connection_ready','connection_error','connection_closed',
  'frames_received','frames_rejected_type','frames_rejected_size','frames_rejected_json','frames_rejected_shape',
  'frames_rejected_hello','frames_rejected_ready','frames_rejected_channel','frames_rejected_version','frames_rejected_data',
  'frames_unknown_message_type','notifications_persisted','notifications_replayed','notifications_signalled',
  'notifications_ignored_decryption','notifications_ignored_classification','ack_attempted','ack_sent','ack_failed',
  'storage_rejected_pending_limit','storage_rejected_payload_limit','storage_rejected_identity_limit',
  'storage_rejected_packet_limit','storage_rejected_version_conflict','storage_rejected_unavailable',
]);
const diagnosticKeys=new Set(PUSH_DIAGNOSTIC_KEYS);
const pressureBytes=Math.floor(PUSH_STORE_LIMITS.payloadBytes*0.8);

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
      observed_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS receiver_diagnostics(key TEXT PRIMARY KEY CHECK(key IN (${PUSH_DIAGNOSTIC_KEYS.map(key=>`'${key}'`).join(',')})),
      count INTEGER NOT NULL CHECK(count>=0),
      first_at INTEGER NOT NULL, last_at INTEGER NOT NULL);`);
  const columns=new Set(db.prepare('PRAGMA table_info(push_events)').all().map(column=>column.name));
  // Nullable metadata keeps legacy packets usable while bounded maintenance resumes.
  for(const column of ['packet_bytes','completed_at','compacted_at'])
    if(!columns.has(column))db.exec(`ALTER TABLE push_events ADD COLUMN ${column} INTEGER`);
  db.exec(`CREATE INDEX IF NOT EXISTS push_pending_order ON push_events(state,received_at);
    CREATE INDEX IF NOT EXISTS push_terminal_retention ON push_events(COALESCE(completed_at,received_at))
      WHERE state<>'pending' AND packet<>'{}';`);
  const transaction = fn => {
    db.exec('BEGIN IMMEDIATE');
    try { const result=fn(); db.exec('COMMIT'); return result; }
    catch(e) { db.exec('ROLLBACK'); throw e; }
  };
  const lookup=db.prepare('SELECT packet_hash FROM push_events WHERE id=?');
  const incrementDiagnostic=db.prepare(`INSERT INTO receiver_diagnostics(key,count,first_at,last_at) VALUES(?,1,?,?)
    ON CONFLICT(key) DO UPDATE SET count=count+1,last_at=MAX(last_at,excluded.last_at)`);
  const diagnostics=(keys,now)=>{
    if(!Array.isArray(keys)||!keys.length||!Number.isSafeInteger(now)||now<0||
       keys.some(key=>!diagnosticKeys.has(key)))throw new Error('invalid_push_diagnostic');
    for(const key of keys)incrementDiagnostic.run(key,now,now);
  };
  const totals=db.prepare(`SELECT count(*) AS eventCount,COALESCE(sum(state='pending'),0) AS pendingCount,
    COALESCE(sum(COALESCE(packet_bytes,length(CAST(packet AS BLOB)))),0) AS payloadBytes,
    COALESCE(sum(compacted_at IS NOT NULL),0) AS compactedCount FROM push_events`);
  const capacity=()=>{
    const counts=totals.get();
    return {...counts,limits:PUSH_STORE_LIMITS,pressureBytes,pressure:counts.payloadBytes>=pressureBytes,
      reason:counts.pendingCount>=PUSH_STORE_LIMITS.pending?'push_pending_limit':
        counts.eventCount>=PUSH_STORE_LIMITS.events?'push_identity_limit':
        counts.payloadBytes>=PUSH_STORE_LIMITS.payloadBytes?'push_payload_limit':null};
  };
  const replay=(id,hash)=>{
    const old=lookup.get(id);
    if(old && old.packet_hash!==hash)throw new Error('push_version_conflict');
    return Boolean(old);
  };
  const store={
    db,
    capacity,
    diagnostics(keys,now=Date.now()) { return transaction(()=>diagnostics(keys,now)); },
    maintain(now=Date.now()) {
      return transaction(()=>{
        const counts=capacity(),cutoff=now-PUSH_STORE_LIMITS.retentionMs;
        const rows=db.prepare(`SELECT id,COALESCE(packet_bytes,length(CAST(packet AS BLOB))) AS bytes,
          COALESCE(completed_at,received_at) AS completed FROM push_events
          WHERE state<>'pending' AND packet<>'{}' AND (COALESCE(completed_at,received_at)<=? OR ?)
          ORDER BY COALESCE(completed_at,received_at),id LIMIT ?`)
          .all(cutoff,counts.pressure?1:0,PUSH_STORE_LIMITS.batch);
        const compact=db.prepare("UPDATE push_events SET packet='{}',packet_bytes=2,compacted_at=? WHERE id=? AND state<>'pending'");
        let bytes=counts.payloadBytes,compacted=0;
        for(const row of rows) {
          if(row.completed>cutoff && bytes<pressureBytes)break;
          compact.run(now,row.id);bytes-=row.bytes-2;compacted++;
        }
        const remaining=counts.pendingCount>0 || Boolean(db.prepare(`SELECT 1 FROM push_events
          WHERE state<>'pending' AND packet<>'{}' AND (COALESCE(completed_at,received_at)<=? OR ?) LIMIT 1`)
          .get(cutoff,bytes>=pressureBytes?1:0));
        return {compacted,remaining};
      });
    },
    persist(packet, now=Date.now()) {
      const id = createHash('sha256').update(packet.channelID+'\n'+packet.version).digest('hex');
      const serialized = JSON.stringify(packet), hash=createHash('sha256').update(JSON.stringify([
        packet.channelID,packet.version,packet.data,packet.headers?.encoding,
        packet.headers?.encryption,packet.headers?.crypto_key])).digest('hex');
      // Replay identity remains valid after compaction and even while capacity is exhausted.
      if(replay(id,hash))return transaction(()=>{
        replay(id,hash);diagnostics(['notifications_replayed'],now);return {id,added:false};
      });
      const bytes=Buffer.byteLength(serialized,'utf8');
      if(bytes>PUSH_STORE_LIMITS.packetBytes)throw new Error('push_packet_limit');
      store.maintain(now);
      return transaction(() => {
        if(replay(id,hash)) {
          diagnostics(['notifications_replayed'],now);return {id,added:false};
        }
        const counts=capacity();
        if(counts.pendingCount>=PUSH_STORE_LIMITS.pending)throw new Error('push_pending_limit');
        if(counts.eventCount>=PUSH_STORE_LIMITS.events)throw new Error('push_identity_limit');
        if(counts.payloadBytes+bytes>PUSH_STORE_LIMITS.payloadBytes)throw new Error('push_payload_limit');
        const saved=db.prepare('INSERT INTO push_events(id,received_at,packet,packet_hash,packet_bytes) VALUES(?,?,?,?,?)')
          .run(id,now,serialized,hash,bytes);
        diagnostics(['notifications_persisted'],now);
        return { id, added: saved.changes === 1 };
      });
    },
    pending() { return db.prepare("SELECT id,packet,received_at FROM push_events WHERE state='pending' ORDER BY received_at LIMIT 50").all(); },
    finish(event, signal, reason, now=Date.now()) {
      return transaction(() => {
        const saved=db.prepare("UPDATE push_events SET state=?,reason=?,completed_at=? WHERE id=? AND state='pending'")
          .run(signal?'signalled':'ignored',reason,now,event.id);
        if(!saved.changes)return false;
        if (signal) db.prepare('INSERT OR IGNORE INTO signals(event_id,received_at,profile,story_id,evidence) VALUES(?,?,?,?,?)')
          .run(event.id,event.received_at,signal.profile,signal.storyId,signal.evidence);
        diagnostics([signal?'notifications_signalled':reason==='push_decryption_rejected'?
          'notifications_ignored_decryption':'notifications_ignored_classification'],now);
        return true;
      });
    },
    health(status, now=Date.now()) {
      db.prepare('INSERT INTO receiver_state VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,observed_at=excluded.observed_at')
        .run(status,now);
    },
    close() { db.close(); },
  };
  return store;
}
