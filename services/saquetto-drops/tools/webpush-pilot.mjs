// Bounded receive-only probe. Persist encrypted pushes before acknowledging them.
// No browser, X polling, message sending, or automatic subscription replacement.
import { readFileSync, chmodSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { resolve, dirname, join } from 'node:path';

process.umask(0o077);
const path = resolve(process.argv[2]);
const registration = JSON.parse(readFileSync(path, 'utf8'));
if (!registration.uaid || !registration.channelID ||
    new URL(registration.endpoint).hostname !== 'updates.push.services.mozilla.com') throw new Error('invalid_registration');
const dbPath = join(dirname(path), 'webpush-pilot.sqlite');
const db = new DatabaseSync(dbPath);
chmodSync(dbPath, 0o600);
db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
  CREATE TABLE IF NOT EXISTS events(version TEXT PRIMARY KEY, received_at TEXT NOT NULL, payload TEXT NOT NULL);`);
const insert = db.prepare('INSERT OR IGNORE INTO events VALUES(?,?,?)');
let socket, stopped = false, retry, handshake, heartbeat, pong, attempts = 0;
const log = event => console.log(JSON.stringify({ event, rssMiB: Math.round(process.memoryUsage().rss / 1048576) }));
const clearTimers = () => { clearTimeout(handshake); clearInterval(heartbeat); clearTimeout(pong); };
const connect = () => {
  socket = new WebSocket('wss://push.services.mozilla.com/');
  handshake = setTimeout(() => socket.close(), 15000);
  socket.addEventListener('open', () => socket.send(JSON.stringify({
    messageType: 'hello', uaid: registration.uaid, use_webpush: true, channelIDs: [registration.channelID],
  })));
  socket.addEventListener('message', ({data}) => {
    try {
      if (typeof data !== 'string' || data.length > 262144) throw new Error('invalid_frame');
      const m = JSON.parse(data);
      if (m.messageType === 'hello') {
        clearTimeout(handshake);
        if (m.status !== 200 || m.uaid !== registration.uaid) {
          stopped = true; log('registration_requires_review'); socket.close(); return;
        }
        attempts = 0; log('connected');
        heartbeat = setInterval(() => {
          socket.send('{}'); pong = setTimeout(() => socket.close(), 45000);
        }, 240000);
      } else if (m.messageType === 'notification') {
        if (m.channelID !== registration.channelID || typeof m.version !== 'string') throw new Error('unexpected_notification');
        const saved = insert.run(m.version, new Date().toISOString(), JSON.stringify(m));
        socket.send(JSON.stringify({messageType:'ack',updates:[{channelID:m.channelID,version:m.version,code:100}]}));
        if (saved.changes) log('push_persisted');
      } else if (!m.messageType || m.messageType === 'ping') clearTimeout(pong);
    } catch { log('frame_not_accepted'); socket.close(); }
  });
  socket.addEventListener('error', () => log('connection_error'));
  socket.addEventListener('close', () => {
    clearTimers();
    if (!stopped) retry = setTimeout(connect, Math.min(60000, 2000 * 2 ** Math.min(attempts++, 5)));
  });
};
const stop = () => { stopped = true; clearTimeout(retry); clearTimers(); socket?.close(); };
process.on('SIGTERM', stop); process.on('SIGINT', stop);
connect();
