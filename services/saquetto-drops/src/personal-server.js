import { createServer } from 'node:http';
import { mkdirSync, writeFileSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { openStore } from './store.js';
import { startWhatsApp } from './whatsapp.js';
import { acquireLock } from './process-lock.js';
import { personalAuthorized, personalManualConfig, personalStatus, personalWhatsAppRouteAllowed, validatePersonalToken } from './personal-runtime.js';
import { createPersonalTextApi, createPersonalTextSender } from './personal-text.js';
import { createPersonalInbox } from './personal-inbox.js';
import { createPersonalInboxApi } from './personal-inbox-api.js';

process.umask(0o077);
const data = process.env.BAILEYS_PERSONAL_DATA || '/var/lib/baileys-personal';
const port = Number(process.env.BAILEYS_PERSONAL_PORT || 8790);
const token = validatePersonalToken(process.env.BAILEYS_PERSONAL_TOKEN);
if (!Number.isSafeInteger(port) || port < 1024 || port > 65535) throw new Error('invalid_personal_port');
mkdirSync(data, { recursive: true, mode: 0o700 });
const releaseLock = acquireLock(join(data, 'service.lock'));
const store = openStore(join(data, 'baileys.sqlite'));
const qrPath = join(data, 'pairing-qr.private.txt');
let qrAvailable = false;
let closing = false;
const personalInbox = createPersonalInbox({ store });

const whatsapp = startWhatsApp({
  personalInbox,
  store,
  onQr(qr) {
    qrAvailable = Boolean(qr);
    if (qr) writeFileSync(qrPath, qr, { mode: 0o600 });
    else { try { unlinkSync(qrPath); } catch {} }
  },
});

const getConfig = () => personalManualConfig(whatsapp);
const canSend = () => !closing && whatsapp.isReady() === true;
const personalText = createPersonalTextApi({ store, getConfig, canSend });
const personalInboxApi = createPersonalInboxApi({ inbox: personalInbox, whatsapp });
const sendNext = createPersonalTextSender({ store, getConfig, whatsapp, canSend });
const timers = [setInterval(() => sendNext().catch(() => {}), 1000)];

const server = createServer(async (req, res) => {
  const reply = (code, value) => {
    res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(value));
  };
  if (!['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(req.socket.remoteAddress)) {
    return reply(403, { code: 'loopback_required' });
  }
  if (!personalAuthorized(req.headers.authorization, token)) return reply(401, { code: 'unauthorized' });
  const url = new URL(req.url, 'http://localhost');
  const path = url.pathname;
  if (req.method === 'GET' && path === '/v1/status') return reply(200, personalStatus({ whatsapp, pairingRequired: qrAvailable }));
  if (personalInboxApi.matches(req.method, path)) {
    const result = await personalInboxApi.route(req, url);
    return reply(result.status, result.body);
  }
  if (personalWhatsAppRouteAllowed(req.method, path)) {
    const result = await personalText.route(req, path);
    return reply(result.status, result.body);
  }
  return reply(404, { code: 'not_found' });
});
server.headersTimeout = 10000;
server.requestTimeout = 10000;
server.listen(port, '127.0.0.1');

const shutdown = async () => {
  if (closing) return;
  closing = true;
  server.close();
  for (const timer of timers) clearInterval(timer);
  await whatsapp.stop().catch(() => {});
  store.close();
  releaseLock();
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
