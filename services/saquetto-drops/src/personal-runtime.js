import { createHash, timingSafeEqual } from 'node:crypto';

const TOKEN = /^[a-f0-9]{64}$/;

export function validatePersonalToken(token) {
  if (typeof token !== 'string' || !TOKEN.test(token)) throw new Error('BAILEYS_PERSONAL_TOKEN_required');
  return token;
}

export function personalAuthorized(value, token) {
  if (!TOKEN.test(token || '')) return false;
  const supplied = String(value || '').replace(/^Bearer\s+/i, '');
  const digest = input => createHash('sha256').update(input).digest();
  return timingSafeEqual(digest(supplied), digest(token));
}

export function maskSenderJid(jid) {
  const raw = String(jid || '');
  const local = raw.split('@')[0].replace(/:.*$/, '').replace(/\D/g, '');
  return local ? `****${local.slice(-4)}` : null;
}

export function personalStatus({ whatsapp, pairingRequired }) {
  const jid = whatsapp.ownDestination?.().jid || '';
  return {
    ok: true,
    whatsapp: whatsapp.state(),
    connected: whatsapp.isReady() === true,
    pairingRequired: pairingRequired === true,
    sender: maskSenderJid(jid),
  };
}


export function personalManualConfig(whatsapp) {
  const self = whatsapp?.isReady?.() === true ? whatsapp.ownDestination?.() : null;
  const verified = self && self.type === 'contact' && typeof self.jid === 'string' && self.jid && self.verified === true;
  return {
    operation: { paused: false, dryRun: false, minDelayMs: 1000 },
    destinations: verified ? { self: { type: 'contact', jid: self.jid, verified: true } } : {},
  };
}

export function personalWhatsAppRouteAllowed(method, path) {
  if (method === 'GET' && path === '/v1/whatsapp/destinations') return true;
  if (method === 'GET' && /^\/v1\/whatsapp\/requests\/[a-f0-9-]{36}$/i.test(path)) return true;
  if (method === 'POST' && (path === '/v1/whatsapp/send' || path === '/v1/whatsapp/send-chat')) return true;
  return false;
}
