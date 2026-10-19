export const APP_IDS = Object.freeze(['clube-uol', 'bilhete-unico', 'super-calendario', 'media-downloader', 'pilltracker', 'lastfm-small']);
export const MAX_BODY = 4096;
const DAY = 86400000;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const TOKEN = /^[0-9a-f]{64}$/;
const encoder = new TextEncoder();

export class HttpError extends Error {
  constructor(status, code) { super(code); this.status = status; this.code = code; }
}
const fail = (status, code) => { throw new HttpError(status, code); };
const result = (status, body) => ({ status, body });
const denied = (status, error) => result(status, { error });

export function webCrypto(crypto) {
  return {
    async hash(value) {
      return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(value))), n => n.toString(16).padStart(2, '0')).join('');
    },
    async equal(a, b) {
      const [left, right] = await Promise.all([a, b].map(v => crypto.subtle.digest('SHA-256', encoder.encode(v))));
      return crypto.subtle.timingSafeEqual(left, right);
    },
    randomHex(bytes) { return Array.from(crypto.getRandomValues(new Uint8Array(bytes)), n => n.toString(16).padStart(2, '0')).join(''); },
  };
}

export function initialState(crypto) {
  return { users: [], ipSalt: crypto.randomHex(32), limits: { day: -1, count: 0, ips: {} } };
}

export function responseFor(outcome) {
  return Response.json(outcome.body, { status: outcome.status, headers: {
    'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'no-referrer', 'Strict-Transport-Security': 'max-age=31536000',
  } });
}

export function errorResult(error) {
  return error instanceof HttpError ? denied(error.status, error.code) : denied(503, 'access_unavailable');
}

// Read no more than 4 KiB even when Content-Length is absent or incorrect.
export async function prepareRequest(request) {
  const url = new URL(request.url);
  if (url.protocol !== 'https:') fail(400, 'https_required');
  if (request.headers.has('Origin')) fail(403, 'native_client_required');
  if (url.search || url.hash) fail(400, 'invalid_request');
  const routes = { '/health': 'GET', '/v1/enroll': 'POST', '/v1/bootstrap': 'POST', '/v1/me': 'GET', '/v1/users': 'GET', '/v1/decision': 'POST' };
  const route = routes[url.pathname] || (/^\/v1\/access\/[^/]+$/.test(url.pathname) ? 'GET' : null);
  if (!route) fail(404, 'not_found');
  if (request.method !== route) fail(405, 'method_not_allowed');
  let body;
  if (route === 'POST') {
    if (!/^application\/json(?:\s*;.*)?$/i.test(request.headers.get('Content-Type') || '')) fail(415, 'json_required');
    if (request.headers.get('Content-Encoding')) fail(415, 'encoding_not_supported');
    const length = request.headers.get('Content-Length');
    if (length !== null && (!/^\d+$/.test(length) || Number(length) > MAX_BODY)) fail(413, 'payload_too_large');
    const reader = request.body?.getReader();
    if (!reader) fail(400, 'invalid_json');
    const chunks = [];
    let size = 0;
    try {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        size += chunk.value.byteLength;
        if (size > MAX_BODY) { await reader.cancel(); fail(413, 'payload_too_large'); }
        chunks.push(chunk.value);
      }
    } finally { reader.releaseLock(); }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
    try { body = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); } catch { fail(400, 'invalid_json'); }
  }
  const authorization = request.headers.get('Authorization') || '';
  if (authorization.length > 256) fail(401, 'unauthorized');
  return { method: request.method, path: url.pathname, bearer: /^Bearer ([^\s]+)$/i.exec(authorization)?.[1] || '', body,
    ip: request.headers.get('CF-Connecting-IP') || 'unknown' };
}

function exactObject(value, keys) {
  return value && typeof value === 'object' && !Array.isArray(value) &&
    Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
}
function identity(body) {
  if (!exactObject(body, ['name', 'deviceId', 'token']) || typeof body.name !== 'string' ||
      body.name.trim().length < 1 || body.name.trim().length > 60 || /[\p{Cc}\p{Cf}]/u.test(body.name) ||
      typeof body.deviceId !== 'string' || !UUID.test(body.deviceId) || typeof body.token !== 'string' || !TOKEN.test(body.token)) fail(400, 'invalid_enrollment');
  return { name: body.name.trim(), id: body.deviceId.toLowerCase(), token: body.token };
}
function decision(body) {
  if (!exactObject(body, ['id', 'status', 'appIds', 'code']) || typeof body.id !== 'string' || !UUID.test(body.id) ||
      !['approved', 'revoked'].includes(body.status) || typeof body.code !== 'string' || !/^[A-F0-9]{8}$/.test(body.code) ||
      !Array.isArray(body.appIds) || body.appIds.length > APP_IDS.length || new Set(body.appIds).size !== body.appIds.length ||
      body.appIds.some(id => !APP_IDS.includes(id))) fail(400, 'invalid_decision');
  return { ...body, id: body.id.toLowerCase() };
}
function profile(user) {
  return { id: user.id, name: user.name, role: user.role, status: user.expired ? 'expired' : user.status,
    appIds: user.status === 'approved' ? [...user.appIds] : [], code: user.code };
}
function expire(state, now) {
  for (const user of state.users) {
    if (user.status === 'pending' && now - user.createdAt >= 7 * DAY) {
      user.status = 'revoked'; user.expired = true; user.appIds = [];
    }
  }
}
function limited(state, ipHash, now) {
  const day = Math.floor(now / DAY);
  if (state.limits.day !== day) state.limits = { day, count: 0, ips: {} };
  const hour = Math.floor(now / 3600000);
  for (const [hash, bucket] of Object.entries(state.limits.ips)) if (bucket.hour !== hour) delete state.limits.ips[hash];
  const bucket = state.limits.ips[ipHash] || { hour, count: 0 };
  if (state.limits.count >= 40 || bucket.count >= 10) return true;
  bucket.count += 1; state.limits.count += 1; state.limits.ips[ipHash] = bucket;
  return false;
}

// store.read() returns a snapshot; store.transact(fn) commits atomically before returning.
// Crypto completes before transactions, leaving no await between auth checks and writes.
export function createAccess({ store, crypto, clock = Date.now, bootstrapToken = '' }) {
  return {
    async handle(input) {
      try {
        if (input.path === '/health') return result(200, { status: 'ready', service: 'saquettoos-access', version: '1.0.0' });
        const enrollment = ['/v1/enroll', '/v1/bootstrap'].includes(input.path) ? identity(input.body) : null;
        const change = input.path === '/v1/decision' ? decision(input.body) : null;
        const bootstrap = input.path === '/v1/bootstrap';
        if (bootstrap && (!bootstrapToken || !await crypto.equal(input.bearer, bootstrapToken))) return denied(401, 'unauthorized');
        const token = enrollment?.token || input.bearer;
        if (!TOKEN.test(token)) return denied(401, 'unauthorized');
        const tokenHash = await crypto.hash(token);
        const ipHash = enrollment && !bootstrap ? await crypto.hash(store.read().ipSalt + ':' + input.ip) : '';
        const code = enrollment ? crypto.randomHex(4).toUpperCase() : '';
        const now = clock();
        return store.transact(state => {
          expire(state, now);
          if (enrollment) {
            const existing = state.users.find(user => user.id === enrollment.id);
            if (bootstrap) {
              const admin = state.users.find(user => user.role === 'admin');
              if (admin) return admin.id === enrollment.id && admin.tokenHash === tokenHash ? result(200, { profile: profile(admin) }) : denied(409, 'bootstrap_closed');
            } else if (existing?.tokenHash === tokenHash) {
              // Only an expired pending registration can explicitly request approval again.
              // Manual revocation and all existing approved grants survive retries unchanged.
              if (existing.expired) {
                if (limited(state, ipHash, now)) return denied(429, 'enrollment_limited');
                if (state.users.filter(user => user.status === 'pending').length >= 32) return denied(429, 'enrollment_capacity');
                existing.status = 'pending'; existing.expired = false; existing.appIds = [];
                existing.createdAt = now; existing.code = code;
              }
              return result(200, { profile: profile(existing) });
            }
            if (!bootstrap && limited(state, ipHash, now)) return denied(429, 'enrollment_limited');
            if (existing || state.users.some(user => user.tokenHash === tokenHash)) return denied(409, 'enrollment_conflict');
            if (!bootstrap && state.users.filter(user => user.status === 'pending').length >= 32) return denied(429, 'enrollment_capacity');
            if (state.users.length >= 128) {
              // Never discard approved users or manual revocations. Expired unapproved
              // registrations may yield their oldest slot to a fresh pending request.
              const oldest = state.users.filter(user => user.expired === true && user.role === 'member')
                .sort((a, b) => a.createdAt - b.createdAt)[0];
              if (!oldest) return denied(429, 'enrollment_capacity');
              state.users.splice(state.users.findIndex(user => user.id === oldest.id), 1);
            }
            const user = { id: enrollment.id, name: enrollment.name, tokenHash, code, createdAt: now,
              role: bootstrap ? 'admin' : 'member', status: bootstrap ? 'approved' : 'pending', appIds: bootstrap ? [...APP_IDS] : [] };
            state.users.push(user);
            return result(201, { profile: profile(user) });
          }
          const actor = state.users.find(user => user.tokenHash === tokenHash);
          if (!actor) return denied(401, 'unauthorized');
          if (input.path === '/v1/me') return result(200, { profile: profile(actor) });
          if (input.path.startsWith('/v1/access/')) {
            const appId = input.path.slice('/v1/access/'.length);
            if (!APP_IDS.includes(appId)) return denied(404, 'app_not_found');
            return actor.status === 'approved' && actor.appIds.includes(appId) ? result(200, { allowed: true }) : denied(403, 'access_denied');
          }
          if (actor.role !== 'admin' || actor.status !== 'approved') return denied(403, 'admin_required');
          if (input.path === '/v1/users') return result(200, { users: state.users.map(profile) });
          if (change) {
            const user = state.users.find(item => item.id === change.id);
            if (!user) return denied(404, 'user_not_found');
            if (user.role === 'admin') return denied(403, 'admin_immutable');
            if (user.code !== change.code) return denied(409, 'confirmation_mismatch');
            if (user.expired) return denied(409, 'enrollment_expired');
            user.status = change.status;
            user.appIds = change.status === 'approved' ? [...change.appIds] : [];
            return result(200, { profile: profile(user) });
          }
          return denied(404, 'not_found');
        });
      } catch (error) { return errorResult(error); }
    },
  };
}
