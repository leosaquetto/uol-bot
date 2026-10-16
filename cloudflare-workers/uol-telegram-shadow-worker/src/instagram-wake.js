// Notification hints only. Instagram sessions and destination credentials never reach phones.
export const WAKE_METADATA_KEY = 'instagram_notification_wake_v1';
export const WAKE_BODY_LIMIT = 4096;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const HOUR = 3600000;

export function wakeError(code, status = 400) {
  return Object.assign(new Error(code), { httpStatus: status, wakeInput: true });
}
export function normalizeWake(value, now = Date.now()) {
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
      Object.keys(value).some(k => !['eventId','profile','source','observedAt'].includes(k)) ||
      !UUID.test(String(value.eventId || '')) || value.profile !== 'clubeuol' ||
      !['ios_shortcuts','scriptable'].includes(value.source) ||
      typeof value.observedAt !== 'string' || value.observedAt.length > 40) throw wakeError('instagram_wake_invalid');
  const at = Date.parse(value.observedAt);
  // Client time is diagnostic. An offline phone may replay its durable hint days
  // later; server receipt time governs budgets and a current snapshot is useful.
  if (!Number.isFinite(at) || at <= 0 || at > now + 300000) throw wakeError('instagram_wake_time_invalid');
  return { eventId: value.eventId.toLowerCase(), profile: 'clubeuol', source: value.source,
    observedAt: new Date(at).toISOString() };
}
export async function authenticateWake(request, env) {
  const supplied = request.headers.get('Authorization') || '';
  const match = /^Bearer (w1\.[a-z0-9-]{1,32}\.[a-f0-9]{64})$/.exec(supplied);
  if (!match) return '';
  let clients;
  try { clients = JSON.parse(env.INSTAGRAM_WAKE_CLIENTS || '{}'); } catch { return ''; }
  const clientId = match[1].split('.')[1], expected = clients?.[clientId];
  if (!/^[a-f0-9]{64}$/.test(expected || '')) return '';
  const actual = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(match[1]))))
    .map(n => n.toString(16).padStart(2, '0')).join('');
  let diff = 0;
  for (let i = 0; i < 64; i++) diff |= actual.charCodeAt(i) ^ expected.charCodeAt(i);
  return diff === 0 ? clientId : '';
}

export class InstagramWakeQueue {
  constructor(read, write, env, fetchImpl = fetch, clock = Date.now) {
    this.read = read; this.write = write; this.env = env;
    // Web-platform globals must not receive this queue instance as their receiver.
    this.fetch = (...args) => fetchImpl(...args);
    this.clock = clock;
    this.inFlight = false;
  }
  load() {
    const raw = this.read(WAKE_METADATA_KEY);
    const state = raw ? JSON.parse(raw) : { schema: 1, pending: [], recent: [], last: null };
    if (state.schema !== 1 || !Array.isArray(state.pending) || !Array.isArray(state.recent) ||
        state.pending.length > 64 || state.recent.length > 512) throw wakeError('instagram_wake_state_invalid', 503);
    return state;
  }
  save(state) { this.write(WAKE_METADATA_KEY, JSON.stringify(state)); }
  enqueue(input, clientId) {
    if (!/^[a-z0-9-]{1,32}$/.test(clientId)) throw wakeError('instagram_wake_client_invalid');
    const now = this.clock(), event = normalizeWake(input, now), state = this.load();
    const previous = state.recent.find(r => r.id === event.eventId);
    const signature = JSON.stringify(event);
    if (previous) {
      if (previous.signature !== signature) throw wakeError('instagram_wake_identity_conflict', 409);
      return { ok: true, accepted: true, duplicate: true, eventId: event.eventId, status: 'recorded' };
    }
    state.recent = state.recent.filter(r => r.at > now - 86400000);
    const hourly = state.recent.filter(r => r.at > now - HOUR);
    if (hourly.length >= 60 || hourly.filter(r => r.client === clientId).length >= 30) {
      throw wakeError('instagram_wake_rate_limit', 429);
    }
    if (state.pending.length >= 64 || state.recent.length >= 512) throw wakeError('instagram_wake_capacity', 429);
    state.recent.push({ id: event.eventId, client: clientId, at: now, signature });
    // Coalesce only work never attempted. In-flight/uncertain relay identity stays fixed.
    let pending = state.pending.find(p => p.attempts === 0);
    if (pending) pending.count += 1;
    else state.pending.push({ event, count: 1, attempts: 0, nextAt: now, createdAt: now });
    this.save(state);
    return { ok: true, accepted: true, duplicate: false, eventId: event.eventId, status: 'queued' };
  }
  async drain() {
    if (this.inFlight) return { ok: true, status: 'busy' };
    this.inFlight = true;
    try {
      for (let n = 0; n < 2; n++) {
        const state = this.load(), now = this.clock(), pending = state.pending[0];
        if (!pending || pending.nextAt > now) return { ok: true, status: pending ? 'retry_wait' : 'idle' };
        let url;
        try { url = new URL(this.env.INSTAGRAM_WAKE_RELAY_URL); } catch { return { ok: false, status: 'not_configured' }; }
        if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash ||
            url.pathname !== '/instagram-wake' || !/^[a-f0-9]{64}$/.test(this.env.INSTAGRAM_WAKE_RELAY_TOKEN || '')) {
          return { ok: false, status: 'not_configured' };
        }
        pending.attempts += 1;
        pending.nextAt = now + Math.min(300000, 15000 * 2 ** Math.min(pending.attempts - 1, 5));
        this.save(state); // Persist retry identity before any external request.
        let accepted = false, httpStatus = null, reason = 'ack_unconfirmed';
        try {
          const response = await this.fetch(url.href, { method: 'POST', redirect: 'manual',
            headers: { Authorization: `Bearer ${this.env.INSTAGRAM_WAKE_RELAY_TOKEN}`, 'Content-Type': 'application/json' },
            body: JSON.stringify(pending.event), signal: AbortSignal.timeout(5000) });
          httpStatus = response.status;
          if (httpStatus >= 300 && httpStatus < 400) reason = 'redirect';
          // Read a bounded acknowledgement; HTTP 202 alone is not an event receipt.
          const reader = response.body?.getReader();
          if (reader) {
            const chunks = []; let bytes = 0;
            try {
              while (true) { const { done, value } = await reader.read(); if (done) break;
                bytes += value.byteLength; if (bytes > 4096) throw new Error('ack_limit'); chunks.push(value); }
              const raw = new Uint8Array(bytes); let offset = 0;
              for (const c of chunks) { raw.set(c, offset); offset += c.length; }
              const ack = JSON.parse(new TextDecoder().decode(raw));
              accepted = response.ok && ack.ok === true && ack.eventId === pending.event.eventId && ack.persisted === true;
            } finally { await reader.cancel().catch(() => {}); }
          }
        } catch (error) {
          // No upstream text, URL or credential is exposed through diagnostics.
          const message = String(error?.message || '');
          reason = /abort|timeout/i.test(String(error?.name || '')) ? 'timeout'
            : /different request|I\/O context|another request/i.test(message) ? 'io_context'
              : /Illegal invocation/i.test(message) ? 'invocation'
                : /DNS|resolve|host|1003|1016/i.test(message) ? 'dns'
                  : /SSL|TLS|certificate/i.test(message) ? 'tls'
                    : /redirect/i.test(message) ? 'redirect'
                      : /network|connection|fetch failed/i.test(message) ? 'network' : 'fetch_error';
        }
        const latest = this.load();
        if (latest.pending[0]?.event.eventId !== pending.event.eventId) throw wakeError('instagram_wake_reservation_changed', 503);
        latest.last = { at: this.clock(), status: accepted ? 'relay_confirmed' : 'relay_unconfirmed', httpStatus, reason: accepted ? 'persisted' : reason };
        if (accepted) latest.pending.shift();
        this.save(latest);
        if (!accepted) return { ok: false, status: 'relay_unconfirmed' };
      }
      return { ok: true, status: 'drained' };
    } finally { this.inFlight = false; }
  }
  status() {
    const state = this.load();
    return { ok: true, pending: state.pending.length, last: state.last,
      oldestPendingAt: state.pending.length ? new Date(state.pending[0].createdAt).toISOString() : null };
  }
}
