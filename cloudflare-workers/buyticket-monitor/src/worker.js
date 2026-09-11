import { DurableObject } from 'cloudflare:workers';
import { EVENTS, eventUrl, parse, qualifyingOffers, format } from './core.js';
const EVENT_SCOPE = 'demi-under-299-v1:' + EVENTS.map(e => `${e.date}:${e.local}`).join(':');
const MONITOR_INTERVAL = 30_000;
const DELIVERY_RECONCILE_INTERVAL = 300_000;
const alertFingerprint = ({ i, idRef }) => `${i}|${idRef}`;
export class Monitor extends DurableObject {
  async scheduleNextAlarm() {
    await this.ctx.storage.setAlarm(Date.now() + MONITOR_INTERVAL);
  }
  async collect() {
    return Promise.all(EVENTS.map(async (event) => {
      const response = await fetch(eventUrl(event), { headers: { RSC: '1', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }, redirect: 'manual', signal: AbortSignal.timeout(20_000) });
      if (!response.ok) throw new Error('source_http_error');
      const body = await response.text();
      if (body.length > 500_000) throw new Error('source_too_large');
      return parse(body);
    }));
  }
  async tick() {
    const at = new Date().toISOString();
    const current = await this.collect();
    const scopeChanged = await this.ctx.storage.get('eventScope') !== EVENT_SCOPE;
    const storedFingerprints = scopeChanged ? null : await this.ctx.storage.get('seenOffers');
    const seenOffers = storedFingerprints || {};
    const observed = qualifyingOffers(current);
    const changes = scopeChanged || !storedFingerprints ? [] : observed.filter(offer => !seenOffers[alertFingerprint(offer)]);
    let fingerprintsChanged = scopeChanged || !storedFingerprints;
    for (const offer of observed) {
      const fingerprint = alertFingerprint(offer);
      if (!seenOffers[fingerprint]) {
        seenOffers[fingerprint] = true;
        fingerprintsChanged = true;
      }
    }
    if (scopeChanged) {
      const oldPending = await this.ctx.storage.get('pending');
      if (oldPending) await this.ctx.storage.put('retiredPending', oldPending);
      await this.ctx.storage.delete('pending');
    }
    await this.ctx.storage.put({
      current, checkedAt: at, error: null, eventScope: EVENT_SCOPE,
      purchasesEnabled: false,
      ...(fingerprintsChanged ? { seenOffers } : {}),
    });
    const pending = await this.ctx.storage.get('pending');
    if (await this.ctx.storage.get('enabled') && !pending && changes.length) {
      await this.ctx.storage.put('pending', { key: `buyticket:${crypto.randomUUID()}`, items: [...new Set(changes.map(d => d.i))].map(i => ({ key: `buyticket:${crypto.randomUUID()}`, link: eventUrl(EVENTS[i]), text: format(current, changes, at, i) })), state: 'queued' });
    }
    await this.deliver();
  }
  async deliver() {
    const pending = await this.ctx.storage.get('pending');
    if (!pending || !['queued', 'unknown'].includes(pending.state) || !await this.ctx.storage.get('enabled')) return;
    if (pending.state === 'unknown' && pending.lastAttemptAt &&
        Date.parse(pending.lastAttemptAt) + DELIVERY_RECONCILE_INTERVAL > Date.now()) return;
    const item = pending.items?.[0] || pending;
    // Reusing the same key lets the gateway return its durable receipt without sending twice.
    await this.ctx.storage.put('pending', { ...pending, state: 'unknown', lastAttemptAt: new Date().toISOString() });
    const response = await fetch(this.env.BEEPER_GATEWAY_URL, { method: 'POST', headers: { Authorization: `Bearer ${this.env.BEEPER_GATEWAY_TOKEN}`, 'Content-Type': 'application/json', 'Idempotency-Key': item.key }, body: JSON.stringify({ link: item.link || eventUrl(EVENTS[0]), text: item.text, title: 'Demi Lovato • BuyTicket' }), signal: AbortSignal.timeout(55_000) });
    const result = await response.json();
    if (response.ok && result.deliveryState === 'confirmed_by_whatsapp_bridge') {
      await this.ctx.storage.put('lastDeliveredAt', new Date().toISOString());
      const remaining = pending.items?.slice(1) || [];
      if (remaining.length) {
        await this.ctx.storage.put('pending', { ...pending, items: remaining, state: 'queued' });
        await this.deliver();
      } else await this.ctx.storage.delete('pending');
    }
  }
  async alarm() {
    if (Date.now() >= Date.parse('2026-09-18T03:00:00Z')) return;
    await this.scheduleNextAlarm();
    try { await this.tick(); } catch { await this.ctx.storage.put('error', 'check_or_delivery_failed'); }
  }
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (request.method === 'POST' && path === '/retire') {
      await this.ctx.storage.put({ enabled: false, purchasesEnabled: false });
      await this.ctx.storage.deleteAlarm();
      return Response.json({ retired: true });
    }
    if (request.method === 'POST' && path === '/snapshot/send') {
      const state = Object.fromEntries(await this.ctx.storage.list());
      const checkedAt = Date.parse(state.checkedAt || '');
      if (!state.enabled || state.eventScope !== EVENT_SCOPE || !state.current ||
          !Number.isFinite(checkedAt) || Date.now() - checkedAt > 120_000) {
        return Response.json({ error: 'snapshot_not_ready' }, { status: 409 });
      }
      if (state.pending) return Response.json({ error: 'delivery_pending' }, { status: 409 });
      if (state.lastSnapshotBroadcastAt === state.checkedAt) {
        return Response.json({ error: 'snapshot_already_sent' }, { status: 409 });
      }
      await this.ctx.storage.put({
        lastSnapshotBroadcastAt: state.checkedAt,
        pending: {
          key: `buyticket:snapshot:${state.checkedAt}`,
          items: EVENTS.map((event, i) => ({
            key: `buyticket:snapshot:${state.checkedAt}:${i}`,
            link: eventUrl(event),
            text: format(state.current, [], state.checkedAt, i),
          })),
          state: 'queued',
        },
      });
      await this.deliver();
      const pending = await this.ctx.storage.get('pending');
      return Response.json({
        broadcast: true,
        deliveryState: pending?.state || 'confirmed_by_whatsapp_bridge',
        snapshotAt: state.checkedAt,
      });
    }
    if (request.method === 'POST' && path === '/start') {
      if (await this.ctx.storage.get('enabled')) return Response.json({ error: 'already_enabled' }, { status: 409 });
      const current = await this.collect();
      const at = new Date().toISOString();
      await this.ctx.storage.put({ current, checkedAt: at, eventScope: EVENT_SCOPE, enabled: true, purchasesEnabled: false, seenOffers: Object.fromEntries(qualifyingOffers(current).map(offer => [alertFingerprint(offer), true])) });
      await this.scheduleNextAlarm();
      return Response.json({ started: true, pending: null });
    }
    if (request.method === 'POST' && path === '/purchases/start') {
      return Response.json({ error: 'purchases_retired' }, { status: 410 });
    }
    if (request.method === 'POST' && path === '/purchases/stop') {
      await this.ctx.storage.put('purchasesEnabled', false);
      await this.scheduleNextAlarm();
      return Response.json({ purchasesEnabled: false });
    }
    if (request.method === 'POST' && path === '/initialize') {
      if (!await this.ctx.storage.get('checkedAt') || await this.ctx.storage.get('eventScope') !== EVENT_SCOPE) await this.tick();
      await this.scheduleNextAlarm();
    } else if (request.method !== 'GET' || !['/status', '/preview'].includes(path)) return new Response('Not found', { status: 404 });
    const state = Object.fromEntries(await this.ctx.storage.list());
    if (path === '/preview') return new Response(state.current && state.eventScope === EVENT_SCOPE ? EVENTS.map((e, i) => format(state.current, [], state.checkedAt, i)).join('\n\n──────── MENSAGEM SEPARADA ────────\n\n') : 'Not initialized');
    return Response.json({ enabled: state.enabled === true, purchasesEnabled: false, days: EVENTS.map(e => e.day), priceLimit: 29900, baselineReady: state.eventScope === EVENT_SCOPE, checkedAt: state.checkedAt, error: state.error, pending: state.pending?.state || null, lastDeliveredAt: state.lastDeliveredAt || null, lastSnapshotBroadcastAt: state.lastSnapshotBroadcastAt || null });
  }
}
export default {
  async fetch(request, env) {
    if (!env.ADMIN_TOKEN || request.headers.get('Authorization') !== `Bearer ${env.ADMIN_TOKEN}`) return new Response('Unauthorized', { status: 401 });
    if (request.method === 'POST' && new URL(request.url).pathname === '/retire-rock-in-rio') {
      return env.MONITOR.getByName('rockinrio2026').fetch(new Request('https://monitor/retire', { method: 'POST' }));
    }
    return env.MONITOR.getByName('demi-lovato-2026').fetch(request);
  },
};
