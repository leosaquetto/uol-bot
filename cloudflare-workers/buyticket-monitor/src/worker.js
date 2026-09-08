import { DurableObject } from 'cloudflare:workers';
import { EVENTS, eventUrl, parse, drops, format } from './core.js';
const EVENT_SCOPE = 'daily-min-v1:' + EVENTS.map(e => e.local).join(':');
const INTERVAL = 300_000;
export class Monitor extends DurableObject {
  async collect() {
    const matrices = [];
    for (const event of EVENTS) {
      const response = await fetch(eventUrl(event), { headers: { RSC: '1', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }, redirect: 'manual', signal: AbortSignal.timeout(20_000) });
      if (!response.ok) throw new Error('source_http_error');
      const body = await response.text();
      if (body.length > 500_000) throw new Error('source_too_large');
      matrices.push(parse(body));
    }
    return matrices;
  }
  async tick() {
    const at = new Date().toISOString();
    const current = await this.collect();
    const previous = await this.ctx.storage.get('current');
    const scopeChanged = await this.ctx.storage.get('eventScope') !== EVENT_SCOPE;
    const changes = scopeChanged ? [] : drops(previous, current);
    if (scopeChanged) {
      const oldPending = await this.ctx.storage.get('pending');
      if (oldPending) await this.ctx.storage.put('retiredPending', oldPending);
      await this.ctx.storage.delete('pending');
    }
    await this.ctx.storage.put({ current, checkedAt: at, error: null, eventScope: EVENT_SCOPE });
    const pending = await this.ctx.storage.get('pending');
    if (await this.ctx.storage.get('enabled') && !pending && changes.length) {
      await this.ctx.storage.put('pending', { key: `buyticket:${crypto.randomUUID()}`, items: [...new Set(changes.map(d => d.i))].map(i => ({ key: `buyticket:${crypto.randomUUID()}`, link: eventUrl(EVENTS[i]), text: format(current, changes, at, i) })), state: 'queued' });
    }
    await this.deliver();
  }
  async deliver() {
    const pending = await this.ctx.storage.get('pending');
    if (!pending || pending.state !== 'queued' || !await this.ctx.storage.get('enabled')) return;
    const item = pending.items?.[0] || pending;
    // Persist uncertainty before dispatch. Never retry an ambiguous send automatically.
    await this.ctx.storage.put('pending', { ...pending, state: 'unknown' });
    const response = await fetch(this.env.BEEPER_GATEWAY_URL, { method: 'POST', headers: { Authorization: `Bearer ${this.env.BEEPER_GATEWAY_TOKEN}`, 'Content-Type': 'application/json', 'Idempotency-Key': item.key }, body: JSON.stringify({ link: item.link || eventUrl(EVENTS[0]), text: item.text, title: 'Rock in Rio 2026' }), signal: AbortSignal.timeout(55_000) });
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
    if (Date.now() >= Date.parse('2026-09-14T03:00:00Z')) return;
    await this.ctx.storage.setAlarm(Date.now() + INTERVAL);
    try { await this.tick(); } catch { await this.ctx.storage.put('error', 'check_or_delivery_failed'); }
  }
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (request.method === 'POST' && path === '/start') {
      if (await this.ctx.storage.get('enabled')) return Response.json({ error: 'already_enabled' }, { status: 409 });
      const current = await this.collect();
      const at = new Date().toISOString();
      await this.ctx.storage.put({ current, checkedAt: at, eventScope: EVENT_SCOPE, enabled: true, pending: { key: `buyticket:${crypto.randomUUID()}`, items: EVENTS.map((e, i) => ({ key: `buyticket:${crypto.randomUUID()}`, link: eventUrl(e), text: format(current, [], at, i) })), state: 'queued' } });
      await this.ctx.storage.setAlarm(Date.now() + INTERVAL);
      await this.deliver();
      return Response.json({ started: true, pending: (await this.ctx.storage.get('pending'))?.state || null });
    }
    if (request.method === 'POST' && path === '/initialize') {
      if (!await this.ctx.storage.get('checkedAt') || await this.ctx.storage.get('eventScope') !== EVENT_SCOPE) await this.tick();
      await this.ctx.storage.setAlarm(Date.now() + INTERVAL);
    } else if (request.method !== 'GET' || !['/status', '/preview'].includes(path)) return new Response('Not found', { status: 404 });
    const state = Object.fromEntries(await this.ctx.storage.list());
    if (path === '/preview') return new Response(state.current && state.eventScope === EVENT_SCOPE ? EVENTS.map((e, i) => format(state.current, [], state.checkedAt, i)).join('\n\n──────── MENSAGEM SEPARADA ────────\n\n') : 'Not initialized');
    return Response.json({ enabled: state.enabled === true, days: EVENTS.map(e => e.day), baselineReady: state.eventScope === EVENT_SCOPE, checkedAt: state.checkedAt, error: state.error, pending: state.pending?.state || null, lastDeliveredAt: state.lastDeliveredAt || null });
  }
}
export default {
  async fetch(request, env) {
    if (!env.ADMIN_TOKEN || request.headers.get('Authorization') !== `Bearer ${env.ADMIN_TOKEN}`) return new Response('Unauthorized', { status: 401 });
    return env.MONITOR.getByName('rockinrio2026').fetch(request);
  },
};
