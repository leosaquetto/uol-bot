import { DurableObject } from 'cloudflare:workers';
import { EVENTS, eventUrl, parse, drops, format, purchaseCandidates, formatPixMessage } from './core.js';
import { runCheckout } from './checkout.js';
const EVENT_SCOPE = 'daily-min-v1:' + EVENTS.map(e => e.local).join(':');
const PASSIVE_INTERVAL = 300_000;
const PURCHASE_INTERVAL = 15_000;
const DELIVERY_RECONCILE_INTERVAL = 300_000;
export class Monitor extends DurableObject {
  async scheduleNextAlarm() {
    const interval = await this.ctx.storage.get('purchasesEnabled') ? PURCHASE_INTERVAL : PASSIVE_INTERVAL;
    await this.ctx.storage.setAlarm(Date.now() + interval);
  }
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
    await this.maybePurchase(current);
    await this.deliverPix();
  }
  async deliver() {
    const pending = await this.ctx.storage.get('pending');
    if (!pending || !['queued', 'unknown'].includes(pending.state) || !await this.ctx.storage.get('enabled')) return;
    if (pending.state === 'unknown' && pending.lastAttemptAt &&
        Date.parse(pending.lastAttemptAt) + DELIVERY_RECONCILE_INTERVAL > Date.now()) return;
    const item = pending.items?.[0] || pending;
    // Reusing the same key lets the gateway return its durable receipt without sending twice.
    await this.ctx.storage.put('pending', { ...pending, state: 'unknown', lastAttemptAt: new Date().toISOString() });
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
  async maybePurchase(current) {
    // Checkout remains gated until the complete billing and PIX contract is verified.
    if (this.env.PIX_CHECKOUT_VALIDATED !== 'true') return;
    if (!await this.ctx.storage.get('purchasesEnabled')) return;
    const state = await this.ctx.storage.get('purchases') || { days: {} };
    const now = Date.now();
    for (let dayIndex = 0; dayIndex < EVENTS.length; dayIndex++) {
      const day = state.days[dayIndex] || { attempts: {} };
      if (['pix_created', 'delivered', 'unknown'].includes(day.status)) continue;
      for (const candidate of purchaseCandidates(current[dayIndex], dayIndex)) {
        const prior = day.attempts[candidate.idRef];
        if (prior && prior.listedPrice === candidate.listedPrice &&
            ['outside_range', 'listing_mismatch'].includes(prior.status)) continue;
        if (prior && prior.listedPrice === candidate.listedPrice && prior.retryAfter &&
            Date.parse(prior.retryAfter) > now) continue;
        const attempt = { status: 'checking', listedPrice: candidate.listedPrice, checkedAt: new Date().toISOString() };
        day.attempts[candidate.idRef] = attempt;
        state.days[dayIndex] = day;
        await this.ctx.storage.put('purchases', state);
        let result;
        try {
          result = await runCheckout(this.env, candidate, {
            beforeCommit: async ({ finalPrice }) => {
              day.status = 'unknown';
              day.idRef = candidate.idRef;
              day.finalPrice = finalPrice;
              day.updatedAt = new Date().toISOString();
              await this.ctx.storage.put('purchases', state);
            },
          });
        } catch {
          day.status = 'unknown';
          day.updatedAt = new Date().toISOString();
          await this.ctx.storage.put('purchases', state);
          return;
        }
        attempt.status = result.status;
        attempt.finalPrice = result.finalPrice;
        attempt.checkedAt = new Date().toISOString();
        if (['browser_rate_limited', 'browser_unavailable'].includes(result.status)) {
          attempt.retryAfter = new Date(Date.now() + 15 * 60_000).toISOString();
        } else if (result.status === 'checkout_failed') {
          attempt.retryAfter = new Date(Date.now() + 5 * 60_000).toISOString();
        } else {
          delete attempt.retryAfter;
        }
        if (result.status === 'pix_created') {
          const deliveryKey = `buyticket:pix:${dayIndex}:${crypto.randomUUID()}`;
          day.status = 'pix_created';
          day.idRef = candidate.idRef;
          day.finalPrice = result.finalPrice;
          day.updatedAt = attempt.checkedAt;
          await this.ctx.storage.put({
            purchases: state,
            pixPending: {
              state: 'queued',
              key: deliveryKey,
              link: eventUrl(EVENTS[dayIndex]),
              text: formatPixMessage({ ...candidate, ...result }, attempt.checkedAt),
              dayIndex,
            },
          });
          return;
        }
        day.status = 'idle';
        await this.ctx.storage.put('purchases', state);
      }
    }
  }
  async deliverPix() {
    const pending = await this.ctx.storage.get('pixPending');
    if (!pending || pending.state !== 'queued') return;
    await this.ctx.storage.put('pixPending', { ...pending, state: 'unknown' });
    const response = await fetch(this.env.BEEPER_GATEWAY_URL, {
      method: 'POST',
      headers: { Authorization: `Bearer ${this.env.BEEPER_GATEWAY_TOKEN}`, 'Content-Type': 'application/json', 'Idempotency-Key': pending.key },
      body: JSON.stringify({ link: pending.link, text: pending.text, title: 'Rock in Rio 2026 • PIX' }),
      signal: AbortSignal.timeout(55_000),
    });
    const result = await response.json();
    if (response.ok && result.deliveryState === 'confirmed_by_whatsapp_bridge') {
      const state = await this.ctx.storage.get('purchases') || { days: {} };
      const day = state.days[pending.dayIndex] || {};
      day.status = 'delivered';
      day.deliveredAt = new Date().toISOString();
      state.days[pending.dayIndex] = day;
      await this.ctx.storage.put({ purchases: state, lastPixDeliveredAt: day.deliveredAt });
      await this.ctx.storage.delete('pixPending');
    }
  }
  async alarm() {
    if (Date.now() >= Date.parse('2026-09-14T03:00:00Z')) return;
    await this.scheduleNextAlarm();
    try { await this.tick(); } catch { await this.ctx.storage.put('error', 'check_or_delivery_failed'); }
  }
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (request.method === 'POST' && path === '/start') {
      if (await this.ctx.storage.get('enabled')) return Response.json({ error: 'already_enabled' }, { status: 409 });
      const current = await this.collect();
      const at = new Date().toISOString();
      await this.ctx.storage.put({ current, checkedAt: at, eventScope: EVENT_SCOPE, enabled: true, pending: { key: `buyticket:${crypto.randomUUID()}`, items: EVENTS.map((e, i) => ({ key: `buyticket:${crypto.randomUUID()}`, link: eventUrl(e), text: format(current, [], at, i) })), state: 'queued' } });
      await this.scheduleNextAlarm();
      await this.deliver();
      return Response.json({ started: true, pending: (await this.ctx.storage.get('pending'))?.state || null });
    }
    if (request.method === 'POST' && path === '/purchases/start') {
      if (this.env.PIX_CHECKOUT_VALIDATED !== 'true') return Response.json({ error: 'checkout_not_validated' }, { status: 409 });
      await this.ctx.storage.put('purchasesEnabled', true);
      await this.scheduleNextAlarm();
      return Response.json({ purchasesEnabled: true });
    }
    if (request.method === 'POST' && path === '/purchases/stop') {
      await this.ctx.storage.put('purchasesEnabled', false);
      await this.scheduleNextAlarm();
      return Response.json({ purchasesEnabled: false });
    }
    if (request.method === 'POST' && path === '/purchases/dry-run') {
      const body = await request.json().catch(() => ({}));
      const dayIndex = Number(body.dayIndex);
      if (![0, 1].includes(dayIndex) || typeof body.idRef !== 'string' || !body.idRef) return Response.json({ error: 'invalid_request' }, { status: 400 });
      const current = await this.collect();
      const candidate = purchaseCandidates(current[dayIndex], dayIndex, 100_000).find(item => item.idRef === body.idRef);
      if (!candidate) return Response.json({ error: 'listing_not_found' }, { status: 404 });
      const result = await runCheckout(this.env, candidate, { dryRun: true });
      return Response.json({ status: result.status, stage: result.stage || null, couponSignal: result.couponSignal || null, listedTotal: result.listedTotal || null, finalPrice: result.finalPrice || null, formReady: result.formReady === true, review: result.review || null, finalActionClicked: false, noOrderCreated: true });
    }
    if (request.method === 'POST' && path === '/initialize') {
      if (!await this.ctx.storage.get('checkedAt') || await this.ctx.storage.get('eventScope') !== EVENT_SCOPE) await this.tick();
      await this.scheduleNextAlarm();
    } else if (request.method !== 'GET' || !['/status', '/preview'].includes(path)) return new Response('Not found', { status: 404 });
    const state = Object.fromEntries(await this.ctx.storage.list());
    if (path === '/preview') return new Response(state.current && state.eventScope === EVENT_SCOPE ? EVENTS.map((e, i) => format(state.current, [], state.checkedAt, i)).join('\n\n──────── MENSAGEM SEPARADA ────────\n\n') : 'Not initialized');
    return Response.json({ enabled: state.enabled === true, purchasesEnabled: state.purchasesEnabled === true, days: EVENTS.map(e => e.day), baselineReady: state.eventScope === EVENT_SCOPE, checkedAt: state.checkedAt, error: state.error, pending: state.pending?.state || null, pixPending: state.pixPending?.state || null, purchaseDays: Object.fromEntries(Object.entries(state.purchases?.days || {}).map(([day, value]) => [day, { status: value.status || 'idle', finalPrice: value.finalPrice || null, updatedAt: value.updatedAt || null, deliveredAt: value.deliveredAt || null }])), lastDeliveredAt: state.lastDeliveredAt || null, lastPixDeliveredAt: state.lastPixDeliveredAt || null });
  }
}
export default {
  async fetch(request, env) {
    if (!env.ADMIN_TOKEN || request.headers.get('Authorization') !== `Bearer ${env.ADMIN_TOKEN}`) return new Response('Unauthorized', { status: 401 });
    return env.MONITOR.getByName('rockinrio2026').fetch(request);
  },
};
