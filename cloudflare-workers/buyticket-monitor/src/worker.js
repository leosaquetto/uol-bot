import { DurableObject } from 'cloudflare:workers';
import { EVENTS, PURCHASE_LISTING_LIMIT, eventUrl, parse, qualifyingOffers, format, purchaseCandidates, formatPixMessage } from './core.js';
import { runCheckout } from './checkout.js';
const EVENT_SCOPE = 'demi-16-no-elderly-under-299-pix-under-100-v5:' + EVENTS.map(e => `${e.date}:${e.local}`).join(':');
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
    if (scopeChanged || !storedFingerprints) {
      for (const offer of observed) seenOffers[alertFingerprint(offer)] = true;
    }
    let pending = await this.ctx.storage.get('pending');
    if (scopeChanged) {
      if (pending) await this.ctx.storage.put('retiredPending', pending);
      await this.ctx.storage.delete('pending');
      await this.ctx.storage.delete('pixPending');
      await this.ctx.storage.delete('purchases');
      await this.ctx.storage.put('purchasesEnabled', false);
      pending = null;
    } else if (pending?.state === 'unknown') {
      // The gateway treats an ambiguous idempotency key as terminal. Preserve it
      // for diagnosis, but do not let it block unrelated future offers.
      await this.ctx.storage.put({ retiredPending: pending, lastUnknownDeliveryAt: at });
      await this.ctx.storage.delete('pending');
      pending = null;
    }
    const enabled = await this.ctx.storage.get('enabled');
    if (enabled && changes.length) {
      const items = [...new Set(changes.map(d => d.i))].map(i => ({
        key: `buyticket:${crypto.randomUUID()}`,
        link: eventUrl(EVENTS[i]),
        text: format(current, changes, at, i),
      }));
      pending = pending
        ? { ...pending, items: [...(pending.items || [pending]), ...items] }
        : { key: `buyticket:${crypto.randomUUID()}`, items, state: 'queued' };
      await this.ctx.storage.put('pending', pending);
      for (const offer of changes) seenOffers[alertFingerprint(offer)] = true;
      fingerprintsChanged = true;
    }
    await this.ctx.storage.put({
      current, checkedAt: at, error: null, eventScope: EVENT_SCOPE,
      ...(fingerprintsChanged ? { seenOffers } : {}),
    });
    await this.maybePurchase(current);
    await this.deliverPix();
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
  async maybePurchase(current) {
    if (this.env.PIX_CHECKOUT_VALIDATED !== 'true') return;
    if (!await this.ctx.storage.get('purchasesEnabled')) return;
    const state = await this.ctx.storage.get('purchases') || { days: {} };
    const now = Date.now();
    for (let dayIndex = 0; dayIndex < EVENTS.length; dayIndex++) {
      const day = state.days[dayIndex] || { attempts: {} };
      if (day.status === 'checking' && Date.parse(day.updatedAt || '') + 7 * 60_000 > now) continue;
      if (['pix_created', 'delivered', 'unknown'].includes(day.status)) continue;
      for (const candidate of purchaseCandidates(current[dayIndex], dayIndex)) {
        const prior = day.attempts[candidate.idRef];
        if (prior?.listedPrice === candidate.listedPrice && prior.status === 'listing_mismatch') continue;
        if (prior?.listedPrice === candidate.listedPrice && prior.retryAfter && Date.parse(prior.retryAfter) > now) continue;
        const attempt = { status: 'checking', listedPrice: candidate.listedPrice, checkedAt: new Date().toISOString() };
        day.attempts[candidate.idRef] = attempt;
        day.status = 'checking';
        day.updatedAt = attempt.checkedAt;
        state.days[dayIndex] = day;
        await this.ctx.storage.put('purchases', state);
        let result;
        try {
          const checkout = globalThis.__runCheckout || runCheckout;
          result = await checkout(this.env, candidate, {
            sessionId: await this.ctx.storage.get('browserSessionId'),
            onSession: async id => this.ctx.storage.put('browserSessionId', id),
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
        } else if (['checkout_failed', 'login_failed', 'pix_unavailable'].includes(result.status)) {
          attempt.retryAfter = new Date(Date.now() + 5 * 60_000).toISOString();
        } else delete attempt.retryAfter;
        if (result.status === 'pix_created') {
          day.status = 'pix_created';
          day.idRef = candidate.idRef;
          day.finalPrice = result.finalPrice;
          day.updatedAt = attempt.checkedAt;
          await this.ctx.storage.put({
            purchases: state,
            pixPending: {
              state: 'queued',
              key: `buyticket:pix:${dayIndex}:${crypto.randomUUID()}`,
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
      body: JSON.stringify({ link: pending.link, text: pending.text, title: 'Demi Lovato • PIX' }),
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
      if (this.env.PIX_CHECKOUT_VALIDATED !== 'true') return Response.json({ error: 'checkout_not_validated' }, { status: 409 });
      await this.ctx.storage.put('purchasesEnabled', true);
      await this.scheduleNextAlarm();
      return Response.json({ purchasesEnabled: true, listingPriceLimit: PURCHASE_LISTING_LIMIT });
    }
    if (request.method === 'POST' && path === '/purchases/stop') {
      await this.ctx.storage.put('purchasesEnabled', false);
      await this.scheduleNextAlarm();
      return Response.json({ purchasesEnabled: false });
    }
    if (request.method === 'POST' && path === '/purchases/dry-run') {
      const body = await request.json().catch(() => ({}));
      const dayIndex = Number(body.dayIndex);
      if (!Number.isInteger(dayIndex) || dayIndex < 0 || dayIndex >= EVENTS.length || typeof body.idRef !== 'string' || !body.idRef) return Response.json({ error: 'invalid_request' }, { status: 400 });
      const current = await this.collect();
      const candidate = purchaseCandidates(current[dayIndex], dayIndex, Number.MAX_SAFE_INTEGER).find(item => item.idRef === body.idRef);
      if (!candidate) return Response.json({ error: 'listing_not_found' }, { status: 404 });
      const result = await runCheckout(this.env, candidate, {
        dryRun: true,
        sessionId: await this.ctx.storage.get('browserSessionId'),
        onSession: async id => this.ctx.storage.put('browserSessionId', id),
      });
      return Response.json({ status: result.status, stage: result.stage || null, couponSignal: result.couponSignal || null, listedTotal: result.listedTotal || null, finalPrice: result.finalPrice || null, formReady: result.formReady === true, review: result.review || null, diagnostic: result.diagnostic || null, finalActionClicked: false, noOrderCreated: true });
    }
    if (request.method === 'POST' && path === '/initialize') {
      if (!await this.ctx.storage.get('checkedAt') || await this.ctx.storage.get('eventScope') !== EVENT_SCOPE) await this.tick();
      await this.scheduleNextAlarm();
    } else if (request.method !== 'GET' || !['/status', '/preview'].includes(path)) return new Response('Not found', { status: 404 });
    const state = Object.fromEntries(await this.ctx.storage.list());
    if (path === '/preview') return new Response(state.current && state.eventScope === EVENT_SCOPE ? EVENTS.map((e, i) => format(state.current, [], state.checkedAt, i)).join('\n\n──────── MENSAGEM SEPARADA ────────\n\n') : 'Not initialized');
    return Response.json({ enabled: state.enabled === true, purchasesEnabled: state.purchasesEnabled === true, days: EVENTS.map(e => e.day), priceLimit: 29900, purchaseListingPriceLimit: PURCHASE_LISTING_LIMIT, baselineReady: state.eventScope === EVENT_SCOPE, checkedAt: state.checkedAt, error: state.error, pending: state.pending?.state || null, pixPending: state.pixPending?.state || null, purchaseDays: Object.fromEntries(Object.entries(state.purchases?.days || {}).map(([day, value]) => { const attempts = Object.values(value.attempts || {}); const lastAttempt = attempts.sort((a, b) => String(b.checkedAt).localeCompare(String(a.checkedAt)))[0]; return [day, { status: value.status || 'idle', lastAttemptStatus: lastAttempt?.status || null, listedPrice: lastAttempt?.listedPrice || null, finalPrice: value.finalPrice || null, updatedAt: value.updatedAt || null, deliveredAt: value.deliveredAt || null }]; })), lastDeliveredAt: state.lastDeliveredAt || null, lastPixDeliveredAt: state.lastPixDeliveredAt || null, lastUnknownDeliveryAt: state.lastUnknownDeliveryAt || null, lastSnapshotBroadcastAt: state.lastSnapshotBroadcastAt || null });
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
