import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const core = new URL('../src/core.js', import.meta.url).href;
const source = (await readFile(new URL('../src/worker.js', import.meta.url), 'utf8'))
  .replace("import { DurableObject } from 'cloudflare:workers';", 'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }')
  .replace("'./core.js'", JSON.stringify(core))
  .replace("import { runCheckout } from './checkout.js';", "const runCheckout = (...args) => globalThis.__runCheckout(...args);");
const { Monitor } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
const { keys } = await import(core);
globalThis.__runCheckout = async () => ({ status: 'checkout_failed' });
test('silent initialization and ticks never dispatch; start sends once; unknown delivery waits before reconciliation', async () => {
  const data = new Map();
  const storage = { get: async k => data.get(k), put: async (k,v) => { if (typeof k === 'object') Object.entries(k).forEach(([a,b]) => data.set(a,b)); else data.set(k,v); }, delete: async k => data.delete(k), list: async () => data, setAlarm: async () => {} };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  m.collect = async () => [0,1].map(() => Object.fromEntries(keys.map(k => [k, { preco_min: 33000, disponivel: 3, id_ref: 'ref' }])));
  let sends = 0;
  const oldFetch = globalThis.fetch;
  globalThis.fetch = async () => { sends++; return Response.json({ code: 'delivery_unknown' }, { status: 503 }); };
  try {
    await m.fetch(new Request('https://monitor/initialize', { method: 'POST' }));
    await m.tick();
    assert.equal(sends, 0);
    assert.equal(data.get('enabled'), undefined);
    await m.fetch(new Request('https://monitor/start', { method: 'POST' }));
    assert.equal(sends, 1);
    assert.equal(data.get('pending').state, 'unknown');
    await m.tick();
    assert.equal(sends, 1);
    assert.equal((await m.fetch(new Request('https://monitor/start', { method: 'POST' }))).status, 409);
  } finally { globalThis.fetch = oldFetch; }
});
test('unknown delivery reconciles through the gateway receipt without creating a new alert', async () => {
  const data = new Map([['enabled', true], ['pending', {
    state: 'unknown', key: 'buyticket:existing', link: 'https://example.test/13', text: 'day13',
  }]]);
  const storage = { get: async k => data.get(k), put: async (k,v) => data.set(k,v), delete: async k => data.delete(k) };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  const oldFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async (_, init) => {
    calls++;
    assert.equal(init.headers['Idempotency-Key'], 'buyticket:existing');
    return Response.json({ deliveryState: 'confirmed_by_whatsapp_bridge', replayed: true });
  };
  try {
    await m.deliver();
    assert.equal(calls, 1);
    assert.equal(data.get('pending'), undefined);
    assert.ok(data.get('lastDeliveredAt'));
  } finally { globalThis.fetch = oldFetch; }
});
test('a previously observed daily minimum is not alerted again after a price rebound', async () => {
  const scope = 'daily-min-v1:1765323797528x513509114247905300:1765323829346x381107157350744060';
  const initial = [matrixFor(90000), matrixFor(39600)];
  const data = new Map([['enabled', true], ['current', initial], ['eventScope', scope]]);
  const storage = { get: async k => data.get(k), put: async (k,v) => { if (typeof k === 'object') Object.entries(k).forEach(([a,b]) => data.set(a,b)); else data.set(k,v); }, delete: async k => data.delete(k) };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  const sequence = [initial, [matrixFor(90000), matrixFor(38500)], initial, [matrixFor(90000), matrixFor(38500)]];
  m.collect = async () => sequence.shift();
  const oldFetch = globalThis.fetch;
  let sends = 0;
  globalThis.fetch = async () => {
    sends++;
    return Response.json({ deliveryState: 'confirmed_by_whatsapp_bridge' });
  };
  try {
    await m.tick(); // Seeds the existing minimum during migration without sending.
    await m.tick(); // First observation of R$385 sends.
    await m.tick(); // Rebound to R$396 does not send.
    await m.tick(); // R$385 was already observed, so it does not repeat.
    assert.equal(sends, 1);
    assert.equal(data.get('pending'), undefined);
  } finally { globalThis.fetch = oldFetch; }
});
test('date change silently replaces baseline and retires old pending delivery', async () => {
  const data = new Map([['enabled', true], ['current', [{}, {}]], ['pending', { state: 'queued', text: 'old date' }]]);
  const storage = { get: async k => data.get(k), put: async (k,v) => { if (typeof k === 'object') Object.entries(k).forEach(([a,b]) => data.set(a,b)); else data.set(k,v); }, delete: async k => data.delete(k), list: async () => data, setAlarm: async () => {} };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  m.collect = async () => [0,1].map(() => Object.fromEntries(keys.map(k => [k, { preco_min: 100, disponivel: 1, id_ref: 'new' }])));
  await m.fetch(new Request('https://monitor/initialize', { method: 'POST' }));
  assert.equal(data.get('enabled'), true);
  assert.equal(data.get('pending'), undefined);
  assert.equal(data.get('retiredPending').text, 'old date');
  const status = await (await m.fetch(new Request('https://monitor/status'))).json();
  assert.deepEqual(status.days, ['12/09/2026', '13/09/2026']);
  assert.equal(status.baselineReady, true);
});
test('two day messages dispatch sequentially with distinct keys and matching links', async () => {
  const data = new Map([['enabled', true], ['pending', { state: 'queued', items: [
    { key: 'buyticket:day12', link: 'https://example.test/12', text: 'day12' },
    { key: 'buyticket:day13', link: 'https://example.test/13', text: 'day13' },
  ] }]]);
  const storage = { get: async k => data.get(k), put: async (k,v) => data.set(k,v), delete: async k => data.delete(k) };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  const oldFetch = globalThis.fetch;
  const sent = [];
  globalThis.fetch = async (_, init) => {
    assert.equal(data.get('pending').state, 'unknown');
    sent.push({ key: init.headers['Idempotency-Key'], ...JSON.parse(init.body) });
    return Response.json({ deliveryState: 'confirmed_by_whatsapp_bridge' });
  };
  try {
    await m.deliver();
    assert.deepEqual(sent.map(x => [x.key, x.link, x.text]), [
      ['buyticket:day12', 'https://example.test/12', 'day12'],
      ['buyticket:day13', 'https://example.test/13', 'day13'],
    ]);
    assert.equal(data.get('pending'), undefined);
  } finally { globalThis.fetch = oldFetch; }
});
test('purchase lane ignores non-candidates and creates one queued PIX for any qualifying category', async () => {
  const data = new Map([['purchasesEnabled', true]]);
  const storage = { get: async k => data.get(k), put: async (k,v) => { if (typeof k === 'object') Object.entries(k).forEach(([a,b]) => data.set(a,b)); else data.set(k,v); }, delete: async k => data.delete(k) };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  let runs = 0;
  globalThis.__runCheckout = async (_env, candidate, options) => {
    runs++;
    assert.equal(candidate.category, 'Meia Professor');
    await options.beforeCommit({ finalPrice: 24000 });
    assert.equal(data.get('purchases').days[1].status, 'unknown');
    return { status: 'pix_created', finalPrice: 24000, pixCode: '000201' + 'A'.repeat(70) };
  };
  const current = [matrixFor(90000), matrixFor(90000)];
  await m.maybePurchase(current);
  assert.equal(runs, 0);
  current[1]['VIP||Meia Professor'] = { preco_min: 25000, disponivel: 1, id_ref: 'candidate' };
  await m.maybePurchase(current);
  assert.equal(runs, 1);
  assert.equal(data.get('purchases').days[1].status, 'pix_created');
  assert.equal(data.get('pixPending').state, 'queued');
  assert.match(data.get('pixPending').text, /🎟️ 1 ingresso/);
  await m.maybePurchase(current);
  assert.equal(runs, 1);
});

test('ambiguous checkout remains terminal and is never repeated', async () => {
  const data = new Map([['purchasesEnabled', true]]);
  const storage = { get: async k => data.get(k), put: async (k,v) => { if (typeof k === 'object') Object.entries(k).forEach(([a,b]) => data.set(a,b)); else data.set(k,v); }, delete: async k => data.delete(k) };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  let runs = 0;
  globalThis.__runCheckout = async (_env, _candidate, options) => {
    runs++;
    await options.beforeCommit({ finalPrice: 24000 });
    throw new Error('purchase_outcome_unknown');
  };
  const current = [matrixFor(90000), matrixFor(90000)];
  current[1]['VIP||Meia Professor'] = { preco_min: 25000, disponivel: 1, id_ref: 'candidate' };
  await m.maybePurchase(current);
  await m.maybePurchase(current);
  assert.equal(runs, 1);
  assert.equal(data.get('purchases').days[1].status, 'unknown');
});

test('browser rate limits are persisted with a cooldown instead of relaunching every tick', async () => {
  const data = new Map([['purchasesEnabled', true]]);
  const storage = { get: async k => data.get(k), put: async (k,v) => { if (typeof k === 'object') Object.entries(k).forEach(([a,b]) => data.set(a,b)); else data.set(k,v); }, delete: async k => data.delete(k) };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  let runs = 0;
  globalThis.__runCheckout = async () => { runs++; return { status: 'browser_rate_limited' }; };
  const current = [matrixFor(90000), matrixFor(90000)];
  current[1]['Gramado||Meia Estudante'] = { preco_min: 27000, disponivel: 1, id_ref: 'rate-limited' };
  await m.maybePurchase(current);
  await m.maybePurchase(current);
  assert.equal(runs, 1);
  assert.match(data.get('purchases').days[1].attempts['rate-limited'].retryAfter, /^20/);
});

function matrixFor(price) {
  return Object.fromEntries(keys.map(key => [key, { preco_min: price, disponivel: 1, id_ref: `${key}:${price}` }]));
}

test('unvalidated checkout cannot be armed or run even with persisted enablement', async () => {
  const data = new Map([['purchasesEnabled', true]]);
  const storage = { get: async k => data.get(k), put: async (k,v) => data.set(k,v), setAlarm: async () => {} };
  const m = new Monitor({ storage }, {});
  globalThis.__runCheckout = async () => { throw new Error('must not launch'); };
  await m.maybePurchase([matrixFor(25000), matrixFor(20000)]);
  const response = await m.fetch(new Request('https://monitor/purchases/start', { method: 'POST' }));
  assert.equal(response.status, 409);
  await m.fetch(new Request('https://monitor/purchases/stop', { method: 'POST' }));
  assert.equal(data.get('purchasesEnabled'), false);
});

test('arming purchases changes the next alarm to the 15-second purchase cadence', async () => {
  const data = new Map([['enabled', true], ['checkedAt', new Date().toISOString()], ['eventScope', 'daily-min-v1:1765323797528x513509114247905300:1765323829346x381107157350744060']]);
  const alarms = [];
  const storage = { get: async k => data.get(k), put: async (k,v) => { if (typeof k === 'object') Object.entries(k).forEach(([a,b]) => data.set(a,b)); else data.set(k,v); }, delete: async k => data.delete(k), list: async () => data, setAlarm: async value => alarms.push(value) };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  await m.fetch(new Request('https://monitor/purchases/start', { method: 'POST' }));
  assert.equal(data.get('purchasesEnabled'), true);
  assert.ok(alarms.at(-1) - Date.now() <= 15_000 && alarms.at(-1) - Date.now() > 0);
});

test('alarm reschedules and runs with purchases enabled', async () => {
  const data = new Map([['purchasesEnabled', true]]);
  const alarms = [];
  const storage = { get: async k => data.get(k), put: async (k,v) => data.set(k,v), delete: async k => data.delete(k), setAlarm: async value => alarms.push(value) };
  const m = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  m.tick = async () => { data.set('ticked', true); };
  await m.alarm();
  assert.equal(data.get('ticked'), true);
  assert.ok(alarms.at(-1) - Date.now() <= 15_000 && alarms.at(-1) - Date.now() > 0);
});
