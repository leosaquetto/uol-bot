import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const core = new URL('../src/core.js', import.meta.url).href;
const checkout = 'data:text/javascript,export async function runCheckout(){throw new Error("checkout_not_stubbed")}' ;
const source = (await readFile(new URL('../src/worker.js', import.meta.url), 'utf8'))
  .replace("import { DurableObject } from 'cloudflare:workers';", 'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }')
  .replace("'./core.js'", JSON.stringify(core))
  .replace("'./checkout.js'", JSON.stringify(checkout));
const { Monitor } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
const { keys } = await import(core);
const scope = 'demi-16-no-elderly-under-299-pix-under-100-v5:1789613999000:1779910250255x792501787503624200';
const matrix = (price, suffix = '') => Object.fromEntries(keys.map(key => [key, { preco_min: price, disponivel: 1, id_ref: `${key}:${price}:${suffix}` }]));
const storageFor = (entries = []) => {
  const data = new Map(entries);
  return { data, storage: {
    get: async key => data.get(key),
    put: async (key, value) => { if (typeof key === 'object') Object.entries(key).forEach(([k, v]) => data.set(k, v)); else data.set(key, value); },
    delete: async key => data.delete(key),
    list: async () => data,
    setAlarm: async value => data.set('alarm', value),
    deleteAlarm: async () => data.delete('alarm'),
  } };
};

test('collects only the September 16 event', async () => {
  const { storage } = storageFor();
  const monitor = new Monitor({ storage }, {});
  const oldFetch = globalThis.fetch;
  let active = 0, peak = 0;
  globalThis.fetch = async () => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => setTimeout(resolve, 10)); active--;
    return new Response(`0:${JSON.stringify({ matriz_preco: matrix(50000) })}`);
  };
  try { assert.equal((await monitor.collect()).length, 1); assert.equal(peak, 1); }
  finally { globalThis.fetch = oldFetch; }
});

test('new scope seeds qualifying listings silently and disables purchases', async () => {
  const { data, storage } = storageFor([['enabled', true], ['eventScope', 'rock-in-rio-old']]);
  const monitor = new Monitor({ storage }, {});
  const current = [matrix(50000)];
  current[0]['Pista||Promocional'] = { preco_min: 25000, disponivel: 1, id_ref: 'existing' };
  monitor.collect = async () => current;
  const oldFetch = globalThis.fetch;
  let sends = 0;
  globalThis.fetch = async () => { sends++; return Response.json({}); };
  try { await monitor.tick(); }
  finally { globalThis.fetch = oldFetch; }
  assert.equal(sends, 0);
  assert.equal(data.get('eventScope'), scope);
  assert.equal(data.get('seenOffers')['0|existing'], true);
  assert.equal(data.get('purchasesEnabled'), false);
});

test('sends a newly observed sub-R$299 listing once', async () => {
  const initial = [matrix(50000)];
  const { data, storage } = storageFor([['enabled', true], ['eventScope', scope], ['current', initial], ['seenOffers', {}]]);
  const monitor = new Monitor({ storage }, {});
  const changed = structuredClone(initial);
  changed[0]['Pista||Meia Estudante'] = { preco_min: 25000, disponivel: 2, id_ref: 'new' };
  monitor.collect = async () => changed;
  const oldFetch = globalThis.fetch;
  const sent = [];
  globalThis.fetch = async (_, init) => { sent.push(JSON.parse(init.body)); return Response.json({ deliveryState: 'confirmed_by_whatsapp_bridge' }); };
  try { await monitor.tick(); await monitor.tick(); }
  finally { globalThis.fetch = oldFetch; }
  assert.equal(sent.length, 1);
  assert.match(sent[0].text, /R\$ 250,00/);
  assert.match(sent[0].link, /data=1789613999000/);
  assert.equal(data.get('pending'), undefined);
});

test('retires an ambiguous delivery and still sends a later offer', async () => {
  const initial = [matrix(50000)];
  const { data, storage } = storageFor([['enabled', true], ['eventScope', scope], ['current', initial], ['seenOffers', {}]]);
  const monitor = new Monitor({ storage }, {});
  const first = structuredClone(initial);
  first[0]['Pista||Inteira'] = { preco_min: 9900, disponivel: 1, id_ref: 'first' };
  const second = structuredClone(first);
  second[0]['Pista||Meia Estudante'] = { preco_min: 5500, disponivel: 1, id_ref: 'second' };
  const snapshots = [first, second];
  monitor.collect = async () => snapshots.shift();
  const oldFetch = globalThis.fetch;
  const sent = [];
  globalThis.fetch = async (_, init) => {
    sent.push(JSON.parse(init.body));
    return sent.length === 1
      ? Response.json({ code: 'delivery_unknown' }, { status: 503 })
      : Response.json({ deliveryState: 'confirmed_by_whatsapp_bridge' });
  };
  try { await monitor.tick(); await monitor.tick(); }
  finally { globalThis.fetch = oldFetch; }
  assert.equal(sent.length, 2);
  assert.match(sent[0].text, /R\$ 99,00/);
  assert.match(sent[1].text, /R\$ 55,00/);
  assert.equal(data.get('pending'), undefined);
  assert.equal(data.get('retiredPending').state, 'unknown');
  assert.ok(data.get('lastUnknownDeliveryAt'));
});

test('retire disables the old object and deletes its alarm', async () => {
  const { data, storage } = storageFor([['enabled', true], ['purchasesEnabled', true], ['alarm', 123]]);
  const monitor = new Monitor({ storage }, {});
  const response = await monitor.fetch(new Request('https://monitor/retire', { method: 'POST' }));
  assert.equal(response.status, 200);
  assert.equal(data.get('enabled'), false);
  assert.equal(data.get('purchasesEnabled'), false);
  assert.equal(data.has('alarm'), false);
});

test('automatic purchase can be armed only after checkout validation', async () => {
  const { storage } = storageFor();
  const monitor = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  const response = await monitor.fetch(new Request('https://monitor/purchases/start', { method: 'POST' }));
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { purchasesEnabled: true, listingPriceLimit: 10000 });
});

test('skips Meia Idoso and creates one PIX for another sub-R$100 category', async () => {
  const { data, storage } = storageFor([['purchasesEnabled', true]]);
  const monitor = new Monitor({ storage }, { PIX_CHECKOUT_VALIDATED: 'true' });
  const current = [matrix(50000)];
  current[0]['Pista||Meia Idoso'] = { preco_min: 5000, disponivel: 1, id_ref: 'elderly' };
  current[0]['Pista||Meia Estudante'] = { preco_min: 5500, disponivel: 1, id_ref: 'cheap' };
  let calls = 0;
  globalThis.__runCheckout = async (_env, candidate, options) => {
    calls++;
    assert.equal(candidate.category, 'Meia Estudante');
    await options.beforeCommit({ finalPrice: 12500 });
    return { status: 'pix_created', finalPrice: 12500, pixCode: '000201' + 'A'.repeat(70) };
  };
  try { await monitor.maybePurchase(current); await monitor.maybePurchase(current); }
  finally { delete globalThis.__runCheckout; }
  assert.equal(calls, 1);
  assert.equal(data.get('purchases').days[0].status, 'pix_created');
  assert.equal(data.get('pixPending').state, 'queued');
  assert.match(data.get('pixPending').text, /16\/09 QUA - DEMI LOVATO/);
  assert.match(data.get('pixPending').text, /Valor anunciado: \*R\$ 55,00\*/);
  assert.match(data.get('pixPending').text, /Valor final com cupom: \*R\$ 125,00\*/);
});
