import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const core = new URL('../src/core.js', import.meta.url).href;
const source = (await readFile(new URL('../src/worker.js', import.meta.url), 'utf8'))
  .replace("import { DurableObject } from 'cloudflare:workers';", 'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }')
  .replace("'./core.js'", JSON.stringify(core));
const { Monitor } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
const { keys } = await import(core);
const scope = 'demi-under-299-v1:1789527599000:1779910250255x792501787503624200:1789613999000:1779910250255x792501787503624200';
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

test('collects both requested events concurrently', async () => {
  const { storage } = storageFor();
  const monitor = new Monitor({ storage }, {});
  const oldFetch = globalThis.fetch;
  let active = 0, peak = 0;
  globalThis.fetch = async () => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => setTimeout(resolve, 10)); active--;
    return new Response(`0:${JSON.stringify({ matriz_preco: matrix(50000) })}`);
  };
  try { assert.equal((await monitor.collect()).length, 2); assert.equal(peak, 2); }
  finally { globalThis.fetch = oldFetch; }
});

test('new scope seeds qualifying listings silently and disables purchases', async () => {
  const { data, storage } = storageFor([['enabled', true], ['eventScope', 'rock-in-rio-old']]);
  const monitor = new Monitor({ storage }, {});
  const current = [matrix(50000), matrix(50000)];
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
  const initial = [matrix(50000), matrix(50000)];
  const { data, storage } = storageFor([['enabled', true], ['eventScope', scope], ['current', initial], ['seenOffers', {}]]);
  const monitor = new Monitor({ storage }, {});
  const changed = structuredClone(initial);
  changed[1]['Pista||Meia Estudante'] = { preco_min: 25000, disponivel: 2, id_ref: 'new' };
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

test('retire disables the old object and deletes its alarm', async () => {
  const { data, storage } = storageFor([['enabled', true], ['purchasesEnabled', true], ['alarm', 123]]);
  const monitor = new Monitor({ storage }, {});
  const response = await monitor.fetch(new Request('https://monitor/retire', { method: 'POST' }));
  assert.equal(response.status, 200);
  assert.equal(data.get('enabled'), false);
  assert.equal(data.get('purchasesEnabled'), false);
  assert.equal(data.has('alarm'), false);
});

test('automatic purchase cannot be armed', async () => {
  const { storage } = storageFor();
  const monitor = new Monitor({ storage }, {});
  const response = await monitor.fetch(new Request('https://monitor/purchases/start', { method: 'POST' }));
  assert.equal(response.status, 410);
  assert.deepEqual(await response.json(), { error: 'purchases_retired' });
});
