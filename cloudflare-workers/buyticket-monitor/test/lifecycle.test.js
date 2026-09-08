import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const core = new URL('../src/core.js', import.meta.url).href;
const source = (await readFile(new URL('../src/worker.js', import.meta.url), 'utf8'))
  .replace("import { DurableObject } from 'cloudflare:workers';", 'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }')
  .replace("'./core.js'", JSON.stringify(core));
const { Monitor } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
const { keys } = await import(core);
test('silent initialization and ticks never dispatch; start sends once; unknown delivery blocks repeats', async () => {
  const data = new Map();
  const storage = { get: async k => data.get(k), put: async (k,v) => { if (typeof k === 'object') Object.entries(k).forEach(([a,b]) => data.set(a,b)); else data.set(k,v); }, delete: async k => data.delete(k), list: async () => data, setAlarm: async () => {} };
  const m = new Monitor({ storage }, {});
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
test('date change silently replaces baseline and retires old pending delivery', async () => {
  const data = new Map([['enabled', true], ['current', [{}, {}]], ['pending', { state: 'queued', text: 'old date' }]]);
  const storage = { get: async k => data.get(k), put: async (k,v) => { if (typeof k === 'object') Object.entries(k).forEach(([a,b]) => data.set(a,b)); else data.set(k,v); }, delete: async k => data.delete(k), list: async () => data, setAlarm: async () => {} };
  const m = new Monitor({ storage }, {});
  m.collect = async () => [0,1].map(() => Object.fromEntries(keys.map(k => [k, { preco_min: 100, disponivel: 1, id_ref: 'new' }])));
  await m.fetch(new Request('https://monitor/initialize', { method: 'POST' }));
  assert.equal(data.get('enabled'), true);
  assert.equal(data.get('pending'), undefined);
  assert.equal(data.get('retiredPending').text, 'old date');
  const status = await (await m.fetch(new Request('https://monitor/status'))).json();
  assert.deepEqual(status.days, ['12/09/2026', '13/09/2026']);
  assert.equal(status.baselineReady, true);
});
