import test from 'node:test';
import assert from 'node:assert/strict';
import { parse, drops, format, keys, purchaseCandidates, finalPriceAllowed, formatPixMessage } from '../src/core.js';
const matrix = p => Object.fromEntries(keys.map(k => [k, { preco_min: p, disponivel: 3, id_ref: 'ref' }]));
test('Flight extraction validates prices and rejects missing data', () => {
  assert.deepEqual(parse('a:' + JSON.stringify(['$', { matriz_preco: matrix(33000) }])), matrix(33000));
  assert.throws(() => parse('a:{}'));
  assert.throws(() => parse('a:' + JSON.stringify({ matriz_preco: matrix(-1) })));
});
test('purchase scans every category and enforces the final per-ticket bounds', () => {
  const current = matrix(90000);
  current['VIP||Meia Professor'] = { preco_min: 26000, disponivel: 1, id_ref: 'candidate' };
  current['Gramado||Promocional'] = { preco_min: 9900, disponivel: 1, id_ref: 'too-cheap' };
  assert.deepEqual(purchaseCandidates(current, 1), [{
    dayIndex: 1, key: 'VIP||Meia Professor', sector: 'VIP', category: 'Meia Professor', idRef: 'candidate', listedPrice: 26000,
  }]);
  assert.equal(finalPriceAllowed(1, 10000), true);
  assert.equal(finalPriceAllowed(1, 27000), true);
  assert.equal(finalPriceAllowed(1, 9999), false);
  assert.equal(finalPriceAllowed(1, 27001), false);
});
test('day 13 purchase lane accepts any category within the R$270 final cap', () => {
  const current = matrix(90000);
  current['Gramado||Meia Estudante'] = { preco_min: 27000, disponivel: 2, id_ref: 'student' };
  current['Gramado||Inteira'] = { preco_min: 20000, disponivel: 2, id_ref: 'whole' };
  current['Comfort Zone||Meia Estudante'] = { preco_min: 24000, disponivel: 2, id_ref: 'comfort' };
  assert.deepEqual(purchaseCandidates(current, 1), [
    { dayIndex: 1, key: 'Gramado||Inteira', sector: 'Gramado', category: 'Inteira', idRef: 'whole', listedPrice: 20000 },
    { dayIndex: 1, key: 'Comfort Zone||Meia Estudante', sector: 'Comfort Zone', category: 'Meia Estudante', idRef: 'comfort', listedPrice: 24000 },
    { dayIndex: 1, key: 'Gramado||Meia Estudante', sector: 'Gramado', category: 'Meia Estudante', idRef: 'student', listedPrice: 27000 },
  ]);
  assert.equal(finalPriceAllowed(1, 27000), true);
  assert.equal(finalPriceAllowed(1, 27001), false);
});
test('purchase lane excludes Meia Idoso on both event days', () => {
  for (const [dayIndex, price] of [[0, 25000], [1, 20000]]) {
    const current = {};
    current['Gramado||Meia Idoso'] = { preco_min: price, disponivel: 2, id_ref: `elderly-${dayIndex}` };
    current['Gramado||Inteira'] = { preco_min: price, disponivel: 1, id_ref: `whole-${dayIndex}` };
    assert.deepEqual(purchaseCandidates(current, dayIndex).map(candidate => candidate.category), ['Inteira']);
  }
});
test('PIX message contains one ticket, coupon-adjusted total and the event link', () => {
  const text = formatPixMessage({ dayIndex: 1, sector: 'Gramado', category: 'Meia Estudante', finalPrice: 24000, pixCode: '000201TESTE' }, '2026-09-08T12:00:00Z');
  assert.match(text, /🎟️ 1 ingresso/);
  assert.match(text, /Valor final com cupom: \*R\$ 240,00\*/);
  assert.match(text, /000201TESTE/);
  assert.match(text, /buyticketbrasil\.com\/evento\/rockinrio2026/);
});
test('baseline, increases, sold out and restock are not price drops', () => {
  assert.deepEqual(drops(null, [matrix(33000)]), []);
  assert.deepEqual(drops([matrix(33000)], [matrix(34000)]), []);
  const empty = Object.fromEntries(keys.map(k => [k, { preco_min: 0, disponivel: 0 }]));
  assert.deepEqual(drops([matrix(33000)], [empty]), []);
  assert.deepEqual(drops([empty], [matrix(29000)]), []);
  assert.equal(drops([matrix(33000)], [matrix(29000)])[0].before, 33000);
});
test('message includes all twelve rows, quantities, exact discount and event links', () => {
  const current = [matrix(29000), matrix(33000)];
  const text = format(current, drops([matrix(33000), matrix(33000)], current), '2026-09-08T00:00:00Z');
  assert.equal((text.match(/🎟️ 3/g) || []).length, 12);
  assert.match(text, /R\$ 40,00/);
  assert.match(text, /13\/09 DOM - HALSEY/);
  assert.match(text, /12\/09 SÁB - DEMI LOVATO/);
});
test('extracts matrix after multiline Flight text records', () => {
  const body = 'a:T100,description\n[ml] text "quotes"\n0:{"matriz_preco":' + JSON.stringify(matrix(33000)) + '}\n';
  assert.deepEqual(parse(body), matrix(33000));
});
test('only daily global minimum triggers, including categories outside reference rows', () => {
  const old = matrix(80000);
  old['Gramado||Inteira'].preco_min = 50000;
  const next = structuredClone(old);
  next['Comfort Zone||Inteira'].preco_min = 60000;
  assert.deepEqual(drops([old], [next]), []);
  next['VIP||Meia Professor'] = { preco_min: 40000, disponivel: 1, id_ref: 'new' };
  const parsed = parse('0:' + JSON.stringify({ matriz_preco: next }));
  const changes = drops([old], [parsed]);
  assert.deepEqual(changes, [{ i: 0, key: 'VIP||Meia Professor', before: 50000, after: 40000 }]);
  const text = format([parsed], changes, '2026-09-08T00:00:00Z');
  assert.ok(text.includes('🔥 *Meia Professor: R$ 400,00 🔻 R$ 100,00 (🎟️ 1)*'));
  assert.ok(text.includes('Meia ♿️'));
  assert.ok(text.includes('Meia 👨🏻‍🎓'));
  assert.ok(!text.includes('Estudante') && !text.includes('PCD'));
  assert.deepEqual(drops([next], [structuredClone(next)]), []);
});
test('day messages contain only their own prices, highlights and link', () => {
  const current = [matrix(20000), matrix(30000)];
  const changes = drops([matrix(40000), matrix(40000)], current);
  for (const i of [0, 1]) {
    const text = format(current, changes, '2026-09-08T00:00:00Z', i);
    assert.equal((text.match(/🎟️ 3/g) || []).length, 6);
    assert.equal((text.match(/Ver ingressos:/g) || []).length, 1);
    assert.ok(text.includes(i === 0 ? 'DEMI LOVATO' : 'HALSEY'));
    assert.ok(!text.includes(i === 0 ? 'HALSEY' : 'DEMI LOVATO'));
  }
});
