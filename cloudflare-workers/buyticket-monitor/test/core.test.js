import test from 'node:test';
import assert from 'node:assert/strict';
import { parse, drops, format, keys } from '../src/core.js';
const matrix = p => Object.fromEntries(keys.map(k => [k, { preco_min: p, disponivel: 3, id_ref: 'ref' }]));
test('Flight extraction validates prices and rejects missing data', () => {
  assert.deepEqual(parse('a:' + JSON.stringify(['$', { matriz_preco: matrix(33000) }])), matrix(33000));
  assert.throws(() => parse('a:{}'));
  assert.throws(() => parse('a:' + JSON.stringify({ matriz_preco: matrix(-1) })));
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
  assert.equal((text.match(/3 disponíveis na categoria/g) || []).length, 12);
  assert.match(text, /R\$ 40,00/);
  assert.match(text, /13\/09\/2026/);
  assert.match(text, /12\/09\/2026/);
});
test('extracts matrix after multiline Flight text records', () => {
  const body = 'a:T100,description\n[ml] text "quotes"\n0:{"matriz_preco":' + JSON.stringify(matrix(33000)) + '}\n';
  assert.deepEqual(parse(body), matrix(33000));
});
