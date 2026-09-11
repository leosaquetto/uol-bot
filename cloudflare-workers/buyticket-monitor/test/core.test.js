import test from 'node:test';
import assert from 'node:assert/strict';
import { ALERT_PRICE_LIMIT, EVENTS, eventUrl, format, keys, parse, qualifyingOffers } from '../src/core.js';

const matrix = price => Object.fromEntries(keys.map(key => [key, { preco_min: price, disponivel: 3, id_ref: `${key}:${price}` }]));

test('extracts and validates a multiline Flight price matrix', () => {
  const expected = matrix(33000);
  assert.deepEqual(parse(`text\n0:{"matriz_preco":${JSON.stringify(expected)}}\n`), expected);
  assert.throws(() => parse('a:{}'), /matrix_invalid/);
  assert.throws(() => parse(`0:${JSON.stringify({ matriz_preco: matrix(-1) })}`), /price_invalid/);
});

test('keeps the event sectors exactly as returned by BuyTicket', () => {
  const source = { 'Pista Premium||Inteira': { preco_min: 50000, disponivel: 2, id_ref: 'pista' } };
  assert.deepEqual(parse(`0:${JSON.stringify({ matriz_preco: source })}`), source);
});

test('qualifies every available listing strictly below R$299', () => {
  const day0 = matrix(ALERT_PRICE_LIMIT);
  day0['VIP||Promocional'] = { preco_min: 29899, disponivel: 1, id_ref: 'under-limit' };
  day0['VIP||Sem estoque'] = { preco_min: 10000, disponivel: 0, id_ref: 'unavailable' };
  const offers = qualifyingOffers([day0, matrix(50000)]);
  assert.deepEqual(offers, [{ i: 0, key: 'VIP||Promocional', price: 29899, available: 1, idRef: 'under-limit' }]);
});

test('formats one compact highlighted alert with quantity and event link', () => {
  const current = [matrix(50000), matrix(50000)];
  current[1]['Pista||Meia Estudante'] = { preco_min: 25000, disponivel: 2, id_ref: 'new-offer' };
  const offers = qualifyingOffers(current);
  const text = format(current, offers, '2026-09-11T15:00:00Z', 1);
  assert.match(text, /OFERTA • DEMI LOVATO/);
  assert.match(text, /17\/09 QUI - DEMI LOVATO/);
  assert.match(text, /🔥 \*Meia 👨🏻‍🎓: R\$ 250,00 \(🎟️ 2\)\*/);
  assert.match(text, /demilovato%E2%80%93itsnotthatdeeptour-2026/);
  assert.doesNotMatch(text, /16\/09/);
});

test('uses the two requested São Paulo event URLs', () => {
  assert.deepEqual(EVENTS.map(event => event.day), ['16/09/2026', '17/09/2026']);
  assert.match(eventUrl(EVENTS[0]), /data=1789527599000/);
  assert.match(eventUrl(EVENTS[1]), /data=1789613999000/);
  assert.ok(EVENTS.every(event => eventUrl(event).includes('cidade=S%C3%A3o+Paulo')));
});
