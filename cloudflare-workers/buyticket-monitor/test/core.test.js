import test from 'node:test';
import assert from 'node:assert/strict';
import { ALERT_PRICE_LIMIT, PURCHASE_LISTING_LIMIT, EVENTS, eventUrl, finalPriceAllowed, format, formatPixMessage, keys, parse, purchaseCandidates, qualifyingOffers } from '../src/core.js';

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
  const offers = qualifyingOffers([day0]);
  assert.deepEqual(offers, [{ i: 0, key: 'VIP||Promocional', price: 29899, available: 1, idRef: 'under-limit' }]);
});

test('formats one compact highlighted alert with quantity and event link', () => {
  const current = [matrix(50000)];
  current[0]['Pista||Meia Estudante'] = { preco_min: 25000, disponivel: 2, id_ref: 'new-offer' };
  const offers = qualifyingOffers(current);
  const text = format(current, offers, '2026-09-11T15:00:00Z', 0);
  assert.match(text, /OFERTA • DEMI LOVATO/);
  assert.match(text, /16\/09 QUA - DEMI LOVATO/);
  assert.match(text, /🔥 \*Meia 👨🏻‍🎓: R\$ 250,00 \(🎟️ 2\)\*/);
  assert.match(text, /demilovato%E2%80%93itsnotthatdeeptour-2026/);
  assert.doesNotMatch(text, /15\/09/);
});

test('uses only the requested September 16 São Paulo event URL', () => {
  assert.deepEqual(EVENTS.map(event => event.day), ['16/09/2026']);
  assert.match(eventUrl(EVENTS[0]), /data=1789613999000/);
  assert.ok(EVENTS.every(event => eventUrl(event).includes('cidade=S%C3%A3o+Paulo')));
});

test('buys any category strictly below R$100 and accepts any positive final total', () => {
  const source = {
    'Pista||Meia Idoso': { preco_min: PURCHASE_LISTING_LIMIT - 1, disponivel: 1, id_ref: 'idoso' },
    'Pista||Inteira': { preco_min: PURCHASE_LISTING_LIMIT, disponivel: 1, id_ref: 'boundary' },
  };
  assert.deepEqual(purchaseCandidates(source, 0), [{
    dayIndex: 0,
    key: 'Pista||Meia Idoso',
    sector: 'Pista',
    category: 'Meia Idoso',
    idRef: 'idoso',
    listedPrice: 9999,
  }]);
  assert.equal(finalPriceAllowed(25000), true);
  assert.equal(finalPriceAllowed(0), false);
  assert.match(formatPixMessage({ dayIndex: 0, sector: 'Pista', category: 'Meia Idoso', listedPrice: 5500, finalPrice: 12500, pixCode: '000201' + 'A'.repeat(70) }, '2026-09-15T15:00:00Z'), /Valor anunciado: \*R\$ 55,00\*[\s\S]*Valor final com cupom: \*R\$ 125,00\*/);
});
