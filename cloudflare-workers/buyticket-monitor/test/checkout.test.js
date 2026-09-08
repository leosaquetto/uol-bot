import test from 'node:test';
import assert from 'node:assert/strict';
import { parseBrl, extractPixTotal, extractPixCode, parseFinalReview } from '../src/checkout-logic.js';

test('parses Brazilian currency and the PIX price after coupon', () => {
  assert.equal(parseBrl('R$ 1.234,56'), 123456);
  assert.equal(extractPixTotal('Método de pagamento\nPIX\nOpção mais econômica\nR$ 386,00\nCartão R$ 400,26'), 38600);
  assert.equal(extractPixTotal('PIX\nR$ 396,00\nR$ 386,00\nCartão de crédito\nR$ 400,26'), 38600);
  assert.equal(parseBrl('sem valor'), null);
});

test('extracts a PIX copy-and-paste payload without accepting short text', () => {
  const code = '000201' + 'A'.repeat(70);
  assert.equal(extractPixCode(['texto', code]), code);
  assert.equal(extractPixCode('000201curto'), null);
});

const review = `Resumo da compra
Ingresso R$ 360,00
Taxa de serviço (10%) R$ 36,00
Cupom de desconto - R$ 10,00
Valor total
R$ 386,00
Método de pagamento
Editar
Pix
Comprar agora`;
test('final review validates fees and coupon with total before payment method', () => {
  assert.deepEqual(parseFinalReview(review), { ticket: 36000, fee: 3600, discount: 1000, total: 38600, payment: 'PIX' });
  assert.equal(parseFinalReview(review.replace('386,00', '385,00')), null);
  assert.equal(parseFinalReview(review.replace('Pix', 'Cartão de crédito')), null);
  assert.equal(parseFinalReview(review.replace('Cupom de desconto - R$ 10,00', '')), null);
  assert.equal(parseBrl('R$ 0,29'), 29);
});
