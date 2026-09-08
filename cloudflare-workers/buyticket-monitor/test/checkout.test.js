import test from 'node:test';
import assert from 'node:assert/strict';
import { parseBrl, extractPixTotal, extractPixCode } from '../src/checkout-logic.js';

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
