import test from 'node:test';
import assert from 'node:assert/strict';
import { planTicketCodeScan, recordTicketCodeResults, protectedTicketCodeIds,
  TICKET_CODE_DAILY_REQUEST_LIMIT } from '../src/ticket-code-policy.js';

const now = Date.parse('2026-10-09T15:00:00Z');
const card = { id: 'tickets-13-10', link: 'https://clube.uol.com.br/campanhasdeingresso/pPS-2-ingressos-13-10' };
test('reserves bounded requests durably and respects interval and daily cap', () => {
  const plan = planTicketCodeScan({}, ['pPQ','pPR','pPS','pPZ','pPA'], now);
  assert.equal(plan.selected.length, 4);
  assert.equal(plan.state.requestsUsed, 8);
  assert.equal(planTicketCodeScan(JSON.parse(JSON.stringify(plan.state)), ['pPS'], now + 1), null);
  assert.equal(planTicketCodeScan({ day: '2026-10-09', requestsUsed: TICKET_CODE_DAILY_REQUEST_LIMIT }, ['pPS'], now), null);
  assert.equal(planTicketCodeScan({ day: '2026-10-08', requestsUsed: TICKET_CODE_DAILY_REQUEST_LIMIT }, ['pPS'], now).state.requestsUsed, 2);
});
test('retains unknown pages and needs two spaced absences before releasing protection', () => {
  let state = planTicketCodeScan({}, ['pPS'], now).state;
  state = recordTicketCodeResults(state, ['pPS'], [{ status: 'found', card, requests: 2 }], now);
  state.entries.pPS.resolvedId = card.id;
  state = recordTicketCodeResults(state, ['pPS'], [{ status: 'unknown', reason: 'http_429', requests: 1 }], now + 300000);
  assert.deepEqual(protectedTicketCodeIds(state, now + 300000), [card.id]);
  assert.equal(state.nextAt, now + 600000);
  state = recordTicketCodeResults(state, ['pPS'], [{ status: 'absent', requests: 1 }], now + 600000);
  assert.deepEqual(protectedTicketCodeIds(state, now + 600000), [card.id]);
  assert.equal(planTicketCodeScan(state, [], now + 600001), null);
  state = recordTicketCodeResults(state, ['pPS'], [{ status: 'absent', requests: 1 }], now + 1200000);
  assert.deepEqual(protectedTicketCodeIds(state, now + 1200000), []);
});
test('rotates unchecked candidates and tracks discovered pages outside recent window', () => {
  const previous = { entries: { pPS: { card, foundAt: now, checkedAt: now-600000, nextAt: 0 } } };
  const plan = planTicketCodeScan(previous, ['pPQ','pPR','pPZ','pPA'], now);
  assert.deepEqual(plan.selected, ['pPQ','pPR','pPZ','pPA']);
  assert.ok(plan.state.entries.pPS.card);
  const next = planTicketCodeScan(plan.state, ['pPQ','pPR','pPZ','pPA'], now+60000);
  assert.deepEqual(next.selected, ['pPS']);
});
