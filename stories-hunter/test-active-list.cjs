const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const template = fs.readFileSync(path.join(__dirname, 'template.html'), 'utf8');
const now = Date.parse('2026-10-18T01:00:00-03:00');
const clone = value => JSON.parse(JSON.stringify(value));
function fn(name) {
  const start = template.indexOf('    function ' + name + '(');
  assert.ok(start > 0, name);
  return template.slice(start, template.indexOf('\n    }', start) + '\n    }'.length);
}
function element() {
  return { children: [], style: { setProperty() {} }, textContent: '',
    set innerHTML(value) { this.markup = value; this.children = []; }, get innerHTML() { return this.markup || ''; },
    appendChild(child) { this.children.push(child); } };
}
function harness(active) {
  let time = now;
  const elements = new Map(), grouped = [], requests = [];
  class Clock extends Date { static now() { return time; } }
  const context = vm.createContext({
    URL, Date: Clock, Intl, console,
    historyData: { active: clone(active), history: [] }, activeFilter: null, isGrouped: true,
    CATEGORY_MAP: { OUTROS: { label: 'Outros', icon: '' } }, ICONS: {},
    document: { createElement: element, getElementById: id => {
      if (!elements.has(id)) elements.set(id, element()); return elements.get(id);
    } },
    localStorage: { getItem: () => null },
    fetch: url => { requests.push(url); throw new Error('unexpected per-card request'); },
    recordSessionImpressions() {}, checkHighlight: () => false, offerImage: () => '',
    isStoriesOnlyOffer: () => false, getRedeemedUsersForOffer: () => [], renderRedeemedFriendsPill: () => '',
    buildOfferGroups: (current, history) => [...current, ...history],
    renderGroupedOffers: current => grouped.push(current.map(item => item.id)),
  });
  const dates = template.slice(template.indexOf('// UI_DATE_HELPERS_START'), template.indexOf('// UI_DATE_HELPERS_END'));
  vm.runInContext(dates + '\n' + ['catalogTime', 'catalogCampaign', 'availabilityEvidence', 'confirmedSoldOutTime',
    'homeOfferSets', 'extractSessionDate', 'renderOffers', 'renderIndividualOffers', 'formatRelativeTime', 'updateCategoryCounts'].map(fn).join('\n'), context);
  return { context, elements, grouped, requests, setTime: value => { time = value; } };
}
const offer = (id, extra = {}) => ({ id, title: id, category: 'OUTROS',
  link: 'https://clube.uol.com.br/campanhasdeingresso/' + id, firstSeen: Date.parse('2026-09-29T12:00:00Z'), ...extra });

test('session date uses public validity and its explicit year, preserving genuinely unknown dates', () => {
  const h = harness([]);
  assert.equal(h.context.sessionDateInfo(offer('validity', { validity: 'Válido até 17/10/2026' })).key, '2026-10-17');
  assert.equal(h.context.sessionDateInfo(offer('older-year', { validity: 'Válido até 31/12/2025' })).key, '2025-12-31');
  assert.equal(h.context.sessionDateInfo(offer('named', { description: 'Apresentação em 17 de outubro de 2026.' })).key, '2026-10-17');
  assert.equal(h.context.sessionDateInfo(offer('unknown', { publishedAt: '2026-10-06T12:00:00Z', lastSeen: now })), null);
  assert.equal(h.context.sessionDateInfo(offer('email-date', { description: 'Ingresso enviado até 17/10/2026 por email.' })), null);
  assert.equal(h.context.sessionDateInfo(offer('invalid', { validity: 'Até 31/02/2026' })), null);
});

test('Home, active list and counters share proven session cutoffs without requests or history changes', () => {
  const active = [
    offer('old-title', { title: '2 INGRESSOS: 17/10/2026 Teatro' }),
    offer('today', { title: '2 INGRESSOS: 18/10/2026 Teatro Claro SP' }),
    offer('old-description', { description: 'Apresentação em 17 de outubro de 2026.' }),
    offer('old-validity', { validity: 'Válido até 17/10/2026' }),
    offer('tomorrow', { validity: 'Válido até 19/10/2026' }),
    offer('unknown', { lastSeen: now }),
  ];
  const h = harness(active), original = clone(h.context.historyData);
  const expected = ['today', 'tomorrow', 'unknown'];
  h.context.renderOffers(h.context.historyData.active);
  assert.deepEqual(clone(h.grouped[0]), expected);
  h.context.updateCategoryCounts(h.context.historyData.active);
  assert.equal(h.elements.get('categoryChips').children[0].textContent, 'Outros 3');
  h.context.isGrouped = false;
  h.context.renderOffers(h.context.historyData.active);
  const renderedIds = () => h.elements.get('cardList').children.map(card => card.innerHTML.match(/openDetails\('([^']+)'\)/)[1]);
  assert.deepEqual(renderedIds(), expected, 'the second tab receives the same filtered active records');
  h.context.updateCategoryCounts(h.context.historyData.active);
  assert.equal(h.elements.get('categoryChips').children[0].textContent, 'Outros 3');
  h.setTime(Date.parse('2026-10-19T00:00:00-03:00'));
  h.context.renderOffers(h.context.historyData.active);
  assert.deepEqual(renderedIds(), ['tomorrow', 'unknown'], 'a date-only session expires at the next São Paulo civil day');
  assert.deepEqual(clone(h.context.historyData), original);
  assert.equal(h.requests.length, 0);
});
