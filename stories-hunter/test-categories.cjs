const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const template = require('./assemble-ui.cjs').assembleUI();
function fn(name) {
  const start = template.search(new RegExp('    (?:async )?function ' + name + '\\('));
  assert.ok(start > 0, name);
  return template.slice(start, template.indexOf('\n    }', start) + '\n    }'.length);
}
const mapStart = template.indexOf('    const CATEGORY_MAP =');
const mapEnd = template.indexOf('\n    };', mapStart) + '\n    };'.length;
function node() {
  return { style: {}, innerHTML: '', textContent: '', children: [], appendChild(child) { this.children.push(child); } };
}
function harness() {
  const elements = new Map();
  const context = vm.createContext({
    console, Date, URL, ICONS: {}, activeFilter: null, isGrouped: false, currentTab: 'home',
    historyData: { active: [], history: [] }, currentRenderedGroups: new Map(),
    homeOfferSets: (active, history) => ({ activeItems: active, soldOutItems: history }),
    document: { getElementById(id) { if (!elements.has(id)) elements.set(id, node()); return elements.get(id); }, createElement: node },
    renderOffers() {}, renderHistoryScreen() {}, updateTimelineBadgeDot() {}, saveHistoryToStorage() {},
    toggleCategoryFilter(id) { context.activeFilter = id; },
  });
  vm.runInContext(template.slice(mapStart, mapEnd) + '\nglobalThis.CATEGORY_MAP = CATEGORY_MAP;\n' + fn('classifyCategory'), context);
  return { context, elements };
}
const football = { id: 'corinthians', title: '2 INGRESSOS: 17/10 Neo Química Arena SP',
  description: 'Assinante UOL: resgate ingressos para Corinthians x EC Vitória, partida válida pelo Campeonato Brasileiro, na Neo Química Arena.' };

test('football uses match/competition evidence in normalized descriptions, never the stadium alone', () => {
  const h = harness();
  const classify = (title, description = '', url = '') => h.context.classifyCategory(title, url, description);
  assert.equal(classify(football.title, football.description), 'FUTEBOL');
  assert.equal(classify('Neo Química Arena', 'Ingresso para a partida de fútebol do Corinthians.'.normalize('NFD')), 'FUTEBOL');
  assert.equal(classify('Corinthians x EC Vitória', '', '/campanha-corinthians-x-vitoria'), 'FUTEBOL');
  assert.equal(classify('Show do Coldplay na Neo Química Arena', 'Estádio do Corinthians e palco de futebol.'), 'MUSICA');
  assert.equal(classify('2 ingressos: Neo Química Arena', 'Show do Coldplay no estádio de futebol do Corinthians.'), 'MUSICA');
  assert.equal(classify('Musical no estádio', 'Peça sobre futebol.'), 'MUSICA');
  assert.equal(classify('Peça Futebol Clube', 'Apresentação no Teatro.'), 'TEATRO');
  assert.equal(classify('2 ingressos: 17/10', 'Espetáculo teatral: uma peça sobre futebol no Teatro Claro.'), 'TEATRO');
  assert.equal(classify('Neo Química Arena', 'Visite o local para aproveitar o benefício.'), 'OUTROS');
  assert.equal(classify('Campeonato Brasileiro de Basquete'), 'OUTROS');
});

test('existing categories keep their contracts and Football exposes only a compact icon', () => {
  const h = harness();
  for (const [title, expected] of [['Exposição Arte', 'EXPOSICOES'], ['Museu de Arte Brasileira', 'MUSEU'],
    ['Teatro UOL', 'TEATRO'], ['Concerto de música', 'MUSICA'], ['Comédia', 'TEATRO'], ['10% OFF no site', 'OUTROS']]) {
    assert.equal(h.context.classifyCategory(title), expected, title);
  }
  assert.equal(h.context.CATEGORY_MAP.FUTEBOL.label, 'Futebol');
  assert.equal(h.context.CATEGORY_MAP.FUTEBOL.icon, '⚽');
});

test('Football appears in the category filter, counter and grouped section', () => {
  const h = harness();
  const items = [football, { ...football, id: 'second-game' }].map(item => ({ ...item, category: 'FUTEBOL' }));
  h.context.historyData.active = items;
  vm.runInContext(fn('updateCategoryCounts'), h.context);
  h.context.updateCategoryCounts(items);
  const chip = h.elements.get('categoryChips').children[0];
  assert.equal(chip.id, 'chip_FUTEBOL');
  assert.equal(chip.textContent, 'Futebol 2');
  chip.onclick();
  assert.equal(h.context.activeFilter, 'FUTEBOL');
  h.context.buildOfferGroups = () => items.map(item => ({ key: item.id, category: item.category, activeItems: [item], isHighlight: false }));
  h.context.createCategoryCarousel = (id, label, groups) => ({ id, label, groups });
  vm.runInContext(fn('renderGroupedOffers'), h.context);
  const container = node();
  h.context.renderGroupedOffers(items, [], container);
  assert.equal(container.children.length, 1);
  assert.equal(container.children[0].label, 'Futebol');
  assert.equal(container.children[0].groups.length, 2);
});

test('description enrichment updates local category and detail badge without changing availability', () => {
  const h = harness();
  const tag = node();
  h.context.activeOffer = { ...football, category: 'OUTROS', availability: 'observed', firstSeen: 123 };
  h.context.historyData.active = [h.context.activeOffer];
  h.context.document.querySelector = () => tag;
  vm.runInContext(fn('updateCategoryCounts') + '\n' + fn('renderModalDescription'), h.context);
  h.context.renderModalDescription(football.description);
  assert.equal(h.context.activeOffer.category, 'FUTEBOL');
  assert.equal(tag.className, 'card-tag tag-category tag-cat-FUTEBOL');
  assert.match(tag.innerHTML, /Futebol/);
  assert.equal(h.context.activeOffer.availability, 'observed');
  assert.equal(h.context.activeOffer.firstSeen, 123);
});

test('Football label reaches timeline and historical category metrics', () => {
  const h = harness();
  const item = { ...football, category: 'FUTEBOL', firstSeen: Date.now() - 1000 };
  h.context.historyData.active = [item];
  Object.assign(h.context, { getGroupKey: item => item.id, offerImage: () => '', isStoriesOnlyOffer: () => false,
    catalogTime: value => Number(value) || 0, confirmedSoldOutTime: () => 0, getAllRedemptionsList: () => [] });
  vm.runInContext(fn('getTimelineEvents'), h.context);
  assert.match(h.context.getTimelineEvents()[0].desc, /Futebol/);
  Object.assign(h.context, { getAllCatalogOffers: () => [item], saoPauloParts: () => ({ year: 2026, month: 10 }),
    selectedHistoryYear: null, selectedHistoryMonth: null, getOfferYearAndMonth: () => ({ year: 2026, month: 9 }),
    MONTH_SHORT_LABELS: Array(12).fill('Out'), historySearchQuery: '', htmlAttribute: value => value,
    renderHistoryCurated() {}, renderHistoryMonthSelector() {}, renderHistoryMonths() {} });
  vm.runInContext(fn('renderHistoryScreen'), h.context);
  h.context.renderHistoryScreen();
  assert.match(h.elements.get('viewHistory').innerHTML, /CATEGORIA TOP<\/div>\s*<div class="metric-val">Futebol<\/div>/);
});
