const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Execute only source functions against synthetic data. No personal build, network, or cookies.
const template = fs.readFileSync(path.join(__dirname, 'template.html'), 'utf8');
const now = Date.parse('2026-10-17T22:00:00-03:00');
const clone = value => JSON.parse(JSON.stringify(value));
const campaign = slug => 'https://clube.uol.com.br/campanhasdeingresso/' + slug;
const offer = (id, overrides = {}) => ({ id, link: campaign(id), title: id, category: 'TEATRO', firstSeen: now - 100000, ...overrides });
function block(name) {
  const start = template.indexOf('// ' + name + '_START');
  const end = template.indexOf('// ' + name + '_END');
  assert.ok(start > 0 && end > start, name);
  return template.slice(start, end);
}
function fn(name) {
  const start = template.search(new RegExp('    (?:async )?function ' + name + '\\('));
  assert.ok(start > 0, name);
  // Top-level functions are indented four spaces, their closing brace likewise.
  const end = template.indexOf('\n    }', start) + '\n    }'.length;
  return template.slice(start, end);
}
function element() {
  let markup = '';
  return { style: { setProperty() {} }, classList: { toggle() {} }, children: [], textContent: '',
    get innerHTML() { return markup; }, set innerHTML(value) { markup = value; this.children.forEach(node => { node.parentNode = null; }); this.children = []; },
    appendChild(node) { this.children.push(node); node.parentNode = this; },
    insertBefore(node, before) { const index = before ? this.children.indexOf(before) : -1; if (index < 0) this.children.push(node); else this.children.splice(index, 0, node); node.parentNode = this; },
    remove() { if (this.parentNode) { this.parentNode.children = this.parentNode.children.filter(node => node !== this); this.parentNode = null; } },
    setAttribute(key, value) { this[key] = value; } };
}
function harness({ active = [], history = [], bundled = [], saved = [], settings = {}, native = false, paused = false } = {}) {
  let time = now;
  class Clock extends Date { static now() { return time; } }
  const storage = new Map([['uol_real_redemptions', JSON.stringify(saved)]]);
  const elements = new Map();
  const requests = [];
  const context = vm.createContext({
    URL, Date: Clock, Intl, console,
    INITIAL: { realRedemptions: clone(bundled), collectionPausedForUpdate: paused, ...(native ? { hunterCollection: { status: 'starting' } } : {}) },
    settings: clone(settings), historyData: clone({ active, history }), activeFilter: null, isGrouped: true,
    currentTab: 'home', currentTimelineFilter: 'all', historyRenderState: null, selectedHistoryMonth: 'all', historySearchQuery: '', historyDateCache: new WeakMap(),
    MONTH_FULL_NAMES: Array.from({ length: 12 }, (_, i) => 'Month ' + i), currentRenderedGroups: new Map(),
    CATEGORY_MAP: { TEATRO: { label: 'Teatro', icon: '' }, OUTROS: { label: 'Outros', icon: '' } },
    ICONS: { tickets: '<svg></svg>', calendar: '<svg></svg>' },
    document: { getElementById(id) { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); }, createElement: element },
    localStorage: { getItem: key => storage.get(key) || null, setItem: (key, value) => storage.set(key, value) },
    fetch: async url => { requests.push(url); throw new Error('offline fixture'); },
    getCachedDescription: () => '', toSentenceCase: text => text, classifyCategory: () => 'TEATRO',
    saveHistoryToStorage() {}, recordSessionImpressions() {}, checkHighlight: () => false,
    normalizeEventInfo: () => ({}), getRedeemedUsersForOffer: () => [], renderRedeemedFriendsPill: () => '',
    getAvatarUrl: () => 'https://example.test/avatar.jpg', renderHistoryScreen() {}, renderGroupedOffers() {},
    createHistoryPosterCard: group => ({ group, className: 'poster-card' }),
  });
  vm.runInContext([
    block('CATALOG_ADAPTER'), block('UI_DATE_HELPERS'), block('REDEMPTION_IDENTITY'), block('REDEMPTION_DATE'),
    ...['extractSessionDate', 'isStoriesOnlyOffer', 'getGroupKey', 'buildOfferGroups', 'formatRelativeTime', 'formatExpiredAgo',
      'formatRedemptionTitle', 'getTimelineEvents', 'getRedeemedUsersForOffer', 'updateTimelineBadgeDot', 'renderTimelineList', 'renderOffers', 'renderIndividualOffers', 'updateCategoryCounts',
      'getOfferYearAndMonth', 'historyMatchesSearch', 'renderHistoryMonths', 'appendHistoryBatch'].map(fn),
  ].join('\n'), context);
  return { context, elements, requests, storage, advance: ms => { time += ms; } };
}

test('opening an offer uses the native sheet route while keeping Hunter state and browser previews separate', () => {
  const opened = [], toasts = [];
  const location = { href: 'hunter-dashboard' };
  const state = { tab: 'history', filter: 'teatro', scrollY: 380 };
  const context = vm.createContext({
    URL, INITIAL: { nativeBrowser: true }, activeOffer: { link: campaign('new-offer') },
    window: { location, open: (...args) => opened.push(args), hunterState: state },
    showToast: message => toasts.push(message),
  });
  vm.runInContext(fn('openSafariCurrent'), context);
  context.openSafariCurrent();
  assert.equal(location.href, 'hunter-browser://open?url=' + encodeURIComponent(campaign('new-offer')));
  assert.equal(opened.length, 0);
  assert.deepEqual(state, { tab: 'history', filter: 'teatro', scrollY: 380 });
  context.openSafariCurrent(true);
  assert.equal(location.href, 'hunter-browser://external?url=' + encodeURIComponent(campaign('new-offer')));
  assert.equal(opened.length, 0);
  assert.deepEqual(state, { tab: 'history', filter: 'teatro', scrollY: 380 });
  context.activeOffer = { link: '/campanhasdeingresso/relative-offer' };
  context.openSafariCurrent();
  assert.equal(location.href, 'hunter-browser://open?url=' + encodeURIComponent(campaign('relative-offer')));
  for (const url of ['javascript:alert(1)', 'http://clube.uol.com.br/', 'https://example.test/', '//example.test/offer', 'https://user:pass@clube.uol.com.br/', 'https://clube.uol.com.br:444/']) {
    context.activeOffer = { link: url };
    for (const external of [false, true]) {
      location.href = 'hunter-dashboard'; context.openSafariCurrent(external);
      assert.equal(location.href, 'hunter-dashboard'); assert.equal(opened.length, 0);
    }
  }
  context.INITIAL.nativeBrowser = false; context.activeOffer = { link: campaign('preview-offer') };
  context.openSafariCurrent();
  assert.equal(location.href, 'hunter-dashboard', 'ordinary web preview must not replace its dashboard');
  assert.deepEqual(opened, [[campaign('preview-offer'), '_blank', 'noopener,noreferrer']]);
  assert.doesNotMatch(fn('openUolDirectLogin') + fn('openUolLogoutAndLogin'), /hunter-browser:/,
    'authentication keeps its existing browser context');
});

test('Home keeps active cache and only explicit exhaustion in the last 24h without deleting history', () => {
  const active = [offer('active'), offer('already-out', { isExpired: true, expiredAt: now - 10 })];
  const history = [offer('boundary', { expiredAt: now - 86400000 }), offer('old', { expiredAt: now - 86400001 }),
    offer('unknown', { lastSeen: now - 10, firstSeen: now - 20 }), offer('future', { expiredAt: now + 1 }),
    offer('same-campaign-other-id', { link: campaign('active'), expiredAt: now - 10 })];
  const h = harness({ active, history });
  const before = clone(h.context.historyData);
  const result = h.context.homeOfferSets(h.context.historyData.active, h.context.historyData.history);
  assert.deepEqual(clone(result.activeItems).map(o => o.id), ['active']);
  assert.deepEqual(clone(result.soldOutItems).map(o => o.id), ['boundary']);
  assert.deepEqual(clone(h.context.historyData), before);
});

test('Home excludes past civil session dates in São Paulo before grouping, preserving today, unknown and all history', () => {
  const active = [offer('past', { title: '16/10/2026 Teatro' }), offer('today', { title: '17/10/2026 Teatro' }),
    offer('future', { title: '18/10/2026 Teatro' }), offer('unknown', { title: 'Teatro sem data' })];
  const history = [offer('sold-past', { title: '16/10/2026 Teatro', expiredAt: now - 10 }),
    offer('sold-today', { title: '17/10/2026 Teatro', expiredAt: now - 10 })];
  const h = harness({ active, history });
  const before = clone(h.context.historyData);
  const sets = h.context.homeOfferSets(h.context.historyData.active, h.context.historyData.history);
  assert.deepEqual(clone(sets.activeItems).map(o => o.id), ['today', 'future', 'unknown']);
  assert.deepEqual(clone(sets.soldOutItems).map(o => o.id), ['sold-today']);
  assert.deepEqual(clone(h.context.historyData), before);
  const midnight = Date.parse('2026-10-18T03:00:00Z');
  assert.deepEqual(clone(h.context.homeOfferSets(active, [], midnight).activeItems).map(o => o.id), ['future', 'unknown']);
  const rollover = Date.parse('2027-01-01T03:00:00Z');
  assert.deepEqual(clone(h.context.homeOfferSets([
    offer('previous-year', { title: '31/12/2026 Teatro' }), offer('new-year', { title: '01/01/2027 Teatro' })
  ], [], rollover).activeItems).map(o => o.id), ['new-year']);
});

test('freshest availability evidence wins, without inventing an unknown sold-out time', () => {
  const h = harness({ history: [offer('reopened', { isExpired: true, expiredAt: now - 5000, soldOutAt: now - 5000 })] });
  h.context.mergeCatalogSnapshot({ stories: [], offers: [{ id: 'reopened', link: campaign('reopened'), title: 'Show',
    availability: 'observed', soldOutAt: now - 5000, availabilityEvidence: { state: 'available', observedAt: now - 1000, source: 'api' } }] });
  assert.equal(h.context.historyData.active.length, 1);
  h.context.mergeCatalogSnapshot({ stories: [], offers: [{ id: 'reopened', link: campaign('reopened'), title: 'Show',
    availability: 'sold_out', soldOutAt: now - 4000 }] });
  assert.equal(h.context.historyData.active.length, 1, 'old exhaustion cannot overwrite later availability');
  h.context.mergeCatalogSnapshot({ stories: [], offers: [{ id: 'unknown', link: campaign('unknown'), title: 'Other',
    availability: 'sold_out', lastSeenAt: now, soldOutAt: null }] });
  assert.equal(h.context.homeOfferSets([], h.context.historyData.history).soldOutItems.length, 0);
  assert.equal(h.context.historyData.history[0].expiredAt, undefined);
  assert.equal(h.context.confirmedSoldOutTime({ expiredAt: now - 100, availabilityEvidence: { state: 'available', observedAt: now - 200, source: 'api' } }), now - 100);
});

test('both Home render modes and category counts use the same recent sold-out boundary', () => {
  const h = harness({ active: [offer('active')], history: [offer('recent', { expiredAt: now - 1 }), offer('old', { expiredAt: now - 86400001 })] });
  let grouped;
  h.context.renderGroupedOffers = (active, history) => { grouped = [...active, ...history].map(o => o.id); };
  h.context.renderOffers(h.context.historyData.active);
  assert.deepEqual(clone(grouped), ['active', 'recent']);
  h.context.updateCategoryCounts(h.context.historyData.active);
  assert.equal(h.elements.get('categoryChips').children[0].textContent, 'Teatro 2');
  h.context.isGrouped = false;
  h.context.renderOffers(h.context.historyData.active);
  const cards = h.elements.get('cardList').children;
  assert.equal(cards.filter(c => c.className.startsWith('card')).length, 2);
  assert.ok(cards.some(c => c.innerHTML.includes('recent')));
  assert.ok(cards.every(c => !c.innerHTML.includes('>old<')));
});

test('poster candidate skips enlarged placeholder and prefers campaign over Story without changing input order', () => {
  const candidates = [offer('one', { imageUrl: 'https://i.imgur.com/UdIgTfI.png' }),
    offer('two', { imageUrl: 'https://example.test/story.jpg', imageSource: 'story' }),
    offer('three', { imageUrl: 'https://d310rdc9v8uuq.cloudfront.net/poster.jpg', source: 'Portal' })];
  const h = harness();
  assert.equal(h.context.offerImage(candidates), candidates[2].imageUrl);
  assert.match(h.context.posterImageMarkup(candidates), /poster\.jpg/);
  assert.doesNotMatch(h.context.posterImageMarkup(candidates), /UdIgTfI/);
  assert.deepEqual(candidates.map(o => o.id), ['one', 'two', 'three']);
  assert.doesNotMatch(h.context.posterImageMarkup({ imageUrl: 'http://example.test/a.jpg' }), /<img/);
});

test('catalog Story enrichment cannot replace an existing campaign poster', () => {
  const h = harness({ active: [offer('one', { imageUrl: 'https://example.test/poster.jpg', source: 'Portal' })] });
  h.context.mergeCatalogSnapshot({ offers: [], stories: [{ link: campaign('one'), imageUrl: 'https://example.test/story.jpg', storyId: '123', publishedAt: now - 1000 }] });
  assert.equal(h.context.historyData.active[0].imageUrl, 'https://example.test/poster.jpg');
  assert.equal(h.context.historyData.active[0].hasStory, true);
});

test('session tags sort all statuses chronologically, normalize padding and prefer available duplicate date', () => {
  const h = harness();
  const active = [offer('later', { title: '22/10 Teatro' }), offer('tomorrow', { title: '18/10 Teatro' }), offer('today', { title: '17/10 Teatro' })];
  const sold = [offer('duplicate', { title: '17/10 Teatro' }), offer('earlier', { title: '1/10 Teatro' }), offer('same', { title: '01/10 Teatro' })];
  const dates = clone(h.context.groupSessionDates(active, sold));
  assert.deepEqual(dates.map(d => d.label), ['1 OUT', 'HOJE', 'AMANHÃ', '22 OUT']);
  assert.equal(dates[1].soldOut, false);
  assert.equal(dates[0].soldOut, true);
  assert.deepEqual(active.map(o => o.id), ['later', 'tomorrow', 'today']);
});

test('session tags keep explicit years and São Paulo rollover, reject invalid dates', () => {
  const h = harness();
  const anchor = Date.parse('2026-12-31T23:00:00-03:00');
  const dates = clone(h.context.groupSessionDates([
    offer('next', { title: '01/01 Show', firstSeen: anchor }),
    offer('current', { title: '31/12 Show', firstSeen: anchor }),
    offer('explicit', { title: '31/12/2027 Show', firstSeen: anchor }),
    offer('invalid', { title: '31/02 Show', firstSeen: anchor }),
  ], [], anchor));
  assert.deepEqual(dates.map(d => d.key), ['2026-12-31', '2027-01-01', '2027-12-31']);
  assert.deepEqual(dates.map(d => d.label), ['HOJE', 'AMANHÃ', '31 DEZ 2027']);
  assert.equal(h.context.sessionDateInfo(offer('until', { title: 'Temporada até 13/12/2027' })).label, 'ATÉ 13 DEZ 2027');
});

test('date strips divide up to three dates equally and preserve scrolling order beyond three', () => {
  const h = harness();
  const items = [1, 2, 3, 4].map(day => offer('day-' + day, { title: (17 + day) + '/10/2026 Teatro' }));
  for (const count of [1, 2, 3]) {
    const html = h.context.dateStrip(h.context.renderSessionDatePills(items.slice(0, count), []));
    assert.doesNotMatch(html, /dates-scroll/);
    assert.equal((html.match(/class="micro-date-pill/g) || []).length, count);
  }
  const html = h.context.dateStrip(h.context.renderSessionDatePills(items, []));
  assert.match(html, /dates-scroll/);
  assert.ok(html.indexOf('AMANHÃ') < html.indexOf('19 OUT'));
  assert.match(template, /\.micro-dates-inline \.micro-date-pill \{[^}]*flex: 1 1 0/);
  assert.match(template, /\.micro-dates-inline\.dates-scroll \.micro-date-pill \{[^}]*flex: 0 0 calc\(\(100% - 10px\) \/ 3\)/);
  const hero = h.context.dateStrip(h.context.renderSessionDatePills(items, []), true);
  assert.match(hero, /dates-compact/);
  assert.doesNotMatch(hero, /dates-scroll/);
  assert.match(template, /\.micro-dates-inline\.dates-compact \.micro-date-pill \{[^}]*flex: 0 0 auto/);
  assert.match(fn('createPosterCard'), /if \(isHeroFullWidth\)[\s\S]*dateStrip\(microDatePillsHtml, true\)/);
});

test('history creates twelve groups per batch after year and month filters without deleting source records', () => {
  const records = Array.from({ length: 29 }, (_, i) => offer('archive-' + i, { title: '20/10/2026 Evento ' + i }));
  records.push(offer('other-month', { title: '20/09/2026 Setembro' }), offer('other-year', { title: '20/10/2025 Antigo' }));
  const h = harness({ history: records });
  h.context.currentTab = 'history';
  h.context.selectedHistoryMonth = 9;
  h.context.buildOfferGroups = (active, history) => [...active, ...history].map(item => ({ key: item.id, parent: item }));
  const container = element();
  const original = clone(h.context.historyData);
  h.context.renderHistoryMonths(h.context.historyData.history, 2026, container);
  const state = h.context.historyRenderState;
  assert.equal(state.rows.length, 29);
  assert.equal(state.cursor, 12);
  assert.equal(state.tracks.get(9).children.length, 12);
  assert.match(state.button.textContent, /17 restantes/);
  state.button.onclick();
  assert.equal(state.cursor, 24);
  state.button.onclick();
  assert.equal(state.cursor, 29);
  assert.equal(state.button.parentNode, null);
  assert.deepEqual(clone(h.context.historyData), original);
  h.context.selectedHistoryMonth = 8;
  h.context.renderHistoryMonths(h.context.historyData.history, 2026, container);
  assert.equal(h.context.historyRenderState.rows.length, 1);
  assert.equal(container.children.length, 1, 'filter reset replaces the visible page');
  h.context.selectedHistoryMonth = 'all';
  h.context.historySearchQuery = 'SÉTEMBRO';
  h.context.renderHistoryMonths(h.context.historyData.history, 2026, container);
  assert.equal(h.context.historyRenderState.rows.length, 1, 'accent-insensitive search runs before the page');
  assert.equal(h.context.historyRenderState.rows[0].group.parent.id, 'other-month');
  h.context.historySearchQuery = 'not found';
  h.context.renderHistoryMonths(h.context.historyData.history, 2026, container);
  assert.equal(h.context.historyRenderState, null);
  assert.match(container.innerHTML, /Nenhuma oferta encontrada/);
});

test('history starts at the current São Paulo month, keeps explicit selection, and never renders another period implicitly', () => {
  const records = Array.from({ length: 15 }, (_, i) => offer('oct-' + i, { title: '20/10/2026 Evento ' + i }));
  records.push(offer('curated-current', { title: 'Coldplay 21/10/2026' }), offer('curated-september', { title: 'Coldplay 21/09/2026' }),
    offer('last-year', { title: '20/03/2025 Teatro' }));
  const h = harness({ history: records });
  Object.assign(h.context, { selectedHistoryYear: null, selectedHistoryMonth: null, currentTab: 'history',
    CURATED_EDITORS_CHOICE_SEEDS: [], EDITORS_CHOICE_KEYWORDS: ['coldplay'], MONTH_SHORT_LABELS: Array.from({ length: 12 }, (_, i) => 'M' + i) });
  h.context.document.querySelectorAll = () => [];
  vm.runInContext(['historyCurationText', 'getAllCatalogOffers', 'getEditorsChoiceOffers', 'renderHistoryScreen', 'renderHistoryCurated', 'renderHistoryMonthSelector',
    'selectHistoryYear', 'selectHistoryMonth'].map(fn).join('\n'), h.context);
  const groupedPeriods = [];
  h.context.buildOfferGroups = (active, history) => {
    const items = [...active, ...history];
    groupedPeriods.push(items.map(o => h.context.getOfferYearAndMonth(o).month));
    return items.map(item => ({ key: item.id, parent: item }));
  };
  const source = clone(h.context.historyData);
  h.context.renderHistoryScreen();
  assert.equal(h.context.selectedHistoryYear, 2026);
  assert.equal(h.context.selectedHistoryMonth, 9);
  assert.equal(h.context.historyRenderState.cursor, 12);
  assert.equal(h.context.historyRenderState.rows.length, 16);
  assert.deepEqual(groupedPeriods.map(periods => [...new Set(periods)]), [[9]], 'month counters cannot group the entire archive');
  assert.deepEqual(h.elements.get('editorsChoiceTrack').children.map(card => card.group.id), ['curated-current']);
  h.context.selectHistoryMonth(8);
  assert.equal(h.context.historyRenderState.rows.length, 1);
  assert.equal(h.context.historyRenderState.rows[0].group.parent.id, 'curated-september');
  h.context.renderHistoryScreen();
  assert.equal(h.context.selectedHistoryMonth, 8, 'catalog refresh keeps the requested month');
  assert.deepEqual(h.elements.get('editorsChoiceTrack').children.map(card => card.group.id), ['curated-september']);
  h.context.selectHistoryYear(2025);
  assert.equal(h.context.selectedHistoryMonth, 2, 'requested old year opens its latest available month');
  assert.equal(h.context.historyRenderState.rows[0].group.parent.id, 'last-year');
  assert.deepEqual(clone(h.context.historyData), source);
});

test('empty current month stays empty and history sold-out cards remain colored without changing Home styling', () => {
  const h = harness({ history: [offer('old', { title: '20/09/2026 Coldplay', isExpired: true })] });
  Object.assign(h.context, { selectedHistoryYear: null, selectedHistoryMonth: null, currentTab: 'history',
    CURATED_EDITORS_CHOICE_SEEDS: [], EDITORS_CHOICE_KEYWORDS: ['coldplay'], MONTH_SHORT_LABELS: Array.from({ length: 12 }, (_, i) => 'M' + i) });
  vm.runInContext(['historyCurationText', 'getAllCatalogOffers', 'getEditorsChoiceOffers', 'renderHistoryScreen', 'renderHistoryCurated', 'renderHistoryMonthSelector'].map(fn).join('\n'), h.context);
  h.context.renderHistoryScreen();
  assert.equal(h.context.selectedHistoryMonth, 9);
  assert.equal(h.context.historyRenderState, null);
  assert.equal(h.elements.get('editorsChoiceTrack').children.length, 0);
  assert.equal(h.elements.get('sectionEditorsChoice').style.display, 'none');
  assert.match(h.elements.get('historyMonthsContainer').innerHTML, /Month 9 de 2026/);
  const historyStyle = template.match(/\.poster-card\.history-clean-card\.card-sold-out-grouped \{[^}]+}/)[0];
  assert.match(historyStyle, /filter: none/);
  assert.match(historyStyle, /opacity: 1/);
  assert.match(template, /\.poster-card\.card-sold-out-grouped \{[^}]*filter: grayscale\(100%\)/);
  assert.match(fn('createHistoryPosterCard'), /if \(isAllSoldOut\) cardClass \+= ' card-sold-out-grouped'/);
});

test('history curation preserves compact aliases and ID/link/image evidence without adding duplicate seeds', () => {
  const records = [
    offer('real-maroon', { title: '2 Ingressos: Maroon5', firstSeen: Date.parse('2026-10-10T12:00:00Z') }),
    offer('real-c6fest', { title: '2 Ingressos: Festival', link: campaign('c6fest-2026'), firstSeen: Date.parse('2026-09-10T12:00:00Z') }),
  ];
  const h = harness({ history: records });
  Object.assign(h.context, { selectedHistoryYear: 2026, selectedHistoryMonth: 9,
    CURATED_EDITORS_CHOICE_SEEDS: [offer('curated-maroon5', { title: 'Maroon 5' }), offer('curated-c6fest', { title: 'C6 Fest' })],
    EDITORS_CHOICE_KEYWORDS: ['zayn', 'maroon 5', 'c6 fest', 'cirque du soleil', 'paul mccartney', 'coldplay', 'bruno mars'] });
  h.context.normalizeEventInfo = () => { throw new Error('history metadata must not use full normalization'); };
  vm.runInContext(['historyCurationText', 'getAllCatalogOffers', 'getEditorsChoiceOffers', 'renderHistoryCurated'].map(fn).join('\n'), h.context);
  const all = h.context.getAllCatalogOffers();
  assert.deepEqual(clone(all).map(o => o.id), ['real-maroon', 'real-c6fest']);
  assert.deepEqual(clone(h.context.getEditorsChoiceOffers(all)).map(o => o.id), ['real-maroon', 'real-c6fest']);
  for (const field of ['id', 'link', 'imageUrl']) {
    const candidate = offer('generic', { title: 'Ingresso', [field]: 'https://example.test/c6fest.jpg' });
    assert.equal(h.context.getEditorsChoiceOffers([candidate]).length, 1, field + ' retains recognition');
  }
  h.context.renderHistoryCurated(all);
  assert.deepEqual(h.elements.get('editorsChoiceTrack').children.map(card => card.group.id), ['real-maroon']);
});

test('redemption parser accepts ISO and strict Brazilian dates in source timezone, retains day precision', () => {
  const h = harness();
  assert.equal(h.context.parseRedemptionDate('16/10/2026 às 15:45'), Date.parse('2026-10-16T15:45:00-03:00'));
  assert.equal(h.context.parseRedemptionDate('2026-10-16T18:45:00Z'), Date.parse('2026-10-16T15:45:00-03:00'));
  for (const value of ['31/02/2026', '17/10/2026 24:15', '2026-02-30T00:00:00Z', 'texto 17/10/2026', '', null]) {
    assert.equal(h.context.parseRedemptionMoment(value), null, String(value));
  }
  const moment = h.context.parseRedemptionMoment('16/10/2026');
  assert.equal(moment.precision, 'day');
  assert.equal(h.context.timelineTimeLabel({ timestamp: moment.timestamp, datePrecision: moment.precision, dateLabel: moment.label }), '16/10/2026');
});

test('unknown redemption remains at end without fabricated hour or unseen badge across launches', () => {
  const r = { id: 'voucher-one', user: 'leosaquetto', title: 'Ingresso', partner: 'Campanhas de Ingressos', redeemedAt: 'invalid' };
  const h = harness({ bundled: [r] });
  const event = h.context.getTimelineEvents()[0];
  assert.equal(event.timestamp, null);
  assert.equal(h.context.timelineTimeLabel(event), 'Data não informada');
  h.context.updateTimelineBadgeDot();
  assert.equal(h.elements.get('timelineBadgeDot').style.display, 'none');
  h.advance(86400000);
  assert.equal(h.context.getTimelineEvents()[0].timestamp, null);
  h.context.renderTimelineList();
  assert.match(h.elements.get('timelineList').innerHTML, /Data não informada/);
  assert.doesNotMatch(h.elements.get('timelineList').innerHTML, /1h atrás/);
});

test('same campaign across accounts and title-only records never borrow another redemption date', () => {
  const r = { title: 'Mesmo show', partner: 'Campanhas de Ingressos', url: 'https://clube.uol.com.br/voucher/one' };
  const h = harness({ bundled: [{ ...r, user: 'leosaquetto', redeemedAt: '16/10/2026' }],
    saved: [{ ...r, user: 'gaoriel', redeemedAt: '' }, { title: 'Mesmo show', user: 'leosaquetto' }, { title: 'Mesmo show', user: 'gaoriel' }] });
  const records = clone(h.context.getAllRedemptionsList());
  assert.equal(records.length, 4);
  assert.equal(records.find(r => r.user === 'gaoriel' && r.url).redeemedAt, '');
  assert.notEqual(h.context.redemptionIdentity({ user: 'leosaquetto', url: campaign('shared'), id: 'voucher-1' }),
    h.context.redemptionIdentity({ user: 'leosaquetto', url: campaign('shared'), id: 'voucher-2' }));
  assert.equal(h.context.redemptionIdentity({ user: 'leosaquetto', title: 'Same', url: campaign('shared') }), null);
});

test('timeline draws thumbnails for offers and redemptions, with unknown date last', () => {
  const h = harness({ active: [offer('one', { imageUrl: 'https://example.test/poster.jpg' })],
    bundled: [{ id: 'r', user: 'leosaquetto', title: 'Ingresso', partner: 'Campanhas de Ingressos', thumb: 'https://example.test/voucher.jpg' }] });
  const events = clone(h.context.getTimelineEvents());
  assert.equal(events.at(-1).type, 'resgate');
  assert.equal(events.at(-1).timestamp, null);
  h.context.renderTimelineList();
  const html = h.elements.get('timelineList').innerHTML;
  assert.equal((html.match(/class="timeline-thumbnail"/g) || []).length, 2);
  assert.match(html, /poster\.jpg/);
  assert.match(html, /voucher\.jpg/);
  assert.match(html, /onerror="this.remove\(\)"/);
});

test('common cards enlarge and separate date pills while compact highlights retain original sizing', () => {
  assert.match(template, /\.micro-dates-inline:not\(\.dates-compact\) \{ margin-top: 10px; \}/);
  assert.match(template, /\.micro-dates-inline:not\(\.dates-compact\) \.micro-date-pill \{[^}]*font-size: 10px/);
  assert.match(template, /\.micro-date-pill \{[^}]*font-size: 8px/);
  assert.match(template, /\.micro-dates-inline\.dates-compact \.micro-date-pill \{ flex: 0 0 auto/);
});

test('timeline filters fit a two-column grid with touch targets and full readable titles', () => {
  const filters = template.match(/\.timeline-filter-bar \.redemptions-filter-pills \{[^}]+}/)[0];
  assert.match(filters, /display: grid/);
  assert.match(filters, /grid-template-columns: repeat\(2, minmax\(0, 1fr\)\)/);
  assert.match(filters, /overflow: visible/);
  const button = template.match(/\.timeline-filter-bar \.redemptions-filter-pill \{[^}]+}/)[0];
  assert.match(button, /min-height: 44px/);
  const title = template.match(/\.timeline-item-title \{[^}]+}/)[0];
  assert.match(title, /font-size: 14px/);
  assert.doesNotMatch(title, /ellipsis|line-clamp|overflow: hidden/);
  const h = harness();
  h.context.getTimelineEvents = () => [{ type: 'nova', title: 'Teatro <teste> - SP', badgeText: 'Nova', desc: '', timestamp: now - 1000 }];
  h.context.renderTimelineList();
  const html = h.elements.get('timelineList').innerHTML;
  assert.match(html, /Teatro &lt;teste&gt;\u00a0-\u00a0SP/);
  assert.equal(h.elements.get('tlFilterAll')['aria-pressed'], 'true');
  assert.equal(h.elements.get('tlFilterExpired')['aria-pressed'], 'false');
});

test('native collector owns source requests on catalog failure, and receipts do not imply delivery', async () => {
  const h = harness({ native: true, active: [offer('cached')] });
  await h.context.streamLiveOffers();
  assert.deepEqual(h.requests, ['https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/catalog']);
  assert.equal(h.context.historyData.active.length, 1);
  h.context.setHunterCollectionStatus({ received: 2, delivered: 0, pending: 1 });
  const text = h.elements.get('hunterCollectionStatus').textContent;
  assert.match(text, /2 recebidas pelo servidor/);
  assert.match(text, /1 pendentes no iPhone/);
  assert.doesNotMatch(text, /entregues/);
});

test('iPhone status distinguishes update, configuration and suspended sources from empty collection', () => {
  const h = harness({ native: true });
  const statusText = summary => {
    h.context.setHunterCollectionStatus(summary);
    return h.elements.get('hunterCollectionStatus').textContent;
  };
  assert.match(statusText({ status: 'starting' }), /buscando novidades/);
  assert.match(statusText({ status: 'updated' }), /atualizado; abra novamente para coletar/);
  assert.match(statusText({ status: 'not_configured' }), /configuração privada pendente/);
  assert.match(statusText({ status: 'not_configured', reason: 'bootstrap_missing' }), /aguardando arquivo privado no iCloud/);
  assert.match(statusText({ status: 'not_configured', reason: 'bootstrap_invalid' }), /arquivo privado inválido/);
  assert.match(statusText({ status: 'not_configured', reason: 'device_mismatch' }), /outro aparelho/);
  assert.match(statusText({ status: 'unavailable' }), /coleta indisponível/);
  const suspended = statusText({ status: 'complete', sources: {
    portal: { status: 'empty', found: 0 }, instagram: { status: 'auth_required', reason: 'authentication_suspended', found: 0 }
  } });
  assert.match(suspended, /Portal: sem novidades/);
  assert.match(suspended, /Instagram: autenticação necessária; coleta suspensa/);
  assert.doesNotMatch(suspended, /nenhuma novidade pendente/);
  const limited = statusText({ status: 'complete', sources: {
    portal: { status: 'cooldown' }, instagram: { status: 'backoff' }
  } });
  assert.match(limited, /Portal: aguardando intervalo mínimo/);
  assert.match(limited, /Instagram: aguardando intervalo de segurança/);
});

test('poster CSS preserves full vertical source and date strip is outside truncated title with isolated click', () => {
  const imageRule = template.match(/\.poster-img, \.thumb-wrap img\.poster-img[^}]+}/)[0];
  assert.match(imageRule, /height: 100%/);
  assert.match(imageRule, /width: auto/);
  assert.match(imageRule, /transform: translateX\(-50%\)/);
  const strip = template.match(/\.micro-dates-inline \{[^}]+}/)[0];
  assert.match(strip, /overflow-x: auto/);
  assert.match(strip, /overscroll-behavior-x: contain/);
  assert.match(template, /<div class="poster-title">\$\{norm\.eventTitle\}<\/div>\s*\$\{dateStrip/);
  const h = harness();
  assert.match(h.context.dateStrip('dates'), /onclick="event\.stopPropagation\(\)"/);
});


test('OTA opening never falls back to Portal collection when the catalog is offline', async () => {
  const h = harness({ paused: true });
  await h.context.streamLiveOffers();
  assert.deepEqual(h.requests, ['https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/catalog']);
  assert.match(template, /if \(!INITIAL.collectionPausedForUpdate\) fetchRealRedemptions\(\);/);
});

test('redemption associations require known account and exact campaign, never title keywords or synthetic IDs', () => {
  const h = harness({ bundled: [
    { user: 'leosaquetto', title: 'Zayn', campaignUrl: campaign('one'), id: 'voucher-a' },
    { user: 'gaoriel', title: 'Zayn', campaignUrl: campaign('two'), id: 'voucher-b' },
    { title: 'Zayn', campaignUrl: campaign('one'), id: 'voucher-c' },
    { user: 'gaoriel', title: 'Zayn', id: 'voucher-d' },
  ] });
  assert.deepEqual(clone(h.context.getRedeemedUsersForOffer(offer('one', { title: 'Zayn' }))), ['leosaquetto']);
  assert.deepEqual(clone(h.context.getRedeemedUsersForOffer(offer('missing', { title: 'Zayn' }))), []);
  assert.equal(h.context.redemptionIdentity({ user: 'leosaquetto', id: 'redemption-leosaquetto-1' }), null);
  assert.equal(h.context.redemptionIdentity({ id: 'voucher-a', url: 'https://clube.uol.com.br/voucher/a' }), null);
});


test('hydration preserves stored vouchers and identical unidentified copies do not create extra timeline entries', () => {
  const known = { user: 'leosaquetto', id: 'voucher-1', title: 'Known' };
  const unknown = { user: 'gaoriel', title: 'Unknown', partner: 'Campanhas de Ingressos' };
  const h = harness({ bundled: [known, unknown], settings: { realRedemptions: [unknown, { user: 'gaoriel', id: 'voucher-2', title: 'Stored' }] }, saved: [unknown] });
  assert.equal(h.context.getAllRedemptionsList().length, 3);
  assert.equal(h.context.getTimelineEvents().length, 1);
  assert.match(template, /settings.realRedemptions = getAllRedemptionsList\(\);/);
});

test('complete embedded script boots every saved tab with an offline catalog', async () => {
  const scriptStart = template.indexOf('<script>') + '<script>'.length;
  const scriptEnd = template.lastIndexOf('</script>');
  assert.ok(scriptStart > '<script>'.length && scriptEnd > scriptStart);
  const completeScript = template.slice(scriptStart, scriptEnd);
  assert.doesNotMatch(completeScript, /<\/script\s*>/i, 'embedded content cannot end the HTML script early');
  assert.doesNotMatch(template.slice(0, template.indexOf('<body')), /<link[^>]*fonts\.googleapis\.com[^>]*rel="stylesheet"/i,
    'external fonts cannot block the initial document');

  for (const tab of ['home', 'tickets', 'history', 'redemptions']) {
    const handlers = new Map();
    const elements = new Map();
    const timers = [];
    const createElement = () => ({
      ...element(), dataset: {}, classList: { add() {}, remove() {}, toggle() {} },
      querySelector: () => createElement(), querySelectorAll: () => [],
    });
    const initial = {
      settings: { currentTab: tab }, icons: {}, credentials: { accounts: {} },
      historyData: { active: [offer('cached', { title: '22/10 Teatro teste' })], history: [] },
      realRedemptions: [], hunterCollection: { status: 'starting' }, collectionPausedForUpdate: true,
    };
    const head = createElement();
    const context = vm.createContext({
      URL, Date, Intl, console,
      window: { scrollY: 0, scrollTo() {}, addEventListener(name, callback) {
        if (!handlers.has(name)) handlers.set(name, []);
        handlers.get(name).push(callback);
      } },
      document: {
        head,
        getElementById(id) {
          if (!elements.has(id)) elements.set(id, createElement());
          return elements.get(id);
        },
        querySelector: createElement, querySelectorAll: () => [], createElement,
      },
      localStorage: { getItem: () => null, setItem() {} },
      fetch: async () => { throw new Error('offline fixture'); },
      setTimeout(callback, delay) { timers.push({ callback, delay }); }, clearTimeout() {}, setInterval() {}, clearInterval() {},
    });
    const script = new vm.Script(completeScript.replace('__INITIAL_DATA_JSON__', JSON.stringify(initial)), { filename: 'hunter-inline.js' });
    script.runInContext(context);
    assert.equal(handlers.get('load').length, 1);
    for (const callback of handlers.get('load')) callback();
    await new Promise(resolve => setImmediate(resolve));
    const view = tab === 'history' ? 'viewHistory' : tab === 'redemptions' ? 'viewRedemptions' : 'viewMain';
    assert.equal(elements.get(view).style.display, 'block', tab + ' must show its saved screen');
    assert.equal(elements.get('skeletonList').style.display, 'none', tab + ' must finish the offline boot');
    assert.equal(elements.get('liveStatusText')['aria-busy'], 'false');
    assert.equal(head.children.length, 0, 'interface boots without waiting for a font request');
    timers.filter(timer => timer.delay === 0).forEach(timer => timer.callback());
    assert.equal(head.children.length, 1);
    assert.equal(head.children[0].rel, 'stylesheet');
    assert.match(head.children[0].href, /^https:\/\/fonts\.googleapis\.com\/css2\?family=Manrope:/);
  }
});
