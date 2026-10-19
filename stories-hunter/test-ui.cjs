const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Execute only source functions against synthetic data. No personal build, network, or cookies.
const template = require('./assemble-ui.cjs').assembleUI();
const now = Date.parse('2026-10-17T22:00:00-03:00');
const clone = value => JSON.parse(JSON.stringify(value));
const campaign = slug => 'https://clube.uol.com.br/campanhasdeingresso/' + slug;
const offer = (id, overrides = {}) => {
  const item = { id, link: campaign(id), title: id, category: 'TEATRO', firstSeen: now - 100000, ...overrides };
  if (item.captureSource === undefined) item.captureSource = 'native';
  if (item.capturedAt === undefined && item.captureSource === 'native') item.capturedAt = item.firstSeen;
  return item;
};
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
  const listeners = new Map();
  return { style: { setProperty() {} }, classList: { toggle() {} }, children: [], textContent: '',
    get innerHTML() { return markup; }, set innerHTML(value) { markup = value; this.children.forEach(node => { node.parentNode = null; }); this.children = []; },
    appendChild(node) { this.children.push(node); node.parentNode = this; },
    insertBefore(node, before) { const index = before ? this.children.indexOf(before) : -1; if (index < 0) this.children.push(node); else this.children.splice(index, 0, node); node.parentNode = this; },
    remove() { if (this.parentNode) { this.parentNode.children = this.parentNode.children.filter(node => node !== this); this.parentNode = null; } },
    addEventListener(type, listener) { if (!listeners.has(type)) listeners.set(type, []); listeners.get(type).push(listener); },
    click() { this.onclick?.(); for (const listener of listeners.get('click') || []) listener({ target: this }); },
    setAttribute(key, value) { this[key] = value; }, getAttribute(key) { return this[key] ?? null; } };
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
      'formatRedemptionTitle', 'escapeReceiptHtml', 'isVerifiedReceipt', 'getTimelineEvents', 'getRedeemedUsersForOffer', 'updateTimelineBadgeDot', 'renderTimelineList', 'renderOffers', 'renderIndividualOffers', 'updateCategoryCounts',
      'getOfferYearAndMonth', 'buildHistoryGroups', 'historyMatchesSearch', 'renderHistoryMonths', 'appendHistoryBatch'].map(fn),
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

test('native history defeats stale local active and local-only cache stays unverified', () => {
  const h = harness();
  vm.runInContext(fn('historyIdentity') + '\n' + fn('reconcileHistorySnapshots'), h.context);
  const sold = offer('same', { availability: 'sold_out', soldOutAt: now - 1000, lastSeen: now - 1000 });
  const stale = offer('same', { availability: 'observed', lastSeen: now - 2000 });
  const nativeOnly = offer('native-live', { lastSeen: now - 1000 });
  const localOnly = offer('local-old', { captureSource: 'legacy', capturedAt: 0, firstSeen: 0 });
  const result = clone(h.context.reconcileHistorySnapshots(
    { active: [nativeOnly], history: [sold] }, { active: [stale, localOnly], history: [] }));
  assert.deepEqual(result.active.map(item => item.id), ['native-live']);
  assert.deepEqual(result.history.map(item => item.id).sort(), ['local-old', 'same']);
  assert.equal(result.history.find(item => item.id === 'same').availability, 'sold_out');
  assert.equal(result.history.find(item => item.id === 'local-old').cacheUnverified, true);
  assert.equal(result.history.find(item => item.id === 'local-old').capturedAt, 0);
});

test('native collection restores a current session despite a newer local-only cache timestamp', () => {
  const stale = offer('same', { cacheUnverified: true, availability: 'sold_out',
    soldOutAt: now - 1000, expiredAt: now - 1000 });
  const current = offer('same', { availability: 'observed', lastSeen: now - 2000 });
  const h = harness({ history: [stale] });
  h.context.updateTimelineBadgeDot = () => {};
  h.context.renderCatalogStatus(true);
  h.context.handleNativeCollectionResult(null, { active: [current], history: [] });
  assert.deepEqual(clone(h.context.historyData.active).map(item => item.id), ['same']);
  assert.equal(h.context.historyData.active[0].cacheUnverified, undefined);
  assert.equal(h.context.historyData.history.length, 0);
  assert.equal(h.elements.get('liveStatusText').textContent, 'Atualizado',
    'a native rerender does not relabel the already synchronized catalog as cache');
});

test('legacy private WebView cache is scrubbed only after native archival acknowledgement', () => {
  const values = new Map([
    ['uol_hub_settings', JSON.stringify({ currentUser: 'gaoriel', userSessions: { gaoriel: { cookie: 'private-cookie' } },
      realRedemptions: [{ id: 'receipt-a', user: 'gaoriel', title: 'Guardado', voucherImageDataUrl: 'data:image/png;base64,AAAA' }] })],
    ['uol_real_redemptions', JSON.stringify([{ id: 'receipt-b', user: 'gaoriel', title: 'Outro', pdfBase64: 'private-pdf' }])],
    ['uol_vc_OLD', JSON.stringify({ imgDataUrl: 'data:image/png;base64,AAAA' })],
  ]);
  const storage = { get length() { return values.size; }, key: index => Array.from(values.keys())[index] || null,
    getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value),
    removeItem: key => values.delete(key) };
  const context = vm.createContext({ INITIAL: { nativeAccounts: true }, localStorage: storage,
    settings: { currentUser: 'leosaquetto', groupedView: true, currentTab: 'home' }, currentTab: 'home' });
  vm.runInContext('const legacyPrivateStorageSnapshot = new Map();\n' +
    ['saveSettingsToStorage', 'isPublicSettingsPayload', 'hunterLegacyPrivateCacheManifest',
      'hunterLegacyPrivateCacheChunk', 'saveSafeLegacyRedemptions', 'hunterLegacyPrivateCacheCommitted'].map(fn).join('\n'), context);
  const manifest = clone(context.hunterLegacyPrivateCacheManifest());
  assert.equal(manifest.length, 3);
  assert.equal(context.hunterLegacyPrivateCacheChunk('uol_vc_OLD', 0, 32768).data, values.get('uol_vc_OLD'));
  assert.match(values.get('uol_hub_settings'), /private-cookie/, 'no delete before native readback and ack');
  assert.equal(context.saveSettingsToStorage(), false, 'public save waits for durable native archive');
  assert.match(values.get('uol_hub_settings'), /private-cookie/);
  context.hunterLegacyPrivateCacheCommitted(manifest.map(item => item.key));
  assert.doesNotMatch(JSON.stringify(Array.from(values.values())), /private-cookie|private-pdf|data:image\/png/);
  assert.equal(values.has('uol_vc_OLD'), false);
  assert.equal(values.has('uol_real_redemptions'), false);
  assert.equal(JSON.parse(values.get('uol_hub_settings')).schemaVersion, 2);
  assert.deepEqual(JSON.parse(values.get('uol_real_redemptions_v2')).map(item => item.id), ['receipt-a', 'receipt-b']);
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
  assert.deepEqual(clone(result.soldOutItems).map(o => o.id), [], 'sold out for 24 hours is no longer shown');
  assert.deepEqual(clone(h.context.historyData), before);
});

test('Home does not present local-only legacy sessions as available or recently sold out', () => {
  const active = [offer('confirmed'), offer('cached-active', { cacheUnverified: true })];
  const history = [offer('confirmed-out', { soldOutAt: now - 1000 }),
    offer('cached-out', { cacheUnverified: true, soldOutAt: now - 1000 })];
  const h = harness({ active, history });
  const sets = h.context.homeOfferSets(h.context.historyData.active, h.context.historyData.history);
  assert.deepEqual(clone(sets.activeItems).map(item => item.id), ['confirmed']);
  assert.deepEqual(clone(sets.soldOutItems).map(item => item.id), ['confirmed-out']);
  assert.equal(h.context.historyData.history.find(item => item.id === 'cached-out').cacheUnverified, true,
    'the uncertain record remains available in History for later reconciliation');
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
  assert.equal(h.context.sessionDateInfo(offer('short-year', { title: '18/10/27 Teatro', firstSeen: Date.parse('2026-10-19T12:00:00-03:00') })).key, '2027-10-18');
});

test('date strips reveal part of the next session and preserve chronological order', () => {
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
  assert.match(template, /\.micro-dates-inline\.dates-scroll \.micro-date-pill \{[^}]*flex: 0 0 29%/);
  const hero = h.context.dateStrip(h.context.renderSessionDatePills(items, []), true);
  assert.match(hero, /dates-compact/);
  assert.match(hero, /dates-scroll/);
  assert.match(template, /\.micro-dates-inline\.dates-compact \.micro-date-pill \{[^}]*flex: 0 0 auto/);
  assert.match(fn('createPosterCard'), /if \(isHeroFullWidth\)[\s\S]*dateStrip\(microDatePillsHtml, true\)/);
});

test('Markdown offer prose yields the event name while legal copy and regex punctuation stay harmless', () => {
  const h = harness();
  h.context.formatVenueString = value => value;
  vm.runInContext(['formatProperTitle', 'extractSmartTitleFromBody', 'normalizeEventInfo'].map(fn).join('\n'), h.context);
  const result = h.context.normalizeEventInfo([{ title: '2 INGRESSOS: 23/10 Tokio Marine Hall SP',
    description: '**Assinante UOL,** Resgate **1 par de ingressos** para curtir o show **Inimigos do Fim,** no Tokio Marine Hall.\nREGRAS DE RESGATE\n1 par de ingressos para qualquer evento no mês vigente.' }]);
  assert.equal(result.eventTitle, 'Inimigos do Fim');
  assert.equal(result.venue, 'Tokio Marine Hall SP');
  assert.doesNotThrow(() => h.context.normalizeEventInfo([{ title: '2 INGRESSOS: Show ( Teatro Claro SP',
    description: 'Resgate 1 par de ingressos para o show Show (.' }]));
});

test('catalog keeps capture, source publication, event date and observation separate', () => {
  const h = harness();
  const capturedAt = '2026-10-17T12:00:00Z';
  h.context.mergeCatalogSnapshot({ offers: [{ id: 'worker-one', title: '2 INGRESSOS: 23/10 Teatro Claro SP',
    link: campaign('one'), firstSeenAt: capturedAt, lastSeenAt: '2026-10-17T14:00:00Z', availability: 'observed', isPortal: true }],
  stories: [{ storyId: 'story-one', link: campaign('one'), publishedAt: '2026-10-16T10:00:00Z',
    firstSeenAt: '2026-10-17T13:00:00Z', expiresAt: '2026-10-18T10:00:00Z' },
    { storyId: 'story-two', link: campaign('unknown'), publishedAt: '2026-10-16T10:00:00Z',
      expiresAt: '2026-10-18T10:00:00Z' }] });
  const one = h.context.historyData.active.find(item => item.link === campaign('one'));
  const unknown = h.context.historyData.active.find(item => item.link === campaign('unknown'));
  assert.equal(one.capturedAt, Date.parse(capturedAt));
  assert.equal(one.captureSource, 'worker');
  assert.equal(one.sourcePublishedAt, Date.parse('2026-10-16T10:00:00Z'));
  assert.equal(one.lastSourceSeenAt, Date.parse('2026-10-17T14:00:00Z'));
  assert.equal(one.eventDate, '2026-10-23');
  assert.equal(unknown.capturedAt, 0);
  assert.equal(unknown.captureSource, 'unknown');
  assert.equal(h.context.getOfferYearAndMonth(unknown).year, 'unknown');
});

test('catalog merge bounds external fields even when called by a local fallback', () => {
  const h = harness();
  h.context.mergeCatalogSnapshot({ offers: [{ id: 'worker-long', link: campaign('long'), title: 'A'.repeat(1500),
    description: 'B'.repeat(25000), partner: 'C'.repeat(800), validity: 'D'.repeat(800),
    availability: 'observed', isPortal: true }], stories: [] });
  const [item] = h.context.historyData.active;
  assert.equal(item.rawTitle.length, 1000);
  assert.equal(item.description.length, 20000);
  assert.equal(item.partner.length, 500);
  assert.equal(item.validity.length, 500);
  assert.equal(h.context.catalogCampaign(campaign('a') + '?x=' + 'x'.repeat(2048)), null);
  assert.equal(h.context.validOfferImage('https://example.test/' + 'x'.repeat(2048)), '');
});

test('legacy firstSeen is raw evidence, never capture chronology or link generation', () => {
  const legacy = offer('legacy', { firstSeen: now - 1000, capturedAt: 0, captureSource: 'legacy' });
  const native = offer('native', { capturedAt: now - 2000, firstSeen: now - 2000, captureSource: 'native' });
  const h = harness({ active: [legacy, native] });
  assert.equal(h.context.captureTime(h.context.historyData.active[0]), 0);
  assert.deepEqual(clone(h.context.getOfferYearAndMonth(legacy)), { year: 'unknown', month: -1 });
  assert.equal(h.context.linkCheckGeneration(legacy), '0');
  assert.equal(h.context.captureTime(native), now - 2000);
  h.context.mergeCatalogSnapshot({ offers: [{ id: 'worker-legacy', link: campaign('legacy'), title: 'Legacy',
    firstSeenAt: new Date(now - 3000).toISOString(), availability: 'observed', isPortal: true }], stories: [] });
  const migrated = h.context.historyData.active.find(item => item.id === 'legacy');
  assert.equal(migrated.captureSource, 'worker');
  assert.equal(migrated.capturedAt, now - 3000);
  assert.equal(migrated.legacyFirstSeen, now - 1000);
  assert.equal(h.context.linkCheckGeneration(migrated), String(now - 3000));
});

test('Home link overlay is per session and unknown checks do not erase confirmed evidence', () => {
  const active = [offer('one', { title: '23/10 Teatro Claro SP' }), offer('two', { title: '24/10 Teatro Claro SP' })];
  const h = harness({ active, history: [offer('older-one', { link: campaign('one'), title: '23/10 Teatro Claro SP', soldOutAt: now - 1000 })] });
  h.context.renderCatalogStatus = () => {};
  const source = clone(h.context.historyData);
  const candidates = clone(h.context.hunterActiveLinkCandidates());
  assert.equal(candidates.length, 2);
  const result = { ...candidates[0], status: 'unavailable', checkedAt: now, unavailableAt: now };
  h.context.hunterApplyLinkCheckResults([result]);
  let sets = h.context.homeOfferSets(h.context.historyData.active, h.context.historyData.history);
  assert.deepEqual(clone(sets.activeItems).map(item => item.id), ['two']);
  assert.deepEqual(clone(sets.unavailableItems).map(item => item.id), ['one']);
  assert.equal(sets.soldOutItems.length, 0, 'an older sold-out copy cannot duplicate an unavailable active campaign');
  assert.equal(h.context.hunterActiveLinkCandidates().length, 2, 'unavailable links remain candidates for recovery');
  h.context.hunterApplyLinkCheckResults([{ ...result, status: 'unknown' }]);
  assert.equal(h.context.homeOfferSets(h.context.historyData.active, []).unavailableItems.length, 1);
  h.context.hunterApplyLinkCheckResults([{ ...result, status: 'available' }]);
  sets = h.context.homeOfferSets(h.context.historyData.active, []);
  assert.equal(sets.activeItems.length, 2);
  assert.deepEqual(clone(h.context.historyData), source, 'overlay never persists into offer history');
});

test('fresh availability clears a persisted dead-link marker across native and catalog refreshes', () => {
  const dead = offer('dead', { title: '24/10 Teatro Claro SP', linkUnavailableAt: now - 10000 });
  const h = harness({ history: [dead] });
  h.context.renderCatalogStatus = () => {};
  h.context.handleNativeCollectionResult(null, { active: [offer('dead', {
    title: '24/10 Teatro Claro SP', availabilityEvidence: {
      state: 'available', observedAt: now - 1000, source: 'iphone' } })], history: [] });
  assert.equal(h.context.historyData.active[0].linkUnavailableAt, undefined);
  assert.deepEqual(clone(h.context.homeOfferSets(h.context.historyData.active, []).activeItems).map(item => item.id), ['dead']);

  const catalog = harness({ history: [dead] });
  catalog.context.mergeCatalogSnapshot({ stories: [], offers: [{ id: 'dead', link: campaign('dead'),
    title: '24/10 Teatro Claro SP', availabilityEvidence: {
      state: 'available', observedAt: now - 1000, source: 'ticket_listing' } }] });
  assert.equal(catalog.context.historyData.active[0].linkUnavailableAt, undefined);
});

test('a future unverified ticket in History is checked and returns to active Home only with positive evidence', () => {
  const cached = offer('cached-ticket', { title: '24/10 Teatro Claro SP', cacheUnverified: true,
    availability: 'sold_out', isExpired: true, soldOutAt: now - 60000, expiredAt: now - 60000 });
  const h = harness({ history: [cached] });
  h.context.renderCatalogStatus = () => {};
  assert.equal(h.context.homeOfferSets([], h.context.historyData.history).soldOutItems.length, 0);
  const [candidate] = clone(h.context.hunterActiveLinkCandidates());
  assert.equal(candidate.id, cached.id, 'unverified future History ticket enters the native check queue');

  h.context.hunterApplyLinkCheckResults([{ ...candidate, status: 'available', checkedAt: now - 10000, cached: true }]);
  assert.equal(h.context.historyData.active.length, 0, 'old native cache is not fresh availability evidence');
  assert.equal(h.context.historyData.history[0].cacheUnverified, true);

  h.context.hunterApplyLinkCheckResults([{ ...candidate, status: 'unknown', checkedAt: now }]);
  assert.equal(h.context.historyData.history[0].cacheUnverified, true);
  assert.equal(h.context.historyData.active.length, 0, 'ambiguous pages never activate cached tickets');

  h.context.hunterApplyLinkCheckResults([{ ...candidate, status: 'available', checkedAt: now }]);
  assert.deepEqual(clone(h.context.historyData.active).map(item => item.id), ['cached-ticket']);
  assert.equal(h.context.historyData.history.length, 0);
  assert.equal(h.context.historyData.active[0].cacheUnverified, undefined);
  assert.equal(h.context.historyData.active[0].availabilityEvidence.state, 'available');
  assert.deepEqual(clone(h.context.homeOfferSets(h.context.historyData.active, []).activeItems).map(item => item.id),
    ['cached-ticket']);
});

test('a sold-out native result moves only its session to History, while confirmed dead History links can recover', () => {
  const active = [offer('first-date', { title: '24/10 Teatro Claro SP' }),
    offer('second-date', { title: '25/10 Teatro Claro SP' })];
  const cached = offer('old-date', { title: '26/10 Teatro Claro SP', cacheUnverified: true });
  const h = harness({ active, history: [cached] });
  h.context.renderCatalogStatus = () => {};
  const candidates = clone(h.context.hunterActiveLinkCandidates());
  assert.deepEqual(candidates.map(item => item.id), ['old-date', 'first-date', 'second-date'],
    'uncertain History offers are checked before already active ones');

  h.context.hunterApplyLinkCheckResults([{ ...candidates[1], status: 'sold_out', checkedAt: now }]);
  assert.deepEqual(clone(h.context.historyData.active).map(item => item.id), ['second-date']);
  assert.deepEqual(clone(h.context.historyData.history).map(item => item.id), ['old-date', 'first-date']);
  assert.equal(h.context.historyData.history[1].availabilityEvidence.state, 'sold_out');
  assert.equal(h.context.historyData.history[1].soldOutAt, now);
  assert.equal(h.context.historyData.history[1].linkUnavailableAt, undefined,
    'a sold-out campaign is not classified as a dead link');

  h.context.hunterApplyLinkCheckResults([{ ...candidates[0], status: 'unavailable', checkedAt: now,
    unavailableAt: now }]);
  assert.equal(h.context.historyData.history[0].cacheUnverified, undefined);
  assert.equal(h.context.historyData.history[0].linkUnavailableAt, now);
  assert.equal(h.context.hunterActiveLinkCandidates().some(item => item.id === 'old-date'), true,
    'a dead link stays in the queue for recovery');
  h.context.hunterApplyLinkCheckResults([{ ...candidates[0], status: 'unknown', checkedAt: now }]);
  assert.equal(h.context.historyData.history[0].linkUnavailableAt, now);
  h.advance(5000);
  h.context.hunterApplyLinkCheckResults([{ ...candidates[0], status: 'available', checkedAt: now + 5000 }]);
  assert.deepEqual(clone(h.context.historyData.active).map(item => item.id), ['second-date', 'old-date']);
  assert.equal(h.context.historyData.history[0].id, 'first-date');
  assert.equal(h.context.historyData.active[1].linkUnavailableAt, undefined);
  assert.equal(h.context.homeOfferSets(h.context.historyData.active, h.context.historyData.history).activeItems.length, 2);
});

test('Worker availability events attach to the local offer identity used by History', async () => {
  const h = harness({ active: [offer('local-one', { catalogOfferId: 'worker-one' })] });
  h.context.INITIAL.nativeBrowser = true;
  h.context.window = {};
  h.context.fetch = async () => ({ ok: true, text: async () => JSON.stringify({ ok: true, schemaVersion: 1,
    events: [{ offerId: 'worker-one', kind: 'restocked', occurredAt: now - 1000 }], nextCursor: null }) });
  await h.context.refreshAvailabilityEvents();
  assert.deepEqual(clone(h.context.window.hunterAvailabilityEvents), [{ offerId: 'local-one',
    eventType: 'restocked', occurredAt: now - 1000, title: 'local-one', imageUrl: '' }]);
});

test('Ingressos pins highlights, sorts by capture and keeps raw external titles as text', () => {
  const h = harness();
  h.context.checkHighlight = (_title, id) => id === 'highlight';
  const cards = [
    offer('old', { rawTitle: 'Título antigo', firstSeen: now - 100000 }),
    offer('unknown', { rawTitle: '<img src=x onerror=alert(1)>', firstSeen: 0 }),
    offer('new', { rawTitle: 'Título novo', firstSeen: now - 1000 }),
    offer('highlight', { rawTitle: 'Destaque', firstSeen: now - 200000 }),
  ];
  const container = element();
  h.context.renderIndividualOffers(cards, container);
  const titles = container.children.map(card => (card.innerHTML.match(/<div class="card-title">([^<]*)<\/div>/) || [])[1]);
  assert.deepEqual(titles, ['Destaque', 'Título novo', 'Título antigo', '&lt;img src=x onerror=alert(1)&gt;']);
  assert.doesNotMatch(container.children.map(card => card.innerHTML).join(''), /onclick=/);
  assert.match(container.children.at(-1).innerHTML, /Data não confirmada/);
  assert.doesNotMatch(container.children.at(-1).innerHTML, /tag-new/);
});

test('Ingressos renders active offers in small batches before sold-out and unavailable sections', () => {
  const sold = offer('sold', { rawTitle: 'Esgotado', availability: 'sold_out', soldOutAt: now - 1000, expiredAt: now - 1000 });
  const h = harness({ history: [sold] });
  h.context.currentTab = 'tickets';
  h.context.checkHighlight = (_title, id) => id === 'highlight';
  const items = Array.from({ length: 26 }, (_, i) => offer('ticket-' + i, {
    firstSeen: now - i * 1000, rawTitle: 'Título original ' + i,
  }));
  items.push(offer('highlight', { firstSeen: now - 100000, rawTitle: 'Destaque original' }));
  const dead = offer('dead', { rawTitle: 'Indisponível' });
  const container = element();
  const activeCards = () => container.children.filter(node => node.className?.split(' ').includes('card') && !node.className.includes('card-expired'));
  const more = () => container.children.find(node => node.className?.includes('tickets-load-more'));
  h.context.renderIndividualOffers(items, container, [dead]);
  assert.equal(activeCards().length, 12, 'initial render is bounded');
  assert.match(activeCards()[0].innerHTML, /Destaque original/);
  assert.match(activeCards()[1].innerHTML, /Título original 0/);
  assert.ok(more(), 'the remaining offers can be requested');
  const moreIndex = container.children.indexOf(more());
  assert.ok(container.children.findIndex(node => node.innerHTML.includes('Esgotado')) > moreIndex);
  assert.ok(container.children.findIndex(node => node.innerHTML.includes('Indisponível')) > moreIndex,
    'terminal sections remain after the active load-more control');
  more().click();
  assert.equal(activeCards().length, 24);
  more().click();
  assert.equal(activeCards().length, 27);
  assert.equal(more(), undefined);
  assert.match(activeCards().at(-1).innerHTML, /Título original 25/);
  const lastActiveIndex = container.children.indexOf(activeCards().at(-1));
  assert.ok(container.children.findIndex(node => node.innerHTML.includes('Esgotado')) > lastActiveIndex);
  assert.ok(container.children.findIndex(node => node.innerHTML.includes('Indisponível')) > lastActiveIndex);
});

test('Ingressos cancels a stale load-more button after rerender or tab change', () => {
  const h = harness({ active: Array.from({ length: 16 }, (_, i) => offer('old-' + i, {
    firstSeen: now - i * 1000, rawTitle: 'Old ' + i,
  })) });
  h.context.currentTab = 'tickets';
  h.context.isGrouped = false;
  const cards = () => h.elements.get('cardList').children.filter(node => node.className?.split(' ').includes('card'));
  const more = () => h.elements.get('cardList').children.find(node => node.className?.includes('tickets-load-more'));
  h.context.renderOffers(h.context.historyData.active);
  const stale = more();
  assert.ok(stale);
  h.context.historyData.active = [offer('new', { rawTitle: 'New live offer' })];
  h.context.renderOffers(h.context.historyData.active);
  stale.click();
  assert.equal(cards().length, 1, 'previous batch cannot append into the new render');
  assert.match(cards()[0].innerHTML, /New live offer/);
  h.context.historyData.active = Array.from({ length: 16 }, (_, i) => offer('again-' + i));
  h.context.renderOffers(h.context.historyData.active);
  const oldTabButton = more();
  assert.ok(oldTabButton);
  h.context.currentTab = 'history';
  oldTabButton.click();
  assert.equal(cards().length, 12, 'a pending Tickets batch cannot run in another tab');
});

test('history creates twelve groups per batch after year and month filters without deleting source records', () => {
  const records = Array.from({ length: 29 }, (_, i) => offer('archive-' + i, { title: '20/10/2026 Evento ' + i }));
  records.push(offer('other-month', { title: '20/09/2026 Setembro', firstSeen: Date.parse('2026-09-17T12:00:00Z') }),
    offer('other-year', { title: '20/10/2025 Antigo', firstSeen: Date.parse('2025-10-17T12:00:00Z') }));
  const h = harness({ history: records });
  h.context.currentTab = 'history';
  h.context.selectedHistoryMonth = 9;
  const container = element();
  const original = clone(h.context.historyData);
  const groups = h.context.buildHistoryGroups(h.context.historyData.history);
  h.context.renderHistoryMonths(groups, 2026, container);
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
  h.context.renderHistoryMonths(groups, 2026, container);
  assert.equal(h.context.historyRenderState.rows.length, 1);
  assert.equal(container.children.length, 1, 'filter reset replaces the visible page');
  h.context.selectedHistoryMonth = 'all';
  h.context.historySearchQuery = 'SÉTEMBRO';
  h.context.renderHistoryMonths(groups, 2026, container);
  assert.equal(h.context.historyRenderState.rows.length, 1, 'accent-insensitive search runs before the page');
  assert.equal(h.context.historyRenderState.rows[0].group.parent.id, 'other-month');
  h.context.historySearchQuery = 'not found';
  h.context.renderHistoryMonths(groups, 2026, container);
  assert.equal(h.context.historyRenderState, null);
  assert.match(container.innerHTML, /Nenhuma oferta encontrada/);
});

test('History assigns a multi-session group to its latest system capture month, not event month', () => {
  const poster = 'https://example.test/tokio-poster-1234567890.png';
  const h = harness({ history: [
    offer('tokio-sep', { title: '2 INGRESSOS: 23/10 Tokio Marine Hall SP', imageUrl: poster,
      firstSeen: Date.parse('2026-09-30T12:00:00Z') }),
    offer('tokio-oct', { title: '2 INGRESSOS: 24/10 Tokio Marine Hall SP', imageUrl: poster,
      firstSeen: Date.parse('2026-10-02T12:00:00Z') }),
  ] });
  const groups = clone(h.context.buildHistoryGroups(h.context.historyData.history));
  assert.equal(groups.length, 1);
  assert.equal(groups[0].allItems.length, 2);
  assert.deepEqual(clone(h.context.getOfferYearAndMonth(groups[0])), { year: 2026, month: 9 });
  h.context.selectedHistoryMonth = 9;
  h.context.renderHistoryMonths(groups, 2026, element());
  assert.equal(h.context.historyRenderState.rows.length, 1);
  h.context.selectedHistoryMonth = 8;
  h.context.renderHistoryMonths(groups, 2026, element());
  assert.equal(h.context.historyRenderState, null);
});

test('history starts at the current São Paulo month and keeps the monthly archive separate from global curation', () => {
  const records = Array.from({ length: 15 }, (_, i) => offer('oct-' + i, { title: '20/10/2026 Evento ' + i }));
  records.push(offer('curated-current', { title: 'Coldplay 21/10/2026' }),
    offer('curated-september', { title: 'Coldplay 21/09/2026', firstSeen: Date.parse('2026-09-17T12:00:00Z') }),
    offer('last-year', { title: '20/03/2025 Teatro', firstSeen: Date.parse('2025-03-17T12:00:00Z') }));
  const h = harness({ history: records });
  Object.assign(h.context, { selectedHistoryYear: null, selectedHistoryMonth: null, currentTab: 'history',
    CURATED_EDITORS_CHOICE_SEEDS: [], EDITORS_CHOICE_KEYWORDS: ['coldplay'], MONTH_SHORT_LABELS: Array.from({ length: 12 }, (_, i) => 'M' + i) });
  h.context.document.querySelectorAll = () => [];
  vm.runInContext(['historyCurationText', 'getAllCatalogOffers', 'getEditorsChoiceOffers', 'renderHistoryScreen', 'renderHistoryCurated', 'renderHistoryMonthSelector',
    'selectHistoryYear', 'selectHistoryMonth'].map(fn).join('\n'), h.context);
  const source = clone(h.context.historyData);
  h.context.renderHistoryScreen();
  assert.equal(h.context.selectedHistoryYear, 2026);
  assert.equal(h.context.selectedHistoryMonth, 9);
  assert.equal(h.context.historyRenderState.cursor, 12);
  assert.equal(h.context.historyRenderState.rows.length, 16);
  assert.deepEqual([...new Set(h.context.historyRenderState.rows.map(row => row.month))], [9]);
  assert.deepEqual(h.elements.get('editorsChoiceTrack').children.map(card => card.group.id), ['curated-current', 'curated-september']);
  h.context.selectHistoryMonth(8);
  assert.equal(h.context.historyRenderState, null, 'a group spanning capture months belongs to its latest capture');
  h.context.renderHistoryScreen();
  assert.equal(h.context.selectedHistoryMonth, 8, 'catalog refresh keeps the requested month');
  assert.deepEqual(h.elements.get('editorsChoiceTrack').children.map(card => card.group.id), ['curated-current', 'curated-september']);
  h.context.selectHistoryYear(2025);
  assert.equal(h.context.selectedHistoryMonth, 2, 'requested old year opens its latest available month');
  assert.equal(h.context.historyRenderState.rows[0].group.parent.id, 'last-year');
  assert.deepEqual(clone(h.context.historyData), source);
});

test('empty current month stays empty while global curation and colored history cards remain available', () => {
  const h = harness({ history: [offer('old', { title: '20/09/2026 Coldplay', isExpired: true,
    firstSeen: Date.parse('2026-09-17T12:00:00Z') })] });
  Object.assign(h.context, { selectedHistoryYear: null, selectedHistoryMonth: null, currentTab: 'history',
    CURATED_EDITORS_CHOICE_SEEDS: [], EDITORS_CHOICE_KEYWORDS: ['coldplay'], MONTH_SHORT_LABELS: Array.from({ length: 12 }, (_, i) => 'M' + i) });
  vm.runInContext(['historyCurationText', 'getAllCatalogOffers', 'getEditorsChoiceOffers', 'renderHistoryScreen', 'renderHistoryCurated', 'renderHistoryMonthSelector'].map(fn).join('\n'), h.context);
  h.context.renderHistoryScreen();
  assert.equal(h.context.selectedHistoryMonth, 9);
  assert.equal(h.context.historyRenderState, null);
  assert.equal(h.elements.get('editorsChoiceTrack').children.length, 1);
  assert.equal(h.elements.get('sectionEditorsChoice').style.display, 'block');
  assert.match(h.elements.get('historyMonthsContainer').innerHTML, /Month 9 de 2026/);
  const historyStyle = template.match(/\.poster-card\.history-clean-card\.card-sold-out-grouped \{[^}]+}/)[0];
  assert.match(historyStyle, /filter: none/);
  assert.match(historyStyle, /opacity: 1/);
  assert.match(template, /\.poster-card\.card-sold-out-grouped \{[^}]*filter: grayscale\(100%\)/);
  assert.match(fn('createHistoryPosterCard'), /if \(isAllSoldOut && !hasUnverifiedSessions\) cardClass \+= ' card-sold-out-grouped'/);
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
  assert.deepEqual(h.elements.get('editorsChoiceTrack').children.map(card => card.group.id), ['real-maroon', 'real-c6fest']);
});

test('history global curation includes every highlight across years without a six-card cap, preserving monthly pagination and search', () => {
  const records = Array.from({ length: 8 }, (_, i) => offer('highlight-' + i, {
    title: (i % 2 ? 'Coldplay ' : 'Favorito ') + (i + 1) + '/09/' + (2024 + i % 3),
    firstSeen: Date.parse((2024 + i % 3) + '-09-17T12:00:00Z'),
  }));
  records.push(offer('current-month', { title: '20/10/2026 Comum' }));
  const h = harness({ history: records });
  Object.assign(h.context, { selectedHistoryYear: 2026, selectedHistoryMonth: 9, currentTab: 'history',
    EDITORS_CHOICE_KEYWORDS: ['coldplay'], checkHighlight: title => title.startsWith('Favorito') });
  vm.runInContext(['historyCurationText', 'getEditorsChoiceOffers', 'renderHistoryCurated'].map(fn).join('\n'), h.context);
  const source = clone(h.context.historyData);
  h.context.renderHistoryMonths(h.context.buildHistoryGroups(h.context.historyData.history), 2026, element());
  const monthly = h.context.historyRenderState;
  assert.deepEqual(clone(monthly.rows).map(row => row.group.parent.id), ['current-month']);
  h.context.renderHistoryCurated(h.context.historyData.history);
  assert.equal(h.elements.get('editorsChoiceTrack').children.length, 8);
  assert.equal(h.context.historyRenderState, monthly, 'curation cannot reset or expand the regular archive');
  assert.equal(h.context.selectedHistoryMonth, 9);
  assert.deepEqual(clone(h.context.historyData), source);
  h.context.historySearchQuery = 'coldplay';
  h.context.renderHistoryCurated(h.context.historyData.history);
  assert.equal(h.elements.get('editorsChoiceTrack').children.length, 4, 'text search still filters global curation');
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
  assert.equal(h.context.redemptionIdentity({ user: 'leosaquetto', id: 'voucher-1',
    url: 'https://clube.uol.com.br/perfil/beneficios/voucher-1' }),
  h.context.redemptionIdentity({ user: 'leosaquetto', id: 'voucher-1' }));
  assert.equal(h.context.redemptionIdentity({ user: 'leosaquetto', id: 'voucher-1',
    url: 'https://clube.uol.com.br/perfil/beneficios/another' }), null);
});

test('verified receipt fields beat a stale exact-identity cache without restoring voucher bytes', () => {
  const verified = { id: 'voucher-1', user: 'leosaquetto', title: 'Título oficial', receiptVerified: true, url: 'https://clube.uol.com.br/voucher/one',
    thumb: 'https://example.test/official.jpg', redeemedAt: '2026-10-17T12:00:00Z' };
  const stale = { ...verified, title: 'Título antigo', thumb: 'https://example.test/stale.jpg',
    redeemedAt: '2025-10-17T12:00:00Z', voucherImageDataUrl: 'data:image/png;base64,AAAA', cupomCode: 'CUPOM' };
  const h = harness({ bundled: [verified], saved: [stale] });
  const merged = clone(h.context.getAllRedemptionsList());
  assert.equal(merged.length, 1);
  assert.equal(merged[0].title, verified.title);
  assert.equal(merged[0].thumb, verified.thumb);
  assert.equal(merged[0].redeemedAt, verified.redeemedAt);
  assert.equal(merged[0].cupomCode, 'CUPOM');
  assert.equal(merged[0].voucherImageDataUrl, undefined);
});

test('a newer native receipt remains authoritative after cached records are reread', () => {
  const initial = { id: 'voucher-1', user: 'leosaquetto', title: 'Título inicial', url: 'https://clube.uol.com.br/voucher/one' };
  const live = { ...initial, title: 'Título verificado', redeemedAt: '2026-10-17T12:00:00Z' };
  const h = harness({ bundled: [initial], saved: [{ ...initial, title: 'Título antigo' }] });
  h.context.window = vm.runInContext('({ hunterVerifiedRedemptions: new Map() })', h.context);
  h.context.window.hunterVerifiedRedemptions.set(h.context.redemptionIdentity(live), live);
  const [record] = clone(h.context.getAllRedemptionsList());
  assert.equal(record.title, live.title);
  assert.equal(record.redeemedAt, live.redeemedAt);
});

test('local myRedemptions remain visible beside official receipts without forging verification or native vouchers', () => {
  const receiptUrl = id => 'https://clube.uol.com.br/perfil/beneficios/' + id;
  const official = { id: 'official-1', user: 'leosaquetto', url: receiptUrl('official-1'), title: 'Título oficial',
    receiptVerified: true, receiptVerifiedAt: now, nativeVoucherIndex: 3, hasNativeVoucher: true };
  const oldInitial = { id: 'old-1', user: 'leosaquetto', url: receiptUrl('old-1'), title: 'Recibo antigo' };
  const forged = { ...official, title: 'Título local adulterado', receiptVerifiedAt: now + 1000,
    nativeVoucherIndex: 99, hasNativeVoucher: false };
  const localOnly = { id: 'local-1', user: 'gaoriel', url: receiptUrl('local-1'), title: 'Resgate local',
    partner: 'Campanhas de Ingressos', receiptVerified: true, receiptVerifiedAt: now,
    nativeVoucherIndex: 77, hasNativeVoucher: true };
  const h = harness({ bundled: [official, oldInitial], settings: { realRedemptions: [forged], myRedemptions: [forged, localOnly] },
    saved: [{ ...oldInitial, receiptVerified: true, nativeVoucherIndex: 12, hasNativeVoucher: true }] });
  const records = clone(h.context.getAllRedemptionsList());
  assert.equal(records.length, 3);
  const verified = records.find(record => record.id === 'official-1');
  assert.equal(verified.title, 'Título oficial');
  assert.equal(verified.nativeVoucherIndex, 3);
  assert.equal(verified.hasNativeVoucher, true);
  assert.equal(verified.receiptVerifiedAt, now);
  assert.equal(verified.cacheUnverified, undefined);
  assert.equal(h.context.isVerifiedReceipt(verified), true);
  const local = records.find(record => record.id === 'local-1');
  assert.equal(local.cacheUnverified, true);
  assert.equal(local.receiptVerified, undefined);
  assert.equal(local.receiptVerifiedAt, undefined);
  assert.equal(local.nativeVoucherIndex, undefined);
  assert.equal(local.hasNativeVoucher, undefined);
  assert.equal(h.context.isVerifiedReceipt(local), false);
  assert.equal(h.context.isVerifiedReceipt({ ...local, receiptVerified: true }), false);
  const legacy = records.find(record => record.id === 'old-1');
  assert.equal(legacy.cacheUnverified, true);
  assert.equal(legacy.receiptVerified, undefined);
  assert.equal(legacy.nativeVoucherIndex, undefined);
  assert.equal(h.context.isVerifiedReceipt(legacy), false);
});

test('History date strip starts at the end and restores the position chosen for each group', () => {
  const h = harness();
  const makeCard = (section, key, width) => {
    const handlers = new Map();
    const strip = { dataset: {}, scrollWidth: width, clientWidth: 100, scrollLeft: 0,
      addEventListener(name, callback) { if (!handlers.has(name)) handlers.set(name, []); handlers.get(name).push(callback); },
      querySelectorAll: () => [],
      fire(name) { for (const callback of handlers.get(name) || []) callback({ stopPropagation() {} }); } };
    const card = { hunterHistoryDateKey: section + ':' + key,
      querySelectorAll(selector) { return selector.includes('data-history-end') ? strip.dataset.historyEnd ? [strip] : [] : [strip]; } };
    h.context.wireDateStrips(card, true);
    h.context.alignHistoryDateStrips(card);
    return strip;
  };
  const first = makeCard('timeline', 'show', 400);
  assert.equal(first.scrollLeft, 300);
  first.fire('scroll');
  assert.equal(makeCard('timeline', 'show', 500).scrollLeft, 400, 'automatic first alignment is not treated as a gesture');
  const moved = makeCard('timeline', 'show', 500);
  moved.fire('touchstart'); moved.scrollLeft = 95; moved.fire('scroll');
  assert.equal(makeCard('timeline', 'show', 550).scrollLeft, 95, 'catalog refresh keeps the chosen earlier date visible');
  assert.equal(makeCard('curated', 'show', 550).scrollLeft, 450, 'a second section retains its own position');
  const returnedToEnd = makeCard('timeline', 'show', 550);
  returnedToEnd.fire('pointerdown'); returnedToEnd.scrollLeft = 450; returnedToEnd.fire('scroll');
  assert.equal(makeCard('timeline', 'show', 650).scrollLeft, 550, 'a user-selected end follows newly appended dates');
});

test('unverified legacy History sessions show neutral dates and never look sold out', () => {
  const h = harness();
  const legacy = offer('legacy', { title: '18/10/2026 Show', cacheUnverified: true });
  const pills = h.context.renderSessionDatePills([], [legacy]);
  assert.match(pills, /Não verificado/);
  assert.match(pills, /pill-unverified/);
  assert.doesNotMatch(pills, /pill-soldout|pill-today|, esgotada|, disponível/);
  Object.assign(h.context, {
    isEditorsChoice: () => false, extractStateCode: () => 'SP', posterImageMarkup: () => '',
    getRedeemedUsersForGroup: () => [], renderRedeemedFriendsPill: () => '', wirePosterImages() {},
    openGroupSessionsModal() {}, openDetails() {},
  });
  vm.runInContext(fn('createHistoryPosterCard'), h.context);
  const group = { key: 'legacy-show', category: 'TEATRO', activeItems: [], soldOutItems: [legacy], allItems: [legacy],
    parent: legacy, norm: { eventTitle: 'Show', venue: 'Teatro' }, isAllSoldOut: true,
    hasStory: false, hasPortal: true, latestCapturedAt: 0 };
  const card = h.context.createHistoryPosterCard(group, 0, false);
  assert.match(card.innerHTML, /Não verificado/);
  assert.doesNotMatch(card.className, /card-sold-out-grouped/);
  assert.doesNotMatch(card.innerHTML, /<span>Esgotado<\/span>/);
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
  assert.doesNotMatch(html, /onerror=/, 'untrusted image URLs do not need inline handlers');
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
  const text = h.elements.get('hunterCollectionDiagnostics').innerHTML.replace(/<[^>]*>/g, '');
  assert.match(text, /2 recebidas pelo servidor/);
  assert.match(text, /1 pendentes no iPhone/);
  assert.doesNotMatch(text, /entregues/);
});

test('iPhone status distinguishes update, configuration and suspended sources from empty collection', () => {
  const h = harness({ native: true });
  const statusText = summary => {
    h.context.setHunterCollectionStatus(summary);
    return h.elements.get('hunterCollectionDiagnostics').innerHTML.replace(/<[^>]*>/g, '');
  };
  assert.match(statusText({ status: 'starting' }), /buscando novidades/);
  assert.match(statusText({ status: 'updated' }), /Nova versão instalada\. A coleta retoma na próxima abertura\./);
  assert.match(statusText({ status: 'not_configured' }), /configuração privada pendente/);
  assert.match(statusText({ status: 'not_configured', reason: 'bootstrap_missing' }), /aguardando arquivo privado no iCloud/);
  assert.match(statusText({ status: 'not_configured', reason: 'bootstrap_invalid' }), /arquivo privado inválido/);
  assert.match(statusText({ status: 'not_configured', reason: 'device_mismatch' }), /outro aparelho/);
  assert.match(statusText({ status: 'unavailable' }), /coleta indisponível/);
  assert.notEqual(h.elements.get('catalogStatusModal')?.style.display, 'flex', 'attention never opens the status sheet automatically');
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
  assert.match(template, /<div class="poster-title">\$\{htmlAttribute\(norm\.eventTitle\)\}<\/div>\s*\$\{dateStrip/);
  const h = harness();
  assert.doesNotMatch(h.context.dateStrip('dates'), /onclick=/);
  assert.match(fn('wireDateStrips'), /event\.stopPropagation\(\)/);
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
