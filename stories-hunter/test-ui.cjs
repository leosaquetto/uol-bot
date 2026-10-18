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
  return { style: { setProperty() {} }, classList: { toggle() {} }, children: [], textContent: '', innerHTML: '',
    appendChild(node) { this.children.push(node); }, setAttribute(key, value) { this[key] = value; } };
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
    currentTab: 'home', currentTimelineFilter: 'all', CATEGORY_MAP: { TEATRO: { label: 'Teatro', icon: '' }, OUTROS: { label: 'Outros', icon: '' } },
    ICONS: { tickets: '<svg></svg>', calendar: '<svg></svg>' },
    document: { getElementById(id) { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); }, createElement: element },
    localStorage: { getItem: key => storage.get(key) || null, setItem: (key, value) => storage.set(key, value) },
    fetch: async url => { requests.push(url); throw new Error('offline fixture'); },
    getCachedDescription: () => '', toSentenceCase: text => text, classifyCategory: () => 'TEATRO',
    saveHistoryToStorage() {}, recordSessionImpressions() {}, checkHighlight: () => false,
    normalizeEventInfo: () => ({}), getRedeemedUsersForOffer: () => [], renderRedeemedFriendsPill: () => '',
    getAvatarUrl: () => 'https://example.test/avatar.jpg', renderHistoryScreen() {}, renderGroupedOffers() {},
  });
  vm.runInContext([
    block('CATALOG_ADAPTER'), block('UI_DATE_HELPERS'), block('REDEMPTION_IDENTITY'), block('REDEMPTION_DATE'),
    ...['extractSessionDate', 'isStoriesOnlyOffer', 'getGroupKey', 'buildOfferGroups', 'formatRelativeTime', 'formatExpiredAgo',
      'formatRedemptionTitle', 'getTimelineEvents', 'getRedeemedUsersForOffer', 'updateTimelineBadgeDot', 'renderTimelineList', 'renderOffers', 'renderIndividualOffers', 'updateCategoryCounts'].map(fn),
  ].join('\n'), context);
  return { context, elements, requests, storage, advance: ms => { time += ms; } };
}

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
