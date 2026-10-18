const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Evaluate only the adapter with synthetic fixtures. Never load the compiled personal package.
const template = fs.readFileSync(path.join(__dirname, 'template.html'), 'utf8');
const start = template.indexOf('// CATALOG_ADAPTER_START');
const end = template.indexOf('// CATALOG_ADAPTER_END');
assert.ok(start > 0 && end > start);
const adapter = template.slice(start, end);
const dateStart = template.indexOf('// UI_DATE_HELPERS_START');
const dateEnd = template.indexOf('// UI_DATE_HELPERS_END');
const extractStart = template.indexOf('    function extractSessionDate(');
const extractEnd = template.indexOf('\n    }', extractStart) + '\n    }'.length;
const dateHelpers = template.slice(dateStart, dateEnd) + '\n' + template.slice(extractStart, extractEnd);
const endpoint = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/catalog';
const key = 'uol_shared_catalog_v1';
const now = Date.parse('2026-10-17T18:00:00Z');
const link = slug => 'https://clube.uol.com.br/campanhasdeingresso/' + slug;
const clone = value => JSON.parse(JSON.stringify(value));
const offer = (slug = 'p123-show', extra = {}) => ({
  id: 'internal-' + slug, link: link(slug), title: 'Show novo', imageUrl: 'https://example.test/poster.jpg',
  description: '', partner: '', validity: '', firstSeenAt: now - 3000, lastSeenAt: now - 1000,
  soldOutAt: null, restockedAt: null, availability: 'observed', isPortal: true, ...extra,
});
const snapshot = (offers = [offer()], extra = {}) => ({
  ok: true, schemaVersion: 1, offers, stories: [],
  coverage: { offerLimit: 300, storyLimit: 30, truncated: false, scope: 'recent_tracked' },
  sources: { api: { healthy: true, lastSuccessAt: now - 1000 }, ticketListing: { healthy: false, lastSuccessAt: null } },
  ...extra,
});
const response = (data, status = 200, etag = '"catalog-one"') => ({
  ok: status >= 200 && status < 300, status,
  headers: { get: name => name.toLowerCase() === 'etag' ? etag : null },
  json: async () => clone(data), text: async () => typeof data === 'string' ? data : JSON.stringify(data),
});

function harness({ cache, history = { active: [], history: [] }, network = () => response(snapshot()) } = {}) {
  const storage = new Map(cache === undefined ? [] : [[key, typeof cache === 'string' ? cache : JSON.stringify(cache)]]);
  const elements = new Map();
  const requests = [];
  const writes = [];
  const rendered = [];
  class Clock extends Date { static now() { return now; } }
  const context = vm.createContext({
    URL, Date: Clock, Intl, console,
    historyData: clone(history), currentTab: 'home',
    localStorage: { getItem: k => storage.get(k) || null, setItem: (k, v) => { storage.set(k, v); writes.push(k); } },
    fetch: async (url, options) => { requests.push({ url, options }); return network(url, options); },
    document: { getElementById: id => {
      if (!elements.has(id)) elements.set(id, { style: {}, textContent: '', setAttribute(name, value) { this[name] = value; } });
      return elements.get(id);
    } },
    getCachedDescription: () => '',
    toSentenceCase: title => title,
    classifyCategory: () => 'MUSICA',
    saveHistoryToStorage: () => {},
    renderOffers: offers => rendered.push(clone(offers)),
    renderHistoryScreen: () => {}, updateCategoryCounts: () => {}, updateTimelineBadgeDot: () => {},
    prefetchActiveOfferDescriptions: () => { throw new Error('Unexpected automatic details fetch'); },
  });
  vm.runInContext(adapter + '\n' + dateHelpers, context);
  return {
    context, storage, requests, writes, elements, rendered,
    run: () => context.streamLiveOffers(),
    merge: data => context.mergeCatalogSnapshot(clone(data)),
    state: () => clone(context.historyData),
  };
}

test('healthy 200 uses only catalog, stores ETag with snapshot, preserves local IDs and richer details', async () => {
  const local = { id: 'old-id', link: link('p123-show') + '?origin=portal', title: 'Show', firstSeen: now - 9000,
    viewsCount: 8, description: 'Descrição local completa do evento', validity: 'Até 13/12/2026', location_summary: 'Teatro local' };
  const h = harness({ history: { active: [local], history: [] } });
  await h.run();
  assert.deepEqual(h.requests.map(r => r.url), [endpoint]);
  assert.equal(h.requests[0].options.credentials, 'omit');
  const item = h.state().active[0];
  assert.equal(item.id, local.id);
  assert.equal(item.firstSeen, local.firstSeen);
  assert.equal(item.viewsCount, 8);
  assert.equal(item.description, local.description);
  assert.equal(item.validity, local.validity);
  assert.equal(item.location_summary, local.location_summary);
  assert.equal(h.elements.get('liveDot').style.backgroundColor, '#00e676');
  assert.deepEqual(h.writes, [key]);
  const stored = JSON.parse(h.storage.get(key));
  assert.equal(stored.etag, '"catalog-one"');
  assert.equal(stored.fetchedAt, now);
  assert.deepEqual(stored.snapshot, snapshot());
  await h.run();
  assert.equal(h.requests.length, 1, 'one refresh per invocation');
});

test('stale valid snapshot sends its ETag and 304 reuses same content', async () => {
  const data = snapshot();
  const h = harness({ cache: { snapshot: data, etag: '"previous"', fetchedAt: now - 120001 }, network: () => response(null, 304) });
  await h.run();
  assert.equal(h.requests[0].options.headers['If-None-Match'], '"previous"');
  assert.equal(h.state().active.length, 1);
  assert.equal(JSON.parse(h.storage.get(key)).fetchedAt, now);
  assert.equal(h.requests.length, 1);
});

test('fresh cache avoids even catalog HTTP for 120 seconds', async () => {
  const h = harness({ cache: { snapshot: snapshot(), etag: '"previous"', fetchedAt: now - 119999 }, network: () => { throw new Error('must not fetch'); } });
  await h.run();
  assert.equal(h.requests.length, 0);
  assert.equal(h.state().active.length, 1);
});

test('manual refresh bypasses client freshness once, preserves ETag and blocks overlapping clicks', async () => {
  let finish;
  const h = harness({ cache: { snapshot: snapshot(), etag: '"previous"', fetchedAt: now - 100 },
    network: () => new Promise(resolve => { finish = resolve; }) });
  await h.run();
  assert.equal(h.requests.length, 0);
  const refresh = h.context.refreshHunterCatalog();
  assert.equal(h.requests.length, 1);
  assert.equal(h.elements.get('btnRefreshCatalog').disabled, true);
  await h.context.refreshHunterCatalog();
  assert.equal(h.requests.length, 1);
  finish(response(null, 304)); await refresh;
  assert.equal(h.requests[0].options.headers['If-None-Match'], '"previous"');
  assert.equal(h.elements.get('btnRefreshCatalog').disabled, false);
  assert.equal(h.state().active.length, 1);
});

test('refresh deadline covers a stalled response body and releases controls while retaining cache', async () => {
  let expire, bodyStarted;
  let cleared = false;
  const started = new Promise(resolve => { bodyStarted = resolve; });
  const h = harness({ cache: { snapshot: snapshot(), etag: '"previous"', fetchedAt: now - 100 },
    network: (_, options) => ({ ok: true, status: 200,
      json: () => new Promise((_, reject) => {
        options.signal.addEventListener('abort', () => reject(new Error('Aborted')));
        bodyStarted();
      }) }) });
  h.context.AbortController = AbortController;
  h.context.setTimeout = callback => { expire = callback; return 1; };
  h.context.clearTimeout = () => { cleared = true; };
  const refresh = h.context.refreshHunterCatalog();
  await started;
  assert.equal(cleared, false);
  assert.equal(h.elements.get('btnRefreshCatalog').disabled, true);
  expire();
  await refresh;
  assert.equal(cleared, true);
  assert.equal(h.elements.get('btnRefreshCatalog').disabled, false);
  assert.equal(h.state().active.length, 1);
  assert.match(h.elements.get('liveStatusText').textContent, /cache/);
});

test('invalid cache cannot send ETag or accept orphan 304; cold fallback runs once', async () => {
  const html = '<div class="beneficio"><a href="/campanhasdeingresso/p123-show"><img data-src="https://example.test/image.jpg"><p class="titulo mb-0">Show novo</p><!-- Fim div beneficio -->';
  const h = harness({ cache: { snapshot: { ok: true }, etag: '"orphan"', fetchedAt: now - 200000 },
    network: url => url === endpoint ? response(null, 304) : url === '/campanhasdeingresso' ? response(html) : response({ stories: [] }) });
  await h.run();
  await h.run();
  assert.equal(h.requests[0].options.headers['If-None-Match'], undefined);
  assert.deepEqual(h.requests.map(r => r.url), [endpoint, '/campanhasdeingresso', 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/stories']);
  assert.equal(h.state().active[0].id, 'p123-show');
});

test('503, network failure, and invalid catalog retain valid stale cache without portal or details calls', async () => {
  for (const network of [() => response({}, 503), () => { throw new Error('offline'); }, () => response({ ok: true, offers: [] })]) {
    const h = harness({ cache: { snapshot: snapshot(), etag: '"old"', fetchedAt: now - 300000 }, network });
    await h.run();
    assert.equal(h.requests.length, 1);
    assert.equal(h.state().active.length, 1);
    assert.match(h.elements.get('liveStatusText').textContent, /em cache/);
    assert.equal(JSON.parse(h.storage.get(key)).fetchedAt, now - 300000);
  }
});

test('empty/truncated/unhealthy catalog preserves known offers and never falls back to portal', async () => {
  const local = { id: 'p999-known', link: link('p999-known'), title: 'Conhecida', firstSeen: now - 99999 };
  const data = snapshot([], {
    coverage: { scope: 'recent_tracked', truncated: true },
    sources: { api: { healthy: false, lastSuccessAt: now - 900000 }, ticketListing: { healthy: false, lastSuccessAt: null } },
  });
  const h = harness({ history: { active: [local], history: [] }, network: () => response(data) });
  await h.run();
  assert.deepEqual(h.state().active, [local]);
  assert.equal(h.state().history.length, 0);
  assert.equal(h.requests.length, 1);
  assert.match(h.elements.get('liveStatusText').textContent, /em cache/);
});

test('truncated catalog stays amber even when its sources are fresh and healthy', async () => {
  const data = snapshot([offer()], { coverage: { offerLimit: 300, storyLimit: 30, scope: 'recent_tracked', truncated: true } });
  const h = harness({ network: () => response(data) });
  await h.run();
  assert.equal(h.elements.get('liveDot').style.backgroundColor, '#ffb300');
  assert.match(h.elements.get('liveStatusText').textContent, /em cache/);
  assert.equal(h.requests.length, 1);
});

test('recently cached snapshot rechecks source timestamp rather than trusting historical healthy flag', async () => {
  const data = snapshot([offer()], { sources: {
    api: { healthy: true, lastSuccessAt: now - 600001 },
    ticketListing: { healthy: false, lastSuccessAt: null },
  } });
  const h = harness({ cache: { snapshot: data, fetchedAt: now - 1000, etag: '"old-source"' } });
  await h.run();
  assert.equal(h.requests.length, 0);
  assert.equal(h.elements.get('liveDot').style.backgroundColor, '#ffb300');
  assert.match(h.elements.get('liveStatusText').textContent, /em cache/);
});

test('canonical duplicate generations choose latest observed offer regardless of response order', () => {
  const older = offer('p123-show', { id: 'old-generation', availability: 'sold_out', firstSeenAt: now - 30000, lastSeenAt: now - 20000, soldOutAt: now - 10000 });
  const newer = offer('p123-show', { id: 'new-generation', title: 'Sessão atual', firstSeenAt: now - 5000, lastSeenAt: now - 1000 });
  for (const entries of [[older, newer], [newer, older]]) {
    const h = harness();
    h.merge(snapshot(entries));
    assert.equal(h.state().active.length, 1);
    assert.equal(h.state().active[0].title, 'Sessão atual');
    assert.equal(h.state().active[0].firstSeen, newer.firstSeenAt);
    assert.equal(h.state().history.length, 0);
  }
});

test('hydration preserves valid saved empty active list and history instead of reviving bundled actives', () => {
  const hydration = template.slice(template.indexOf('// Hidratação via localStorage instantâneo'), template.indexOf('// Reclassifica categorias de ofertas em cache'));
  const bundled = { active: [{ id: 'bundled-active' }], history: [] };
  const saved = { active: [], history: [{ id: 'sold-out', expiredAt: now - 1000 }] };
  for (const data of [saved, { active: [], history: [] }]) {
    const context = vm.createContext({ historyData: clone(bundled), settings: {}, localStorage: {
      getItem: key => key === 'uol_offers_history' ? JSON.stringify(data) : null,
    } });
    vm.runInContext(hydration, context);
    assert.deepEqual(clone(context.historyData), data);
  }
  const invalid = vm.createContext({ historyData: clone(bundled), settings: {}, localStorage: {
    getItem: key => key === 'uol_offers_history' ? '{"active":[],"history":"bad"}' : null,
  } });
  vm.runInContext(hydration, invalid);
  assert.deepEqual(clone(invalid.historyData), bundled);
});

test('only explicit sold_out moves existing offer to history and never trims unrelated history', () => {
  const old = { id: 'preserved', link: link('p123-show'), title: 'Show', firstSeen: now - 99999, viewsCount: 7 };
  const historic = { id: 'ancient', link: link('p111-ancient'), expiredAt: now - 10 * 86400000 };
  const h = harness({ history: { active: [old], history: [historic] } });
  h.merge(snapshot([offer('p123-show', { availability: 'sold_out', soldOutAt: now - 500 })]));
  assert.equal(h.state().active.length, 0);
  const ended = h.state().history.find(o => o.id === 'preserved');
  assert.equal(ended.expiredAt, now - 500);
  assert.equal(ended.firstSeen, old.firstSeen);
  assert.equal(ended.viewsCount, 7);
  assert.ok(h.state().history.some(o => o.id === 'ancient'));
});

test('observed never revives historical offer without newer explicit restock evidence', () => {
  const old = { id: 'preserved', link: link('p123-show'), expiredAt: now - 5000, isExpired: true };
  const h = harness({ history: { active: [], history: [old] } });
  h.merge(snapshot([offer()]));
  assert.equal(h.state().active.length, 0);
  h.merge(snapshot([offer('p123-show', { restockedAt: now - 6000 })]));
  assert.equal(h.state().active.length, 0);
  h.merge(snapshot([offer('p123-show', { restockedAt: now - 3000 })]));
  assert.equal(h.state().active[0].id, 'preserved');
  assert.equal(h.state().active[0].expiredAt, undefined);
  assert.equal(h.state().active[0].isExpired, false);
  assert.equal(h.state().history.length, 0);
  h.merge(snapshot([offer('p123-show', { availability: 'sold_out', soldOutAt: now - 4000 })]));
  assert.equal(h.state().active.length, 1, 'stale sold-out observation cannot undo proven restock');
});

test('Story-only campaigns enter existing card model; expiry does not infer ticket exhaustion', () => {
  const story = { storyId: 'story-1', link: link('p777-festival'), imageUrl: 'https://example.test/story.jpg',
    publishedAt: now - 90000, expiresAt: now - 1000, firstSeenAt: now - 80000 };
  const h = harness();
  h.merge(snapshot([], { stories: [story] }));
  const item = h.state().active[0];
  assert.equal(item.id, 'p777-festival');
  assert.equal(item.hasStory, true);
  assert.equal(item.isPortal, false);
  assert.equal(item.source, 'Stories_Only');
  assert.equal(item.expiredAt, undefined);
  assert.equal(h.state().history.length, 0);
  h.merge(snapshot());
  assert.ok(h.state().active.some(o => o.id === 'p777-festival'), 'missing Story is not deletion evidence');
});

test('API-only offer outside portal listing is not falsely labeled as Story', () => {
  const h = harness();
  h.merge(snapshot([offer('p123-show', { isPortal: false })]));
  assert.equal(h.state().active[0].hasStory, false);
  assert.equal(h.state().active[0].source, 'Worker');
  assert.equal(h.state().active[0].isPortal, false);
});

test('Story merges into same campaign without duplicate, identity reset, or revival', () => {
  const old = { id: 'stable', link: link('p123-show'), title: 'Show', expiredAt: now - 1000, isPortal: true };
  const h = harness({ history: { active: [], history: [old] } });
  h.merge(snapshot([offer()], { stories: [{ storyId: 'story-1', link: link('p123-show').toUpperCase(), firstSeenAt: now }] }));
  assert.equal(h.state().active.length, 0);
  assert.equal(h.state().history.length, 1);
  assert.equal(h.state().history[0].id, 'stable');
  assert.equal(h.state().history[0].hasStory, true);
  assert.equal(h.state().history[0].isPortal, true);
});

test('cold fallback failures and partial portal listing preserve all previously known data', async () => {
  const old = { id: 'kept', link: link('p888-known'), title: 'Outra oferta' };
  for (const portal of [response('', 503), response('<html>login</html>'), response('<div class="beneficio"><a href="/campanhasdeingresso/p123-show"><img data-src="https://example.test/image.jpg"><p class="titulo mb-0">Show</p><!-- Fim div beneficio -->')]) {
    const h = harness({ history: { active: [old], history: [] }, network: url => url === endpoint ? response({}, 503) : url === '/campanhasdeingresso' ? portal : response({ stories: [] }) });
    await h.run();
    assert.ok(h.state().active.some(o => o.id === 'kept'));
    assert.equal(h.state().history.length, 0);
  }
});

test('unsafe campaign URLs and malformed snapshots are rejected without trusting their ETag', async () => {
  for (const badLink of ['https://evil.test/campanhasdeingresso/p123-show', 'javascript:alert(1)', 'https://clube.uol.com.br/perfil/beneficios/123', 'https://user:pass@clube.uol.com.br/campanhasdeingresso/p123-show']) {
    const h = harness({ cache: { snapshot: snapshot([offer('p123-show', { link: badLink })]), etag: '"unsafe"', fetchedAt: now }, network: url => url === endpoint ? response(snapshot()) : response('') });
    await h.run();
    assert.equal(h.requests[0].options.headers['If-None-Match'], undefined);
    assert.equal(h.state().active[0].link, link('p123-show'));
  }
});
