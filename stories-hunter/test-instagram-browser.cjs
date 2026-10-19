const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const B = require('./native/instagram-browser.js');
const C = require('./collector.js');
const ORIGIN = 'https://www.instagram.com', PATH = '/stories/clubeuol/';
const OWNER = '12345678901234567890', STORY = '45678901234567890123';
const NOW = Date.parse('2026-10-19T15:00:00Z'), SECRET = 'private-synthetic-value';
const LINK = 'https://clube.uol.com.br/campanhasdeingresso/pRp-expo';
function payload() {
  return { require: [['module', [], null, [{ __bbox: { result: { data: {
    xdt_viewer: { password: SECRET }, xdt_api__v1__feed__reels_media: { reels_media: [{
      id: OWNER, user: { id: OWNER, pk: OWNER, username: 'clubeuol', email: SECRET },
      viewers: [SECRET], items: [{ id: STORY + '_' + OWNER, pk: STORY, taken_at: NOW / 1000 - 60, expiring_at: NOW / 1000 + 86340,
        original_width: 1080, original_height: 1920, user: { id: OWNER, pk: OWNER }, viewer: { cookies: SECRET },
        image_versions2: { candidates: [{ width: 1080, height: 1920, url: 'https://scontent.cdninstagram.com/expo.jpg', private: SECRET }] },
        story_link_stickers: [{ story_link: { url: LINK, private: SECRET } }] }]
    }] }
  } } } }]]] };
}
const target = value => value.require[0][3][0].__bbox.result.data.xdt_api__v1__feed__reels_media;
function execute(code, options = {}) {
  let ticks = 0;
  return new Promise((resolve, reject) => {
    const context = { location: options.location || { origin: ORIGIN, pathname: PATH },
      document: { querySelector: () => options.login || null,
        querySelectorAll: selector => selector === 'button,[role="button"]' ? (options.buttons || []) : (options.scripts ? options.scripts(ticks) : [options.raw ?? JSON.stringify(payload())]).map(textContent => ({ textContent })) },
      performance: { getEntriesByType: () => [{ responseStatus: options.status }] },
      Date: { now: () => ticks * 250, parse: Date.parse }, setTimeout: fn => { ticks++; fn(); },
      completion: value => { options.onCompletion?.(value); queueMicrotask(() => resolve(JSON.parse(value))); },
      URL, TextDecoder, getComputedStyle: button => button.style || { visibility: 'visible', display: 'block' },
      fetch: options.fetch || (() => { throw Error('No collector fetch permitted'); }),
      XMLHttpRequest: options.XMLHttpRequest,
    };
    options.onContext?.(context);
    try {
      const immediate = vm.runInNewContext(code, context);
      if (immediate && typeof immediate.then === 'function') reject(Error('Unsupported immediate Promise'));
    } catch (error) { reject(error); }
  });
}
function page(options = {}) {
  return execute('(' + B.pageCapture.toString() + ')((() => { const error = () => new Error(); return ' + C.losslessJSON.toString() + '; })()); "";', options);
}
function browser(options = {}) {
  const views = [], calls = [], timers = [];
  const adapter = B.create({ losslessJSON: C.losslessJSON,
    timer(milliseconds, callback) { const t = { milliseconds, callback, invalidate() {} }; timers.push(t); return t; },
    webView() {
      const location = { origin: ORIGIN, pathname: '/' };
      const view = {
        async loadURL(url) { calls.push(['load', url]); location.pathname = new URL(url).pathname; if (options.load) await options.load(location); },
        async present() { calls.push(['present']); if (options.present) await options.present(location); },
        async evaluateJavaScript(code, callback) {
          if (options.evaluate) return options.evaluate(code, callback);
          if (!callback) return JSON.stringify({ origin: location.origin, path: location.pathname });
          return JSON.stringify(await execute(code, { location, ...options }));
        }
      }; views.push(view); return view;
    }
  });
  return { adapter, views, calls, timers };
}

test('official page JSON preserves large IDs, projects only target and strips private fields', async () => {
  const value = payload(); target(value).reels_media.push({ user: { username: 'another' }, private: SECRET });
  const raw = JSON.stringify(value).replaceAll('"id":"' + OWNER + '"', '"id":' + OWNER).replace('"pk":"' + STORY + '"', '"pk":' + STORY);
  const result = await page({ raw });
  assert.equal(result.profile.id, OWNER); assert.equal(result.reels.reels_media.length, 1);
  const profile = C.profileIdentity({ data: { user: result.profile } });
  const offers = C.parseReels(result.reels, profile, NOW);
  assert.equal(offers[0].storyId, STORY); assert.equal(offers[0].link, LINK);
  assert.doesNotMatch(JSON.stringify(result), /private-synthetic|email|password|viewer|cookies|csrf/);
});
test('scoped empty reel is empty; absent, unrelated or unscoped data is unknown', async () => {
  const value = payload(); target(value).reels_media[0].items = [];
  const empty = await page({ raw: JSON.stringify(value) });
  assert.deepEqual(C.parseReels(empty.reels, C.profileIdentity({ data: { user: empty.profile } }), NOW), []);
  for (const value of [{}, { xdt_api__v1__feed__reels_media: { reels_media: [] } }, { xdt_api__v1__feed__reels_media: { reels_media: [{ user: { username: 'other' }, items: [] }] } }]) {
    assert.equal((await page({ raw: JSON.stringify(value) })).error, 'browser_story_data_missing');
  }
});
test('bounded hydration waits for inserted JSON without extra requests', async () => {
  const result = await page({ scripts: ticks => ticks < 3 ? [] : [JSON.stringify(payload())] });
  assert.equal(result.profile.id, OWNER);
});
test('login redirect/form and challenge stop with authentication pending', async () => {
  for (const pathname of ['/accounts/login/', '/challenge/', '/checkpoint/', '/consent/'])
    assert.equal((await page({ location: { origin: ORIGIN, pathname } })).error, 'login_required');
  assert.equal((await page({ login: {} })).error, 'login_required');
  assert.equal((await page({ location: { origin: 'https://evil.example', pathname: PATH } })).error, 'login_required');
});
test('page errors use actual navigation status only; missing status is not manufactured', async () => {
  assert.deepEqual(await page({ status: 429 }), { error: 'http_429', statusCode: 429 });
  assert.equal((await page({ status: 403 })).error, 'http_auth_required');
  assert.deepEqual(await page({ status: 500 }), { error: 'http_status', statusCode: 500 });
  assert.equal((await page()).statusCode, undefined);
  assert.equal((await page({ location: { origin: ORIGIN, pathname: '/someone-else/' } })).error, 'browser_story_page_invalid');
});
test('identity conflicts are rejected by the existing native parser', async () => {
  const value = payload(); target(value).reels_media[0].user.pk = '2';
  const result = await page({ raw: JSON.stringify(value) });
  assert.throws(() => C.profileIdentity({ data: { user: result.profile } }), /profile_identity_mismatch/);
  const other = payload(); target(other).reels_media[0].items[0].pk = '12345678901234567891';
  const mismatched = await page({ raw: JSON.stringify(other) });
  assert.throws(() => C.parseReels(mismatched.reels, C.profileIdentity({ data: { user: mismatched.profile } }), NOW), /malformed_story_payload/);
});
test('malformed, oversized, deep and excessive script structures fail closed', async () => {
  assert.equal((await page({ raw: '{"xdt_api__v1__feed__reels_media":' })).error, 'browser_story_data_invalid');
  assert.equal((await page({ raw: 'x'.repeat(4194305) })).error, 'body_limit');
  assert.equal((await page({ scripts: () => Array(257).fill('{}') })).error, 'structure_limit');
  let deep = payload(); for (let i=0; i<60; i++) deep = { nested: deep };
  assert.equal((await page({ raw: JSON.stringify(deep) })).error, 'structure_limit');
  const many = payload(); target(many).reels_media = Array(101).fill({});
  assert.equal((await page({ raw: JSON.stringify(many) })).error, 'browser_story_data_invalid');
});
test('capture loads official page and retains the login WebView; bridge immediate value is supported', async () => {
  const h = browser({ present: loc => { loc.pathname = '/'; } });
  assert.equal((await h.adapter.connect()).status, 'ready');
  assert.equal((await h.adapter.capture()).profile.id, OWNER);
  assert.equal((await h.adapter.capture()).profile.id, OWNER);
  assert.equal(h.views.length, 1);
  assert.deepEqual(h.calls, [['load', ORIGIN + '/accounts/login/'], ['present'], ['load', ORIGIN + PATH], ['load', ORIGIN + PATH]]);
});
test('automatic capture does not present login and never falls back to manual API', async () => {
  const h = browser({ load: loc => { loc.pathname = '/accounts/login/'; } });
  await assert.rejects(h.adapter.capture(), e => e.status === 'auth_required');
  assert.equal(h.calls.some(c => c[0] === 'present'), false);
  assert.equal(h.adapter.request, undefined);
  assert.doesNotMatch(B.pageCapture.toString(), /fetch\(|new\s+XMLHttpRequest|document\.cookie|localStorage|sessionStorage/);
});
test('cancelled login does not claim readiness', async () => {
  assert.equal((await browser().adapter.connect()).status, 'cancelled');
});
test('errors crossing native bridge are sanitized', async () => {
  const h = browser({ evaluate: () => { throw Error('JavaScript execution returned a result of an unsupported type ' + SECRET); } });
  await assert.rejects(h.adapter.capture(), e => e.reason === 'browser_bridge_unsupported' && !e.message.includes(SECRET));
  const invalid = browser({ evaluate: () => '{private}' });
  await assert.rejects(invalid.adapter.capture(), e => e.reason === 'browser_response_invalid');
});
test('in-flight collection blocks another capture/login; timeout disables old view', async () => {
  let release;
  const h = browser({ load: () => new Promise(resolve => { release = resolve; }) });
  const first = h.adapter.capture();
  await assert.rejects(h.adapter.capture(), e => e.reason === 'browser_busy');
  assert.equal((await h.adapter.connect()).status, 'busy');
  h.timers[0].callback();
  await assert.rejects(first, e => e.reason === 'browser_timeout');
  assert.equal(h.views[0].shouldAllowRequest({ url: ORIGIN + PATH }), false); release();
});
test('WebView navigation restricts origins and does not leak device credentials', async () => {
  const h = browser(); await h.adapter.capture();
  assert.equal(h.views[0].shouldAllowRequest({ url: 'https://evil.example/' }), false);
  assert.equal(h.views[0].shouldAllowRequest({ url: 'https://instagram.com.evil.example/' }), false);
  assert.equal(h.views[0].shouldAllowRequest({ url: 'https://scontent.cdninstagram.com/image.jpg' }), true);
  assert.doesNotMatch(JSON.stringify(h.calls), /token|sessionid|password|cookie/);
});


test('story owner may omit username only when every numeric ID matches scoped profile', async () => {
  const value = payload();
  const result = await page({ raw: JSON.stringify(value) });
  const profile = C.profileIdentity({ data: { user: result.profile } });
  assert.equal(C.parseReels(result.reels, profile, NOW).length, 1);
  for (const owner of [{ id: '2' }, { id: OWNER, pk: '2' }, { id: OWNER, username: 'other' }, {}]) {
    result.reels.reels_media[0].items[0].user = owner;
    assert.throws(() => C.parseReels(result.reels, profile, NOW), /owner_mismatch/);
  }
});


test('missing data clicks one exact visible story gate once and reads its hydrated payload', async () => {
  for (const label of ['Ver story', 'View story', 'See story']) {
    let clicks = 0;
    const button = { textContent: label, getAttribute: () => null, getClientRects: () => [{}], click: () => clicks++ };
    const result = await page({ buttons: [button], scripts: ticks => clicks && ticks >= 2 ? [JSON.stringify(payload())] : [] });
    assert.equal(result.profile.id, OWNER); assert.equal(clicks, 1);
  }
});
test('fallback never clicks hidden, ambiguous, unrelated or login controls', async () => {
  let clicks = 0;
  const button = textContent => ({ textContent, getAttribute: () => null, getClientRects: () => [{}], click: () => clicks++ });
  for (const buttons of [[button('Entrar')], [button('Ver story'),button('Ver story')], [{...button('Ver story'),getClientRects:()=>[]}], [{...button('Ver story'),disabled:true}]]) {
    assert.equal((await page({ buttons, scripts:()=>[] })).error, 'browser_story_data_missing');
  }
  await page({ buttons: [button('Ver story')] });
  await page({ login: {}, buttons: [button('Ver story')], scripts:()=>[] });
  await page({ status:429, buttons:[button('Ver story')],scripts:()=>[] });
  assert.equal(clicks, 0);
});

function gate(click, label = 'Ver story') {
  return { textContent: label, getAttribute: () => null, getClientRects: () => [{}], click };
}
function networkPayload() {
  return JSON.stringify({ data: { xdt_api__v1__feed__reels_media: target(payload()) } })
    .replaceAll('"id":"' + OWNER + '"', '"id":' + OWNER).replace('"pk":"' + STORY + '"', '"pk":' + STORY);
}
function response(raw, options = {}) {
  let position = 0, cancellations = 0;
  const chunks = options.chunks || [new TextEncoder().encode(raw)];
  const value = { status: options.status || 200, url: options.url || ORIGIN + '/graphql/query',
    headers: { get: name => name === 'retry-after' ? options.retryAfter ?? null : null },
    clone: () => ({ body: { getReader: () => ({
      read: async () => position < chunks.length ? { done: false, value: chunks[position++] } : { done: true },
      cancel: () => { cancellations++; }
    }) } }) };
  return { value, cancellations: () => cancellations };
}
function xhrHarness(deliver) {
  const calls = [], instances = [];
  class XHR {
    constructor() { this.listeners = new Set(); this.responseType = ''; instances.push(this); }
    open(method, url) { this.responseURL = new URL(url, ORIGIN).href; calls.push(['open', method, url]); }
    send(body) { calls.push(['send', body]); deliver?.(this); }
    addEventListener(type, fn) { assert.equal(type, 'loadend'); this.listeners.add(fn); }
    removeEventListener(type, fn) { this.listeners.delete(fn); }
    getResponseHeader(name) { return name === 'retry-after' ? this.retryAfter ?? null : null; }
    finish(raw, options = {}) {
      this.status = options.status || 200; this.retryAfter = options.retryAfter;
      this.responseText = raw; this.responseType = options.responseType ?? this.responseType;
      for (const listener of [...this.listeners]) listener();
    }
  }
  return { XHR, calls, instances, open: XHR.prototype.open, send: XHR.prototype.send };
}
test('gate fetch hydration preserves exact IDs, strips unrelated data and restores the original fetch', async () => {
  let context, clicks = 0, calls = 0;
  const reply = response(networkPayload());
  const originalFetch = function (url, init) { calls++; assert.equal(this, context); assert.equal(url, '/graphql/query'); assert.equal(init.body, SECRET); return Promise.resolve(reply.value); };
  const result = await page({ scripts: () => [], fetch: originalFetch, onContext: value => { context = value; },
    buttons: [gate(() => { clicks++; context.fetch('/graphql/query', { method: 'POST', body: SECRET }); })] });
  assert.equal(result.profile.id, OWNER); assert.equal(result.reels.reels_media[0].items[0].pk, STORY);
  assert.doesNotMatch(JSON.stringify(result), /private-synthetic|password|cookies|viewer|email/);
  assert.equal(clicks, 1); assert.equal(calls, 1); assert.equal(context.fetch, originalFetch);
  assert.equal(reply.cancellations(), 1);
});
test('gate XHR hydration observes text without issuing requests or rounding IDs and restores both hooks', async () => {
  let context, clicks = 0;
  const h = xhrHarness(xhr => xhr.finish(networkPayload()));
  const result = await page({ scripts: () => [], XMLHttpRequest: h.XHR, onContext: value => { context = value; },
    buttons: [gate(() => { clicks++; const xhr = new context.XMLHttpRequest(); xhr.open('POST', '/graphql/query'); xhr.send(SECRET); })] });
  assert.equal(result.profile.id, OWNER); assert.equal(result.reels.reels_media[0].items[0].pk, STORY);
  assert.equal(clicks, 1); assert.deepEqual(h.calls, [['open', 'POST', '/graphql/query'], ['send', SECRET]]);
  assert.equal(h.XHR.prototype.open, h.open); assert.equal(h.XHR.prototype.send, h.send);
  assert.equal(h.instances[0].listeners.size, 0); assert.doesNotMatch(JSON.stringify(result), /private-synthetic/);
});
test('direct same-origin reels response is scoped; unrelated and cross-origin hydration is ignored', async () => {
  for (const endpoint of ['/api/v1/feed/reels_media/', ORIGIN + '/api/v1/feed/reels_media/']) {
    let context;
    const result = await page({ scripts: () => [], onContext: value => { context = value; },
      fetch: () => Promise.resolve(response(JSON.stringify(target(payload())), { url: new URL(endpoint, ORIGIN).href }).value),
      buttons: [gate(() => context.fetch(endpoint))] });
    assert.equal(result.profile.id, OWNER);
  }
  for (const url of ['https://evil.example/graphql/query', ORIGIN + '/api/v1/accounts/current_user/']) {
    let context, clones = 0, calls = 0;
    const result = await page({ scripts: () => [], onContext: value => { context = value; },
      fetch: () => { calls++; return Promise.resolve({ status: 200, url, clone: () => { clones++; throw Error(SECRET); } }); },
      buttons: [gate(() => context.fetch(url))] });
    assert.equal(result.error, 'browser_story_data_missing'); assert.equal(calls, 1); assert.equal(clones, 0);
  }
});
test('only actual observed HTTP failures carry rate-limit deadlines; hooks are restored on failure', async () => {
  for (const transport of ['fetch', 'xhr']) {
    let context;
    const h = xhrHarness(xhr => xhr.finish('', { status: 429, retryAfter: '120' }));
    const originalFetch = () => Promise.resolve(response('', { status: 429, retryAfter: '120' }).value);
    const result = await page({ scripts: () => [], XMLHttpRequest: h.XHR, fetch: originalFetch, onContext: value => { context = value; },
      buttons: [gate(() => {
        if (transport === 'fetch') context.fetch('/graphql/query');
        else { const xhr = new context.XMLHttpRequest(); xhr.open('POST', '/graphql/query'); xhr.send(); }
      })] });
    assert.deepEqual(result, { error: 'http_429', statusCode: 429, retryAfter: 120000 });
    assert.equal(context.fetch, originalFetch); assert.equal(h.XHR.prototype.open, h.open); assert.equal(h.XHR.prototype.send, h.send);
  }
  const h = browser({ evaluate: () => JSON.stringify({ error: 'http_429', statusCode: 429, retryAfter: 120000 }) });
  await assert.rejects(h.adapter.capture(), e => e.status === 'rate_limited' && e.retryAfter === 120000 && e.statusCode === 429);
});
test('XHR JSON, absent bodies and unsupported fetch streams never use parsed or rounded responses', async () => {
  for (const type of ['json', 'arraybuffer']) {
    let context;
    const h = xhrHarness(xhr => { xhr.response = JSON.parse(networkPayload()); xhr.finish(networkPayload(), { responseType: type }); });
    const result = await page({ scripts: () => [], XMLHttpRequest: h.XHR, onContext: value => { context = value; },
      buttons: [gate(() => { const xhr = new context.XMLHttpRequest(); xhr.open('POST', '/graphql/query'); xhr.send(); })] });
    assert.equal(result.error, 'browser_story_data_missing');
  }
  let context;
  const h = xhrHarness(xhr => xhr.finish(undefined));
  assert.equal((await page({ scripts: () => [], XMLHttpRequest: h.XHR, onContext: value => { context = value; },
    buttons: [gate(() => { const xhr = new context.XMLHttpRequest(); xhr.open('POST', '/graphql/query'); xhr.send(); })] })).error, 'browser_story_data_missing');
  const result = await page({ scripts: () => [], onContext: value => { context = value; },
    fetch: () => Promise.resolve({ status: 200, url: ORIGIN + '/graphql/query', headers: { get: () => null }, clone: () => ({ body: null }) }),
    buttons: [gate(() => context.fetch('/graphql/query'))] });
  assert.equal(result.error, 'browser_story_data_missing');
});
test('capture expiry restores hooks and ignores late fetch and detached XHR callbacks', async () => {
  let context, resolveFetch, detachedListener, completions = 0, clones = 0;
  const h = xhrHarness(xhr => { detachedListener = [...xhr.listeners][0]; });
  const originalFetch = () => new Promise(resolve => { resolveFetch = resolve; });
  const result = await page({ scripts: () => [], XMLHttpRequest: h.XHR, fetch: originalFetch, onContext: value => { context = value; },
    onCompletion: () => completions++, buttons: [gate(() => {
      context.fetch('/graphql/query'); const xhr = new context.XMLHttpRequest(); xhr.open('POST', '/graphql/query'); xhr.send();
    })] });
  assert.equal(result.error, 'browser_story_data_missing'); assert.equal(h.instances[0].listeners.size, 0);
  assert.equal(context.fetch, originalFetch); assert.equal(h.XHR.prototype.open, h.open); assert.equal(h.XHR.prototype.send, h.send);
  resolveFetch({ url: ORIGIN + '/graphql/query', status: 200, clone: () => { clones++; throw Error(SECRET); } });
  h.instances[0].status = 200; h.instances[0].responseText = networkPayload(); detachedListener();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(completions, 1); assert.equal(clones, 0);
});
test('network observation enforces byte and response budgets, and never clicks CSS-hidden gates', async () => {
  for (const [raw, reason] of [['x'.repeat(4194305), 'body_limit'], ['{"xdt_api__v1__feed__reels_media":', 'browser_story_data_invalid']]) {
    let context;
    const result = await page({ scripts: () => [], onContext: value => { context = value; }, fetch: () => Promise.resolve(response(raw).value),
      buttons: [gate(() => context.fetch('/graphql/query'))] });
    assert.equal(result.error, reason);
  }
  let context, calls = 0;
  const result = await page({ scripts: () => [], onContext: value => { context = value; }, fetch: () => { calls++; return Promise.resolve(response('{}').value); },
    buttons: [gate(() => { for (let i = 0; i < 17; i++) context.fetch('/graphql/query'); })] });
  assert.equal(result.error, 'structure_limit'); assert.equal(calls, 17);
  for (const style of [{ visibility: 'hidden', display: 'block' }, { visibility: 'visible', display: 'none' }]) {
    let clicks = 0;
    await page({ scripts: () => [], buttons: [{ ...gate(() => clicks++), style }] });
    assert.equal(clicks, 0);
  }
});
