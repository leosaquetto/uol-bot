const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const B = require('./native/instagram-browser.js');
const C = require('./collector.js');

const ORIGIN = 'https://www.instagram.com';
const PROFILE = '/api/v1/users/web_profile_info/?username=clubeuol';
const OWNER = '12345678901234567890';
const STORY = '45678901234567890123';
const REELS = '/api/v1/feed/reels_media/?reel_ids=' + OWNER;
const NOW = Date.parse('2026-10-19T15:00:00Z');
const SECRET = 'private-synthetic-value';
const LINK = 'https://clube.uol.com.br/campanhasdeingresso/pRp-expo';
const IMAGE = 'https://scontent.cdninstagram.com/expo.jpg?oe=123';
const profile = () => ({ data: { user: { id: OWNER, username: 'clubeuol', email: SECRET } }, viewer: { sessionid: SECRET } });
function reels() {
  const taken = NOW / 1000 - 60;
  return { status: 'ok', csrf_token: SECRET, viewer: { password: SECRET }, reels: { [OWNER]: {
    id: OWNER, user: { id: OWNER, username: 'clubeuol', phone_number: SECRET },
    items: [{ id: STORY + '_' + OWNER, pk: STORY, taken_at: taken, expiring_at: taken + 86400,
      original_width: 1080, original_height: 1920, viewer: { cookies: SECRET },
      image_versions2: { candidates: [{ width: 1080, height: 1920, url: IMAGE, private: SECRET }] },
      story_link_stickers: [{ story_link: { url: LINK, private: SECRET }, viewer: SECRET }] }],
  }, '111': { private: SECRET } } };
}
function apiResponse(path, options = {}) {
  const bytes = new TextEncoder().encode(options.raw ?? JSON.stringify(Object.hasOwn(options, 'value') ? options.value : profile()));
  let read = false;
  return { status: options.status ?? 200, type: options.responseType ?? 'basic', url: options.url ?? ORIGIN + path,
    headers: { get: key => ({ 'content-type': options.type ?? 'application/json; charset=utf-8', 'retry-after': options.retryAfter ?? '' })[key] ?? null },
    body: { getReader: () => ({ async read() { if (read) return { done: true }; read = true; return { done: false, value: bytes }; },
      async cancel() { options.onCancel?.(); } }) } };
}
function executePage(code, { location = { origin: ORIGIN, pathname: '/clubeuol/' }, response = {}, fetchImpl } = {}) {
  const calls = [];
  const promise = new Promise((resolve, reject) => {
    const context = { location, AbortController, TextDecoder, setTimeout: () => 1, clearTimeout() {},
      completion: value => resolve(JSON.parse(value)),
      fetch: async (path, options) => { calls.push({ path, options }); return fetchImpl ? fetchImpl(path, options) : apiResponse(path, response); } };
    try { Promise.resolve(vm.runInNewContext(code, context)).catch(reject); } catch (error) { reject(error); }
  });
  return { promise, calls };
}
function page(path = PROFILE, options = {}, maxBytes = 4194304) {
  const code = '(' + B.pageRequest.toString() + ')(' + JSON.stringify(path) + ',' + maxBytes +
    ',(() => { const error = () => new Error(); return ' + C.losslessJSON.toString() + '; })());';
  return executePage(code, options);
}
function browser(options = {}) {
  const views = [], calls = [], timers = [];
  const adapter = B.create({ losslessJSON: C.losslessJSON,
    timer(milliseconds, callback) { const timer = { milliseconds, callback, invalidated: false, invalidate() { this.invalidated = true; } }; timers.push(timer); return timer; },
    webView() {
      const location = { origin: ORIGIN, pathname: '/' };
      const view = {
        async loadURL(url) {
          calls.push({ action: 'load', url }); location.origin = new URL(url).origin; location.pathname = new URL(url).pathname;
          if (options.load) await options.load(url, location);
        },
        async present(fullscreen) { calls.push({ action: 'present', fullscreen }); if (options.present) await options.present(location); },
        async evaluateJavaScript(code, callback) {
          calls.push({ action: 'evaluate', callback, origin: location.origin });
          if (!callback) return JSON.stringify({ origin: location.origin, path: location.pathname });
          if (options.evaluate) return options.evaluate(code, callback);
          const result = executePage(code, { location, response: options.response, fetchImpl: options.fetchImpl });
          const output = await result.promise; calls.push(...result.calls.map(call => ({ action: 'fetch', ...call })));
          return JSON.stringify(output);
        },
      };
      views.push(view); return view;
    },
  });
  return { adapter, calls, views, timers };
}

test('profile projection preserves an unquoted long ID and strips private response fields inside the page', async () => {
  const raw = JSON.stringify(profile()).replace('"id":"' + OWNER + '"', '"id":' + OWNER);
  const request = page(PROFILE, { response: { raw } });
  const result = await request.promise;
  assert.deepEqual(JSON.parse(result.body), { data: { user: { id: OWNER, username: 'clubeuol' } } });
  assert.doesNotMatch(JSON.stringify(result), /private-synthetic|sessionid|email|viewer|cookies|password/);
  assert.equal(request.calls[0].path, PROFILE);
  assert.equal(request.calls[0].options.credentials, 'same-origin');
  assert.equal(request.calls[0].options.redirect, 'manual');
  assert.doesNotMatch(JSON.stringify(request.calls[0].options.headers), /Cookie|Authorization|sessionid|token|password/);
});

test('reel projection keeps only capture metadata and the collector accepts precise numeric IDs', async () => {
  const raw = JSON.stringify(reels()).replaceAll('"id":"' + OWNER + '"', '"id":' + OWNER).replace('"pk":"' + STORY + '"', '"pk":' + STORY);
  const result = await page(REELS, { response: { raw } }).promise;
  const projected = JSON.parse(result.body);
  assert.deepEqual(Object.keys(projected.reels), [OWNER]);
  assert.equal(projected.reels[OWNER].items[0].pk, STORY);
  const offers = C.parseReels(projected, { id: OWNER, username: 'clubeuol' }, NOW);
  assert.equal(offers.length, 1); assert.equal(offers[0].storyId, STORY); assert.equal(offers[0].link, LINK);
  assert.doesNotMatch(JSON.stringify(result), /private-synthetic|csrf|phone_number|viewer|cookies|password/);
  const media = { status: 'ok', reels_media: [reels().reels[OWNER]] };
  const alternate = await page(REELS, { response: { value: media } }).promise;
  assert.equal(C.parseReels(JSON.parse(alternate.body), { id: OWNER, username: 'clubeuol' }, NOW).length, 1);
});

test('expired login, challenge and foreign page origin stop before querying Instagram', async () => {
  for (const location of [
    { origin: ORIGIN, pathname: '/accounts/login/' }, { origin: ORIGIN, pathname: '/challenge/' },
    { origin: ORIGIN, pathname: '/checkpoint/' }, { origin: ORIGIN, pathname: '/consent/' },
    { origin: 'https://example.invalid', pathname: '/clubeuol/' },
  ]) {
    const request = page(PROFILE, { location });
    assert.deepEqual(await request.promise, { error: 'login_required' }); assert.equal(request.calls.length, 0);
  }
  for (const value of [{ message: 'login_required', password: SECRET }, { is_logged_in: false }, { challenge_required: true }]) {
    assert.deepEqual(await page(PROFILE, { response: { value } }).promise, { error: 'login_required' });
  }
  assert.deepEqual(await page(PROFILE, { response: { status: 0, responseType: 'opaqueredirect' } }).promise, { error: 'login_required' });
});

test('malformed API responses and excessive collections never return their body', async () => {
  for (const response of [
    { raw: '<html>' + SECRET + '</html>', type: 'text/html' }, { raw: '{"password":' + SECRET },
    { value: [] }, { value: null }, { value: { data: { user: { id: {} } } } },
  ]) {
    const result = await page(PROFILE, { response }).promise;
    assert.ok(result.error); assert.doesNotMatch(JSON.stringify(result), /private-synthetic/); assert.equal(result.body, undefined);
  }
  const many = reels(); many.reels[OWNER].items = Array(101).fill(many.reels[OWNER].items[0]);
  assert.equal((await page(REELS, { response: { value: many } }).promise).error, 'browser_response_invalid');
});

test('API byte limit cancels reading and non-200 responses retain only safe status metadata', async () => {
  let cancelled = false;
  const large = await page(PROFILE, { response: { raw: 'é'.repeat(40), onCancel: () => { cancelled = true; } } }, 64).promise;
  assert.equal(large.error, 'body_limit'); assert.equal(cancelled, true);
  for (const status of [401, 403, 429, 500]) {
    const result = await page(PROFILE, { response: { status, raw: SECRET, retryAfter: '3600' } }).promise;
    assert.equal(result.statusCode, status); assert.equal(result.body, ''); assert.equal(result.retryAfter, '3600');
    assert.doesNotMatch(JSON.stringify(result), /private-synthetic/);
  }
});

test('network failure remains a bounded safe error without returning the underlying exception', async () => {
  const request = page(PROFILE, { fetchImpl: async () => { throw new TypeError(SECRET); } });
  const result = await request.promise;
  assert.equal(result.error, 'browser_network_error'); assert.doesNotMatch(JSON.stringify(result), /private-synthetic/);
  const h = browser({ fetchImpl: async () => { throw new TypeError(SECRET); } });
  await assert.rejects(h.adapter.request(PROFILE), error => error.status === 'unknown' && !String(error).includes(SECRET));
  assert.equal(h.calls.filter(call => call.action === 'present').length, 0);
});

test('automatic capture evaluates fetch in the real Instagram page without presenting login', async () => {
  const h = browser();
  const result = await h.adapter.request(PROFILE);
  assert.equal(JSON.parse(result.body).data.user.id, OWNER);
  assert.deepEqual(h.calls.filter(call => call.action === 'load'), [{ action: 'load', url: ORIGIN + '/clubeuol/' }]);
  assert.equal(h.calls.filter(call => call.action === 'present').length, 0);
  assert.ok(h.calls.some(call => call.action === 'evaluate' && call.callback === true && call.origin === ORIGIN));
  assert.equal(h.calls.filter(call => call.action === 'fetch').length, 1);
  await h.adapter.request(PROFILE);
  assert.equal(h.calls.filter(call => call.action === 'load').length, 1, 'reuse only the existing WebView within this execution');
  assert.ok(h.timers.every(timer => timer.invalidated));
});

test('official login cancellation stays unverified; closing a logged-in page is only ready for a real capture', async () => {
  const cancelled = browser();
  assert.deepEqual(await cancelled.adapter.connect(), { status: 'cancelled', reason: 'login_incomplete' });
  assert.equal(cancelled.calls.filter(call => call.action === 'fetch').length, 0);
  const connected = browser({ present: async location => { location.pathname = '/'; } });
  assert.deepEqual(await connected.adapter.connect(), { status: 'ready' });
  assert.deepEqual(connected.calls.filter(call => call.action === 'load'), [{ action: 'load', url: ORIGIN + '/accounts/login/' }]);
  assert.deepEqual(connected.calls.filter(call => call.action === 'present'), [{ action: 'present', fullscreen: true }]);
  assert.equal(connected.calls.filter(call => call.action === 'fetch').length, 0);
  await connected.adapter.request(PROFILE);
  assert.equal(connected.calls.filter(call => call.action === 'fetch').length, 1);
});

test('automatic capture reports an expired WebView session without displaying authentication', async () => {
  const h = browser({ load: async (_url, location) => { location.pathname = '/accounts/login/'; } });
  await assert.rejects(h.adapter.request(PROFILE), error => error.status === 'auth_required' && error.reason === 'login_required');
  assert.equal(h.calls.filter(call => call.action === 'present' || call.action === 'fetch').length, 0);
});

test('adapter accepts only the fixed profile and numeric reels routes before creating a browser', async () => {
  const h = browser();
  for (const route of ['https://www.instagram.com' + PROFILE, PROFILE + '&token=' + SECRET,
    '/api/v1/users/web_profile_info/?username=someone-else', '/api/v1/feed/reels_media/?reel_ids=0',
    '/api/v1/feed/reels_media/?reel_ids=' + '1'.repeat(25), REELS + '&other=1', '/accounts/login/']) {
    await assert.rejects(h.adapter.request(route), error => error.reason === 'browser_path_invalid');
  }
  assert.equal(h.views.length, 0);
  assert.equal(B.allowedPath(PROFILE), true); assert.equal(B.allowedPath(REELS), true);
});

test('WebView navigation allows only Instagram asset origins and blocks lookalikes or non-HTTPS', async () => {
  const h = browser(); await h.adapter.request(PROFILE);
  const allow = h.views[0].shouldAllowRequest;
  for (const url of ['about:blank', ORIGIN + '/', 'https://scontent.cdninstagram.com/a.jpg', 'https://scontent.fbcdn.net/a.jpg']) assert.equal(allow({ url }), true);
  for (const url of ['http://www.instagram.com/', 'https://instagram.com.evil.invalid/', 'https://evilinstagram.com/', 'https://instagram.com@evil.invalid/', 'file:///tmp/file', 'javascript:alert(1)']) assert.equal(allow({ url }), false);
  assert.equal(allow({}), false);
});

test('concurrent automatic or manual capture cannot overlap the adapter operation', async () => {
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const h = browser({ load: () => gate });
  const pending = h.adapter.request(PROFILE);
  await assert.rejects(h.adapter.request(REELS), error => error.reason === 'browser_busy');
  assert.deepEqual(await h.adapter.connect(), { status: 'busy' });
  assert.equal(h.views.length, 1); release(); await pending;
  await h.adapter.request(PROFILE);
  assert.equal(h.calls.filter(call => call.action === 'fetch').length, 2, 'the completed request releases the lock');
});

test('load timeout disables the orphaned WebView and permits a fresh capture attempt', async () => {
  let first = true;
  const h = browser({ load: () => { if (first) { first = false; return new Promise(() => {}); } } });
  const pending = h.adapter.request(PROFILE);
  h.timers[0].callback();
  await assert.rejects(pending, error => error.reason === 'browser_timeout');
  assert.equal(h.views[0].shouldAllowRequest({ url: ORIGIN + '/' }), false);
  await h.adapter.request(PROFILE); assert.equal(h.views.length, 2);
});

test('unexpected native callback content is rejected without surfacing credentials', async () => {
  for (const raw of ['not JSON ' + SECRET, JSON.stringify({ error: SECRET }), 'x'.repeat(4194305)]) {
    const h = browser({ evaluate: async () => raw });
    await assert.rejects(h.adapter.request(PROFILE), error => error.reason === 'browser_response_invalid' && !String(error).includes(SECRET));
  }
});
