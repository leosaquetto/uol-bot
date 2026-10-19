const { test } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const C = require('./collector.js');
const LINK = 'https://clube.uol.com.br/campanhasdeingresso/pAa-show';
const IMAGE = 'https://scontent.cdninstagram.com/test.jpg?oe=123';
const NOW = Date.parse('2026-10-17T18:00:00Z');
const OWNER = '12345678901234567890';
const STORY = '45678901234567890123';
const HTML = '<div class="col beneficio"><a href="/campanhasdeingresso/pAa-show"><img data-src="https://d310rdc9v8uuq.cloudfront.net/banner.jpg"><p class="titulo"><strong>2 ingressos &amp; show</strong></p><p class="parceiro">Teatro</p></a></div><!-- Fim div beneficio -->';
const PROFILE = { username: 'clubeuol', id: OWNER };
const session = () => ({ user_agent: 'test-browser', cookies: [{ name: 'sessionid', value: 'synthetic-session', domain: '.instagram.com' }, { name: 'ds_user_id', value: '100', domain: '.instagram.com' }] });
function reel(item = {}) {
  const taken = NOW / 1000 - 60;
  return { status: 'ok', reels: { [OWNER]: { id: OWNER, user: PROFILE, items: [{ id: STORY + '_' + OWNER, pk: STORY, taken_at: taken, expiring_at: taken + 86400,
    original_width: 1080, original_height: 1920, image_versions2: { candidates: [{ width: 1080, height: 1920, url: IMAGE }] }, story_link_stickers: [{ story_link: { url: LINK } }], ...item }] } } };
}
function harness({ instagram = false } = {}) {
  const files = new Map(), keys = new Map(), calls = []; let at = NOW, counter = 0, responder;
  const uuid = () => '00000000-0000-4000-8000-' + String(++counter).padStart(12, '0');
  const fm = { documentsDirectory: () => '/docs', joinPath: path.join, fileExists: p => files.has(p), createDirectory: p => files.set(p, null),
    listContents: p => [...files.keys()].filter(x => x.startsWith(p + '/') && !x.slice(p.length + 1).includes('/')).map(x => x.slice(p.length + 1)),
    readString: p => { if (!files.has(p)) throw Error('missing'); return files.get(p); }, writeString: (p, v) => files.set(p, v),
    move: (a, b) => { if (files.has(b)) throw Error('exists'); files.set(b, files.get(a)); files.delete(a); }, remove: p => files.delete(p) };
  const env = { fm, now: () => at, uuid, keychain: { contains: k => keys.has(k), get: k => keys.get(k), set: (k, v) => keys.set(k, v) }, pause: () => Promise.resolve(),
    request: url => { const req = { async loadString() { calls.push({ url, body: req.body, headers: req.headers, method: req.method });
      const result = await responder(url, req); if (result instanceof Error) throw result;
      req.response = { statusCode: result.code || 200, headers: result.headers || { 'Content-Type': typeof result.value === 'string' ? 'text/html' : 'application/json' }, url: result.url || url };
      return typeof result.value === 'string' ? result.value : JSON.stringify(result.value);
    } }; return req; } };
  if (instagram) env.instagramBrowser = {
    connect: async () => ({ status: 'ready' }),
    capture: async () => {
      const url = 'https://www.instagram.com/stories/clubeuol/';
      calls.push({ url, transport: 'webview' });
      const result = await responder(url, {}); if (result instanceof Error) throw result;
      if (result.code && result.code !== 200) throw Object.assign(Error('safe-adapter-error'), {
        reason: result.code === 429 ? 'http_429' : [401,403].includes(result.code) ? 'http_auth_required' : 'http_status',
        status: result.code === 429 ? 'rate_limited' : [401,403].includes(result.code) ? 'auth_required' : 'unknown',
        statusCode: result.code, retryAfter: Number(result.headers?.['Retry-After'] || 0) * 1000 });
      if (result.value?.message === 'login_required') throw Object.assign(Error('login_required'), { reason: 'login_required', status: 'auth_required' });
      return { profile: PROFILE, reels: result.value };
    }
  };
  const collector = C.create(env);
  collector.importBootstrap({ deviceId: 'test-phone', token: 'h1.test-phone.' + 'a'.repeat(64), ...(instagram ? { instagramSession: session() } : {}) });
  const defaultResponder = (url, req) => {
    if (url.endsWith('/hunter-intake') && req.method === 'POST') return { code: 202, value: { ok: true, accepted: true, persisted: true, receipts: JSON.parse(req.body).observations.map(e => ({ eventId: e.eventId, persisted: true, duplicate: false, status: 'queued' })) } };
    if (url.includes('/hunter-intake-status?')) return { value: { ok: true, persisted: true, status: 'delivered', eventId: decodeURIComponent(url.split('=')[1]) } };
    if (url.includes('/stories/clubeuol/')) return { value: reel() };
    return { value: HTML };
  };
  responder = defaultResponder;
  return { collector, env, files, keys, calls, uuid, now: () => at, advance: n => { at += n; }, setResponder: r => { responder = r; }, defaultResponder };
}

test('lossless JSON preserves ID digits and normal strings while rejecting malformed input', () => {
  const v = C.losslessJSON('{"id":12345678901234567890,"text":"id: 12345678901234567890 \\" hi", "n":12,"float":1.5}');
  assert.equal(v.id, OWNER); assert.equal(v.n, 12); assert.equal(v.float, 1.5);
  assert.match(v.text, /12345678901234567890/);
  assert.throws(() => C.losslessJSON('{"id":01}'));
});
test('Instagram only accepts exact owner, valid times and poster-sized image with canonical ticket links', () => {
  const p = C.parseReels(reel(), PROFILE, NOW); assert.equal(p[0].storyId, STORY); assert.equal(p[0].link, LINK); assert.equal(p[0].imageHeight, 1920);
  assert.equal(C.parseReels(reel({ taken_at: NOW / 1000 - 90000, expiring_at: NOW / 1000 - 3600 }), PROFILE, NOW).length, 0);
  const bad = reel(); bad.reels[OWNER].user = { id: OWNER, username: 'someone-else' };
  assert.throws(() => C.parseReels(bad, PROFILE, NOW), /owner_mismatch/);
  assert.throws(() => C.parseReels(reel({ pk: '9'.repeat(25) }), PROFILE, NOW));
  assert.throws(() => C.parseReels(reel({ owner: { id: '1', username: 'clubeuol' } }), PROFILE, NOW));
  assert.equal(C.parseReels(reel({ story_link_stickers: [{ story_link: { url: 'https://evil.example/login' } }] }), PROFILE, NOW).length, 0);
  assert.equal(C.media('https://scontent.cdninstagram.com/a.jpg?sessionid=secret'), '');
  assert.equal(C.campaign(LINK + '/resgatar'), null);
  assert.equal(C.campaign('https://l.instagram.com/?u=' + encodeURIComponent(LINK + '?utm_source=test')), LINK);
  assert.throws(() => C.parseReels({}, PROFILE, NOW), /no_validated_story_structure/);
  assert.deepEqual(C.parseReels({ reels: { [OWNER]: { user: PROFILE, items: [] } } }, PROFILE, NOW), []);
});
test('Portal extracts campaign metadata only, bounds UTF-8 titles, and rejects unvalidated HTML', () => {
  assert.deepEqual(C.parsePortal(HTML), [{ link: LINK, title: '2 ingressos & show', imageUrl: 'https://d310rdc9v8uuq.cloudfront.net/banner.jpg', partner: 'Teatro' }]);
  assert.throws(() => C.parsePortal('<html>Login</html>'), /no_validated_portal_structure/);
  const title = C.parsePortal(HTML.replace('2 ingressos &amp; show', 'á'.repeat(500)))[0].title;
  assert.equal(Buffer.byteLength(title), 280);
  assert.equal(C.parsePortal(HTML.replace('d310rdc9v8uuq.cloudfront.net', 'ddrxgn8ucibei.cloudfront.net'))[0].imageUrl,
    'https://ddrxgn8ucibei.cloudfront.net/banner.jpg');
  assert.equal(C.media('https://other.cloudfront.net/banner.jpg', false), '');
});
test('bootstrap only imports missing credentials and validates narrow device/session scope', () => {
  const h = harness({ instagram: true }); const original = [...h.keys];
  h.collector.importBootstrap({ deviceId: 'other-phone', token: 'h1.other-phone.' + 'b'.repeat(64), instagramSession: { ...session(), cookies: [{ name: 'sessionid', value: 'other' }] } });
  assert.deepEqual([...h.keys], original);
  assert.throws(() => C.normalizeSession({ ...session(), cookies: 'bad' }), /session_invalid/);
  assert.throws(() => C.normalizeSession({ ...session(), cookies: [{ name: 'sessionid', value: 'x', domain: 'evil.example' }] }), /session_invalid/);
});
test('native open uses one official Stories page, persists before intake and deduplicates reopenings', async () => {
  const h = harness({ instagram: true });
  h.setResponder((url, req) => { if (req.method === 'POST') for (const e of JSON.parse(req.body).observations) assert([...h.files.keys()].some(k => k.endsWith('/event-' + e.eventId + '.json'))); return h.defaultResponder(url, req); });
  const result = await h.collector.run(); assert.equal(result.discovered, 2); assert.equal(result.received, 2); assert.equal(result.delivered, 0);
  assert.equal(h.calls.filter(c => c.url.includes('instagram.com')).length, 1);
  await h.collector.run(); assert.equal(h.calls.filter(c => c.url.includes('instagram.com')).length, 2); // no inherited five-minute cooldown
  h.advance(300000); await C.create(h.env).run();
  assert.equal(h.calls.filter(c => c.url.includes('/api/')).length, 0);
  assert.equal(h.calls.filter(c => c.url.includes('/stories/clubeuol/')).length, 3);
  assert.equal(h.calls.filter(c => c.method === 'POST').length, 1, 'unchanged observation never gets a new event');
  assert.equal(h.collector.summary('test').delivered, 2);
  const outbound = h.calls.filter(c => c.method === 'POST').map(c => c.body).join('');
  assert.doesNotMatch(outbound, /synthetic-session|user_agent|Cookie|headers|sessionid/);
});
test('lost response/restart resends same immutable IDs and only matching persisted receipts release', async () => {
  const h = harness(); h.setResponder((url, req) => req.method === 'POST' ? new Error('lost response') : h.defaultResponder(url, req));
  await h.collector.run(); assert.equal(h.collector.summary('test').pending, 1);
  const first = JSON.parse(h.calls.find(c => c.method === 'POST').body);
  h.advance(120000);
  h.setResponder((url, req) => req.method === 'POST' ? { value: { ok: true, accepted: true, persisted: true, receipts: [{ eventId: h.uuid(), persisted: true, status: 'queued' }] } } : h.defaultResponder(url, req));
  await C.create(h.env).flush(); assert.equal(h.collector.summary('test').pending, 1);
  h.setResponder(h.defaultResponder); await C.create(h.env).flush();
  assert.equal(h.collector.summary('test').pending, 0);
  for (const call of h.calls.filter(c => c.method === 'POST')) assert.deepEqual(JSON.parse(call.body), first);
});
test('batch stops at 8 and no false successful/unknown receipt removes events', async () => {
  const h = harness(); for (let i = 0; i < 10; i++) h.collector.enqueue('portal', { link: LINK + '-' + i, title: 'Offer' });
  h.setResponder((url, req) => ({ value: { ok: false, accepted: false, persisted: true, receipts: JSON.parse(req.body).observations.map(e => ({ eventId: e.eventId, persisted: true, status: 'queued' })) } }));
  await h.collector.flush(); assert.equal(h.collector.summary('test').pending, 10); assert.equal(JSON.parse(h.calls[0].body).observations.length, 8);
  h.advance(120000); h.setResponder(h.defaultResponder); await h.collector.flush(); assert.equal(h.collector.summary('test').pending, 2);
});
test('Instagram 429 persists minimum 6h/Retry-After and auth suspension across restarts', async () => {
  const h = harness({ instagram: true }); h.setResponder(url => url.includes('instagram.com') ? { code: 429, headers: { 'Retry-After': '25000' }, value: '' } : { value: HTML });
  assert.equal((await h.collector.collect('instagram')).status, 'rate_limited'); h.advance(21600000);
  assert.equal((await C.create(h.env).collect('instagram')).status, 'rate_limited'); assert.equal(h.calls.length, 1);
  h.advance(4000000); h.setResponder(() => ({ code: 200, value: { message: 'login_required' } }));
  assert.equal((await C.create(h.env).collect('instagram')).status, 'auth_required'); h.advance(86400000);
  assert.equal((await C.create(h.env).collect('instagram')).status, 'auth_required'); assert.equal(h.calls.length, 2);
});
test('Portal cooldown and per-source 30 actual-request UTC budget survive restart', async () => {
  const h = harness(); for (let i = 0; i < 30; i++) { assert.equal((await C.create(h.env).collect('portal')).status, 'found'); h.advance(120000); }
  assert.equal((await h.collector.collect('portal')).status, 'daily_budget'); assert.equal(h.calls.length, 30);
  h.advance(86400000); assert.equal((await C.create(h.env).collect('portal')).status, 'found');
});
test('concurrent openings elect one collector and never erase another event', async () => {
  const h = harness(); const b = C.create(h.env);
  const results = await Promise.all([h.collector.run(), b.run()]);
  assert.deepEqual(results.map(r => r.status).sort(), ['busy', 'complete']);
  assert.equal(h.calls.filter(c => c.url.endsWith('/campanhasdeingresso')).length, 1);
  assert.equal(h.collector.summary('test').received, 1);
  assert.equal([...h.files.keys()].filter(k => k.includes('/lease-')).length, 0);
});

test('receipt refresh uses owner-scoped status endpoint and registration never means delivery', async () => {
  const h = harness(); await h.collector.run();
  h.advance(120000);
  h.setResponder(url => ({ value: { ok: true, persisted: true, eventId: decodeURIComponent(url.split('=')[1]), status: 'registered' } }));
  await h.collector.refreshReceipts();
  assert.match(h.calls.at(-1).url, /\/hunter-intake-status\?eventId=/);
  assert.equal(h.collector.summary('test').received, 1);
  assert.equal(h.collector.summary('test').delivered, 0);
  const calls = h.calls.length; h.advance(120000); await h.collector.refreshReceipts();
  assert.equal(h.calls.length, calls, 'registered Portal receipt is terminal for intake');
});
test('receipt refresh limits two reads and persists auth suspension', async () => {
  const h = harness(); for (let i = 0; i < 4; i++) h.collector.enqueue('portal', { link: LINK + '-' + i, title: 'Offer' });
  await h.collector.flush(); await h.collector.refreshReceipts();
  assert.equal(h.calls.filter(c => c.url.includes('/hunter-intake-status?')).length, 2);
  h.setResponder(() => ({ code: 401, value: {} }));
  await h.collector.refreshReceipts();
  const calls = h.calls.length; h.advance(86400000);
  await C.create(h.env).refreshReceipts(); await C.create(h.env).flush();
  assert.equal(h.calls.length, calls);
});
test('Instagram budget counts page captures only and does not exceed 30', async () => {
  const h = harness({ instagram: true });
  for (let i = 0; i < 30; i++) { assert.equal((await h.collector.collect('instagram')).status, 'found'); h.advance(300000); }
  assert.equal(h.calls.length, 30);
  assert.equal((await C.create(h.env).collect('instagram')).status, 'daily_budget');
  assert.equal(h.calls.length, 30);
});

test('uncertain receipt remains observable at 15 minute intervals without resending', async () => {
  const h = harness(); await h.collector.run();
  h.setResponder(url => ({ value: { ok: true, persisted: true, eventId: decodeURIComponent(url.split('=')[1]), status: 'needs_reconciliation' } }));
  await h.collector.refreshReceipts();
  const calls = h.calls.length;
  h.advance(899999); await C.create(h.env).refreshReceipts(); assert.equal(h.calls.length, calls);
  h.advance(1); h.setResponder(h.defaultResponder); await C.create(h.env).refreshReceipts();
  assert.equal(h.calls.length, calls + 1);
  assert.equal(h.collector.summary('test').delivered, 1);
  assert.equal(h.calls.filter(c => c.method === 'POST').length, 1);
});

test('WebView capture is independent of legacy session, suspension and budget', async () => {
  const h = harness({ instagram: true });
  assert.equal(h.keys.has(C.KEYS.instagram), false);
  h.keys.set(C.KEYS.instagram, 'invalid-legacy-private-session');
  const root = '/docs/uol-hunter-collector-v1/';
  h.files.set(root + 'state-instagram-old.json', JSON.stringify({ at: NOW, suspended: true, retryAt: NOW + 86400000 }));
  for (let i = 0; i < 30; i++) h.files.set(root + 'request-instagram-old-' + i + '.json', JSON.stringify({ at: NOW }));
  const before = new Map(h.files);
  const result = await h.collector.runInstagram();
  assert.equal(result.sources.instagram.status, 'found');
  assert.equal(result.sources.instagram.transport, 'webview');
  assert.equal(h.keys.get(C.KEYS.instagram), 'invalid-legacy-private-session');
  for (const [key,value] of before) assert.equal(h.files.get(key), value);
  assert.ok([...h.files.keys()].some(k => k.includes('state-instagramWebView-')));
  assert.equal([...h.files.keys()].filter(k => k.includes('/state-instagram-')).length, 1, '5.8.6 sees only its own state');
  assert.doesNotMatch(JSON.stringify(result), /private-session|Cookie|sessionid|h1\./);
});

test('login sheet closure does not prove authentication and cancelled login does not query', async () => {
  const h = harness({ instagram: true });
  h.env.instagramBrowser.connect = async () => ({ status: 'cancelled' });
  assert.equal((await h.collector.runInstagram({ connect: true })).sources.instagram.status, 'cancelled');
  assert.equal(h.calls.length, 0);
  h.env.instagramBrowser.connect = async () => ({ status: 'ready' });
  h.setResponder(() => ({ code: 401, value: {} }));
  assert.equal((await h.collector.runInstagram({ connect: true })).sources.instagram.status, 'auth_required');
  const count = h.calls.length;
  await C.create(h.env).runInstagram(); assert.equal(h.calls.length, count);
  h.setResponder(h.defaultResponder);
  assert.equal((await h.collector.runInstagram({ connect: true })).sources.instagram.status, 'found');
});

test('login never clears a real WebView 429 or its daily request budget', async () => {
  for (const limited of ['rate', 'daily']) {
    const h = harness({ instagram: true }); let logins = 0;
    h.env.instagramBrowser.connect = async () => { logins++; return { status: 'ready' }; };
    if (limited === 'rate') {
      h.setResponder(() => ({ code: 429, value: {}, headers: { 'Retry-After': '21600' } }));
      await h.collector.collect('instagram');
    } else {
      for (let i = 0; i < C.LIMITS.requests; i++) h.files.set('/docs/uol-hunter-collector-v1/request-instagramWebView-budget-' + i + '.json', JSON.stringify({ at: NOW }));
    }
    const count = h.calls.length;
    const result = await h.collector.runInstagram({ connect: true });
    assert.equal(result.sources.instagram.status, 'connected_waiting');
    assert.equal(result.sources.instagram.reason, limited === 'rate' ? 'http_429' : 'daily_budget');
    assert.ok(Date.parse(result.sources.instagram.nextAttemptAt) > NOW);
    assert.equal(h.calls.length, count); assert.equal(logins, 1);
  }
});

test('network errors are sanitized and do not create a five-minute pause', async () => {
  const h = harness({ instagram: true });
  h.setResponder(() => new Error('private-page-response'));
  const first = await h.collector.runInstagram();
  assert.equal(first.sources.instagram.status, 'unknown');
  assert.doesNotMatch(JSON.stringify(first), /private-page-response/);
  h.setResponder(h.defaultResponder);
  assert.equal((await h.collector.runInstagram()).sources.instagram.status, 'found');
});

test('overlapping automatic and manual collection use the same lease', async () => {
  const h = harness({ instagram: true }); let release;
  h.setResponder(async () => { await new Promise(resolve => { release = resolve; }); return { value: HTML }; });
  const running = h.collector.run();
  while (!release) await new Promise(resolve => setImmediate(resolve));
  assert.equal((await C.create(h.env).runInstagram()).status, 'busy');
  h.setResponder(h.defaultResponder); release(); await running;
});

test('last budget slot permits exactly one complete capture; next opening is blocked', async () => {
  const h = harness({ instagram: true });
  for (let i=0; i<29; i++) h.files.set('/docs/uol-hunter-collector-v1/request-instagramWebView-old-' + i + '.json', JSON.stringify({ at: NOW }));
  const result = await h.collector.runInstagram();
  assert.equal(result.sources.instagram.status, 'found');
  const count = h.calls.length;
  assert.equal((await h.collector.runInstagram()).sources.instagram.status, 'daily_budget');
  assert.equal(h.calls.length, count);
  assert.equal(h.calls.filter(c => c.url.includes('instagram.com')).length, 1);
});


test('WebView failures retain only the allowlisted reason, query stage and HTTP status', async () => {
  const h = harness({ instagram: true });
  h.setResponder(() => ({ code: 400, value: { private: 'never-display' } }));
  const first = (await h.collector.runInstagram()).sources.instagram;
  assert.equal(first.reason, 'http_status'); assert.equal(first.stage, 'stories_page'); assert.equal(first.statusCode, 400);
  h.setResponder((url, req) => url.includes('/stories/clubeuol/') ? { code: 500, value: {} } : h.defaultResponder(url, req));
  const second = (await h.collector.runInstagram()).sources.instagram;
  assert.equal(second.stage, 'stories_page'); assert.equal(second.statusCode, 500);
  h.env.instagramBrowser.capture = async () => { throw Object.assign(Error('private message'), { reason: 'browser_evaluation_failed', statusCode: 'private status' }); };
  const third = (await h.collector.runInstagram()).sources.instagram;
  assert.equal(third.reason, 'browser_evaluation_failed'); assert.equal(third.stage, 'stories_page'); assert.equal(third.statusCode, undefined);
  h.env.instagramBrowser.connect = async () => ({ status: 'unknown', reason: 'browser_bridge_unsupported', details: 'private login' });
  const login = (await h.collector.runInstagram({ connect: true })).sources.instagram;
  assert.equal(login.reason, 'browser_bridge_unsupported'); assert.equal(login.stage, 'login');
  assert.doesNotMatch(JSON.stringify([first, second, third, login]), /never-display|private/);
});


test('only the retired private API 429 wait is migrated; page 429s and daily budget remain', async () => {
  for (const stage of ['profile', 'stories']) {
    const h = harness({ instagram: true });
    h.files.set('/docs/uol-hunter-collector-v1/state-instagramWebView-existing.json', JSON.stringify({ at: NOW - 1, status: 'rate_limited', reason: 'http_429', stage, retryAt: NOW + 21600000 }));
    assert.equal((await h.collector.runInstagram()).sources.instagram.status, 'found');
    assert.equal(h.calls.filter(c => c.url.includes('instagram.com')).length, 1);
  }
  for (const stage of ['stories_page', undefined]) {
    const h = harness({ instagram: true });
    h.files.set('/docs/uol-hunter-collector-v1/state-instagramWebView-existing.json', JSON.stringify({ at: NOW - 1, status: 'rate_limited', reason: 'http_429', stage, retryAt: NOW + 21600000 }));
    assert.equal((await h.collector.runInstagram()).sources.instagram.status, 'rate_limited');
    assert.equal(h.calls.length, 0);
  }
  const h = harness({ instagram: true });
  h.files.set('/docs/uol-hunter-collector-v1/state-instagramWebView-existing.json', JSON.stringify({ at: NOW - 1, status: 'rate_limited', reason: 'http_429', stage:'profile', retryAt: NOW + 21600000 }));
  for (let i=0;i<30;i++) h.files.set('/docs/uol-hunter-collector-v1/request-instagramWebView-old-' + i + '.json', JSON.stringify({ at: NOW }));
  assert.equal((await h.collector.runInstagram()).sources.instagram.status, 'daily_budget');
  assert.equal(h.calls.length, 0);
});
