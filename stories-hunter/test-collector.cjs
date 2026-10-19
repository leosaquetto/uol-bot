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
  const collector = C.create(env);
  collector.importBootstrap({ deviceId: 'test-phone', token: 'h1.test-phone.' + 'a'.repeat(64), ...(instagram ? { instagramSession: session() } : {}) });
  const defaultResponder = (url, req) => {
    if (url.endsWith('/hunter-intake') && req.method === 'POST') return { code: 202, value: { ok: true, accepted: true, persisted: true, receipts: JSON.parse(req.body).observations.map(e => ({ eventId: e.eventId, persisted: true, duplicate: false, status: 'queued' })) } };
    if (url.includes('/hunter-intake-status?')) return { value: { ok: true, persisted: true, status: 'delivered', eventId: decodeURIComponent(url.split('=')[1]) } };
    if (url.includes('web_profile_info')) return { value: { data: { user: PROFILE } } };
    if (url.includes('reels_media')) return { value: reel() };
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
test('native open collects both sources, persists before intake and caches validated profile for 24h', async () => {
  const h = harness({ instagram: true });
  h.setResponder((url, req) => { if (req.method === 'POST') for (const e of JSON.parse(req.body).observations) assert([...h.files.keys()].some(k => k.endsWith('/event-' + e.eventId + '.json'))); return h.defaultResponder(url, req); });
  const result = await h.collector.run(); assert.equal(result.discovered, 2); assert.equal(result.received, 2); assert.equal(result.delivered, 0);
  assert.equal(h.calls.filter(c => c.url.includes('instagram.com')).length, 2);
  await h.collector.run(); assert.equal(h.calls.filter(c => c.url.includes('instagram.com')).length, 2); // cooldown
  h.advance(300000); await C.create(h.env).run();
  assert.equal(h.calls.filter(c => c.url.includes('web_profile_info')).length, 1);
  assert.equal(h.calls.filter(c => c.url.includes('reels_media')).length, 2);
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
  assert.equal((await C.create(h.env).collect('instagram')).status, 'backoff'); assert.equal(h.calls.length, 1);
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
test('Instagram budget counts identity and reel requests and does not exceed 30', async () => {
  const h = harness({ instagram: true });
  for (let i = 0; i < 29; i++) { assert.equal((await h.collector.collect('instagram')).status, 'found'); h.advance(300000); }
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

test('explicit Instagram reconnection replaces only a fresh session and preserves evidence', async () => {
  const h = harness({ instagram: true });
  h.setResponder(() => ({ code: 401, value: {} }));
  await h.collector.collect('instagram');
  h.advance(C.LIMITS.instagram);
  const before = new Map(h.files);
  h.setResponder(h.defaultResponder);
  const fresh = session(); fresh.cookies[0].value = 'renewed-private-session';
  const result = await h.collector.reconnectInstagram(fresh);
  assert.equal(result.sessionUpdated, true);
  assert.equal(result.sources.instagram.status, 'found');
  assert.equal(result.sources.instagram.found, 1);
  assert.equal(JSON.parse(h.keys.get(C.KEYS.instagram)).cookies[0].value, 'renewed-private-session');
  for (const [key, value] of before) assert.equal(h.files.get(key), value, 'history changed: ' + key);
  assert.ok(h.calls.every(c => !c.url.endsWith('/campanhasdeingresso')));
  assert.doesNotMatch(JSON.stringify(result), /renewed-private-session|synthetic-session|cookies|user_agent|h1\./);
});

test('same Instagram session and mismatched account never release authentication suspension', async () => {
  const h = harness({ instagram: true });
  h.setResponder(() => ({ code: 401, value: {} })); await h.collector.collect('instagram');
  const count = h.calls.length, previous = h.keys.get(C.KEYS.instagram);
  assert.equal((await h.collector.reconnectInstagram(session())).status, 'session_unchanged');
  assert.equal((await h.collector.collect('instagram')).reason, 'authentication_suspended');
  assert.equal(h.calls.length, count); assert.equal(h.keys.get(C.KEYS.instagram), previous);
  const old = session();
  h.keys.set(C.KEYS.instagram, JSON.stringify(old));
  const fresh = session(); fresh.cookies[0].value = 'renewed'; fresh.cookies[1].value = '200';
  assert.equal((await h.collector.reconnectInstagram(fresh)).reason, 'instagram_account_mismatch');
  fresh.cookies.pop();
  assert.equal((await h.collector.reconnectInstagram(fresh)).reason, 'instagram_account_unverified');
  assert.equal(h.calls.length, count);
});

test('failed suspension write rolls back session so explicit recovery can be retried', async () => {
  const h = harness({ instagram: true });
  h.setResponder(() => ({ code: 401, value: {} })); await h.collector.collect('instagram');
  h.advance(C.LIMITS.instagram);
  const previous = h.keys.get(C.KEYS.instagram), write = h.env.fm.writeString;
  let fail = true;
  h.env.fm.writeString = (p, text) => {
    if (fail && text.includes('"status":"reconnected"')) { fail = false; throw Error('disk unavailable'); }
    return write(p, text);
  };
  const fresh = session(); fresh.cookies[0].value = 'renewed';
  assert.equal((await h.collector.reconnectInstagram(fresh)).reason, 'reconnect_storage_failed');
  assert.equal(h.keys.get(C.KEYS.instagram), previous);
  assert.equal((await h.collector.collect('instagram')).reason, 'authentication_suspended');
  h.setResponder(h.defaultResponder);
  assert.equal((await h.collector.reconnectInstagram(fresh)).sources.instagram.status, 'found');
});

test('renewed Instagram session respects rate-limit deadline and existing daily request budget', async () => {
  for (const limited of ['rate', 'daily']) {
    const h = harness({ instagram: true });
    if (limited === 'rate') {
      h.setResponder(() => ({ code: 429, value: {}, headers: { 'Retry-After': '21600' } }));
      await h.collector.collect('instagram');
    } else {
      for (let i = 0; i < C.LIMITS.requests; i++) h.files.set('/docs/uol-hunter-collector-v1/request-instagram-budget-' + i + '.json', JSON.stringify({ at: NOW - C.LIMITS.instagram - 1 }));
    }
    const count = h.calls.length, before = new Map(h.files);
    const fresh = session(); fresh.cookies[0].value = 'renewed';
    const result = await h.collector.reconnectInstagram(fresh);
    assert.equal(result.sessionUpdated, true);
    assert.equal(result.sources.instagram.status, limited === 'rate' ? 'backoff' : 'daily_budget');
    assert.equal(h.calls.length, count);
    for (const [key, value] of before) assert.equal(h.files.get(key), value);
  }
});

test('overlapping collection and reconnection use the same lease', async () => {
  const h = harness({ instagram: true }); let release;
  h.setResponder(async () => { await new Promise(resolve => { release = resolve; }); return { value: HTML }; });
  const running = h.collector.run();
  while (!release) await new Promise(resolve => setImmediate(resolve));
  const fresh = session(); fresh.cookies[0].value = 'renewed';
  const original = h.keys.get(C.KEYS.instagram);
  assert.equal((await h.collector.reconnectInstagram(fresh)).status, 'busy');
  assert.equal(h.keys.get(C.KEYS.instagram), original);
  h.setResponder(h.defaultResponder); release(); await running;
});
