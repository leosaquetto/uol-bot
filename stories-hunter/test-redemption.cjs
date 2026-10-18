const { test } = require('node:test');
const assert = require('node:assert/strict');
const R = require('./redemption.js');
const CLUB = 'https://clube.uol.com.br';
const HISTORY = CLUB + '/perfil/beneficios';
const OFFER = CLUB + '/campanhasdeingresso/example';
const NOW = Date.parse('2026-10-18T12:00:00Z');
const IMAGE = 'https://img.uol.com.br/example.jpg';
const NAMES = { leosaquetto: 'Leonardo', gaoriel: 'Gabriel' };
function harness() {
  const keys = new Map(), calls = []; let sequence = 0;
  const histories = { leosaquetto: [], gaoriel: [] };
  let historyName = user => NAMES[user], responder;
  const offer = { url: OFFER, title: '2 INGRESSOS: exemplo', description: 'Descrição', imageUrl: IMAGE, available: true, redeemUrl: OFFER + '/resgatar', identityEvidence: { scoped: true, titleCount: 1, descriptionCount: 1, canonicalMatches: true, actionCount: 1, actionMatches: true } };
  const keychain = { contains: key => keys.has(key), get: key => keys.get(key), set: (key, value) => keys.set(key, value) };
  const env = { keychain, now: () => NOW, expectedName: NAMES, uuid: () => '00000000-0000-4000-8000-' + String(++sequence).padStart(12, '0'),
    loadCookies: user => [{ name: 'SESS', value: 'private-' + user, domain: '.uol.com.br', path: '/', secure: true, httpOnly: true, expires: NOW / 1000 + 3600 }, { name: 'PHPSESSID', value: user + '-club', domain: 'clube.uol.com.br', path: '/', hostOnly: true, secure: true, expires: -1 }],
    parseHistory: text => JSON.parse(text), parseOffer: text => JSON.parse(text),
    request: url => { const req = { async loadString() {
      assert.equal(req.method, 'GET'); assert.equal(req.timeoutInterval, 15); assert.equal(req.onRedirect(), null);
      const user = req.headers.Cookie.includes('private-gaoriel') ? 'gaoriel' : 'leosaquetto';
      calls.push({ url, user, headers: req.headers });
      const result = responder ? await responder(url, user, req) : null;
      if (result?.error) throw result.error;
      req.response = { statusCode: result?.status || 200, url: result?.url || url, headers: result?.headers || {} };
      if (result && Object.hasOwn(result, 'text')) return result.text;
      if (url === HISTORY) return JSON.stringify({ authenticated: true, historyScopeFound: true, hasPagination: false, signedInName: historyName(user), entries: histories[user] });
      if (url === OFFER) return JSON.stringify(offer);
      return '';
    } }; return req; } };
  const receipt = (id, changes = {}) => ({ id, url: HISTORY + '/' + id, title: offer.title, category: 'campanhasdeingresso', imageUrl: IMAGE, offerUrl: null, redemptionDate: null, ...changes });
  return { env, keys, calls, histories, offer, receipt, client: R.create(env), setResponder: value => { responder = value; }, setName: value => { historyName = value; } };
}
const mutationCalls = h => h.calls.filter(call => call.url.endsWith('/resgatar'));
function privateFree(value) { assert.doesNotMatch(JSON.stringify(value), /private-|PHPSESSID|SESS|cookies|Authorization|<html|password/); }

test('history isolates native cookie jars and never invents receipt dates', async () => {
  const h = harness(); h.histories.leosaquetto.push(h.receipt('leo-voucher')); h.histories.gaoriel.push(h.receipt('gab-voucher'));
  const leo = await h.client.history('leosaquetto'), gab = await h.client.history('gaoriel');
  assert.equal(leo.status, 'authenticated'); assert.equal(gab.status, 'authenticated');
  assert.equal(leo.records[0].id, 'leo-voucher'); assert.equal(gab.records[0].id, 'gab-voucher');
  assert.equal(leo.records[0].redemptionDate, null); assert.ok(!('timestamp' in leo.records[0]));
  assert.ok(h.calls[0].headers.Cookie.includes('private-leosaquetto')); assert.ok(!h.calls[0].headers.Cookie.includes('gaoriel'));
  privateFree(leo); privateFree(gab);
  const stored = JSON.parse(h.keys.get(R.COOKIE_KEY + 'gaoriel'));
  assert.equal(stored[0].httpOnly, true); assert.equal(stored[0].secure, true); assert.equal(stored[0].expires, NOW / 1000 + 3600);
});

test('wrong account identity fails before even fetching the offer or redeeming', async () => {
  const h = harness(); h.setName(() => 'Gabriel');
  const result = await h.client.redeem('leosaquetto', OFFER);
  assert.equal(result.status, 'identity_mismatch'); assert.equal(h.calls.length, 1); assert.equal(mutationCalls(h).length, 0);
  assert.equal(h.keys.has(R.ATTEMPT_KEY + 'leosaquetto'), false); privateFree(result);
});

test('missing or paginated account history cannot authorize redemption', async () => {
  const h = harness(); h.setResponder(url => url === HISTORY ? { text: JSON.stringify({ authenticated: true, historyScopeFound: true, hasPagination: true, signedInName: 'Leonardo', entries: [] }) } : null);
  const result = await h.client.redeem('leosaquetto', OFFER);
  assert.equal(result.reason, 'history_incomplete'); assert.equal(mutationCalls(h).length, 0);
});

test('overlapping calls durably reserve one mutation and preserve account independence', async () => {
  const h = harness(); let release;
  const pending = new Promise(resolve => { release = resolve; });
  h.setResponder(async (url, user) => {
    if (!url.endsWith('/resgatar')) return null;
    const reservation = JSON.parse(h.keys.get(R.ATTEMPT_KEY + user));
    assert.equal(reservation.status, 'reserved'); assert.equal(reservation.offerUrl, OFFER);
    if (user === 'leosaquetto') await pending;
    return { status: 302, headers: { Location: '/?error=max-redeem' } };
  });
  const first = h.client.redeem('leosaquetto', OFFER);
  while (mutationCalls(h).length < 1) await new Promise(resolve => setImmediate(resolve));
  const second = await R.create(h.env).redeem('leosaquetto', OFFER);
  assert.equal(second.status, 'uncertain'); assert.equal(mutationCalls(h).length, 1);
  const gab = await h.client.redeem('gaoriel', OFFER);
  assert.equal(gab.status, 'quota_used'); assert.equal(mutationCalls(h).length, 2);
  release(); const leo = await first; assert.equal(leo.status, 'quota_used');
  assert.notEqual(leo.attemptId, gab.attemptId);
});

test('HTTP 500 does not count as success; restart and absent receipt never resend', async () => {
  const h = harness(); h.setResponder(url => url.endsWith('/resgatar') ? { status: 500, text: 'private-password server failure' } : null);
  const result = await h.client.redeem('leosaquetto', OFFER);
  assert.equal(result.status, 'uncertain'); assert.equal(result.receipt, undefined); privateFree(result);
  const resumed = await R.create(h.env).redeem('leosaquetto', OFFER);
  assert.equal(resumed.status, 'uncertain'); assert.equal(resumed.attemptId, result.attemptId);
  assert.equal(mutationCalls(h).length, 1); assert.equal(h.calls.at(-1).url, HISTORY);
});

test('lost response retains reservation and reconciles only a new matching receipt', async () => {
  const h = harness(); h.histories.leosaquetto.push(h.receipt('old'));
  h.setResponder(url => url.endsWith('/resgatar') ? { error: new Error('private transport timeout') } : null);
  const result = await h.client.redeem('leosaquetto', OFFER);
  assert.equal(result.status, 'uncertain'); assert.equal(mutationCalls(h).length, 1);
  h.histories.leosaquetto.push(h.receipt('new-wrong-image', { imageUrl: 'https://img.uol.com.br/other.jpg' }));
  assert.equal((await R.create(h.env).redeem('leosaquetto', OFFER)).status, 'uncertain');
  h.histories.leosaquetto.push(h.receipt('new-match', { redemptionDate: '17/10/2026' }));
  const confirmed = await R.create(h.env).redeem('leosaquetto', OFFER);
  assert.equal(confirmed.status, 'confirmed'); assert.equal(confirmed.attemptId, result.attemptId);
  assert.equal(confirmed.receipt.id, 'new-match'); assert.equal(confirmed.receipt.redemptionDate, '17/10/2026'); assert.equal(confirmed.receipt.observedAt, NOW);
  assert.equal(mutationCalls(h).length, 1); privateFree(confirmed);
});

test('new receipt linked explicitly to the offer confirms despite changed display title', async () => {
  const h = harness(); h.setResponder(url => {
    if (url.endsWith('/resgatar')) { h.histories.leosaquetto.push(h.receipt('direct', { title: 'Título do voucher', offerUrl: OFFER, imageUrl: null })); return { status: 302, headers: { Location: '/perfil/beneficios/direct' } }; }
    return null;
  });
  const result = await h.client.redeem('leosaquetto', OFFER);
  assert.equal(result.status, 'confirmed'); assert.equal(result.receipt.id, 'direct'); assert.equal(result.receipt.redemptionDate, null); assert.equal(mutationCalls(h).length, 1);
});

test('ambiguous pair of new receipts stays uncertain and old matching voucher is insufficient', async () => {
  const h = harness(); h.histories.leosaquetto.push(h.receipt('old'));
  h.setResponder(url => { if (url.endsWith('/resgatar')) h.histories.leosaquetto.push(h.receipt('new-a'), h.receipt('new-b')); return null; });
  const result = await h.client.redeem('leosaquetto', OFFER);
  assert.equal(result.status, 'uncertain'); assert.equal(result.receipt, undefined);
});

test('explicit quota, authentication and sold out outcomes are recognized; arbitrary 200 stays uncertain', async () => {
  for (const [reply, status] of [
    [{ status: 302, headers: { Location: '/?error=max-redeem' } }, 'quota_used'],
    [{ status: 200, text: 'Limite de resgates excedido' }, 'quota_used'],
    [{ status: 302, headers: { Location: '/auth/uol/login' } }, 'auth_required'],
    [{ status: 200, text: 'Ingressos esgotados' }, 'sold_out'],
    [{ status: 200, text: 'ok' }, 'uncertain'],
    [{ status: 500, text: 'Limite de resgates excedido' }, 'uncertain'],
  ]) {
    const h = harness(); h.setResponder(url => url.endsWith('/resgatar') ? reply : null);
    const result = await h.client.redeem('leosaquetto', OFFER); assert.equal(result.status, status); assert.equal(result.receipt, undefined); privateFree(result);
  }
});

test('read redirects block mutation, external hosts and nested destinations before sending cookies', async () => {
  for (const location of ['https://evil.example/', OFFER + '/resgatar', 'https://conta.uol.com.br/login?dest=' + encodeURIComponent(OFFER + '/resgatar'), 'https://clube.uol.com.br.evil.example/perfil/beneficios']) {
    const h = harness(); h.setResponder(() => ({ status: 302, headers: { Location: location } }));
    const result = await h.client.history('leosaquetto'); assert.equal(result.reason, 'url_blocked'); assert.equal(h.calls.length, 1); privateFree(result);
  }
});

test('SSO restoration uses bounded manual reads and domain/path-specific cookies', async () => {
  const h = harness(); let reads = 0;
  h.env.loadCookies = user => [
    { name: 'SESS', value: 'private-' + user, domain: 'api.uol.com.br', path: '/oauth', hostOnly: true },
    { name: 'PHPSESSID', value: user + '-club', domain: 'clube.uol.com.br', path: '/perfil', hostOnly: true },
    { name: 'DNA', value: 'expired', domain: '.uol.com.br', expires: 1 },
  ];
  h.setResponder((url, user, req) => {
    assert.ok(!req.headers.Cookie.includes('expired'));
    if (url === HISTORY && ++reads === 1) return { status: 200, text: JSON.stringify({ authenticated: false }) };
    if (url === CLUB + '/auth/uol/login') { assert.equal(req.headers.Cookie, ''); return { status: 302, headers: { Location: 'https://api.uol.com.br/oauth/auth?response_type=code&client_id=test&redirect_uri=' + encodeURIComponent(CLUB + '/auth/uol') } }; }
    if (url.startsWith('https://api.uol.com.br/')) { assert.ok(req.headers.Cookie.includes('SESS=private-leosaquetto')); assert.ok(!req.headers.Cookie.includes('PHPSESSID')); return { status: 302, headers: { Location: CLUB + '/' } }; }
    if (url === CLUB + '/') return { status: 200, text: '<html>restored</html>' };
    if (url === HISTORY) assert.equal(req.headers.Cookie, 'PHPSESSID=leosaquetto-club');
    return null;
  });
  const result = await R.create(h.env).history('leosaquetto');
  assert.equal(result.status, 'authenticated'); assert.equal(mutationCalls(h).length, 0); assert.equal(reads, 2);
});

test('unverified offer, malformed journal and rejected Keychain reservation cannot dispatch', async () => {
  const badOffer = harness(); badOffer.offer.identityEvidence.canonicalMatches = false;
  assert.equal((await badOffer.client.redeem('leosaquetto', OFFER)).reason, 'offer_unverified'); assert.equal(mutationCalls(badOffer).length, 0);
  const broken = harness(); broken.keys.set(R.ATTEMPT_KEY + 'leosaquetto', '{}');
  assert.equal((await broken.client.redeem('leosaquetto', OFFER)).reason, 'attempt_journal_invalid'); assert.equal(mutationCalls(broken).length, 0);
  const storage = harness(); const original = storage.env.keychain.set;
  storage.env.keychain.set = (key, value) => { if (key.startsWith(R.ATTEMPT_KEY)) throw new Error('storage error'); original(key, value); };
  const result = await storage.client.redeem('leosaquetto', OFFER); assert.equal(result.status, 'uncertain'); assert.equal(mutationCalls(storage).length, 0);
});

test('oversized response is rejected, secrets sanitized, and automatic native redirect is refused', async () => {
  for (const reply of [{ text: 'a'.repeat(1048577) }, { text: '', headers: { 'Content-Length': '1048577' } }, { text: '', url: 'https://evil.example/private-cookie' }]) {
    const h = harness(); h.setResponder(() => reply);
    const result = await h.client.redeem('leosaquetto', OFFER); assert.equal(result.status, 'unavailable'); assert.equal(mutationCalls(h).length, 0); privateFree(result);
  }
});

test('a delayed mutation response cannot overwrite a receipt proved by concurrent history', async () => {
  const h = harness(); let release;
  h.setResponder(url => url.endsWith('/resgatar') ? new Promise(resolve => { release = () => resolve({ status: 500, text: 'late server error' }); }) : null);
  const pending = h.client.redeem('leosaquetto', OFFER);
  while (!release) await new Promise(resolve => setImmediate(resolve));
  h.histories.leosaquetto.push(h.receipt('proved'));
  const read = await R.create(h.env).history('leosaquetto'); assert.equal(read.attempt.status, 'confirmed');
  release(); const result = await pending;
  assert.equal(result.status, 'confirmed'); assert.equal(result.receipt.id, 'proved');
  assert.equal(JSON.parse(h.keys.get(R.ATTEMPT_KEY + 'leosaquetto')).status, 'confirmed'); assert.equal(mutationCalls(h).length, 1);
});

test('native runtime needs neither URL nor URLSearchParams browser globals', async () => {
  const vm = require('node:vm'), fs = require('node:fs');
  const sandbox = { module: { exports: {} }, URL: undefined, URLSearchParams: undefined };
  vm.runInNewContext(fs.readFileSync(require.resolve('./redemption.js'), 'utf8'), sandbox);
  const h = harness(); h.setResponder(url => url.endsWith('/resgatar') ? { status: 302, headers: { Location: '/?error=max-redeem' } } : null);
  const result = await sandbox.module.exports.create(h.env).redeem('leosaquetto', OFFER);
  assert.equal(result.status, 'quota_used'); assert.equal(mutationCalls(h).length, 1);
});

test('HTTP 500 history does not trigger SSO and unknown account or relative offer never dispatches', async () => {
  const h = harness(); h.setResponder(() => ({ status: 500, text: 'unavailable' }));
  assert.equal((await h.client.history('leosaquetto')).reason, 'history_unavailable'); assert.equal(h.calls.length, 1);
  assert.equal((await h.client.redeem('unconfigured', OFFER)).reason, 'account_invalid');
  assert.equal((await h.client.redeem('leosaquetto', '/campanhasdeingresso/example')).reason, 'offer_url_invalid');
  assert.equal(h.calls.length, 1);
});

test('opaque async identity verification supports both accounts without civil names in configuration', async () => {
  const h = harness(); delete h.env.expectedName;
  const checked = [];
  h.env.parseHistory = async text => { const value = JSON.parse(text); return { ...value, signedInName: undefined, identityDigest: value.signedInName === 'Leonardo' ? 'opaque-leo' : 'opaque-gab' }; };
  h.env.verifyIdentity = async (user, parsed) => { checked.push(user); return parsed.identityDigest === (user === 'leosaquetto' ? 'opaque-leo' : 'opaque-gab'); };
  h.setResponder(url => url.endsWith('/resgatar') ? { status: 302, headers: { Location: '/?error=max-redeem' } } : null);
  assert.equal((await R.create(h.env).redeem('leosaquetto', OFFER)).status, 'quota_used');
  assert.equal((await R.create(h.env).redeem('gaoriel', OFFER)).status, 'quota_used');
  assert.deepEqual(checked, ['leosaquetto', 'gaoriel']); assert.equal(mutationCalls(h).length, 2);
});

test('wrong or missing identity digest, truthy verifier result and verifier failure never authorize mutation', async () => {
  for (const mode of ['wrong', 'missing', 'truthy', 'throws']) {
    const h = harness(); delete h.env.expectedName;
    h.env.parseHistory = async text => ({ ...JSON.parse(text), signedInName: undefined, identityDigest: mode === 'missing' ? undefined : 'wrong-account-digest' });
    h.env.verifyIdentity = async (_user, parsed) => {
      if (mode === 'throws') throw new Error('private identity configuration');
      if (mode === 'truthy') return 'true';
      return parsed.identityDigest === 'expected-account-digest';
    };
    const result = await R.create(h.env).redeem('leosaquetto', OFFER);
    assert.equal(result.status, 'identity_mismatch'); assert.equal(h.calls.length, 1); assert.equal(mutationCalls(h).length, 0);
    assert.equal(h.keys.has(R.ATTEMPT_KEY + 'leosaquetto'), false); privateFree(result);
  }
});

test('authenticated history after SSO must pass the opaque verifier too', async () => {
  const h = harness(); delete h.env.expectedName; let reads = 0, verified = 0;
  // The restoration endpoint normally redirects to Home. Model that manually.
  h.setResponder(url => {
    if (url === HISTORY && ++reads === 1) return { text: JSON.stringify({ authenticated: false }) };
    if (url === CLUB + '/auth/uol/login') return { status: 302, headers: { Location: '/' } };
    if (url === CLUB + '/') return { text: '<html>Restored</html>' };
    return null;
  });
  h.env.verifyIdentity = async () => { verified++; return false; };
  const result = await R.create(h.env).redeem('leosaquetto', OFFER);
  assert.equal(result.status, 'identity_mismatch'); assert.equal(reads, 2); assert.equal(verified, 1); assert.equal(mutationCalls(h).length, 0);
});

test('revoked or absent personal permission blocks history and redemption even with stored account cookies', async () => {
  for (const approval of [false, undefined, null, 'true', new Error('private permission service error')]) {
    const h = harness();
    const cookies = JSON.stringify(await h.env.loadCookies('leosaquetto'));
    h.keys.set(R.COOKIE_KEY + 'leosaquetto', cookies);
    h.env.loadCookies = () => { throw new Error('stored cookies must not be read or imported'); };
    let checks = 0;
    h.env.authorizeAccount = async user => { checks++; assert.equal(user, 'leosaquetto'); if (approval instanceof Error) throw approval; return approval; };
    const client = R.create(h.env);
    const history = await client.history('leosaquetto'), redemption = await client.redeem('leosaquetto', OFFER);
    for (const result of [history, redemption]) {
      assert.deepEqual(result, { status: 'account_access_required', account: 'leosaquetto', reason: 'account_access_required' }); privateFree(result);
    }
    assert.equal(checks, 2); assert.equal(h.calls.length, 0); assert.equal(h.keys.get(R.COOKIE_KEY + 'leosaquetto'), cookies);
    assert.equal(h.keys.has(R.ATTEMPT_KEY + 'leosaquetto'), false);
  }
  const allowed = harness(); allowed.env.authorizeAccount = async () => true;
  assert.equal((await R.create(allowed.env).history('leosaquetto')).status, 'authenticated');
});
