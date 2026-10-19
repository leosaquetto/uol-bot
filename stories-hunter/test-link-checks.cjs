const test = require('node:test');
const assert = require('node:assert/strict');
const HunterLinkChecks = require('./native/link-checks.js');

const base = Date.parse('2026-10-19T12:00:00.000Z');
const link = code => `https://clube.uol.com.br/campanhasdeingresso/${code}`;
const validOffer = url => ({ url, title: 'Show', description: 'Apresentação confirmada',
  available: true, soldOut: false, redeemUrl: url + '/resgatar',
  identityEvidence: { scoped: true, titleCount: 1, descriptionCount: 1, canonicalMatches: true,
    actionCount: 1, actionMatches: true } });
const soldOutOffer = url => ({ url, title: 'Show', description: 'Ingressos esgotados',
  available: false, soldOut: true, redeemUrl: null,
  identityEvidence: { scoped: true, titleCount: 1, descriptionCount: 1, canonicalMatches: true,
    actionCount: 0, actionMatches: false } });

function harness(responses, options = {}) {
  let time = options.time ?? base;
  const queue = [...responses], requests = [], pauses = [];
  const env = {
    now: () => time,
    pause: async ms => { pauses.push(ms); time += ms; },
    request: url => {
      const req = { response: null, async loadString() {
        const next = queue.shift();
        assert.ok(next, `unexpected request: ${url}`);
        if (next.error) throw next.error;
        this.response = { statusCode: next.code, url, headers: next.headers || {},
          ...(next.responseUrl ? { url: next.responseUrl } : {}) };
        return next.body ?? '';
      } };
      requests.push({ url, req });
      return req;
    },
    parseOffer: options.parseOffer || ((_, url) => validOffer(url)),
  };
  return { checker: HunterLinkChecks.create(env), requests, pauses,
    advance: ms => { time += ms; }, time: () => new Date(time).toISOString() };
}

test('canonical ticket validation rejects forged origins, actions and unsafe paths', async () => {
  assert.equal(HunterLinkChecks.canonicalTicketUrl(link('pRs-Inimigos') + '?utm_source=app'), link('pRs-Inimigos'));
  for (const unsafe of [
    'http://clube.uol.com.br/campanhasdeingresso/pRs',
    'https://clube.uol.com.br.evil.test/campanhasdeingresso/pRs',
    'https://user@clube.uol.com.br/campanhasdeingresso/pRs',
    link('pRs') + '/resgatar', link('pRs') + '#private',
    'https://clube.uol.com.br/campanhasdeingresso/%2e%2e/pRs',
    'https://clube.uol.com.br/campanhasdeingresso/pRs\nCookie:secret',
  ]) assert.equal(HunterLinkChecks.canonicalTicketUrl(unsafe), '', unsafe);
  const h = harness([]);
  assert.equal((await h.checker.check({ url: 'https://evil.test/pRs' })).status, 'unknown');
  assert.equal(h.requests.length, 0);
});

test('verified campaign 200 clears link-unavailable state and confines cookies to UOL', async () => {
  const h = harness([{ code: 200, body: '<main>show</main>' }]);
  const result = await h.checker.check({ url: link('pRs'), cookieHeader: 'session=private' });
  assert.deepEqual(result, { status: 'available', checkedAt: '2026-10-19T12:00:00.000Z' });
  assert.equal(h.requests[0].url, link('pRs'));
  assert.equal(h.requests[0].req.method, 'GET');
  assert.equal(h.requests[0].req.timeoutInterval, 4);
  assert.equal(h.requests[0].req.onRedirect(), null);
  assert.equal(h.requests[0].req.headers.Cookie, 'session=private');
});

test('verified campaign 200 distinguishes sold-out page from redeemable action', async () => {
  const soldOut = harness([{ code: 200, body: '<main>esgotado</main>' }],
    { parseOffer: (_, url) => soldOutOffer(url) });
  assert.deepEqual(await soldOut.checker.check(link('pRs')),
    { status: 'sold_out', checkedAt: '2026-10-19T12:00:00.000Z' });

  const falsePositive = harness([{ code: 200, body: '<main>login</main>' }],
    { parseOffer: (_, url) => ({ ...soldOutOffer(url), identityEvidence: {
      ...soldOutOffer(url).identityEvidence, canonicalMatches: false } }) });
  assert.equal((await falsePositive.checker.check(link('pRs'))).status, 'unknown');

  const noRedeemAction = harness([{ code: 200, body: '<main>show</main>' }],
    { parseOffer: (_, url) => ({ ...validOffer(url), redeemUrl: null }) });
  assert.equal((await noRedeemAction.checker.check(link('pRs'))).status, 'unknown');

  const soldOutWordsWithAction = harness([{ code: 200, body: '<main>Uma data esgotada; outras abertas</main>' }],
    { parseOffer: (_, url) => ({ ...soldOutOffer(url), redeemUrl: url + '/resgatar',
      identityEvidence: { ...soldOutOffer(url).identityEvidence,
        actionCount: 1, actionMatches: true } }) });
  assert.equal((await soldOutWordsWithAction.checker.check(link('pRs'))).status, 'unknown');
});

test('two direct gone results five seconds apart and a distinct verified control confirm unavailability', async () => {
  const h = harness([{ code: 404 }, { code: 410 }, { code: 200, body: '<main>control</main>' }]);
  const result = await h.checker.check({ url: link('pRs'), cookieHeader: 'session=private' }, [link('pRc')]);
  assert.deepEqual(result, { status: 'unavailable', checkedAt: '2026-10-19T12:00:05.000Z',
    unavailableAt: '2026-10-19T12:00:05.000Z', goneCount: 2,
    firstGoneAt: '2026-10-19T12:00:00.000Z' });
  assert.deepEqual(h.pauses, [5000]);
  assert.deepEqual(h.requests.map(item => item.url), [link('pRs'), link('pRs'), link('pRc')]);
  assert.equal(h.requests[2].req.headers.Cookie, 'session=private');
});

test('a parser-confirmed sold-out campaign is a healthy control for a missing page', async () => {
  const h = harness([{ code: 404 }, { code: 410 }, { code: 200, body: '<main>esgotado</main>' }],
    { parseOffer: (_, url) => soldOutOffer(url) });
  assert.equal((await h.checker.check(link('pRs'), [link('pRc')])).status, 'unavailable');
});

test('a page recovered as sold out after one 404 is reported as sold out', async () => {
  const h = harness([{ code: 404 }, { code: 200, body: '<main>esgotado</main>' }],
    { parseOffer: (_, url) => soldOutOffer(url) });
  assert.deepEqual(await h.checker.check(link('pRs'), [link('pRc')]),
    { status: 'sold_out', checkedAt: '2026-10-19T12:00:05.000Z' });
  assert.equal(h.requests.length, 2);
});

test('redirect to Home counts as gone, but login redirect and ambiguous 200 remain unknown', async () => {
  const home = harness([{ code: 302, headers: { Location: '/' } },
    { code: 302, headers: { location: 'https://clube.uol.com.br/campanhasdeingresso' } },
    { code: 200, body: '<main>control</main>' }]);
  assert.equal((await home.checker.check(link('pRs'), [link('pRc')])).status, 'unavailable');
  const login = harness([{ code: 302, headers: { Location: '/auth/uol/login' } }]);
  assert.equal((await login.checker.check(link('pRs'), [link('pRc')])).status, 'unknown');
  assert.equal(login.requests.length, 1);
  const ambiguous = harness([{ code: 200, body: '<main>login</main>' }], { parseOffer: () => ({
    url: link('pRs'), title: 'Login', description: '', requiresLogin: true,
    identityEvidence: { scoped: false },
  }) });
  assert.equal((await ambiguous.checker.check(link('pRs'))).status, 'unknown');
});

test('temporary HTTP, timeout and failed control cannot mark unavailable', async () => {
  for (const response of [{ code: 403 }, { code: 429 }, { code: 503 },
    { error: new Error('timeout') }, { code: 200, responseUrl: 'https://clube.uol.com.br/' },
    { code: 404, body: '<title>Just a moment</title><div class="challenge">Wait</div>' }]) {
    const h = harness([response]);
    const result = await h.checker.check(link('pRs'), [link('pRc')]);
    assert.equal(result.status, 'unknown');
    assert.equal(h.requests.length, 1);
  }
  const outage = harness([{ code: 404 }, { code: 404 }, { code: 503 }]);
  const result = await outage.checker.check(link('pRs'), [link('pRc')]);
  assert.deepEqual(result, { status: 'unknown', checkedAt: '2026-10-19T12:00:05.000Z',
    goneCount: 2, firstGoneAt: '2026-10-19T12:00:00.000Z' });
  const noDistinctControl = harness([{ code: 404 }, { code: 404 }]);
  assert.equal((await noDistinctControl.checker.check(link('pRs'), [link('pRs')])).status, 'unknown');
  assert.equal(noDistinctControl.requests.length, 2);
});

test('recent prior negative can be confirmed; stale or too-early evidence needs another probe', async () => {
  const recent = harness([{ code: 410 }, { code: 200, body: '<main>control</main>' }], { time: base + 6000 });
  const confirmed = await recent.checker.check(link('pRs'), [link('pRc')],
    { goneCount: 1, firstGoneAt: new Date(base).toISOString() });
  assert.equal(confirmed.status, 'unavailable');
  assert.equal(recent.pauses.length, 0);
  const stale = harness([{ code: 404 }, { code: 404 }, { code: 200, body: '<main>control</main>' }],
    { time: base + 130000 });
  const staleResult = await stale.checker.check(link('pRs'), [link('pRc')],
    { goneCount: 1, firstGoneAt: new Date(base).toISOString() });
  assert.equal(staleResult.status, 'unavailable');
  assert.deepEqual(stale.pauses, [5000]);
  const early = harness([{ code: 404 }, { code: 404 }, { code: 200, body: '<main>control</main>' }],
    { time: base + 2000 });
  assert.equal((await early.checker.check(link('pRs'), [link('pRc')],
    { goneCount: 1, firstGoneAt: new Date(base).toISOString() })).status, 'unavailable');
  assert.deepEqual(early.pauses, [3000]);
});
