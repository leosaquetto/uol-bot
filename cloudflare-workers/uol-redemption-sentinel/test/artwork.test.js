import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { filterConfidentWords, isAllowedArtworkUrl, isAllowedOcrAsset, recognizeArtwork } from '../src/artwork.js';

const imageUrl = 'https://d310rdc9v8uuq.cloudfront.net/beneficios/abcd1234.png';
const png = Uint8Array.from([137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 0]);
const response = () => new Response(png, { headers: { 'content-type': 'image/png' } });
function fixture({ words = [{ text: 'ZAYN', confidence: 96 }], error, hang = false, asset } = {}) {
  const calls = { closes: 0, routes: [], contexts: [] };
  const page = {
    setDefaultTimeout() {},
    async goto(url) {
      calls.pageUrl = url;
      await calls.route({ request: () => ({ url: () => url, method: () => 'GET' }), fulfill: async value => { calls.html = value.body; } });
    },
    async addScriptTag(value) { calls.script = value; },
    async evaluate(fn, payload) {
      calls.ocrFunction = String(fn);
      calls.payload = payload;
      if (asset) await calls.route(asset);
      if (error) throw new Error(error);
      if (hang) return new Promise(() => {});
      return words;
    },
  };
  const browser = {
    async newContext(options) { calls.contexts.push(options); return { async route(pattern, callback) { calls.route = callback; calls.routes.push(pattern); }, async newPage() { return page; } }; },
    async close() { calls.closes++; },
  };
  return { calls, launchBrowser: async () => browser, browserBinding: {}, fetchImpl: async (url, options) => {
    if (isAllowedOcrAsset(url)) return new Response('/* fixed public OCR script */', { headers: { 'content-type': 'application/javascript' } });
    calls.fetch = { url, options }; return response();
  } };
}

test('public artwork URL and OCR assets allow only exact origins and versioned paths', () => {
  assert.equal(isAllowedArtworkUrl(imageUrl), true);
  assert.equal(isAllowedArtworkUrl(imageUrl.replace('d310rdc9v8uuq', 'ddrxgn8ucibei')), true);
  for (const bad of [imageUrl + '?token=x', imageUrl + '#x', imageUrl.replace('/beneficios/', '/private/'), imageUrl.replace('https:', 'http:'), imageUrl.replace('cloudfront.net', 'cloudfront.net.evil.test'), imageUrl.replace('d310rdc9v8uuq', 'unknown-distribution'), 'https://clube.uol.com.br/campanhasdeingresso/foo/resgatar']) assert.equal(isAllowedArtworkUrl(bad), false, bad);
  assert.equal(isAllowedOcrAsset('https://cdn.jsdelivr.net/npm/tesseract.js@6.0.1/dist/worker.min.js'), true);
  assert.equal(isAllowedOcrAsset('https://cdn.jsdelivr.net/npm/tesseract.js@latest/dist/worker.min.js'), false);
  assert.equal(isAllowedOcrAsset('https://clube.uol.com.br/'), false);
});

test('confidence filtering cannot autocorrect or infer the expected artist', () => {
  assert.deepEqual(filterConfidentWords([{ text: 'ZAYN', confidence: 94.9 }, { text: 'ZAVN', confidence: 98 }, { text: 'ZAYN', confidence: '99' }, { text: ' ZAYN ', confidence: 95 }, { text: 'bad', confidence: 101 }]), [{ text: 'ZAVN', confidence: 98 }, { text: 'ZAYN', confidence: 95 }]);
  assert.throws(() => filterConfidentWords(null), /OCR_OUTPUT_INVALID/);
});

test('OCR hashes exact fetched bytes and returns only confident words; closes fresh browser', async () => {
  const mock = fixture({ words: [{ text: 'ZAYN', confidence: 96 }, { text: 'guess', confidence: 70 }] });
  const result = await recognizeArtwork(imageUrl, mock);
  assert.equal(result.ok, true);
  assert.equal(result.text, 'ZAYN');
  assert.equal(result.hash, createHash('sha256').update(png).digest('hex'));
  assert.deepEqual(mock.calls.contexts, [{ serviceWorkers: 'block', acceptDownloads: false }]);
  assert.equal(mock.calls.fetch.options.redirect, 'manual');
  assert.equal(mock.calls.fetch.options.credentials, 'omit');
  assert.equal(mock.calls.closes, 1);
  assert.ok(mock.calls.html.includes("default-src 'none'"));
  assert.equal(mock.calls.ocrFunction.includes('ZAYN'), false);
  assert.equal(mock.calls.payload.langPath, 'https://tessdata.projectnaptha.com/4.0.0');
});

test('image redirects, oversized bodies, wrong MIME and missing binding stop before browser', async () => {
  for (const makeResponse of [() => new Response(null, { status: 302, headers: { Location: 'https://clube.uol.com.br/' } }), () => new Response(png, { headers: { 'content-type': 'image/png', 'content-length': String(4 * 1024 * 1024 + 1) } }), () => new Response('<svg></svg>', { headers: { 'content-type': 'image/svg+xml' } }), () => new Response('not png', { headers: { 'content-type': 'image/png' } })]) {
    let launches = 0;
    const result = await recognizeArtwork(imageUrl, { browserBinding: {}, fetchImpl: async () => makeResponse(), launchBrowser: async () => { launches++; } });
    assert.equal(result.ok, false);
    assert.equal(launches, 0);
  }
  assert.equal((await recognizeArtwork(imageUrl, {})).reason, 'OCR_BINDING_MISSING');
});

test('CDN raster MIME mismatch uses the validated bytes type without accepting arbitrary content', async () => {
  const mock = fixture();
  const original = mock.fetchImpl;
  mock.fetchImpl = async (...args) => isAllowedOcrAsset(args[0]) ? original(...args) : new Response(Uint8Array.from([255,216,255,224,0,16]), { headers: { 'content-type': 'image/png' } });
  const result = await recognizeArtwork(imageUrl, mock);
  assert.equal(result.ok, true);
  assert.equal(mock.calls.payload.type, 'image/jpeg');
});

test('cached OCR requires the same fetched bytes and never trusts the cached text field', async () => {
  const hash = createHash('sha256').update(png).digest('hex');
  const cached = { ok: true, hash, text: 'ZAYN', words: [{ text: 'MAROON', confidence: 98 }] };
  const same = fixture();
  same.launchBrowser = async () => { throw Error('unchanged bytes must not launch OCR'); };
  assert.equal((await recognizeArtwork(imageUrl, { ...same, cached })).text, 'MAROON');
  const changed = fixture({ words: [{ text: 'OTHER', confidence: 99 }] });
  const original = changed.fetchImpl;
  changed.fetchImpl = async (...args) => isAllowedOcrAsset(args[0]) ? original(...args) : new Response(Uint8Array.from([...png, 1]), { headers: { 'content-type': 'image/png' } });
  assert.equal((await recognizeArtwork(imageUrl, { ...changed, cached })).text, 'OTHER');
  assert.equal(changed.calls.closes, 1);
});

test('engine initialization failure returns no evidence and cannot expose untrusted diagnostic text', async () => {
  const mock = fixture({ words: { error: 'OCR_ENGINE_INIT', causeCode: 'private remote URL must never appear' } });
  const result = await recognizeArtwork(imageUrl, mock);
  assert.equal(result.ok, false);
  assert.deepEqual(result.words, []);
  assert.equal(result.causeCode, 'ocr_runtime_error');
  assert.equal(JSON.stringify(result).includes('private remote URL'), false);
  assert.equal(mock.calls.closes, 1);
});

test('streaming image size cap works when Content-Length is missing', async () => {
  const body = new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(4 * 1024 * 1024 + 1)); controller.close(); } });
  const result = await recognizeArtwork(imageUrl, { browserBinding: {}, fetchImpl: async () => new Response(body, { headers: { 'content-type': 'image/png' } }) });
  assert.equal(result.reason, 'ARTWORK_TOO_LARGE');
});

test('network guard refuses UOL requests and does not follow redirects from allowed assets', async () => {
  const attempted = [];
  const asset = { request: () => ({ method: () => 'GET', url: () => 'https://clube.uol.com.br/' }), abort: async (reason) => attempted.push(reason), fetch: async () => { throw new Error('must not fetch UOL'); } };
  const denied = fixture({ asset });
  assert.equal((await recognizeArtwork(imageUrl, denied)).reason, 'OCR_ASSET_REJECTED');
  assert.deepEqual(attempted, ['blockedbyclient']);
  let options;
  const redirected = fixture({ asset: { request: () => ({ method: () => 'GET', url: () => 'https://cdn.jsdelivr.net/npm/tesseract.js@6.0.1/dist/worker.min.js' }), abort: async () => {}, fetch: async (value) => { options = value; return { status: () => 302 }; }, fulfill: async () => { throw new Error('must not fulfill redirect'); } } });
  assert.equal((await recognizeArtwork(imageUrl, redirected)).reason, 'OCR_ASSET_REJECTED');
  assert.equal(options.maxRedirects, 0);
});

test('timeout and OCR errors always close the browser and never return successful evidence', async () => {
  const hang = fixture({ hang: true });
  const result = await recognizeArtwork(imageUrl, { ...hang, timeoutMs: 40 });
  assert.equal(result.reason, 'OCR_TIMEOUT');
  assert.equal(hang.calls.closes, 1);
  const failed = fixture({ error: 'private remote URL should not be reflected' });
  assert.equal((await recognizeArtwork(imageUrl, failed)).reason, 'OCR_FAILED');
  assert.equal(failed.calls.closes, 1);
  const noWords = fixture({ words: [{ text: 'Zayn', confidence: 94 }] });
  assert.equal((await recognizeArtwork(imageUrl, noWords)).reason, 'OCR_NO_CONFIDENT_WORDS');
});

test('a browser acquired after the operation deadline is closed without starting OCR', async () => {
  let closed = 0;
  let opened = 0;
  const result = await recognizeArtwork(imageUrl, {
    browserBinding: {}, fetchImpl: async url => isAllowedOcrAsset(url) ? new Response('/* script */', { headers: { 'content-type': 'application/javascript' } }) : response(), timeoutMs: 30,
    launchBrowser: async () => { await new Promise((resolve) => setTimeout(resolve, 50)); return { close: async () => { closed++; }, newContext: async () => { opened++; throw new Error('must not open'); } }; },
  });
  assert.equal(result.reason, 'OCR_TIMEOUT');
  await new Promise((resolve) => setTimeout(resolve, 35));
  assert.equal(closed, 1);
  assert.equal(opened, 0);
});
