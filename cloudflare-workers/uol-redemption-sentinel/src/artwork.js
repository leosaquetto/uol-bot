const MAX_IMAGE_BYTES = 4 * 1024 * 1024;
const MAX_PIXELS = 8_000_000;
const MIN_CONFIDENCE = 95;
// Exact image distributions observed on official UOL catalog/detail pages.
const IMAGE_HOSTS = new Set(['d310rdc9v8uuq.cloudfront.net', 'ddrxgn8ucibei.cloudfront.net']);
const SCRIPT = 'https://cdn.jsdelivr.net/npm/tesseract.js@6.0.1/dist/tesseract.min.js';
const WORKER = 'https://cdn.jsdelivr.net/npm/tesseract.js@6.0.1/dist/worker.min.js';
const CORE = 'https://cdn.jsdelivr.net/npm/tesseract.js-core@6.0.0';
const EMBEDDED_CORE = `${CORE}/tesseract-core-lstm.wasm.js`;
const LANGUAGE = 'https://tessdata.projectnaptha.com/4.0.0';
const OCR_PAGE = 'https://uol-redemption-sentinel.leosaquetto.workers.dev/__isolated_ocr';
const OCR_HTML = `<!doctype html><html><head><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src https://cdn.jsdelivr.net 'unsafe-eval' 'wasm-unsafe-eval'; worker-src blob: https://cdn.jsdelivr.net; connect-src https://cdn.jsdelivr.net https://tessdata.projectnaptha.com; img-src blob: data:; base-uri 'none'; form-action 'none'"></head><body></body></html>`;
const ASSETS = new Set([SCRIPT, WORKER, `${LANGUAGE}/eng.traineddata.gz`, ...['', '-simd', '-lstm', '-simd-lstm'].flatMap((variant) => [`${CORE}/tesseract-core${variant}.wasm.js`, `${CORE}/tesseract-core${variant}.wasm`])]);
const KNOWN_REASONS = new Set(['ARTWORK_URL_REJECTED', 'ARTWORK_HTTP_REJECTED', 'ARTWORK_TOO_LARGE', 'ARTWORK_TYPE_REJECTED', 'ARTWORK_EMPTY', 'OCR_TIMEOUT', 'OCR_BINDING_MISSING', 'OCR_ASSET_REJECTED', 'OCR_NO_CONFIDENT_WORDS', 'OCR_IMAGE_DIMENSIONS', 'OCR_OUTPUT_INVALID', 'OCR_ENGINE_MISSING', 'OCR_IMAGE_DECODE', 'OCR_ENGINE_INIT', 'OCR_ENGINE_RECOGNIZE']);
const CAUSE_CODES = new Set(['ocr_engine_missing','ocr_origin_storage','ocr_wasm','ocr_language_archive','ocr_language_data','ocr_asset_load','ocr_csp','ocr_runtime_error']);

export function isAllowedArtworkUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && IMAGE_HOSTS.has(url.hostname) && !url.port && !url.username && !url.password && !url.search && !url.hash && /^\/beneficios\/[a-zA-Z0-9_-]+\.(?:png|jpe?g|webp)$/i.test(url.pathname);
  } catch { return false; }
}

export function isAllowedOcrAsset(value) { return ASSETS.has(value); }

// Confidence is an OCR score, not a probability. No expected artist is supplied
// to the OCR engine or used to correct uncertain characters.
export function filterConfidentWords(input) {
  if (!Array.isArray(input) || input.length > 10_000) throw new Error('OCR_OUTPUT_INVALID');
  return input.filter((word) => typeof word?.text === 'string' && word.text.trim() && word.text.length <= 200 && typeof word.confidence === 'number' && Number.isFinite(word.confidence) && word.confidence >= MIN_CONFIDENCE && word.confidence <= 100)
    .map((word) => ({ text: word.text.replace(/\s+/g, ' ').trim(), confidence: word.confidence }));
}

async function imageBytes(url, fetchImpl, signal) {
  if (!isAllowedArtworkUrl(url)) throw new Error('ARTWORK_URL_REJECTED');
  const response = await fetchImpl(url, { method: 'GET', redirect: 'manual', credentials: 'omit', signal, headers: {
    Accept: 'image/png,image/jpeg,image/webp', Referer: 'https://clube.uol.com.br/',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
  } });
  if (response.status !== 200 || response.redirected) throw Object.assign(new Error('ARTWORK_HTTP_REJECTED'), { httpStatus: response.status });
  const type = (response.headers.get('content-type') || '').split(';')[0].trim().toLowerCase();
  if (!['image/png', 'image/jpeg', 'image/webp'].includes(type)) throw new Error('ARTWORK_TYPE_REJECTED');
  const declared = Number(response.headers.get('content-length'));
  if (declared > MAX_IMAGE_BYTES) throw new Error('ARTWORK_TOO_LARGE');
  const reader = response.body?.getReader();
  if (!reader) throw new Error('ARTWORK_EMPTY');
  const chunks = [];
  let size = 0;
  try {
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      size += part.value.byteLength;
      if (size > MAX_IMAGE_BYTES) {
        await reader.cancel();
        throw new Error('ARTWORK_TOO_LARGE');
      }
      chunks.push(part.value);
    }
  } finally { reader.releaseLock(); }
  if (!size) throw new Error('ARTWORK_EMPTY');
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  const png = bytes.length >= 8 && [137, 80, 78, 71, 13, 10, 26, 10].every((value, index) => bytes[index] === value);
  const jpeg = bytes.length >= 3 && bytes[0] === 255 && bytes[1] === 216 && bytes[2] === 255;
  const webp = bytes.length >= 12 && String.fromCharCode(...bytes.slice(0, 4)) === 'RIFF' && String.fromCharCode(...bytes.slice(8, 12)) === 'WEBP';
  // UOL's CDN can label JPEG bytes as image/png. Accept only a known raster
  // signature and use its actual type; never trust the extension or MIME alone.
  if (!png && !jpeg && !webp) throw new Error('ARTWORK_TYPE_REJECTED');
  return { bytes, type: png ? 'image/png' : jpeg ? 'image/jpeg' : 'image/webp' };
}

function base64(bytes) {
  let binary = '';
  for (let start = 0; start < bytes.length; start += 8192) binary += String.fromCharCode(...bytes.subarray(start, start + 8192));
  return btoa(binary);
}

async function ocrScript(url, fetchImpl, signal) {
  if (![WORKER, EMBEDDED_CORE].includes(url)) throw new Error('OCR_ASSET_REJECTED');
  const response = await fetchImpl(url, { method: 'GET', redirect: 'manual', credentials: 'omit', signal });
  if (response.status !== 200 || response.redirected || !/^(?:application|text)\/javascript\b/i.test(response.headers.get('content-type') || '')) throw new Error('OCR_ASSET_REJECTED');
  const reader = response.body?.getReader();
  if (!reader) throw new Error('OCR_ASSET_REJECTED');
  let size = 0, text = '';
  const decoder = new TextDecoder();
  try {
    for (;;) {
      const part = await reader.read(); if (part.done) break;
      size += part.value.byteLength;
      if (size > 6 * 1024 * 1024) { await reader.cancel(); throw new Error('OCR_ASSET_REJECTED'); }
      text += decoder.decode(part.value, { stream: true });
    }
    return text + decoder.decode();
  } finally { reader.releaseLock(); }
}

async function defaultLaunch(binding) {
  const { launch } = await import('@cloudflare/playwright');
  return launch(binding, { keep_alive: 60_000 });
}

/** OCRs only the supplied public artwork. Does not open UOL or use its session. */
export async function recognizeArtwork(imageUrl, { browserBinding, fetchImpl = fetch, launchBrowser = defaultLaunch, timeoutMs = 60_000, cached = null } = {}) {
  let browser;
  let hash = null;
  let stage = 'image';
  let stopped = false;
  let blockedAsset = false;
  let timeout;
  let fetchTimeout;
  const budget = Number.isFinite(timeoutMs) ? Math.max(10, Math.min(timeoutMs, 60_000)) : 60_000;
  const cleanupMs = Math.min(1000, Math.max(1, Math.floor(budget / 10)));
  const abort = new AbortController();
  const failure = (reason) => ({ ok: false, reason, text: '', words: [], hash });
  const close = async (target) => {
    if (!target) return;
    let timer;
    try { await Promise.race([Promise.resolve().then(() => target.close()), new Promise((resolve) => { timer = setTimeout(resolve, cleanupMs); })]); }
    catch { /* The session also has a bounded idle lifetime. */ }
    finally { clearTimeout(timer); }
  };
  try {
    if (!isAllowedArtworkUrl(imageUrl)) return failure('ARTWORK_URL_REJECTED');
    if (!browserBinding) return failure('OCR_BINDING_MISSING');
    const work = async () => {
      const imageAbort = new AbortController();
      fetchTimeout = setTimeout(() => imageAbort.abort(), 15_000);
      let image;
      try { image = await imageBytes(imageUrl, fetchImpl, AbortSignal.any([abort.signal, imageAbort.signal])); }
      finally { clearTimeout(fetchTimeout); }
      const digest = await crypto.subtle.digest('SHA-256', image.bytes);
      hash = [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
      if(cached?.ok===true && cached.hash===hash){
        const words=filterConfidentWords(cached.words);
        if(words.length)return {ok:true,hash,words,text:words.map(w=>w.text).join(' ')};
      }
      if (stopped) throw new Error('OCR_TIMEOUT');
      stage = 'assets';
      // Browser Run's dedicated worker cannot reliably fetch its own script.
      // The upstream core supports an already-present TesseractCore global.
      const [workerSource, coreSource] = await Promise.all([ocrScript(WORKER, fetchImpl, abort.signal), ocrScript(EMBEDDED_CORE, fetchImpl, abort.signal)]);
      if (stopped) throw new Error('OCR_TIMEOUT');
      stage = 'launch';
      const launched = await launchBrowser(browserBinding);
      if (stopped) { await close(launched); throw new Error('OCR_TIMEOUT'); }
      browser = launched;
      stage = 'context';
      const context = await browser.newContext({ serviceWorkers: 'block', acceptDownloads: false });
      // Context routing also covers dedicated OCR worker requests. Never follow
      // redirects from a permitted URL to an unpermitted URL.
      await context.route('**/*', async (route) => {
        const request = route.request();
        if (!stopped && request.method() === 'GET' && request.url() === OCR_PAGE) {
          await route.fulfill({ status: 200, contentType: 'text/html', body: OCR_HTML });
          return;
        }
        if (stopped || request.method() !== 'GET' || !isAllowedOcrAsset(request.url())) {
          blockedAsset = true;
          await route.abort('blockedbyclient');
          return;
        }
        try {
          const response = await route.fetch({ maxRedirects: 0, timeout: 15_000 });
          if (response.status() !== 200) { blockedAsset = true; await route.abort('blockedbyclient'); return; }
          await route.fulfill({ response });
        } catch { blockedAsset = true; await route.abort('failed').catch(() => {}); }
      });
      const page = await context.newPage();
      stage = 'assets';
      page.setDefaultTimeout(Math.min(15_000, budget));
      // Serve an empty HTTPS document locally to Chromium. Blob workers need a
      // regular origin; no request reaches the sentinel endpoint or UOL.
      await page.goto(OCR_PAGE, { waitUntil: 'domcontentloaded' });
      await page.addScriptTag({ url: SCRIPT });
      stage = 'recognize';
      const result = await page.evaluate(async ({ imageBase64, type, workerPath, corePath, langPath, maxPixels, workerSource }) => {
        let worker;
        let workerUrl;
        let step = 'decode';
        if (!globalThis.Tesseract?.createWorker) return { error: 'OCR_ENGINE_MISSING', causeCode: 'ocr_engine_missing' };
        try {
        const binary = atob(imageBase64);
        const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
        const blob = new Blob([bytes], { type });
        const bitmap = await createImageBitmap(blob);
        try {
          if (!bitmap.width || !bitmap.height || bitmap.width * bitmap.height > maxPixels) throw new Error('OCR_IMAGE_DIMENSIONS');
        } finally { bitmap.close(); }
          step = 'initialize';
          workerUrl = URL.createObjectURL(new Blob([workerSource], { type: 'application/javascript' }));
          worker = await globalThis.Tesseract.createWorker('eng', 1, { workerPath: workerUrl, workerBlobURL: false, corePath, langPath, cacheMethod: 'none', gzip: true });
          step = 'recognize';
          await worker.setParameters({ tessedit_pageseg_mode: '11' });
          const { data } = await worker.recognize(blob, {}, { text: false, blocks: true });
          const words = [];
          // Version 6 can omit layout levels. Read explicit word arrays through
          // known containers without treating paragraph/block confidence as words.
          const nodes = Array.isArray(data.blocks) ? [...data.blocks] : [];
          let visited = 0;
          while (nodes.length && visited++ < 10_000) {
            const node = nodes.pop();
            for (const word of node.words || []) words.push({ text: word.text, confidence: word.confidence });
            for (const key of ['blocks', 'paragraphs', 'lines']) if (Array.isArray(node[key])) nodes.push(...node[key]);
          }
          return words;
        } catch (error) {
          // Tesseract rejects some jobs with a string, rather than an Error.
          const message = String(error?.message || error || '');
          const causeCode = /indexeddb|IDB|database|origin.*null/i.test(message) ? 'ocr_origin_storage'
            : /wasm|webassembly/i.test(message) ? 'ocr_wasm'
            : /incorrect header|gzip|inflate/i.test(message) ? 'ocr_language_archive'
            : /language|traineddata|tessdata/i.test(message) ? 'ocr_language_data'
            : /fetch|network|load|importscript/i.test(message) ? 'ocr_asset_load'
            : /content.security|CSP|script-src/i.test(message) ? 'ocr_csp' : 'ocr_runtime_error';
          return { error: error?.message === 'OCR_IMAGE_DIMENSIONS' ? 'OCR_IMAGE_DIMENSIONS'
            : step === 'decode' ? 'OCR_IMAGE_DECODE' : step === 'initialize' ? 'OCR_ENGINE_INIT' : 'OCR_ENGINE_RECOGNIZE', causeCode,
            assetCode: message.includes(workerPath) || (workerUrl && message.includes(workerUrl)) ? 'worker' : message.includes(corePath) ? 'core' : message.includes(langPath) ? 'language' : 'unknown' };
        } finally { if (worker) await worker.terminate().catch(() => {}); if (workerUrl) URL.revokeObjectURL(workerUrl); }
      }, { imageBase64: base64(image.bytes), type: image.type, workerPath: WORKER, corePath: CORE, langPath: LANGUAGE, maxPixels: MAX_PIXELS, workerSource: `${coreSource}\n${workerSource}` });
      if (blockedAsset) throw new Error('OCR_ASSET_REJECTED');
      if (result && !Array.isArray(result) && KNOWN_REASONS.has(result.error)) return { ...failure(result.error), stage, causeCode: CAUSE_CODES.has(result.causeCode) ? result.causeCode : 'ocr_runtime_error',
        assetCode: ['worker','core','language'].includes(result.assetCode)?result.assetCode:'unknown' };
      const words = filterConfidentWords(result);
      return words.length ? { ok: true, text: words.map((word) => word.text).join(' '), words, hash }
        : { ...failure('OCR_NO_CONFIDENT_WORDS'), totalWords: result.length,
            maxConfidence: Math.max(0, ...result.map(w => Number.isFinite(w?.confidence) ? w.confidence : 0)) };
    };
    return await Promise.race([work(), new Promise((_, reject) => { timeout = setTimeout(() => { stopped = true; abort.abort(); reject(new Error('OCR_TIMEOUT')); }, budget - cleanupMs); })]);
  } catch (error) {
    const causeCode = /429|rate.limit|concurrent.*limit/i.test(error?.message||'') ? 'browser_rate_limited'
      : /not.*(?:supported|implemented)/i.test(error?.message||'') ? 'unsupported_api'
      : /(?:content.security|script-src|CSP)/i.test(error?.message||'') ? 'ocr_csp'
      : /Tesseract.*(?:undefined|defined)/i.test(error?.message||'') ? 'ocr_engine_missing' : 'ocr_runtime_error';
    return { ...failure(blockedAsset ? 'OCR_ASSET_REJECTED' : KNOWN_REASONS.has(error?.message) ? error.message : 'OCR_FAILED'), stage, causeCode,
      ...(Number.isInteger(error?.httpStatus)?{httpStatus:error.httpStatus}:{}) };
  } finally {
    stopped = true;
    abort.abort();
    clearTimeout(timeout);
    clearTimeout(fetchTimeout);
    await close(browser);
  }
}
