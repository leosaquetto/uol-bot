import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { clickStoryGate, collectBrowser, hasTargetStoryData, installStoryObserver, pageSnapshot, targetPage } from './instagram_browser.mjs';

const target = 'https://www.instagram.com/stories/clubeuol/';
const payload = JSON.stringify({ reels_media: [{ user: { username: 'clubeuol' }, items: [{ pk: '4011415238472326441' }] }] });
function element(text, options = {}) {
  return { innerText: text, textContent: text, disabled: false, clicks: 0,
    getAttribute: () => null, getBoundingClientRect: () => ({ width: 100, height: 40 }),
    click() { this.clicks++; }, ...options };
}
function pageRun(fn, { url = target, buttons = [], scripts = [], login = false, observer } = {}) {
  return vm.runInNewContext(`(${fn.toString()})()`, {
    URL, TextEncoder, location: { href: url }, window: { __uolStoryCapture: observer },
    getComputedStyle: node => node.style || { display: 'block', visibility: 'visible', opacity: '1' },
    document: { querySelector: () => login ? {} : null,
      querySelectorAll: selector => selector.startsWith('script') ? scripts.map(textContent => ({ textContent })) : buttons },
  });
}

test('gate clicks exactly one visible localized Story control', () => {
  for (const text of ['Ver story', 'View story']) {
    const button = element(text);
    assert.equal(pageRun(clickStoryGate, { buttons: [button, element('Seguir')] }), 'clicked');
    assert.equal(button.clicks, 1);
  }
});
test('gate rejects unrelated, ambiguous, hidden, disabled and login controls', () => {
  const cases = [
    { buttons: [element('Ver stories')] },
    { buttons: [element('Ver story'), element('View story')] },
    { buttons: [element('Ver story', { disabled: true })] },
    { buttons: [element('Ver story', { style: { display: 'none' } })] },
    { buttons: [element('Ver story')], url: 'https://www.instagram.com/stories/other/' },
    { buttons: [element('Ver story')], login: true },
  ];
  for (const options of cases) {
    assert.notEqual(pageRun(clickStoryGate, options), 'clicked');
    assert.ok(options.buttons.every(button => button.clicks === 0));
  }
});
test('only exact target URL and scoped target structure count as loaded', () => {
  assert.ok(targetPage(target + '4011415238472326441/'));
  assert.ok(!targetPage('https://www.instagram.com.evil.test/stories/clubeuol/'));
  assert.ok(hasTargetStoryData(payload));
  assert.ok(!hasTargetStoryData(' {"reels_media":[],"username":"clubeuol"}'));
  assert.ok(!hasTargetStoryData('{"user":{"username":"other"},"items":[]}'));
  const snapshot = pageRun(pageSnapshot, { scripts: ['{"reels_media":[]}'], observer: { scripts: [payload] } });
  assert.equal(snapshot.hasTargetData, true);
  assert.ok(snapshot.scripts.includes(payload));
});

function fakeBrowser({ initialData = false, gate = 'clicked', status = 200, stuckContextClose = false } = {}) {
  let clicked = false, evaluations = 0, responseListener, closed = 0, contextClosed = 0;
  const page = {
    on: (_, fn) => { responseListener = fn; },
    goto: async () => {
      if (status !== 200) responseListener({ url: () => target, status: () => status,
        headers: () => ({ 'retry-after': '7200' }), request: () => ({ resourceType: () => 'document' }) });
      return { status: () => status };
    },
    url: () => target, waitForTimeout: async () => {},
    evaluate: async fn => {
      if (fn.name === 'clickStoryGate') { evaluations++; clicked = gate === 'clicked'; return gate; }
      return { scripts: initialData || clicked ? [payload] : ['{"reels_media":[]}'], hasTargetData: initialData || clicked };
    },
  };
  const context = { addInitScript: async () => {}, addCookies: async () => {}, route: async () => {},
    newPage: async () => page, close: async () => {
      contextClosed++;
      if (stuckContextClose) await new Promise(() => {});
    } };
  const chromium = { launch: async () => ({ newContext: async () => context, close: async () => { closed++; } }) };
  return { chromium, stats: () => ({ evaluations, closed, contextClosed }) };
}
const input = { userAgent: 'fixture', cookies: [{ name: 'sessionid', value: 'PRIVATE', domain: '.instagram.com', path: '/' }] };
test('missing data clicks once and reads fresh page; existing data never clicks', async () => {
  for (const initialData of [false, true]) {
    const fake = fakeBrowser({ initialData });
    const result = await collectBrowser(input, fake.chromium);
    assert.equal(result.status, 'loaded');
    assert.equal(result.gateClicked, !initialData);
    assert.equal(fake.stats().evaluations, initialData ? 0 : 1);
    assert.equal(fake.stats().closed, 1);
    assert.ok(!JSON.stringify(result).includes('PRIVATE'));
  }
});
test('HTTP 429 and auth close browser without gate click', async () => {
  for (const status of [429, 401, 403]) {
    const fake = fakeBrowser({ status });
    const result = await collectBrowser(input, fake.chromium);
    assert.equal(result.status, status === 429 ? 'rate_limited' : 'auth_required');
    assert.equal(fake.stats().evaluations, 0);
    assert.equal(fake.stats().closed, 1);
  }
});
test('a stalled context close cannot prevent closing the browser', { timeout: 1000 }, async () => {
  const fake = fakeBrowser({ initialData: true, stuckContextClose: true });
  const result = await collectBrowser(input, fake.chromium);
  assert.equal(result.status, 'loaded');
  assert.equal(fake.stats().closed, 1);
  assert.equal(fake.stats().contextClosed, 0);
});
test('observed JSON removes only the for-loop prefix and preserves numeric IDs', () => {
  class FakeXHR {
    addEventListener(_, listener) { this.loaded = listener; }
    send() { this.loaded(); }
  }
  const window = { fetch: () => Promise.resolve() };
  vm.runInNewContext(`(${installStoryObserver.toString()})()`, {
    window, XMLHttpRequest: FakeXHR, URL, TextEncoder, location: { href: target },
  });
  const raw = '{"reels_media":[{"user":{"username":"clubeuol"},"items":[{"pk":4011415238472326441}]}]}';
  for (const prefix of ['for (;;);', 'while (1);']) {
    const request = new FakeXHR();
    Object.assign(request, { responseURL: 'https://www.instagram.com/graphql/query',
      responseType: 'text', status: 200, responseText: prefix + raw });
    request.send();
  }
  const observer = window.__uolStoryCapture;
  assert.equal(observer.scripts[0], raw);
  assert.equal(observer.scripts[1], 'while (1);' + raw);
  assert.equal(pageRun(pageSnapshot, { observer }).hasTargetData, true);
  assert.equal(pageRun(pageSnapshot, { observer: { scripts: [observer.scripts[1]] } }).hasTargetData, false);
});
