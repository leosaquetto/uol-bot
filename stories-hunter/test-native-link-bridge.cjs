const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');

const host = fs.readFileSync(path.join(__dirname, 'native/host.js'), 'utf8');
const bridge = host.slice(host.indexOf('function readHunterLinkCheckCache('), host.indexOf('\nfunction installHunterBrowser('));
const canonical = require('./native/link-checks.js').canonicalTicketUrl;

test('native link queue pages every candidate, runs at most two requests, and stores only native evidence', async () => {
  const files = new Map();
  const rows = Array.from({ length: 205 }, (_, index) => ({ id: 'p' + index,
    link: 'https://clube.uol.com.br/campanhasdeingresso/p' + index, generation: '123' }));
  let active = 0, maximum = 0, checks = 0, writes = 0;
  const view = { hunterCurrentUser: 'gaoriel', notifications: [], async evaluateJavaScript(script, completion) {
    if (completion) {
      const offset = Number(/hunterActiveLinkCandidates\((\d+),100\)/.exec(script)[1]);
      return JSON.stringify(rows.slice(offset, offset + 100));
    }
    this.notifications.push(script);
  } };
  const context = vm.createContext({
    LINK_CHECK_FILE_NAME: 'checks.json', Date, Promise, JSON,
    FileManager: { local: () => ({ documentsDirectory: () => '/private', joinPath: (root, name) => root + '/' + name,
      fileExists: name => files.has(name), readString: name => files.get(name), writeString: (name, value) => { writes++; files.set(name, value); } }) },
    HunterLinkChecks: { canonicalTicketUrl: canonical, create: () => ({ async check(url, controls, previous) {
      assert.ok(url.startsWith('https://clube.uol.com.br/campanhasdeingresso/'));
      assert.equal(controls.length, rows.length);
      assert.equal(previous, null, 'WebView metadata cannot provide previous evidence');
      active++; maximum = Math.max(maximum, active); checks++;
      await new Promise(resolve => setImmediate(resolve));
      active--;
      return { status: 'available', checkedAt: '2026-10-19T12:00:00.000Z' };
    } }) },
    loadCurrentAccountPreference: account => account,
    loadSessionCookies: account => { assert.equal(account, 'gaoriel'); return []; },
    Request: class {}, Timer: { schedule: (_ms, _repeat, callback) => callback() },
    parseHunterReceiptHtml: () => null
  });
  vm.runInContext(bridge, context);
  await context.runNativeLinkChecks(view);
  assert.equal(checks, 205);
  assert.equal(maximum, 2);
  assert.equal(writes, Math.ceil(rows.length / 8), 'cache is persisted once per bounded batch');
  assert.equal(view.notifications.length, 1 + Math.ceil(rows.length / 8), 'WebView receives one replay and one update per batch');
  assert.equal(Object.keys(JSON.parse(files.get('/private/checks.json')).records).length, 205);
  assert.ok(view.notifications.some(code => code.includes('"cached":true')),
    'preflight native cache replay is identified separately from fresh probes');
  assert.ok(view.notifications.some(code => code.includes('"pending":0')));
});

test('native link queue flushes in-flight evidence on dismissal and revalidates it next opening', async () => {
  const files = new Map();
  const rows = Array.from({ length: 12 }, (_, index) => ({ id: 'p' + index,
    link: 'https://clube.uol.com.br/campanhasdeingresso/p' + index, generation: '123' }));
  let checks = 0, priorEvidence = 0, dismissedOnce = false;
  const view = { hunterCurrentUser: 'gaoriel', notifications: [], async evaluateJavaScript(script, completion) {
    if (completion) {
      const offset = Number(/hunterActiveLinkCandidates\((\d+),100\)/.exec(script)[1]);
      return JSON.stringify(rows.slice(offset, offset + 100));
    }
    this.notifications.push(script);
  } };
  const context = vm.createContext({
    LINK_CHECK_FILE_NAME: 'checks.json', Date, Promise, JSON,
    FileManager: { local: () => ({ documentsDirectory: () => '/private', joinPath: (root, name) => root + '/' + name,
      fileExists: name => files.has(name), readString: name => files.get(name), writeString: (name, value) => files.set(name, value) }) },
    HunterLinkChecks: { canonicalTicketUrl: canonical, create: () => ({ async check(_url, _controls, previous) {
      if (previous) priorEvidence++;
      checks++;
      await new Promise(resolve => setImmediate(resolve));
      if (!dismissedOnce) { dismissedOnce = true; view.hunterDismissed = true; }
      return { status: 'available', checkedAt: '2026-10-19T12:00:00.000Z' };
    } }) },
    loadCurrentAccountPreference: account => account,
    loadSessionCookies: () => [], Request: class {},
    Timer: { schedule: (_ms, _repeat, callback) => callback() },
    parseHunterReceiptHtml: () => null
  });
  vm.runInContext(bridge, context);
  await context.runNativeLinkChecks(view);
  assert.equal(checks, 2, 'both in-flight requests finish after dismissal');
  assert.equal(Object.keys(JSON.parse(files.get('/private/checks.json')).records).length, 2,
    'partial batch is stored even when UI can no longer receive a notification');
  assert.equal(view.notifications.length, 1, 'dismissed WebView receives no further update');

  view.hunterDismissed = false;
  await context.runNativeLinkChecks(view);
  assert.equal(checks, 14, 'next opening checks the entire current candidate set again');
  assert.equal(priorEvidence, 2, 'stored in-flight results are available for revalidation');
  assert.equal(Object.keys(JSON.parse(files.get('/private/checks.json')).records).length, 12);
  assert.ok(view.notifications.some(code => code.includes('"pending":0')));
});
