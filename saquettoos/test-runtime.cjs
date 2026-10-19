'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function library(stubs = {}) {
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, 'native/runtime.js'), 'utf8'), { module, ...stubs }, { filename: 'saquettoos/native/runtime.js' });
  return module.exports;
}

function loaderFixture({ names = ['Bilhete U\u0301nico.js'], original = 'Script.complete(); Script.setShortcutOutput("ignored");', allowedHash = 'reviewed', actualHash = 'reviewed', downloaded = true } = {}) {
  const events = [];
  const fm = {
    listContents() { return names; }, documentsDirectory() { return '/mock/scripts'; }, joinPath(a, b) { return a + '/' + b; },
    isFileDownloaded() { return downloaded; }, async downloadFileFromiCloud(filename) { events.push(['download', filename]); },
    readString(filename) { events.push(['read', filename]); return original; }
  };
  let completionCalls = 0, outputCalls = 0;
  const runtime = library();
  const loader = runtime.createLoader({ fm, catalog: [{ id: 'bilhete-unico', scriptName: 'Bilhete Único' }],
    compatibility: { 'bilhete-unico': { sha256: [allowedHash] } }, hash(value) { events.push(['hash', value]); return actualHash; },
    script: { name: () => 'saquettoOS', complete() { completionCalls++; }, setShortcutOutput() { outputCalls++; } },
    globals: { args: { queryParameters: { private: 'not-inherited' } }, module: { filename: '/wrong', exports: { inherited: true } }, config: { runsInNotification: true } }
  });
  return { loader, events, get completionCalls() { return completionCalls; }, get outputCalls() { return outputCalls; } };
}

function runtimeFixture({ id = 'pilltracker', localFiles = {}, html = '<html><head></head><body><main>Fixture</main></body></html>', beforeOpen = async () => {} } = {}) {
  const calls = [], writes = [], requests = [], timers = [], files = new Map(Object.entries(localFiles));
  let common, uuidCounter = 0, readbackMismatch = false, responseStatus = 200;
  let panelHandler = async (action, payload) => ({ action, payload });
  let dispose = async () => {};
  let responseHeaders = { 'Content-Type': 'application/json', ETag: 'fixture', 'Set-Cookie': 'must-not-return', Authorization: 'must-not-return' };
  const adapter = { create(deps) {
    common = deps;
    return { async open() { await beforeOpen(); return { html, handle(action, payload) { return panelHandler(action, payload); }, dispose() { return dispose(); } }; } };
  } };
  const lib = library({ SaquettoNativeApps: adapter, SaquettoMediaApp: adapter, SaquettoWebviewApps: adapter });
  const local = {
    documentsDirectory() { return '/mock/state'; }, joinPath(a, b) { return a + '/' + b; }, fileExists(filename) { return files.has(filename); },
    readString(filename) { return readbackMismatch && writes.length ? 'mismatched readback' : files.get(filename); },
    writeString(filename, value) { writes.push({ filename, value }); files.set(filename, value); }
  };
  const runtime = lib.create({
    fm: { listContents() { return []; }, documentsDirectory() { return '/mock/scripts'; } }, local,
    catalog: [{ id, name: 'Fixture App', scriptName: 'Fixture App' }], compatibility: {}, hash: () => 'fixture', script: {}, globals: {}, theme: {},
    uuid: () => 'instance-' + (++uuidCounter),
    async shell(code) {
      const match = /^window\.(saquettoShowApp|saquettoFrameMessage)\(([\s\S]*)\);$/.exec(code);
      if (!match) throw new Error('Unexpected shell command');
      const payload = JSON.parse(match[2]); calls.push({ kind: match[1], payload });
      return match[1] === 'saquettoShowApp' ? payload.instanceId : true;
    },
    timer(ms, callback) { const timer = { ms, callback, invalidated: false, invalidate() { this.invalidated = true; } }; timers.push(timer); return timer; },
    request(url) {
      const req = { url, async load() { req.loaded = true; req.response = { statusCode: responseStatus, headers: responseHeaders }; return { toBase64String() { return Buffer.from('{"fixture":true}').toString('base64'); } }; } };
      requests.push(req); return req;
    },
    nativeWebView: class {}, openURL: async () => { throw new Error('No real URL may be opened in this test'); }
  });
  return {
    runtime, calls, writes, requests, timers, files,
    get common() { return common; }, get instanceId() { return calls.filter(call => call.kind === 'saquettoShowApp').at(-1)?.payload.instanceId; },
    set panelHandler(value) { panelHandler = value; }, set dispose(value) { dispose = value; }, set readbackMismatch(value) { readbackMismatch = value; },
    set responseStatus(value) { responseStatus = value; }, set responseHeaders(value) { responseHeaders = value; },
    payload(request) { return { appId: id, instanceId: this.instanceId, request }; },
    action(action, payload = {}) { return runtime.dispatch('appAction', this.payload({ id: 'request', action, payload })); }
  };
}

test('loader verifies the original fingerprint before transformation or execution', async () => {
  const f = loaderFixture({ original: 'not valid JavaScript', actualHash: 'different' });
  let transformed = false;
  await assert.rejects(f.loader.load('bilhete-unico', '({})', () => { transformed = true; return ''; }), /adapter_source_changed/);
  assert.equal(transformed, false); assert.equal(f.completionCalls, 0); assert.equal(f.outputCalls, 0);
  assert.deepEqual(f.events.map(event => event[0]), ['read', 'hash']);
});

test('loader preserves the original Unicode filename and neutralizes parent Script, args and module context', async () => {
  const f = loaderFixture({ downloaded: false });
  const result = await f.loader.load('bilhete-unico', '({name:Script.name(),filename:module.filename,args,config,exports:module.exports})', original => original);
  assert.equal(result.name, 'Bilhete U\u0301nico');
  assert.equal(result.filename, '/mock/scripts/Bilhete U\u0301nico.js');
  assert.equal(result.args.shortcutParameter, null); assert.equal(Object.keys(result.args.queryParameters).length, 0);
  assert.equal(result.args.urls.length, 0); assert.equal(result.args.images.length, 0);
  assert.equal(result.config.runsInApp, true); assert.equal(result.config.runsInNotification, false);
  assert.equal(Object.keys(result.exports).length, 0);
  assert.equal(f.completionCalls, 0); assert.equal(f.outputCalls, 0);
  assert.deepEqual(f.events.map(event => event[0]), ['download', 'read', 'hash']);
});

test('loader rejects missing scripts, ambiguous NFC names and invalid transforms', async () => {
  await assert.rejects(loaderFixture({ names: [] }).loader.load('bilhete-unico', '{}', value => value), /app_not_installed/);
  await assert.rejects(loaderFixture({ names: ['Bilhete Único.js', 'Bilhete U\u0301nico.js'] }).loader.load('bilhete-unico', '{}', value => value), /script_name_conflict/);
  await assert.rejects(loaderFixture().loader.load('bilhete-unico', '{}', () => null), /adapter_source_changed/);
});

test('render round-trips real HTML safely through the shell JSON', async () => {
  const f = runtimeFixture(); await f.runtime.open('pilltracker');
  const call = f.calls.find(item => item.kind === 'saquettoShowApp');
  assert.ok(call.payload.html.startsWith('<html><head>'), 'Rendered HTML must contain real tags, not literal backslash-u escapes');
  assert.ok(call.payload.html.includes('<main>Fixture</main>'));
  assert.ok(call.payload.html.includes('window.SaquettoApp'));
  assert.equal(f.runtime.activeId, 'pilltracker');
});

test('runtime rejects foreign app and stale instance requests and invalid close sessions', async () => {
  const f = runtimeFixture(); await f.runtime.open('pilltracker');
  const good = f.payload({ action: 'read', payload: {} });
  await assert.rejects(f.runtime.dispatch('appAction', { ...good, appId: 'other' }), /app_instance_changed/);
  await assert.rejects(f.runtime.dispatch('appAction', { ...good, instanceId: 'stale' }), /app_instance_changed/);
  await assert.rejects(f.runtime.close({ appId: 'pilltracker', instanceId: 'stale' }), /app_instance_changed/);
  assert.equal(f.runtime.activeId, 'pilltracker');
  const result = await f.action('read', { key: 'fixture' }); assert.equal(result.action, 'read');
  assert.equal(f.runtime.shouldAllowRequest({ url: 'about:srcdoc' }), true);
  assert.equal(f.runtime.shouldAllowRequest({ url: 'https://unknown.example/' }), false);
});

test('storage rejects dangerous keys and oversize writes before touching disk', async () => {
  const f = runtimeFixture(); await f.runtime.open('pilltracker');
  for (const key of ['__proto__', 'constructor', 'prototype', 'x'.repeat(161)]) await assert.rejects(f.action('storage.set', { key, value: 'x' }), /storage_key_invalid/);
  await assert.rejects(f.action('storage.set', { key: 'large', value: 'x'.repeat(2 * 1024 * 1024) }), /storage_full/);
  assert.equal(f.writes.length, 0);
  assert.equal((await f.action('storage.set', { key: 'theme', value: 'dark' })).status, 'saved');
  assert.equal(JSON.parse(f.writes[0].value).theme, 'dark');
  await f.action('storage.remove', { key: 'theme' }); assert.equal(Object.keys(JSON.parse(f.writes.at(-1).value)).length, 0);
});

test('failed storage readback blocks subsequent writes instead of claiming saved', async () => {
  const f = runtimeFixture(); await f.runtime.open('pilltracker'); f.readbackMismatch = true;
  await assert.rejects(f.action('storage.set', { key: 'theme', value: 'dark' }), /app_storage_unverified/);
  assert.equal(f.writes.length, 1);
  f.readbackMismatch = false;
  await assert.rejects(f.action('storage.clear'), /app_storage_unverified/);
  assert.equal(f.writes.length, 1);
});

test('invalid persisted app storage fails closed without opening an interface', async () => {
  for (const value of ['not JSON', '[]', '{"secret":2}']) {
    const f = runtimeFixture({ localFiles: { '/mock/state/saquettoos.appstate.pilltracker.json': value } });
    await assert.rejects(f.runtime.open('pilltracker'), /app_storage_unreadable/); assert.equal(f.calls.length, 0);
  }
});

test('evaluation responses complete while another app action is awaiting them', async () => {
  const f = runtimeFixture(); await f.runtime.open('pilltracker');
  f.panelHandler = async action => action === 'needs-eval' ? { value: await f.common.evalFrame('2 + 2') } : null;
  const pendingAction = f.action('needs-eval');
  const evaluation = f.calls.find(call => call.kind === 'saquettoFrameMessage').payload;
  const received = await f.runtime.dispatch('appEvalResult', { appId: 'pilltracker', instanceId: f.instanceId, id: evaluation.id, value: 4 });
  assert.equal(received.status, 'received'); assert.equal((await pendingAction).value, 4);
  assert.equal(f.timers[0].invalidated, true);
  await assert.rejects(f.runtime.dispatch('appEvalResult', { appId: 'pilltracker', instanceId: f.instanceId, id: evaluation.id, value: 4 }), /evaluation_invalid/);
});

test('UOL proxy allows only exact HTTPS hosts, safe read methods and credential-free headers', async () => {
  const f = runtimeFixture({ id: 'clube-uol' }); await f.runtime.open('clube-uol');
  const rejected = [
    { url: 'http://clube.uol.com.br/data', method: 'GET' },
    { url: 'https://clube.uol.com.br.evil.example/data', method: 'GET' },
    { url: 'https://user:pass@clube.uol.com.br/data', method: 'GET' },
    { url: 'https://clube.uol.com.br:443/data', method: 'GET' },
    { url: 'https://clube.uol.com.br/data', method: 'POST' },
    { url: 'https://clube.uol.com.br/data', method: 'GET', headers: { Cookie: 'private=fixture' } },
    { url: 'https://clube.uol.com.br/data', method: 'GET', headers: { Authorization: 'Bearer fixture' } },
    { url: 'https://clube.uol.com.br/data', method: 'GET', headers: { Accept: 'json\r\nCookie: fixture' } }
  ];
  for (const payload of rejected) await assert.rejects(f.action('web.fetch', payload), /network_not_allowed/);
  assert.equal(f.requests.filter(request => request.loaded).length, 0);
  const result = await f.action('web.fetch', { url: 'https://clube.uol.com.br/data', method: 'GET', headers: { Accept: 'application/json', 'If-None-Match': 'fixture' } });
  const sent = f.requests.at(-1); assert.equal(sent.onRedirect(), null); assert.equal(sent.method, 'GET');
  assert.equal(sent.headers.Accept, 'application/json'); assert.equal(sent.timeoutInterval, 30);
  assert.equal(result.status, 200); assert.equal(result.headers['Set-Cookie'], undefined); assert.equal(result.headers.Authorization, undefined);
  assert.equal(Buffer.from(result.body, 'base64').toString(), '{"fixture":true}');
  const other = runtimeFixture(); await other.runtime.open('pilltracker');
  await assert.rejects(other.action('web.fetch', { url: 'https://clube.uol.com.br/data', method: 'GET' }), /network_not_allowed/);
});

test('UOL proxy rejects redirect responses even on an allowed host', async () => {
  const f = runtimeFixture({ id: 'clube-uol' }); await f.runtime.open('clube-uol'); f.responseStatus = 302;
  await assert.rejects(f.action('web.fetch', { url: 'https://clube.uol.com.br/data', method: 'GET' }), /redirect_not_allowed/);
  f.responseStatus = 304;
  assert.equal((await f.action('web.fetch', { url: 'https://clube.uol.com.br/data', method: 'HEAD' })).status, 304);
});

test('shutdown during opening drains adapter, never renders late and rejects later opens', async () => {
  let release, disposed = 0;
  const f = runtimeFixture({ beforeOpen: () => new Promise(resolve => { release = resolve; }) });
  f.dispose = async () => { disposed++; };
  const opening = f.runtime.open('pilltracker');
  const failedOpen = assert.rejects(opening, /app_closed/);
  const stopped = f.runtime.shutdown();
  release();
  await failedOpen; await stopped;
  assert.equal(f.calls.length, 0); assert.equal(disposed, 1); assert.equal(f.runtime.activeId, null);
  await assert.rejects(f.runtime.open('pilltracker'), /app_closed/);
});
