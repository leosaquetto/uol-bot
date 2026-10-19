'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'native/host.js'), 'utf8');
const main = source.slice(source.indexOf('async function saquettoMain()'));
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
async function until(predicate, message) {
  const deadline = Date.now() + 500;
  while (!predicate()) {
    if (Date.now() >= deadline) assert.fail(message);
    await new Promise(resolve => setImmediate(resolve));
  }
}

function harness(options = {}) {
  const presentation = deferred(), nativeAction = deferred();
  const events = [], evaluations = [], requests = [], alerts = [], resolutions = [], dispatches = [];
  const timers = new Set(), callbacks = new Set(), files = new Map();
  const queue = [...(options.queue || [])];
  let view, runtimeOptions, closed = false, complete = false, done = false, error;
  let shellLoaded = false, presentationSettled = false;
  const state = () => ({ preferences: { appearance: 'dark', material: 'glass' }, profile: { status: 'approved' }, assets: { logo: 'synthetic-image-data' } });
  const browser = {
    document: { getElementById: id => shellLoaded && id === 'shell' ? { getBoundingClientRect: () => ({ width: 390, height: 844 }) } : null },
    __SAQUETTO_QUEUE: queue,
    __SAQUETTO_WAKE: null,
  };
  browser.window = browser;
  const browserContext = vm.createContext(browser);
  function installShell() {
    browser.saquettoSetState = () => { events.push('state'); };
    browser.saquettoNative = () => {};
    browser.__SAQUETTO_RENDERED = true;
    browser.saquettoResolve = (id, result) => { resolutions.push({ id, result }); };
  }
  class WebView {
    constructor() { view = this; }
    async loadHTML(html, baseURL) {
      const guard = this.shouldAllowRequest;
      const request = { url: options.bootstrapURL || 'about:blank' };
      const allowed = typeof guard !== 'function' || guard(request);
      requests.push({ phase: 'load', guard, allowed, html, baseURL });
      events.push('load');
      shellLoaded = allowed && html.includes('id="shell"') && !options.emptyDocument;
      if (shellLoaded) installShell();
    }
    evaluateJavaScript(code, useCallback = false) {
      evaluations.push({ code, useCallback, afterDismiss: closed, beforePresentationSettled: !presentationSettled });
      events.push('evaluate');
      if (!useCallback) {
        if (options.evaluate) return options.evaluate(code);
        try { return Promise.resolve(vm.runInContext(code, browserContext)); }
        catch (failure) { return Promise.reject(failure); }
      }
      const result = deferred();
      callbacks.add(result);
      browser.completion = value => { callbacks.delete(result); result.resolve(value); };
      try { vm.runInContext(code, browserContext); }
      catch (failure) { callbacks.delete(result); result.reject(failure); }
      return result.promise;
    }
    present() { events.push('present'); return presentation.promise; }
  }
  class Alert {
    addAction() {}
    async presentAlert() { alerts.push({ title: this.title, message: this.message }); events.push('alert'); }
  }
  const runtime = {
    nativeActive: false,
    async waitForNative() { events.push('native_wait'); await nativeAction.promise; },
    shouldAllowRequest(request) { return request.url === 'about:blank' || request.url === 'about:srcdoc'; },
    async shutdown() { events.push('shutdown'); },
  };
  const host = {
    async state() { return state(); },
    async start() { events.push('start'); return options.start ? options.start.promise : state(); },
    attachRuntime(value) { assert.equal(value, runtime); },
    async dispatch(action, payload) {
      dispatches.push({ action, payload });
      if (action === 'openApp') {
        runtime.nativeActive = true;
        try { return await nativeAction.promise; }
        finally { runtime.nativeActive = false; }
      }
      if (action === 'nativeAction') return nativeAction.promise;
      if (action === 'appEvalResult') { nativeAction.resolve({ status: 'finished' }); return { status: 'received' }; }
      return { status: 'finished' };
    },
  };
  const fm = { isFileStoredIniCloud: () => false, documentsDirectory: () => '/local', joinPath: (a, b) => a + '/' + b,
    writeString: (name, value) => files.set(name, value) };
  const context = {
    SAQUETTO_VERSION: 'test', SAQUETTO_API: 'https://synthetic.example', SAQUETTO_CATALOG: [], SAQUETTO_COMPATIBILITY: {}, SAQUETTO_HTML: '<main id="shell">__SAQUETTO_INITIAL_JSON__</main>',
    SaquettoHost: { create: () => host, safeJSON: value => JSON.stringify(value) },
    SaquettoUpdater: { fingerprint: () => '' },
    SaquettoRuntime: { create: value => { runtimeOptions = value; return runtime; } },
    FileManager: { local: () => fm, iCloud: () => fm }, Keychain: {},
    Script: { name: () => 'saquettoOS', complete: () => { complete = true; events.push('complete'); } },
    args: {}, config: {}, module: { filename: '/local/saquettoOS.js' },
    WebView, Alert, Device: { isUsingDarkAppearance: () => true },
    UUID: { string: () => 'synthetic-id' }, Safari: { open: async () => {} }, Request: class {},
    Timer: { schedule(milliseconds, repeats, callback) {
      assert.equal(repeats, false);
      assert.ok(milliseconds > 0);
      const timer = { invalidate() { timers.delete(timer); }, fire() {
        if (!timers.delete(timer)) return;
        if (milliseconds === 800) presentationSettled = true;
        callback();
      } };
      timers.add(timer); events.push('timer');
      // Advance startup only; idle bridge timers remain controlled by dismissal.
      if (!evaluations.length) setImmediate(() => timer.fire());
      return timer;
    } },
  };
  const execution = vm.runInNewContext('(async () => {\n' + main + '\n})()', context);
  execution.then(() => { done = true; }, failure => { error = failure; done = true; });
  return {
    events, evaluations, requests, alerts, resolutions, dispatches, timers, callbacks, files,
    get view() { return view; }, get runtimeOptions() { return runtimeOptions; },
    get complete() { return complete; }, get done() { return done; }, get error() { return error; },
    dismiss() { if (!closed) { closed = true; events.push('dismiss'); presentation.resolve(); } },
    settleNative() { nativeAction.resolve({ status: 'finished' }); },
    async finished() { await until(() => done, 'saquettoMain did not finish'); if (error) throw error; },
  };
}

test('initial HTML loads before runtime navigation guard; subsequent requests remain restricted', async t => {
  const h = harness({ bootstrapURL: 'file:///private/Scriptable/bootstrap/index.html' }); t.after(() => h.dismiss());
  await until(() => h.evaluations.length || h.done, 'initial shell was not evaluated');
  assert.equal(h.error, undefined);
  assert.equal(h.requests[0].guard, undefined, 'initial local document must not be filtered as an app navigation');
  assert.equal(h.requests[0].allowed, true);
  assert.equal(typeof h.view.shouldAllowRequest, 'function');
  assert.equal(h.view.shouldAllowRequest({ url: 'https://untrusted.example/' }), false);
  assert.equal(h.view.shouldAllowRequest({ url: 'about:blank' }), true);
  assert.ok(h.events.indexOf('present') < h.events.indexOf('evaluate'), 'native presentation must precede evaluation');
  assert.equal(h.evaluations.some(item => item.beforePresentationSettled), false);
  assert.equal(h.requests.length, 1, 'home screen is the first and only document');
  assert.ok(h.requests[0].html.includes('id="shell"'));
  assert.equal(h.requests[0].html.includes('Preparando seu espaço'), false);
  assert.ok(h.requests[0].html.includes('"status":"loading"'), 'do not grant access before verification');
  await until(() => h.events.includes('state'), 'verified access state was not applied');
  assert.equal(h.evaluations.some(item => item.code.includes('synthetic-image-data')), false, 'assets must not cross the evaluation bridge again');
  h.dismiss(); await h.finished(); assert.equal(h.complete, true);
});

test('an empty dashboard restores the readable startup and reports failure after dismissal', async t => {
  const h = harness({ emptyDocument: true }); t.after(() => h.dismiss());
  await until(() => h.requests.length === 2 || h.done, 'empty shell was not handled');
  assert.ok(h.requests[1].html.includes('não terminou de carregar'));
  h.dismiss();
  await h.finished(); assert.equal(h.complete, true);
  assert.equal(h.alerts.length, 1);
  assert.match(h.alerts[0].message, /dashboard_failed/);
  assert.ok(JSON.parse(h.files.get('/local/saquettoos.startup.v1.json')).stages.some(item => item.stage === 'dashboard_failed'));
});

test('slow access verification does not delay the first home screen or replace its document', async t => {
  const start = deferred(); const h = harness({ start });
  t.after(() => { start.resolve({ preferences: {}, profile: { status: 'approved' } }); h.dismiss(); });
  await until(() => h.evaluations.length >= 2 || h.done, 'home did not become ready before access request');
  assert.equal(h.requests.length, 1);
  assert.ok(h.requests[0].html.includes('id="shell"'));
  assert.equal(h.events.includes('state'), false);
  start.resolve({ preferences: {}, profile: { status: 'approved' } });
  for (const timer of [...h.timers]) timer.fire();
  await until(() => h.events.includes('state'), 'approved state missing');
  assert.equal(h.requests.length, 1);
  h.dismiss(); await h.finished();
});

test('idle dismissal cancels native polling and reaches Script.complete without held WebView callbacks', async t => {
  const h = harness(); t.after(() => h.dismiss());
  await until(() => h.evaluations.length >= 2 || h.callbacks.size || h.done, 'bridge did not start');
  assert.equal(h.callbacks.size, 0, 'native completion callback must not wait indefinitely for a UI action');
  assert.ok(h.timers.size > 0, 'idle bridge must yield via a native timer');
  h.dismiss(); await h.finished();
  assert.equal(h.complete, true);
  await assert.rejects(h.runtimeOptions.shell('window.saquettoSetState({})'), /app_closed/);
  assert.equal(h.timers.size, 0);
  assert.equal(h.callbacks.size, 0);
  assert.equal(h.evaluations.some(item => item.useCallback), false);
  assert.equal(h.evaluations.some(item => item.afterDismiss), false);
});

test('six successive sessions finish on dismissal even when native evaluation never settles', async t => {
  for (let session = 1; session <= 6; session++) {
    const h = harness({ evaluate: () => new Promise(() => {}) });
    t.after(() => h.dismiss());
    await until(() => h.evaluations.length === 1 || h.done, 'session ' + session + ' did not begin evaluation');
    assert.equal(h.done, false);
    h.dismiss();
    await h.finished();
    assert.equal(h.complete, true, 'session ' + session + ' must release the script on Close');
    assert.equal(h.alerts.length, 0, 'normal dismissal must not show a startup failure');
    assert.equal(h.requests.length, 1, 'dismissal must not replace the closed document');
    assert.equal(h.timers.size, 0);
    assert.equal(h.callbacks.size, 0);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(h.evaluations.length, 1, 'a closed session must not resume evaluation');
    assert.equal(h.evaluations.some(item => item.afterDismiss), false);
  }
});

test('native app pauses central polling until closure then leaves central running', async t => {
  const h = harness({ queue: [{ id: 'open', action: 'openApp', payload: { id: 'clube-uol' } }] });
  t.after(() => { h.settleNative(); h.dismiss(); });
  await until(() => h.dispatches.length === 1 || h.done, 'native app did not start after access synchronization');
  for (const timer of [...h.timers]) timer.fire();
  await until(() => h.events.includes('native_wait') || h.done, 'native session did not pause bridge');
  const count = h.evaluations.length;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(h.evaluations.length, count);
  assert.equal(h.complete, false);
  h.settleNative();
  await until(() => h.resolutions.length === 1 || h.done, 'native return was not acknowledged');
  assert.equal(h.complete, false, 'child completion must not end saquettoOS');
  assert.ok(h.evaluations.length > count);
  h.dismiss(); await h.finished(); assert.equal(h.complete, true);
});

test('bridge dispatches evaluation results while another native action is waiting', async t => {
  const h = harness({ queue: [
    { id: 'action', action: 'nativeAction', payload: {} },
    { id: 'evaluation', action: 'appEvalResult', payload: { value: true } },
  ] }); t.after(() => h.dismiss());
  await until(() => h.resolutions.length === 2 || h.done, 'concurrent app evaluation result was not delivered');
  assert.deepEqual(h.dispatches.map(item => item.action), ['nativeAction', 'appEvalResult']);
  assert.equal(h.resolutions.every(item => item.result.ok), true);
  h.dismiss(); await h.finished(); assert.equal(h.complete, true);
});

test('dismissal waits for active native work but performs no later shell evaluation', async t => {
  const h = harness({ queue: [{ id: 'action', action: 'nativeAction', payload: {} }] });
  t.after(() => { h.dismiss(); h.settleNative(); });
  await until(() => h.dispatches.length === 1 || h.done, 'native action was not dispatched');
  h.dismiss();
  await until(() => h.events.includes('shutdown') || h.done, 'runtime was not shut down');
  assert.equal(h.complete, false, 'native work must settle before completing the script');
  h.settleNative(); await h.finished();
  assert.equal(h.complete, true);
  assert.equal(h.evaluations.some(item => item.afterDismiss), false);
  assert.equal(h.timers.size, 0);
});

test('native WebView evaluations are serialized and queued work is rejected after dismissal', async t => {
  const h = harness(); t.after(() => h.dismiss());
  await until(() => h.evaluations.length >= 2 || h.done, 'bridge did not start');
  const pending = deferred(); let calls = 0;
  h.view.evaluateJavaScript = () => { calls++; return pending.promise; };
  const first = h.runtimeOptions.shell('first');
  const second = h.runtimeOptions.shell('second');
  let settled = 0;
  const firstRejected = assert.rejects(first, /app_closed/).then(() => { settled++; });
  const secondRejected = assert.rejects(second, /app_closed/).then(() => { settled++; });
  await until(() => calls === 1, 'evaluation did not start');
  assert.equal(calls, 1, 'second native evaluation must wait for the first');
  h.dismiss();
  await h.finished();
  await until(() => settled === 2, 'pending and queued evaluations did not reject on dismissal');
  await firstRejected; await secondRejected;
  assert.equal(h.complete, true, 'completion must not depend on the native promise settling');
  assert.equal(h.timers.size, 0);
  pending.resolve(true);
  await new Promise(resolve => setImmediate(resolve));
  await h.finished(); assert.equal(calls, 1);
  assert.equal(h.timers.size, 0);
  assert.equal(h.callbacks.size, 0);
});

test('access initialization settling after dismissal never evaluates the closed shell', async t => {
  const start = deferred();
  const h = harness({ start });
  t.after(() => { h.dismiss(); start.resolve({ profile: { status: 'approved' } }); });
  await until(() => h.events.includes('start') || h.done, 'access initialization did not start');
  h.dismiss(); start.resolve({ profile: { status: 'approved' } });
  await h.finished();
  assert.equal(h.complete, true);
  assert.equal(h.evaluations.some(item => item.afterDismiss), false);
  assert.equal(h.callbacks.size, 0);
  assert.equal(h.timers.size, 0);
});
