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
  const timers = new Set(), callbacks = new Set();
  const queue = [...(options.queue || [])];
  let view, runtimeOptions, closed = false, complete = false, done = false, error;
  let shellLoaded = false;
  const state = () => ({ preferences: { appearance: 'dark', material: 'glass' }, profile: { status: 'approved' } });
  const browser = {
    document: { getElementById: id => shellLoaded && id === 'shell' ? {} : null },
    __SAQUETTO_QUEUE: queue,
    __SAQUETTO_WAKE: null,
  };
  browser.window = browser;
  const browserContext = vm.createContext(browser);
  function installShell() {
    browser.saquettoSetState = () => { events.push('state'); };
    browser.saquettoNative = () => {};
    browser.saquettoResolve = (id, result) => { resolutions.push({ id, result }); };
  }
  class WebView {
    constructor() { view = this; }
    async loadHTML() {
      const guard = this.shouldAllowRequest;
      const request = { url: options.bootstrapURL || 'about:blank' };
      const allowed = typeof guard !== 'function' || guard(request);
      requests.push({ phase: 'load', guard, allowed });
      events.push('load');
      shellLoaded = allowed && !options.emptyDocument;
      if (shellLoaded) installShell();
    }
    evaluateJavaScript(code, useCallback = false) {
      evaluations.push({ code, useCallback, afterDismiss: closed });
      events.push('evaluate');
      if (!useCallback) {
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
    shouldAllowRequest(request) { return request.url === 'about:blank' || request.url === 'about:srcdoc'; },
    async shutdown() { events.push('shutdown'); },
  };
  const host = {
    async state() { return state(); },
    async start() { events.push('start'); return options.start ? options.start.promise : state(); },
    attachRuntime(value) { assert.equal(value, runtime); },
    async dispatch(action, payload) {
      dispatches.push({ action, payload });
      if (action === 'nativeAction') return nativeAction.promise;
      if (action === 'appEvalResult') { nativeAction.resolve({ status: 'finished' }); return { status: 'received' }; }
      return { status: 'finished' };
    },
  };
  const fm = { isFileStoredIniCloud: () => false };
  const context = {
    SAQUETTO_VERSION: 'test', SAQUETTO_CATALOG: [], SAQUETTO_COMPATIBILITY: {}, SAQUETTO_HTML: '<main id="shell">__SAQUETTO_INITIAL_JSON__</main>',
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
      const timer = { invalidate() { timers.delete(timer); }, fire() { timers.delete(timer); callback(); } };
      timers.add(timer); events.push('timer'); return timer;
    } },
  };
  const execution = vm.runInNewContext('(async () => {\n' + main + '\n})()', context);
  execution.then(() => { done = true; }, failure => { error = failure; done = true; });
  return {
    events, evaluations, requests, alerts, resolutions, dispatches, timers, callbacks,
    get view() { return view; }, get runtimeOptions() { return runtimeOptions; },
    get complete() { return complete; }, get done() { return done; }, get error() { return error; },
    dismiss() { if (!closed) { closed = true; events.push('dismiss'); presentation.resolve(); } },
    settleNative() { nativeAction.resolve({ status: 'finished' }); },
    async finished() { await until(() => done, 'saquettoMain did not finish'); if (error) throw error; },
  };
}

test('initial HTML loads before runtime navigation guard; subsequent requests remain restricted', async t => {
  const h = harness({ bootstrapURL: 'file:///private/Scriptable/bootstrap/index.html' }); t.after(() => h.dismiss());
  await until(() => h.events.includes('present') || h.done, 'initial shell was not presented');
  assert.equal(h.error, undefined);
  assert.equal(h.requests[0].guard, undefined, 'initial local document must not be filtered as an app navigation');
  assert.equal(h.requests[0].allowed, true);
  assert.equal(typeof h.view.shouldAllowRequest, 'function');
  assert.equal(h.view.shouldAllowRequest({ url: 'https://untrusted.example/' }), false);
  assert.equal(h.view.shouldAllowRequest({ url: 'about:blank' }), true);
  assert.ok(h.events.indexOf('evaluate') < h.events.indexOf('present'), 'render readiness must be checked before presenting');
  h.dismiss(); await h.finished(); assert.equal(h.complete, true);
});

test('an empty document reports initialization failure without presenting a black WebView', async t => {
  const h = harness({ emptyDocument: true }); t.after(() => h.dismiss());
  await until(() => h.alerts.length || h.events.includes('present') || h.done, 'empty shell was not handled');
  assert.equal(h.events.includes('present'), false);
  assert.equal(h.alerts.length, 1);
  await h.finished(); assert.equal(h.complete, true);
});

test('idle dismissal cancels native polling and reaches Script.complete without held WebView callbacks', async t => {
  const h = harness(); t.after(() => h.dismiss());
  await until(() => h.timers.size || h.callbacks.size || h.done, 'bridge did not start');
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
