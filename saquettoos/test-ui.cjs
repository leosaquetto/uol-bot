'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'ui/app.js'), 'utf8');

function fixture({ preview = false, profile = {}, app = {}, dark = true } = {}) {
  const nodes = new Map();
  const frames = [];
  const windowEvents = {};
  const documentEvents = {};
  const timers = new Map();
  let timerSequence = 0;
  let wakeCount = 0;
  const state = {
    version: '1.0.0-test', profile: { name: 'Leo', role: 'admin', status: 'approved', ...profile },
    apps: [{ id: 'clube-uol', name: 'Clube UOL Premium', scriptName: 'Clube UOL Premium', installed: true, allowed: true, mode: 'integrated', ...app }],
    preferences: { appearance: 'system', material: 'glass', favorites: ['clube-uol'] }, assets: {}, notifications: [], update: {}
  };
  const element = () => ({
    textContent: '', innerHTML: '', value: '', hidden: false, dataset: {}, style: {}, attributes: {}, events: {}, disabled: false, isConnected: true,
    setAttribute(name, value) { this.attributes[name] = value; }, removeAttribute(name) { delete this.attributes[name]; },
    focus() { document.activeElement = this; }, querySelectorAll() { return []; }, querySelector() { return null; },
    addEventListener(name, fn) { this.events[name] = fn; }, replaceChildren(...children) { this.children = children; }
  });
  const document = {
    documentElement: { dataset: {} }, body: { style: {} }, activeElement: null,
    querySelector(selector) { if (!nodes.has(selector)) nodes.set(selector, element()); return nodes.get(selector); },
    querySelectorAll() { return []; }, addEventListener(name, fn) { documentEvents[name] = fn; },
    createElement(tag) {
      const created = element(); created.tag = tag; created.sent = [];
      created.contentWindow = { postMessage(value, targetOrigin) { created.sent.push({ value, targetOrigin }); } };
      frames.push(created); return created;
    }
  };
  document.querySelector('#saquetto-initial').textContent = JSON.stringify(state);
  const media = { matches: dark, addEventListener(name, fn) { this.listener = fn; } };
  const window = {
    __SAQUETTO_PREVIEW__: preview, __SAQUETTO_WAKE() { wakeCount++; },
    matchMedia() { return media; }, addEventListener(name, fn) { windowEvents[name] = fn; }
  };
  const context = vm.createContext({ window, document,
    setTimeout(fn) { timers.set(++timerSequence, fn); return timerSequence; }, clearTimeout(id) { timers.delete(id); }, console });
  vm.runInContext(source, context, { filename: 'saquettoos/ui/app.js' });
  return {
    window, document, frames, state, nodes, media, windowEvents, documentEvents, timers,
    get wakeCount() { return wakeCount; },
    get frame() { return frames[frames.length - 1]; },
    get queue() { return window.__SAQUETTO_QUEUE; },
    show(instanceId = 'session-a') { return window.saquettoShowApp({ id: 'clube-uol', name: 'Clube UOL Premium', html: '<p>App</p>', instanceId }); },
    message(frame, data) { return windowEvents.message({ source: frame.contentWindow, data }); },
    resolveNext(data, error) {
      const request = window.__SAQUETTO_QUEUE.shift();
      assert.ok(request, 'Expected a queued native request');
      window.saquettoResolve(request.id, error ? { ok: false, error } : { ok: true, data });
      return request;
    },
    click(dataset) {
      const button = { dataset, disabled: false };
      return documentEvents.click({ target: { closest() { return button; } } });
    }
  };
}

test('appearance follows system and honors explicit light/dark and material preferences', () => {
  const f = fixture();
  assert.equal(f.document.documentElement.dataset.appearance, 'dark');
  assert.equal(f.document.documentElement.dataset.material, 'glass');
  f.media.matches = false; f.media.listener();
  assert.equal(f.document.documentElement.dataset.appearance, 'light');
  f.window.saquettoSetState({ preferences: { appearance: 'dark', material: 'solid' } });
  f.media.listener();
  assert.equal(f.document.documentElement.dataset.appearance, 'dark');
  assert.equal(f.document.documentElement.dataset.material, 'solid');
  f.window.saquettoSetState({ preferences: { appearance: 'light', material: 'glass' } });
  assert.equal(f.document.documentElement.dataset.appearance, 'light');
  assert.equal(f.document.documentElement.dataset.material, 'glass');
});

test('only approved, installed and authorized integrated apps receive a frame', () => {
  for (const options of [{ profile: { status: 'pending' } }, { profile: { status: 'revoked' } }, { app: { allowed: false } }, { app: { installed: false } }, { app: { mode: 'external' } }, { app: { mode: 'native' } }]) {
    const denied = fixture(options); assert.equal(denied.show(), false); assert.equal(denied.frames.length, 0);
  }
  const f = fixture(); assert.equal(f.show(), 'session-a');
  assert.equal(f.frame.attributes.sandbox, 'allow-scripts allow-forms');
  assert.equal(f.frame.attributes.referrerpolicy, 'no-referrer');
  assert.equal(f.show(), 'session-a'); assert.equal(f.frames.length, 1, 'Reopening the current instance preserves its frame');
});

test('native sessions wait for closure without a 90 second timeout or iframe', async () => {
  const f = fixture({ app: { mode: 'native' } });
  const session = f.window.saquettoNative('openApp', { id: 'clube-uol' });
  assert.equal(f.timers.size, 0);
  assert.equal(f.frames.length, 0);
  f.resolveNext({ status: 'returned', mode: 'native' });
  assert.equal((await session).status, 'returned');
  const ordinary = f.window.saquettoNative('checkUpdates');
  assert.equal(f.timers.size, 1, 'ordinary requests still have a deadline');
  f.resolveNext({}); await ordinary;
  assert.equal(f.timers.size, 0);
});

test('bridge ignores foreign windows, invalid envelopes and unsupported kinds', async () => {
  const f = fixture(); f.show(); f.frame.events.load();
  const request = { saquetto: 1, instanceId: 'session-a', kind: 'request', id: 'request-a', action: 'read', payload: {} };
  await f.windowEvents.message({ source: {}, data: request });
  for (const data of [null, [], { ...request, instanceId: undefined }, { ...request, saquetto: 2 }, { ...request, kind: 'eval', code: 'danger()' }, { ...request, id: '' }, { ...request, instanceId: 'old' }, { ...request, action: '' }, { ...request, payload: [] }]) await f.message(f.frame, data);
  assert.equal(f.queue.length, 0);
  assert.equal(f.wakeCount, 0);
  assert.equal(f.frame.sent.length, 2, 'Only malformed requests from the active frame get error responses');
  for (const message of f.frame.sent) assert.equal(message.value.error, 'payload_invalid');
});

test('requests are bound to the active app and return native data or failure', async () => {
  const f = fixture(); f.show(); f.frame.events.load();
  const message = { saquetto: 1, instanceId: 'session-a', kind: 'request', id: 'request-a', action: 'read', appId: 'spoofed', payload: { scope: 'local' } };
  const requestTask = f.message(f.frame, message);
  assert.equal(f.wakeCount, 1);
  assert.equal(f.queue[0].action, 'appAction');
  assert.equal(f.queue[0].payload.appId, 'clube-uol');
  assert.equal(f.queue[0].payload.instanceId, 'session-a');
  assert.equal(f.queue[0].payload.request.action, 'read');
  f.resolveNext({ count: 3 }); await requestTask;
  const response = f.frame.sent.at(-1);
  assert.equal(response.targetOrigin, '*'); // The sandbox deliberately has an opaque origin.
  assert.equal(response.value.ok, true); assert.equal(response.value.data.count, 3);
  assert.equal(response.value.instanceId, 'session-a'); assert.equal(response.value.id, 'request-a');
  const failedTask = f.message(f.frame, { ...message, id: 'request-b' });
  f.resolveNext(null, 'access_denied'); await failedTask;
  assert.equal(f.frame.sent.at(-1).value.ok, false); assert.equal(f.frame.sent.at(-1).value.error, 'access_denied');
});

test('host evaluation waits for frame load and accepts its result only once', async () => {
  const f = fixture(); f.show();
  assert.equal(f.window.saquettoFrameMessage({ kind: 'request', id: 'no', code: '1' }), false);
  assert.equal(f.window.saquettoFrameMessage({ kind: 'eval', id: 'eval-a', instanceId: 'old', code: '1' }), false);
  assert.equal(f.window.saquettoFrameMessage({ kind: 'eval', id: 'eval-a', code: '1 + 1', useCallback: true }), true);
  assert.equal(f.frame.sent.length, 0);
  f.frame.events.load(); assert.equal(f.frame.sent[0].value.kind, 'eval');
  assert.equal(f.frame.sent[0].value.useCallback, true);
  await f.message(f.frame, { saquetto: 1, instanceId: 'session-a', kind: 'evalResult', id: 'unknown', value: 2 }); assert.equal(f.queue.length, 0);
  const result = { saquetto: 1, instanceId: 'session-a', kind: 'evalResult', id: 'eval-a', value: 2 };
  const task = f.message(f.frame, result);
  assert.equal(f.queue[0].action, 'appEvalResult'); assert.equal(f.queue[0].payload.value, 2);
  assert.equal(f.queue[0].payload.instanceId, 'session-a');
  f.resolveNext({ received: true }); await task;
  await f.message(f.frame, result); assert.equal(f.queue.length, 0);
});

test('replacing HTML drops stale source, responses and evals without growing instance ids', async () => {
  const f = fixture(); f.show(); const oldFrame = f.frame; oldFrame.events.load();
  const pending = f.message(oldFrame, { saquetto: 1, instanceId: 'session-a', kind: 'request', id: 'old-request', action: 'read', payload: {} });
  f.window.saquettoFrameMessage({ kind: 'eval', id: 'old-eval', code: '1' });
  const originalSent = oldFrame.sent.length;
  let instance;
  const seen = new Set();
  for (let index = 0; index < 100; index++) {
    instance = f.window.saquettoSetAppHTML({ html: '<p>Updated</p>' });
    assert.equal(seen.has(instance), false); seen.add(instance); assert.ok(instance.length < 30);
  }
  f.frame.events.load();
  f.resolveNext({ old: true }); await pending;
  assert.equal(oldFrame.sent.length, originalSent); assert.equal(f.frame.sent.length, 0);
  await f.message(oldFrame, { saquetto: 1, instanceId: 'session-a', kind: 'request', id: 'stale', action: 'write', payload: {} });
  await f.message(f.frame, { saquetto: 1, instanceId: 'session-a', kind: 'evalResult', id: 'old-eval', value: 1 });
  assert.equal(f.queue.length, 0);
});

test('Home preserves a busy app and removes its frame only after confirmed native close', async () => {
  const f = fixture(); f.show(); f.frame.events.load();
  const failedClose = f.window.saquettoHome();
  assert.equal(f.queue[0].action, 'closeApp'); assert.equal(f.queue[0].payload.instanceId, 'session-a');
  assert.equal(f.document.querySelector('#app-stage').hidden, false);
  await f.message(f.frame, { saquetto: 1, instanceId: 'session-a', kind: 'request', id: 'during-close', action: 'write', payload: {} });
  assert.equal(f.frame.sent.at(-1).value.error, 'app_closing'); assert.equal(f.queue.length, 1);
  f.resolveNext(null, 'app_busy'); assert.equal(await failedClose, false);
  assert.equal(f.document.querySelector('#app-stage').hidden, false);
  const finished = f.window.saquettoHome(); f.resolveNext({ status: 'closed' }); assert.equal(await finished, true);
  assert.equal(f.document.querySelector('#app-stage').hidden, true);
  assert.equal(f.document.querySelector('#app-frame-container').children.length, 0);
});

test('a second iframe load revokes bridge authority before closing the app', async () => {
  const f = fixture(); f.show(); f.frame.events.load();
  f.frame.events.load();
  assert.equal(f.queue.length, 1); assert.equal(f.queue[0].action, 'closeApp');
  await f.message(f.frame, { saquetto: 1, instanceId: 'session-a', kind: 'request', id: 'navigated', action: 'write', payload: {} });
  assert.equal(f.queue.length, 1);
  assert.equal(f.window.saquettoFrameMessage({ kind: 'eval', id: 'late', code: '1' }), false);
  f.resolveNext({ status: 'closed' });
  await Promise.resolve(); await Promise.resolve();
  assert.equal(f.document.querySelector('#app-stage').hidden, true);
});

test('late close confirmation cannot remove a newer app instance', async () => {
  const f = fixture(); f.show();
  const closing = f.window.saquettoHome();
  assert.equal(f.show('session-b'), 'session-b');
  f.resolveNext({ status: 'closed' }); assert.equal(await closing, false);
  assert.equal(f.document.querySelector('#app-stage').hidden, false);
  assert.equal(f.document.querySelector('#app-frame-container').children[0], f.frame);
});

test('preview permits local appearance changes but performs no native actions', async () => {
  const f = fixture({ preview: true });
  assert.equal(f.document.querySelector('#preview-note').hidden, false);
  await assert.rejects(f.window.saquettoNative('openApp', { id: 'clube-uol' }), /preview_only/);
  await f.click({ preference: 'appearance', value: 'light' });
  assert.equal(f.document.documentElement.dataset.appearance, 'light');
  await f.click({ preference: 'material', value: 'solid' });
  assert.equal(f.document.documentElement.dataset.material, 'solid');
  await f.click({ action: 'checkUpdates' });
  assert.equal(f.queue.length, 0); assert.equal(f.wakeCount, 0);
  assert.match(f.document.querySelector('#toast').textContent, /prévia visual/);
});

test('failed installation never displays success while installed results do', async () => {
  const f = fixture();
  const failed = f.click({ install: 'clube-uol' }); f.resolveNext({ status: 'rolled_back' }); await failed;
  assert.match(f.document.querySelector('#toast').textContent, /não foi concluída/);
  assert.equal(f.document.querySelector('#toast').dataset.error, 'true');
  const installed = f.click({ install: 'clube-uol' }); f.resolveNext({ status: 'installed', version: '1.2.0' }); await installed;
  assert.match(f.document.querySelector('#toast').textContent, /Atualização instalada/);
  assert.equal(f.document.querySelector('#toast').dataset.error, 'false');
});
