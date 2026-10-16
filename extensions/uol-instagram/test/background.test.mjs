import test from 'node:test';
import assert from 'node:assert/strict';

const TOKEN = 'offline-test-token-for-background';
const EXTENSION_ID = 'abcdefghijklmnopabcdefghijklmnop';
const ORIGIN = `chrome-extension://${EXTENSION_ID}/`;
const settings = () => ({ token: TOKEN, profiles: ['clubeuol'], intervalMinutes: 5, enabled: false, notifications: true });
let instance = 0;

async function boot({ saved = {}, existingAlarm = false } = {}) {
  const originalChrome = globalThis.chrome, originalFetch = globalThis.fetch;
  const data = structuredClone(saved);
  const calls = { access: [], fetch: [], alarms: [], badges: [], notifications: [], tabs: [], storage: [] };
  const listeners = {};
  const event = name => ({ addListener(fn) { assert.equal(listeners[name], undefined); listeners[name] = fn; } });
  globalThis.chrome = {
    storage: { local: {
      async setAccessLevel(value) { calls.access.push(value); },
      async get(keys) { return Object.fromEntries(keys.filter(key => key in data).map(key => [key, structuredClone(data[key])])); },
      async set(value) { calls.storage.push(structuredClone(value)); Object.assign(data, structuredClone(value)); },
    } },
    runtime: {
      id: EXTENSION_ID, getURL: path => `${ORIGIN}${path}`,
      onStartup: event('startup'), onInstalled: event('installed'), onMessage: event('message'),
    },
    alarms: {
      onAlarm: event('alarm'),
      async get(name) { return existingAlarm ? { name, periodInMinutes: 5 } : undefined; },
      async create(name, options) { calls.alarms.push({ name, ...options }); },
    },
    action: {
      async setBadgeBackgroundColor(value) { calls.badges.push(value); },
      async setBadgeText(value) { calls.badges.push(value); },
    },
    notifications: {
      onClicked: event('notification'),
      async create(...args) { calls.notifications.push(args); },
      async clear(id) { calls.notifications.push({ clear: id }); },
    },
    tabs: { async create(value) { calls.tabs.push(value); } },
  };
  globalThis.fetch = async url => {
    calls.fetch.push(String(url));
    assert.equal(String(url), `${ORIGIN}provisioning.json`, 'bootstrap must not make an external request');
    throw new TypeError('Local provisioning file absent');
  };
  const restore = () => {
    if (originalChrome === undefined) delete globalThis.chrome; else globalThis.chrome = originalChrome;
    globalThis.fetch = originalFetch;
  };
  try {
    await import(`../src/background.js?offline-bootstrap=${++instance}`);
  } catch (error) { restore(); throw error; }
  const sender = { id: EXTENSION_ID, url: `${ORIGIN}popup.html` };
  const message = value => new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Background message did not reply')), 1000);
    const retained = listeners.message(value, sender, reply => { clearTimeout(timeout); resolve(reply); });
    assert.equal(retained, true, 'asynchronous response channel stays open');
  });
  return { calls, data, listeners, message, restore };
}

test('actual MV3 bootstrap restricts storage and tolerates missing optional provisioning', async () => {
  const app = await boot();
  try {
    const result = await app.message({ action: 'getState' });
    assert.equal(result.ok, true);
    assert.equal(result.state.configured, false);
    assert.deepEqual(app.calls.access, [{ accessLevel: 'TRUSTED_CONTEXTS' }]);
    assert.deepEqual(app.calls.fetch, [`${ORIGIN}provisioning.json`]);
    assert.deepEqual(app.calls.alarms, [{ name: 'uol-instagram-check', delayInMinutes: 1, periodInMinutes: 5 }]);
    assert.deepEqual(Object.keys(app.listeners).sort(), ['alarm', 'installed', 'message', 'notification', 'startup']);
    assert.equal(app.data.monitor.schema, 1);
  } finally { app.restore(); }
});

test('foreign senders and Instagram content scripts cannot configure the extension or request fetches', async () => {
  const app = await boot({ saved: { settings: settings() } });
  try {
    await app.message({ action: 'getState' });
    const storageCount = app.calls.storage.length;
    let replies = 0;
    for (const sender of [
      { id: 'foreign-extension', url: `${ORIGIN}options.html` },
      { id: EXTENSION_ID, url: 'https://www.instagram.com/' },
      { id: EXTENSION_ID, url: `chrome-extension://${EXTENSION_ID}.example/options.html` },
      { id: EXTENSION_ID },
    ]) {
      for (const action of ['configure', 'check', 'fetch']) {
        assert.equal(app.listeners.message({ action, config: { enabled: true }, url: 'https://example.invalid/' }, sender, () => replies++), false);
      }
    }
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(replies, 0);
    assert.equal(app.calls.storage.length, storageCount);
    assert.equal(app.data.settings.enabled, false);
    assert.deepEqual(app.calls.fetch, []);
  } finally { app.restore(); }
});

test('configured extension returns sanitized state and rejects an arbitrary fetch action', async () => {
  const app = await boot({ saved: { settings: settings() }, existingAlarm: true });
  try {
    const result = await app.message({ action: 'getState' });
    assert.equal(result.ok, true);
    assert.equal(result.state.configured, true);
    assert.equal(result.state.enabled, false);
    assert.equal(JSON.stringify(result).includes(TOKEN), false);
    assert.equal('token' in result.state, false);
    assert.equal(app.data.settings.token, TOKEN, 'private token remains in restricted storage');
    assert.deepEqual(app.calls.alarms, [], 'preserve an existing scheduled alarm');
    assert.deepEqual(await app.message({ action: 'fetch', url: 'https://example.invalid/' }), { ok: false, error: 'operation_failed' });
    assert.deepEqual(app.calls.fetch, []);
  } finally { app.restore(); }
});

test('scheduled and lifecycle checks honor the stored disabled setting', async () => {
  const app = await boot({ saved: { settings: settings() } });
  try {
    await app.message({ action: 'getState' });
    const count = app.calls.badges.length;
    app.listeners.alarm({ name: 'unrelated-alarm' });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(app.calls.badges.length, count);
    app.listeners.alarm({ name: 'uol-instagram-check' });
    app.listeners.startup();
    app.listeners.installed();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(app.calls.badges.filter(value => value.text === 'Ⅱ').length, 4);
    assert.deepEqual(app.calls.fetch, []);
    const result = await app.message({ action: 'check' });
    assert.equal(result.state.enabled, false);
    assert.equal(result.state.lastCheckAt, '');
    assert.deepEqual(app.calls.notifications, []);
  } finally { app.restore(); }
});

test('notification clicks only open a validated Instagram username', async () => {
  const app = await boot({ saved: { settings: settings() } });
  try {
    await app.message({ action: 'getState' });
    for (const id of ['https://example.invalid/', 'story:clubeuol/?next=evil', 'story:../evil', 'story:clubeuol\n']) app.listeners.notification(id);
    assert.deepEqual(app.calls.tabs, []);
    app.listeners.notification('story:clubeuol');
    assert.deepEqual(app.calls.tabs, [{ url: 'https://www.instagram.com/stories/clubeuol/' }]);
    assert.deepEqual(app.calls.notifications, [{ clear: 'story:clubeuol' }]);
    assert.deepEqual(app.calls.fetch, []);
  } finally { app.restore(); }
});
