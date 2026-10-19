'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const catalog = JSON.parse(fs.readFileSync(path.join(__dirname, 'catalog.json')));
const identity = { deviceId: '12345678-1234-1234-1234-123456789abc', token: 'a'.repeat(64) };
function harness(route, options = {}) {
  const files = new Map(Object.entries(options.files || {}));
  const values = new Map(options.identity === false ? [] : [['saquettoos.identity.v1', JSON.stringify(identity)]]);
  const launched = [], requests = [], notices = [], installations = [];
  const fm = { documentsDirectory: () => '/cloud', joinPath: (a, b) => a + '/' + b,
    fileExists: p => files.has(p), readString: p => files.get(p), writeString: (p, v) => files.set(p, v), remove: p => files.delete(p),
    listContents: p => [...files.keys()].filter(name => name.startsWith(p + '/')).map(name => name.slice(p.length + 1)) };
  const local = { ...fm, documentsDirectory: () => '/local' };
  const keychain = { contains: name => values.has(name), get: name => values.get(name), set: (name, value) => values.set(name, value) };
  const updater = { inspect: async () => ({ status: 'installed' }), check: async app => ({ status: app.manifestBase ? 'up_to_date' : 'not_configured' }),
    install: async app => { installations.push(app.id); return { status: 'installed', version: '1.0.1' }; }, recover: async () => ({ status: 'no_recovery' }) };
  const context = { SaquettoUpdater: { create: () => updater }, Map, Date, JSON, Object, String, Array, Error };
  const source = fs.readFileSync(path.join(__dirname, 'native/host.js'), 'utf8').split('\nasync function saquettoMain()')[0]
    .replace('__SAQUETTO_CATALOG__', JSON.stringify(catalog)).replace('__SAQUETTO_HTML__', '""').replace('__SAQUETTO_ASSETS__', '{}').replace('__SAQUETTO_UPDATER__', '')
    .replace('__SAQUETTO_COMPATIBILITY__', '{}').replace('__SAQUETTO_ADAPTERS__', '').replace('__SAQUETTO_RUNTIME__', '');
  vm.runInNewContext(source + '\nthis.Host = SaquettoHost;', context);
  const host = context.Host.create({ fm, local, keychain, request: url => {
    const req = { response: { statusCode: 200 }, loadJSON: async () => {
      requests.push({ url, method: req.method, body: req.body && JSON.parse(req.body) });
      const result = await route(url.replace('https://saquettoos-access.leosaquetto.workers.dev', ''), req);
      req.response.statusCode = result.status || 200; return result.data;
    } }; return req;
  }, uuid: () => identity.deviceId, launch: async url => launched.push(url), notify: async item => notices.push(item), now: () => 1000 });
  return { host, Host: context.Host, files, values, launched, requests, notices, installations };
}
const approved = { id: identity.deviceId, name: 'Synthetic owner', role: 'admin', status: 'approved', appIds: catalog.map(app => app.id) };
test('public UI receives no device token, native launch resolves decomposed names', async () => {
  const h = harness(path => ({ data: path === '/v1/me' ? { profile: approved } : { allowed: true } }),
    { files: { ['/cloud/Bilhete Único.js']: '// untouched' } });
  const state = await h.host.start();
  assert.equal(state.apps.find(app => app.id === 'bilhete-unico').installed, true);
  assert.equal(JSON.stringify(state).includes(identity.token), false);
  await h.host.dispatch('launchStandalone', { id: 'bilhete-unico' });
  assert.equal(h.launched[0], 'scriptable:///run?scriptName=' + encodeURIComponent('Bilhete Único'));
  assert.equal(h.files.get('/cloud/Bilhete Único.js'), '// untouched');
});
test('integrated launch checks access before passing control to runtime', async () => {
  const opened = [];
  const h = harness(path => ({ data: path === '/v1/me' ? { profile: approved } : { allowed: true } }),
    { files: { '/cloud/PillTracker.js': '// original' } });
  h.host.attachRuntime({ activeId: null, open: async id => { opened.push(id); return { status: 'opened', mode: 'integrated' }; } });
  await h.host.start();
  assert.equal((await h.host.dispatch('openApp', { id: 'pilltracker' })).mode, 'integrated');
  assert.deepEqual(opened, ['pilltracker']); assert.equal(h.launched.length, 0);
  h.host.attachRuntime({ activeId: 'pilltracker' });
  await assert.rejects(h.host.dispatch('installUpdate', { id: 'saquettoos' }), /close_app_before_update/);
});
test('revocation and unavailable server deny launch despite previously approved UI', async () => {
  const h = harness(path => path === '/v1/me' ? { data: { profile: approved } } : { status: 403, data: { error: 'access_denied' } },
    { files: { '/cloud/PillTracker.js': '// original' } });
  await h.host.start();
  await assert.rejects(h.host.dispatch('openApp', { id: 'pilltracker' }), /access_denied/);
  assert.equal(h.launched.length, 0);
});
test('settings cannot grant admin privileges or app permissions', async () => {
  const h = harness(() => ({ data: { profile: { ...approved, role: 'member', status: 'pending', appIds: [] } } }));
  await h.host.start();
  await h.host.dispatch('setPreferences', { profile: approved, appearance: 'invalid', favorites: ['unknown', 'clube-uol', 'clube-uol'] });
  const state = await h.host.state();
  assert.equal(state.profile.role, 'member');
  assert.equal(state.preferences.appearance, 'system');
  assert.deepEqual(Array.from(state.preferences.favorites), ['clube-uol']);
  assert.equal(state.apps.some(app => app.allowed), false);
});
test('duplicate Unicode filenames fail closed; no arbitrary script launch', async () => {
  const h = harness(() => ({ data: { allowed: true } }), { files: { '/cloud/Bilhete Único.js': 'a', '/cloud/Bilhete Único.js': 'b' } });
  await h.host.start();
  await assert.rejects(h.host.dispatch('openApp', { id: 'bilhete-unico' }), /script_name_conflict/);
  await assert.rejects(h.host.dispatch('openApp', { id: '../../private' }), /app_invalid/);
  assert.equal(h.launched.length, 0);
});
test('bootstrap consumes only verified owner identity; conflict preserves both', async () => {
  const bootstrap = { ...identity, api: 'https://saquettoos-access.leosaquetto.workers.dev' };
  const h = harness(() => ({ data: { profile: approved } }), { identity: false, files: { '/cloud/saquettoos-owner-bootstrap.json': JSON.stringify(bootstrap), '/cloud/PillTracker.json': 'private data' } });
  await h.host.start();
  assert.equal(h.files.has('/cloud/saquettoos-owner-bootstrap.json'), false);
  assert.equal(h.files.get('/cloud/PillTracker.json'), 'private data');
  assert.equal(h.values.has('saquettoos.identity.v1'), true);
  const conflict = harness(() => ({ data: {} }), { files: { '/cloud/saquettoos-owner-bootstrap.json': JSON.stringify({ ...bootstrap, token: 'b'.repeat(64) }) } });
  await assert.rejects(conflict.host.start(), /identity_conflict/);
  assert.equal(conflict.files.has('/cloud/saquettoos-owner-bootstrap.json'), true);
});
test('notification opt-in is explicit and no other script notifications are touched', async () => {
  const h = harness(() => ({ data: { profile: approved } }));
  await h.host.start();
  await assert.rejects(h.host.dispatch('testNotification'), /notifications_disabled/);
  await h.host.dispatch('setNotificationPreferences', { enabled: true });
  assert.equal((await h.host.dispatch('testNotification')).status, 'scheduled');
  assert.equal(h.notices.length, 1);
});
test('app update rechecks authorization and missing sources remain explicit', async () => {
  const h = harness(path => path === '/v1/me' ? { data: { profile: approved } } : { status: 403, data: { error: 'access_denied' } });
  await h.host.start();
  const updates = await h.host.dispatch('checkUpdates');
  assert.equal(updates.items.filter(item => item.status === 'not_configured').length, 5);
  await assert.rejects(h.host.dispatch('installUpdate', { id: 'clube-uol' }), /access_denied/);
  assert.equal(h.installations.length, 0);
});
test('server outage never grants cached access', async () => {
  const h = harness(() => { throw new Error('timeout'); });
  const state = await h.host.start();
  assert.equal(state.profile.status, 'unavailable');
  assert.equal(state.apps.some(app => app.allowed), false);
});
