const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const A = require('./access.js');
const C = require('./collector.js');
const source = fs.readFileSync(path.join(__dirname, 'build.js'), 'utf8');
const nativeCode = source.slice(source.indexOf('async function runNativeHunterCollector('), source.indexOf('async function main()'));
const browserCode = source.slice(source.indexOf('function installHunterBrowser('), source.indexOf('function nativeRedemptionKey('));

test('native host activates without an iCloud file and collects only after approval and private Keychain import', async () => {
  const keys = new Map(), requests = [];
  let state = 'pending', time = Date.parse('2026-10-18T12:00:00Z'), random = 0, collections = 0;
  const session = { user_agent: 'test-agent', cookies: [{ name: 'sessionid', value: 'native-private-session', domain: '.instagram.com' }] };
  const files = { documentsDirectory: () => '/local', joinPath: path.join, fileExists: () => false };
  const context = vm.createContext({
    Date: { now: () => time }, FileManager: { local: () => files }, getFileManager: () => files,
    Keychain: { contains: key => keys.has(key), get: key => keys.get(key), set: (key, value) => keys.set(key, value) },
    UUID: { string: () => '00000000-0000-4000-8000-' + String(++random).padStart(12, '0') },
    HunterAccess: A,
    HunterCollector: { KEYS: A.KEYS, normalizeSession: C.normalizeSession, create: () => ({
      importBootstrap: () => assert.fail('activation must not require a bootstrap file'),
      run: async () => {
        collections++;
        assert.equal(JSON.parse(keys.get(A.KEYS.instagram)).cookies[0].value, 'native-private-session');
        return { status: 'complete', discovered: 1, received: 1, delivered: 0, sources: {} };
      },
    }) },
    Request: class {
      constructor(url) { this.url = url; }
      async loadString() {
        requests.push({ url: this.url, method: this.method, body: this.body, token: this.headers.Authorization });
        assert.equal(this.onRedirect(), null);
        this.response = { statusCode: this.url.endsWith('/request') ? 202 : 200,
          headers: { 'Content-Type': 'application/json' }, url: this.url };
        return JSON.stringify(this.url.endsWith('/config')
          ? { ok: true, deviceId: keys.get(A.KEYS.device), role: 'member', instagramSession: session }
          : { ok: true, deviceId: keys.get(A.KEYS.device), state, label: 'iPhone amigo', role: 'member', code: 'ABCD1234' });
      }
    },
  });
  vm.runInContext(nativeCode, context);
  const fresh = await context.runNativeHunterCollector();
  assert.equal(fresh.status, 'activation_required');
  assert.equal(requests.length, 0); assert.equal(collections, 0); assert.equal(keys.size, 0);
  const pending = await context.createHunterAccess().requestAccess('iPhone amigo');
  assert.equal(pending.status, 'access_pending');
  const identity = keys.get(A.KEYS.token);
  assert.match(identity, /^h1\.phone-[a-f0-9]{24}\.[a-f0-9]{64}$/);
  assert.deepEqual(JSON.parse(requests[0].body), { label: 'iPhone amigo' });
  assert.equal((await context.runNativeHunterCollector()).status, 'access_pending');
  assert.equal(collections, 0); assert.equal(keys.has(A.KEYS.instagram), false);
  state = 'approved'; time += A.TTL.pending;
  const accepted = await context.runNativeHunterCollector();
  assert.equal(accepted.status, 'complete'); assert.equal(accepted.access.status, 'approved');
  assert.equal(collections, 1); assert.equal(keys.get(A.KEYS.token), identity);
  assert.deepEqual(requests.map(request => new URL(request.url).pathname), ['/hunter-access/request', '/hunter-access/status', '/hunter-access/config']);
  assert.doesNotMatch(JSON.stringify(accepted), /native-private-session|h1\.|cookies|instagramSession|user_agent/);
  state = 'revoked'; time += A.TTL.approved;
  const revoked = await context.runNativeHunterCollector();
  assert.equal(revoked.status, 'access_revoked'); assert.equal(collections, 1);
  assert.equal(JSON.parse(keys.get(A.KEYS.instagram)).cookies[0].value, 'native-private-session', 'revocation preserves the local session');
});

test('activation route cancels navigation synchronously and shares the offer-sheet lock', async () => {
  const view = {}, browsers = [], warnings = []; let dismiss, activations = 0;
  const context = vm.createContext({
    manageNativeHunterAccess: async received => {
      assert.equal(received, view); activations++;
      await new Promise(resolve => { dismiss = resolve; });
    },
    hunterNotice: async (title, message) => warnings.push({ title, message }),
    Safari: { open: url => browsers.push(url), openInApp: url => browsers.push(url) },
    console: { warn() {} },
  });
  vm.runInContext(browserCode, context); context.installHunterBrowser(view);
  assert.equal(view.shouldAllowRequest({ url: 'hunter-access://manage' }), false);
  assert.ok(view.hunterActionTask && typeof view.hunterActionTask.then === 'function');
  assert.equal(activations, 0, 'native alerts start after the WebView navigation callback returns');
  assert.equal(view.shouldAllowRequest({ url: 'hunter-access://manage/' }), false);
  assert.equal(view.shouldAllowRequest({ url: 'hunter-browser://external?url=' + encodeURIComponent('https://clube.uol.com.br/oferta') }), false);
  await Promise.resolve(); assert.equal(activations, 1); assert.equal(browsers.length, 0);
  dismiss(); await view.hunterActionTask;
  assert.equal(view.shouldAllowRequest({ url: 'hunter-access://manage' }), false);
  await Promise.resolve(); assert.equal(activations, 2);
  dismiss(); await view.hunterActionTask; assert.equal(warnings.length, 0);
  context.manageNativeHunterAccess = async () => { throw new Error('private native session'); };
  view.shouldAllowRequest({ url: 'hunter-access://manage' }); await view.hunterActionTask;
  assert.equal(warnings.length, 1); assert.doesNotMatch(JSON.stringify(warnings), /private native session/);
  view.shouldAllowRequest({ url: 'hunter-browser://external?url=' + encodeURIComponent('https://clube.uol.com.br/oferta') });
  await new Promise(resolve => setImmediate(resolve)); assert.equal(browsers.length, 1, 'a failed activation releases the shared lock');
});
