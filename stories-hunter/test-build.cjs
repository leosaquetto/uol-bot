const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const source = require("./test-support/build-source.cjs")();
const optionCode = source.slice(source.indexOf('const buildOptions'), source.indexOf('// BUILD_OPTIONS_END'));
const installCode = source.slice(source.indexOf('// PUBLIC_ARTIFACT_START'));
const privateHeader = 'const BUNDLED_COOKIES_LEO = [{"value":"synthetic-leo"}];\nconst BUNDLED_COOKIES_GAORIEL = [{"value":"synthetic-gab"}];\nconst BUNDLED_REDEMPTIONS = [\n  {\n    "id": "fixture"\n  }\n];';
function install(options) {
  const writes = [], copies = [], notifications = [];
  const context = {
    process: { argv: ['node', 'build.js', ...options] },
    fs: {
      existsSync: p => p !== '/icloud/vouchers/example.pdf',
      writeFileSync: (p, value) => writes.push([p, value]),
      statSync: () => ({ size: 120 }), mkdirSync() {},
      readdirSync: () => ['example.pdf'], copyFileSync: (a, b) => copies.push([a, b])
    },
    path, __dirname: '/source', targetScriptPath: '/repo/hunter.js',
    icloudScriptPath: '/icloud/Hunter.js', scriptHeader: privateHeader,
    bundledCookiesLeo: [{ value: 'synthetic-leo' }], bundledCookiesGaoriel: [{ value: 'synthetic-gab' }],
    bundledRedemptions: [{ id: 'fixture' }], console: { log() {}, warn() {} },
    require: name => { assert.equal(name, 'child_process'); return { execSync: command => { notifications.push(command); return ''; } }; }
  };
  vm.runInNewContext(optionCode + installCode, context);
  return { writes, copies, notifications };
}
test('prepare creates only artifact without iCloud writes or notification', () => {
  const r = install(['--prepare']);
  assert.deepEqual(r.writes.map(x => x[0]), ['/repo/hunter.js']);
  assert.equal(r.copies.length, 0); assert.equal(r.notifications.length, 0);
  assert.doesNotMatch(r.writes[0][1], /synthetic-leo|synthetic-gab|fixture/);
});
test('code-only install preserves redemption database and vouchers', () => {
  const r = install(['--code-only']);
  assert.deepEqual(r.writes.map(x => x[0]), ['/repo/hunter.js', '/icloud/Hunter.js']);
  assert.equal(r.copies.length, 0); assert.equal(r.notifications.length, 2);
  assert.match(r.notifications[0], /stage\?target=clube-uol/);
  assert.equal(r.writes[1][1], privateHeader, 'private install retains both shared accounts');
});
test('default build retains existing full sync behavior and rejects unknown options', () => {
  const r = install([]);
  assert.equal(r.writes.length, 3); assert.equal(r.copies.length, 1);
  assert.equal(r.notifications.length, 2);
  assert.throws(() => install(['--typo']), /Unknown build option/);
});

test('private bootstrap seeds missing files once and public code reuses both saved accounts', () => {
  const files = new Map([['/docs/uol_session_cookies_leosaquetto.json', '[{"value":"newer-local"}]'], ['/docs/redemptions.json', '[{"id":"existing-voucher"}]']]);
  const context = vm.createContext({
    BUNDLED_COOKIES_LEO: [{ value: 'older-bootstrap' }], BUNDLED_COOKIES_GAORIEL: [{ value: 'shared-gab' }], BUNDLED_REDEMPTIONS: [{ id: 'older-voucher' }],
    REDEMPTIONS_DB_FILE_NAME: 'redemptions.json', SETTINGS_FILE_NAME: 'settings.json', COOKIES_FILE_NAME: 'legacy.json', console,
    getFileManager: () => ({ documentsDirectory: () => '/docs', joinPath: path.join,
      fileExists: p => files.has(p), readString: p => files.get(p), writeString: (p, v) => files.set(p, v) })
  });
  const seed = source.slice(source.indexOf('function persistPrivateBootstrap()'), source.indexOf('function loadHistory()'));
  const loaders = source.slice(source.indexOf('function loadSettings()'), source.indexOf('async function loadNativeRedemptions('));
  vm.runInContext(seed + loaders, context);
  context.persistPrivateBootstrap();
  assert.equal(JSON.parse(files.get('/docs/uol_session_cookies_leosaquetto.json'))[0].value, 'newer-local');
  assert.equal(JSON.parse(files.get('/docs/redemptions.json'))[0].id, 'existing-voucher');
  assert.equal(JSON.parse(files.get('/docs/uol_session_cookies_gaoriel.json'))[0].value, 'shared-gab');
  context.BUNDLED_COOKIES_LEO = []; context.BUNDLED_COOKIES_GAORIEL = []; context.BUNDLED_REDEMPTIONS = [];
  context.persistPrivateBootstrap();
  const settings = context.loadSettings();
  assert.equal(settings.userSessions.leosaquetto[0].value, 'newer-local');
  assert.equal(settings.userSessions.gaoriel[0].value, 'shared-gab');
  assert.equal(files.size, 3);
});

test('iCloud download is awaited before any private read and failure does not overwrite database', async () => {
  let resolveDownload, downloaded = false, writes = 0;
  const download = new Promise(resolve => { resolveDownload = resolve; });
  const context = vm.createContext({
    HISTORY_FILE_NAME: 'history.json', SETTINGS_FILE_NAME: 'settings.json', COOKIES_FILE_NAME: 'cookies.json', REDEMPTIONS_DB_FILE_NAME: 'redemptions.json',
    BUNDLED_REDEMPTIONS: [], console: { log() {} },
    getFileManager: () => ({ documentsDirectory: () => '/docs', joinPath: path.join,
      fileExists: p => p.endsWith('/redemptions.json'), isFileDownloaded: () => downloaded,
      downloadFileFromiCloud: async () => { await download; downloaded = true; },
      readString: () => { throw new Error('private read unavailable'); }, writeString: () => { writes++; } })
  });
  const prepare = source.slice(source.indexOf('async function preparePrivateFiles()'), source.indexOf('function persistPrivateBootstrap()'));
  const loader = source.slice(source.indexOf('function loadRedemptionsDatabase()'), source.indexOf('function getFileManager()'));
  vm.runInContext(prepare + loader, context);
  let finished = false;
  const pending = context.preparePrivateFiles().then(() => { finished = true; });
  await Promise.resolve();
  assert.equal(finished, false);
  resolveDownload(); await pending;
  assert.equal(downloaded, true);
  assert.equal(context.loadRedemptionsDatabase().length, 0);
  assert.equal(writes, 0);
  downloaded = false;
  context.getFileManager = () => ({ documentsDirectory: () => '/docs', joinPath: path.join,
    fileExists: () => true, isFileDownloaded: () => false, downloadFileFromiCloud: async () => { throw new Error('offline'); } });
  await assert.rejects(context.preparePrivateFiles(), /offline/);
  assert.equal(writes, 0);
});

test('OTA first opening shows UI without collection; next opening and explicit notifications collect', async () => {
  const files = new Map(); let action = 'open', collects = 0, displayed = 0, updated = 0, accessReads = 0, lastInitial, lastCollection, closeWindow;
  const access = { status: 'approved', role: 'owner', accounts: ['leosaquetto', 'gaoriel'] };
  const context = vm.createContext({
    SCRIPT_VERSION: 'test-1', STARTUP_HTML: '__STARTUP_TITLE__ __STARTUP_DESCRIPTION__', console: { log() {} },
    Timer: { schedule: (milliseconds, repeats, callback) => { assert.equal(milliseconds, 800); assert.equal(repeats, false); callback(); } },
    FileManager: { local: () => ({ documentsDirectory: () => '/local', joinPath: path.join, fileExists: p => files.has(p),
      readString: p => files.get(p), writeString: (p, v) => files.set(p, v) }) },
    hunterAction: () => action,
    createHunterAccess: () => ({ status: async () => { accessReads++; return access; } }),
    runNativeHunterCollector: async () => { collects++; return { status: 'complete' }; },
    performSelfUpdate: async () => { updated++; },
    preparePrivateFiles: async () => { assert.ok(displayed > 0, 'loading is visible before private data waits'); }, persistPrivateBootstrap() {},
    loadSettings: () => ({}), loadHistory: () => [], loadCredentials: () => ({}), loadSessionCookies: () => [], loadRedemptionsDatabase: () => [],
    buildDashboardHTML: (...args) => { lastInitial = args[5]; lastCollection = args[6]; return '<html></html>'; },
    WebView: class { async loadHTML(html) { if (html === '<html></html>') closeWindow(); } async evaluateJavaScript() { throw Error('unexpected presentation bridge'); } present() { displayed++; return new Promise(resolve => { closeWindow = resolve; }); } },
    Script: { complete() {}, setShortcutOutput() {} }
  });
  const gate = source.slice(source.indexOf('function hunterCollectionPausedForUpdate('), source.indexOf('async function runNativeHunterCollector('));
  const main = source.slice(source.indexOf('async function main()'), source.indexOf('function nativeRedemptionKey('));
  vm.runInContext(gate + main, context);
  await context.main(); assert.equal(collects, 0); assert.equal(displayed, 1); assert.equal(lastInitial, true);
  assert.equal(lastCollection.status, 'updated');
  assert.equal(lastCollection.access, access); assert.equal(accessReads, 1);
  await context.main(); assert.equal(collects, 1); assert.equal(displayed, 2); assert.equal(lastInitial, false);
  assert.equal(lastCollection.status, 'complete');
  context.SCRIPT_VERSION = 'test-2'; action = 'notification'; await context.main();
  assert.equal(collects, 2); assert.equal(displayed, 2, 'explicit notification is headless and eligible');
  action = 'update'; context.SCRIPT_VERSION = 'test-3'; await context.main();
  assert.equal(updated, 1); assert.equal(collects, 2);
  action = 'open'; await context.main(); assert.equal(collects, 2); assert.equal(displayed, 3);
  assert.equal(lastCollection.access, access); assert.equal(accessReads, 2);
});

test('closing the loading window never reopens it after delayed native data completes', async () => {
  const loaded = []; let releasePrivate, closeWindow, closed = false;
  const prepared = new Promise(resolve => { releasePrivate = resolve; });
  const context = vm.createContext({
    console: { log() {} }, STARTUP_HTML: '__STARTUP_TITLE__ __STARTUP_DESCRIPTION__',
    hunterAction: () => 'open', hunterCollectionPausedForUpdate: () => false,
    runNativeHunterCollector: async () => ({ status: 'complete', received: 1 }),
    Timer: { schedule: (_, __, callback) => callback() },
    preparePrivateFiles: async () => prepared, persistPrivateBootstrap() {},
    loadSettings: () => ({}), loadHistory: () => [], loadCredentials: () => ({}), loadSessionCookies: () => [], loadRedemptionsDatabase: () => [],
    buildDashboardHTML: () => '<html>dashboard</html>',
    WebView: class { async loadHTML(html) { assert.equal(closed, false); loaded.push(html); } present() { return new Promise(resolve => { closeWindow = () => { closed = true; resolve(); }; }); } },
    Script: { complete() {} },
  });
  const main = source.slice(source.indexOf('async function main()'), source.indexOf('function nativeRedemptionKey('));
  vm.runInContext(main, context);
  const running = context.main();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(loaded.length, 1); assert.match(loaded[0], /Preparando suas ofertas/);
  closeWindow(); releasePrivate(); await running;
  assert.equal(loaded.length, 1);
});

test('offer browser cancels only its explicit route and keeps one sheet open until dismissal', async () => {
  const calls = [], view = {}; let dismiss;
  const context = vm.createContext({
    Safari: { openInApp: (url, fullscreen) => { calls.push({ url, fullscreen }); return new Promise(resolve => { dismiss = resolve; }); } },
    console: { warn() {} },
  });
  const code = source.slice(source.indexOf('function installHunterBrowser('), source.indexOf('function nativeRedemptionKey('));
  vm.runInContext(code, context);
  context.installHunterBrowser(view);
  const url = 'https://clube.uol.com.br/campanhasdeingresso/show?date=18%2F10';
  const route = 'hunter-browser://open?url=' + encodeURIComponent(url);
  assert.equal(view.shouldAllowRequest({ url: route }), false, 'navigation is cancelled synchronously');
  assert.equal(calls.length, 0, 'native presentation starts after cancellation');
  await Promise.resolve();
  assert.deepEqual(calls, [{ url, fullscreen: false }]);
  for (const resource of ['https://clube.uol.com.br/imagem.png', 'https://fonts.googleapis.com/css2', 'blob:synthetic-voucher', 'scriptable:///run?scriptName=Hunter']) {
    assert.equal(view.shouldAllowRequest({ url: resource }), true, 'ordinary requests retain their existing path');
  }
  assert.equal(view.shouldAllowRequest({ url: route }), false);
  await Promise.resolve(); assert.equal(calls.length, 1, 'repeated taps cannot stack sheets');
  dismiss(); await new Promise(resolve => setImmediate(resolve));
  assert.equal(view.shouldAllowRequest({ url: route }), false);
  await Promise.resolve(); assert.equal(calls.length, 2, 'dismissal permits the next offer');
  dismiss(); await new Promise(resolve => setImmediate(resolve));
});

test('external offer route opens Safari without replacing Hunter or bypassing the browser lock', async () => {
  const externalCalls = [], internalCalls = [], view = {}; let dismiss;
  const context = vm.createContext({
    Safari: {
      open: url => { externalCalls.push(url); },
      openInApp: url => { internalCalls.push(url); return new Promise(resolve => { dismiss = resolve; }); },
    },
    console: { warn() {} },
  });
  const code = source.slice(source.indexOf('function installHunterBrowser('), source.indexOf('function nativeRedemptionKey('));
  vm.runInContext(code, context);
  context.installHunterBrowser(view);
  const url = 'https://clube.uol.com.br/campanhasdeingresso/show';
  const internal = 'hunter-browser://open?url=' + encodeURIComponent(url);
  const external = 'hunter-browser://external?url=' + encodeURIComponent(url);
  assert.equal(view.shouldAllowRequest({ url: internal }), false);
  await Promise.resolve(); assert.deepEqual(internalCalls, [url]);
  assert.equal(view.shouldAllowRequest({ url: external }), false);
  await Promise.resolve(); assert.equal(externalCalls.length, 0, 'external button shares the active-sheet lock');
  dismiss(); await new Promise(resolve => setImmediate(resolve));
  assert.equal(view.shouldAllowRequest({ url: external }), false, 'external navigation is also cancelled synchronously');
  assert.equal(externalCalls.length, 0);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(externalCalls, [url]); assert.deepEqual(internalCalls, [url]);
  assert.equal(view.shouldAllowRequest({ url: external }), false);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(externalCalls.length, 2, 'synchronous Safari.open releases the lock');
});

test('offer browser rejects unsafe destinations and releases its lock after a sanitized failure', async () => {
  const calls = [], warnings = [], view = {};
  const context = vm.createContext({
    Safari: {
      openInApp: async url => { calls.push(url); throw new Error('synthetic private session detail'); },
      open: url => { calls.push(url); throw new Error('synthetic private session detail'); },
    },
    console: { warn: message => warnings.push(message) },
  });
  const code = source.slice(source.indexOf('function installHunterBrowser('), source.indexOf('function nativeRedemptionKey('));
  vm.runInContext(code, context);
  context.installHunterBrowser(view);
  const prefixes = ['hunter-browser://open?url=', 'hunter-browser://external?url='];
  for (const prefix of prefixes) {
    for (const target of ['http://clube.uol.com.br/', 'https://example.test/', 'https://clube.uol.com.br.evil.test/',
      'https://clube.uol.com.br@evil.test/', 'https://clube.uol.com.br:444/', 'https://clube.uol.com.br/with space',
      'https://clube.uol.com.br/with\nnewline', 'https://clube.uol.com.br/with\\backslash', 'https://clube.uol.com.br/' + 'x'.repeat(4096)]) {
      assert.equal(view.shouldAllowRequest({ url: prefix + encodeURIComponent(target) }), false);
    }
    assert.equal(view.shouldAllowRequest({ url: prefix + '%broken' }), false);
  }
  await Promise.resolve(); assert.equal(calls.length, 0);
  for (const prefix of prefixes) {
    const priorCalls = calls.length;
    const route = prefix + encodeURIComponent('https://clubeuol.uol.com.br/');
    assert.equal(view.shouldAllowRequest({ url: route }), false);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(calls.length, priorCalls + 1); assert.equal(warnings.length, calls.length);
    assert.doesNotMatch(warnings.at(-1), /synthetic private session detail/);
    assert.equal(view.shouldAllowRequest({ url: route }), false);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(calls.length, priorCalls + 2, 'a rejected browser does not permanently block navigation');
  }
});

test('native collector requires approval and retains the optional private local fallback without exposing values', async () => {
  const files = new Map(), keys = new Map();
  const manager = root => ({ documentsDirectory: () => root, joinPath: path.join,
    fileExists: p => files.has(p), readString: p => files.get(p) });
  const privateConfig = { deviceId: 'test-phone', token: 'private-synthetic-token' };
  const context = vm.createContext({
    FileManager: { local: () => manager('/local') }, getFileManager: () => manager('/cloud'),
    Keychain: { contains: k => keys.has(k), get: k => keys.get(k) },
    HunterCollector: { KEYS: { token: 'token', device: 'device' }, create: () => ({
      importBootstrap: value => { keys.set('token', value.token); },
      run: async () => ({ status: keys.has('token') ? 'complete' : 'not_configured', sources: {} })
    }) },
    HunterAccess: { create: () => ({ status: async () => keys.has('token')
      ? { status: 'approved', role: 'member' }
      : { status: 'not_configured', reason: 'activation_required' } }) },
  });
  const code = source.slice(source.indexOf('async function runNativeHunterCollector('), source.indexOf('async function main()'));
  vm.runInContext(code, context);
  let result = await context.runNativeHunterCollector();
  assert.equal(result.status, 'activation_required');
  assert.equal(result.reason, 'activation_required');
  files.set('/local/uol_hunter_private_bootstrap.json', JSON.stringify(privateConfig));
  files.set('/cloud/uol_hunter_private_bootstrap.json', 'unavailable-cloud-copy');
  result = await context.runNativeHunterCollector();
  assert.equal(result.status, 'complete');
  assert.equal(JSON.stringify(result).includes(privateConfig.token), false);
  keys.clear(); keys.set('device', 'previous-phone');
  result = await context.runNativeHunterCollector();
  assert.equal(result.reason, 'device_mismatch'); assert.equal(keys.has('token'), false);
});

test('generated HTML embeds exact data safely without replacement-string expansion or double escaped SVG', () => {
  const artifact = fs.readFileSync(path.join(__dirname, 'dist/Hunter.js'), 'utf8');
  const start = artifact.indexOf('function buildDashboardHTML(');
  const end = artifact.indexOf('\nawait main();', start);
  const icons = { tickets: '<svg viewBox="0 0 24 24"></svg>' };
  const credentials = { test: "$& $` $' $$ </script><script>" };
  const context = vm.createContext({ BUNDLED_COOKIES_GAORIEL: [], BUNDLED_COOKIES_LEO: [],
    BUNDLED_REDEMPTIONS: [], ICONS: icons, NTFY_TOPIC: 'fixture', SCRIPT_VERSION: 'test', loadNavigationTheme: () => 'white' });
  context.HUNTER_LOGO_BASE64 = require('./test-support/artifact.cjs').literal(artifact, 'const HUNTER_LOGO_BASE64 = ');
  const resolverStart = artifact.indexOf('function resolveHunterAssets(');
  vm.runInContext(artifact.slice(resolverStart, artifact.indexOf('\n}', resolverStart) + 2), context);
  vm.runInContext(artifact.slice(start, end), context);
  const html = context.buildDashboardHTML({ active: [], history: [] }, { userSessions: {} }, credentials, [], [], false, { status: 'complete' });
  assert.equal((html.match(/<script>/g) || []).length, 1);
  const inline = html.slice(html.indexOf('<script>') + 8, html.lastIndexOf('</script>'));
  new vm.Script(inline);
  const initialLine = inline.slice(inline.indexOf('const INITIAL ='), inline.indexOf('\n', inline.indexOf('const INITIAL =')));
  vm.runInContext(initialLine + '; globalThis.initial = INITIAL;', context);
  assert.deepEqual(JSON.parse(JSON.stringify(context.initial.credentials)), credentials);
  assert.deepEqual(JSON.parse(JSON.stringify(context.initial.icons)), icons);
  assert.equal(context.initial.hunterCollection.status, 'complete');
  assert.equal(context.initial.navigationBarTheme, 'white');
});

test('redemption identity never merges accounts, missing accounts, or synthetic row indexes', () => {
  const context = vm.createContext({});
  vm.runInContext(source.slice(source.indexOf('function nativeRedemptionKey('), source.indexOf('function loadRedemptionsDatabase(')), context);
  assert.equal(context.nativeRedemptionKey({ id: 'voucher-1' }), '');
  assert.equal(context.nativeRedemptionKey({ user: 'leo', id: 'redemption-leo-1' }), '');
  assert.notEqual(context.nativeRedemptionKey({ user: 'leo', url: '/voucher/1' }), context.nativeRedemptionKey({ user: 'gab', url: '/voucher/1' }));
  assert.equal(context.nativeRedemptionKey({ user: 'leo', id: 'voucher-1' }), 'leo|id:voucher-1');
});
