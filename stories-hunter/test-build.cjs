const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, 'build.js'), 'utf8');
const optionCode = source.slice(source.indexOf('const buildOptions'), source.indexOf('const logoBase64'));
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
  const files = new Map(); let action = 'open', collects = 0, displayed = 0, updated = 0, lastInitial, lastCollection, closeWindow;
  const context = vm.createContext({
    SCRIPT_VERSION: 'test-1', STARTUP_HTML: '__STARTUP_TITLE__ __STARTUP_DESCRIPTION__', console: { log() {} },
    Timer: { schedule: (milliseconds, repeats, callback) => { assert.equal(milliseconds, 800); assert.equal(repeats, false); callback(); } },
    FileManager: { local: () => ({ documentsDirectory: () => '/local', joinPath: path.join, fileExists: p => files.has(p),
      readString: p => files.get(p), writeString: (p, v) => files.set(p, v) }) },
    hunterAction: () => action,
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
  await context.main(); assert.equal(collects, 1); assert.equal(displayed, 2); assert.equal(lastInitial, false);
  assert.equal(lastCollection.status, 'complete');
  context.SCRIPT_VERSION = 'test-2'; action = 'notification'; await context.main();
  assert.equal(collects, 2); assert.equal(displayed, 2, 'explicit notification is headless and eligible');
  action = 'update'; context.SCRIPT_VERSION = 'test-3'; await context.main();
  assert.equal(updated, 1); assert.equal(collects, 2);
  action = 'open'; await context.main(); assert.equal(collects, 2); assert.equal(displayed, 3);
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

test('native collector imports private local fallback and reports a missing bootstrap without exposing values', async () => {
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
  });
  const code = source.slice(source.indexOf('async function runNativeHunterCollector('), source.indexOf('async function main()'));
  vm.runInContext(code, context);
  let result = await context.runNativeHunterCollector();
  assert.equal(result.reason, 'bootstrap_missing');
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
  const artifact = fs.readFileSync(path.join(__dirname, '../scriptable_uol_stories_hunter.js'), 'utf8');
  const start = artifact.indexOf('function buildDashboardHTML(');
  const end = artifact.indexOf('\nawait main();', start);
  const icons = { tickets: '<svg viewBox="0 0 24 24"></svg>' };
  const credentials = { test: "$& $` $' $$ </script><script>" };
  const context = vm.createContext({ BUNDLED_COOKIES_GAORIEL: [], BUNDLED_COOKIES_LEO: [],
    BUNDLED_REDEMPTIONS: [], ICONS: icons, NTFY_TOPIC: 'fixture', SCRIPT_VERSION: 'test' });
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
});

test('redemption identity never merges accounts, missing accounts, or synthetic row indexes', () => {
  const context = vm.createContext({});
  vm.runInContext(source.slice(source.indexOf('function nativeRedemptionKey('), source.indexOf('function loadRedemptionsDatabase(')), context);
  assert.equal(context.nativeRedemptionKey({ id: 'voucher-1' }), '');
  assert.equal(context.nativeRedemptionKey({ user: 'leo', id: 'redemption-leo-1' }), '');
  assert.notEqual(context.nativeRedemptionKey({ user: 'leo', url: '/voucher/1' }), context.nativeRedemptionKey({ user: 'gab', url: '/voucher/1' }));
  assert.equal(context.nativeRedemptionKey({ user: 'leo', id: 'voucher-1' }), 'leo|id:voucher-1');
});
