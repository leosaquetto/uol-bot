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
