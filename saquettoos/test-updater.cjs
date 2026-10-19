const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const updater = require('./native/updater.js');

const base = 'https://raw.githubusercontent.com/leosaquetto/uol-bot/';
const apps = {
  os: { id: 'saquettoos', scriptName: 'saquettoOS', manifestBase: base + 'main/saquettoos/releases/', artifactPath: 'saquettoos/dist/saquettoOS.js', envelope: 'saquettoos' },
  uol: { id: 'clube-uol', scriptName: 'Clube UOL Premium', manifestBase: base + 'main/stories-hunter/releases/', artifactPath: 'stories-hunter/dist/Hunter.js', envelope: 'uol' }
};
const hash = code => crypto.createHash('sha256').update(code).digest('hex');
function code(version = '1.0.0', kind = 'os') {
  if (kind === 'uol') return `// Variables used by Scriptable.\n// These must be at the very top of the file.\n// Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter\nconst SCRIPT_VERSION = "${version}";\nconst HunterCollector = (() => { return {}; })();\nfunction hunterCollectionPausedForUpdate(action) {}\nasync function main() {}\nfunction buildDashboardHTML() {}\nawait main();\nScript.complete();\n`;
  return `// saquettoOS Scriptable launcher\nconst SAQUETTO_VERSION = '${version}';\nasync function saquettoMain() {}\n// Português, ações, 🎟️\nawait saquettoMain();\nScript.complete();\n`;
}
function harness(kind = 'os') {
  const app = apps[kind], target = '/cloud/' + app.scriptName + '.js';
  const previous = code('1.0.0', kind), next = code('1.1.0', kind);
  const files = new Map([[target, previous], ['/cloud/saquettoOS.js', code()], ['/cloud/PillTracker/dados.json', '{"testFixture":"preserve"}'], ['/local/session.json', '{"testFixture":"preserve"}']]);
  const state = { next, requestHook: null, writeHook: null, moveHook: null, response: {}, manifestResponse: {}, failNetwork: false, downloads: [], requests: [], writes: [], moves: [], duplicate: false, icloudPending: false };
  const publication = value => ({ schema: 1, updaterProtocol: 1, version: '1.1.0', channel: 'stable', artifact: base + 'a'.repeat(40) + '/' + app.artifactPath, size: Buffer.byteLength(value), sha256: hash(value), publishedAt: '2026-10-18T17:00:00.000Z' });
  state.manifest = publication(next);
  function manager(dir) {
    return {
      documentsDirectory: () => dir,
      joinPath: (a, b) => a + '/' + b,
      listContents: path => { const names = [...files.keys()].filter(p => p.startsWith(path + '/') && !p.slice(path.length + 1).includes('/')).map(p => p.slice(path.length + 1)); return state.duplicate ? names.concat(app.scriptName + '.js') : names; },
      fileExists: path => files.has(path), isDirectory: () => false,
      readString: path => { if (!files.has(path)) throw new Error('missing'); return files.get(path); },
      writeString: (path, value) => { state.writes.push(path); if (state.writeHook && state.writeHook(path, value) === false) return; files.set(path, value); },
      move: (from, to) => { state.moves.push([from, to]); if (state.moveHook && state.moveHook(from, to) === false) return; if (!files.has(from)) throw new Error('missing'); files.set(to, files.get(from)); files.delete(from); },
      isFileDownloaded: () => !state.icloudPending,
      downloadFileFromiCloud: async path => { state.downloads.push(path); state.icloudPending = false; }
    };
  }
  const fm = manager('/cloud'), local = manager('/local'); let count = 0;
  const request = url => ({
    response: null,
    async load() {
      state.requests.push(url); if (state.requestHook) await state.requestHook(url);
      if (state.failNetwork) throw new Error('network');
      const isManifest = url.endsWith('.json'), input = isManifest ? typeof state.manifest === 'string' ? state.manifest : JSON.stringify(state.manifest) : state.next;
      const bytes = Buffer.isBuffer(input) ? input : Buffer.from(input);
      this.response = { statusCode: 200, url, headers: { 'content-type': isManifest ? 'application/json' : 'text/plain' }, ...(isManifest ? state.manifestResponse : state.response) };
      return { getBytes: () => Array.from(bytes), toRawString: () => bytes.toString('utf8') };
    }
  });
  const api = updater.create({ fm, local, request, uuid: () => '12345678-abcd-abcd-abcd-' + String(++count).padStart(12, '0'), scriptName: 'saquettoOS' });
  const journal = () => [...files].find(([p]) => /^\/local\/saquettoos_ota_[a-f0-9]+\.json$/.test(p));
  const preserved = () => { assert.equal(files.get('/cloud/PillTracker/dados.json'), '{"testFixture":"preserve"}'); assert.equal(files.get('/local/session.json'), '{"testFixture":"preserve"}'); };
  return { app, target, previous, next, files, state, api, fm, local, request, publication, journal, preserved };
}

test('module import has no filesystem, UI or execution side effects', () => {
  assert.deepEqual(Object.keys(updater), ['create', 'fingerprint']);
  assert.equal(updater.fingerprint('ação'), require('node:crypto').createHash('sha256').update('ação').digest('hex'));
});
test('version discovery downloads iCloud source and reports stable/beta without downgrade', async () => {
  const h = harness(); h.state.icloudPending = true;
  assert.equal((await h.api.inspect(h.app)).version, '1.0.0'); assert.deepEqual(h.state.downloads, [h.target]);
  const available = await h.api.check(h.app); assert.equal(available.status, 'available'); assert.equal(available.latestVersion, '1.1.0');
  h.state.manifest.channel = 'beta'; assert.equal((await h.api.check(h.app, 'beta')).status, 'available');
  h.state.manifest.version = '0.9.0'; assert.equal((await h.api.check(h.app, 'beta')).status, 'up_to_date');
  assert.equal(h.state.writes.length, 0);
});
test('unconfigured apps, altered allowlist fields, invalid channels and unknown installs never write', async () => {
  for (const app of [{ id: 'pilltracker' }, { id: '__proto__' }, { id: 'constructor' }]) {
    const h = harness(); assert.equal((await h.api.install(app)).status, 'not_configured'); assert.equal(h.state.requests.length, 0);
  }
  for (const key of ['scriptName', 'manifestBase', 'artifactPath', 'envelope']) {
    const h = harness(); assert.equal((await h.api.install({ ...h.app, [key]: '../arbitrary' })).status, 'invalid_source'); assert.equal(h.state.writes.length, 0);
  }
  const h = harness(); assert.equal((await h.api.install(h.app, 'nightly')).status, 'invalid_channel');
  h.files.set(h.target, 'standalone script'); assert.equal((await h.api.install(h.app)).status, 'unrecognized'); assert.equal(h.state.writes.length, 0);
  h.files.delete(h.target); assert.equal((await h.api.install(h.app)).status, 'not_installed');
});
test('duplicate canonical names and launcher rename fail closed', async () => {
  const h = harness(); h.state.duplicate = true; assert.equal((await h.api.install(h.app)).status, 'ambiguous_installation');
  const other = updater.create({ fm: h.fm, local: h.local, request: h.request, uuid: () => '12345678', scriptName: 'Other' });
  assert.equal((await other.inspect(h.app)).status, 'script_name_mismatch'); assert.equal(h.state.writes.length, 0);
});
test('manifest validates exact pinned artifact path, schema, date, hash, byte cap and version', async () => {
  for (const patch of [{ schema: 2 }, { updaterProtocol: 2 }, { channel: 'beta' }, { version: '01.1.0' }, { version: '1.1.0-beta' }, { size: 8388609 }, { size: 0 }, { sha256: '0'.repeat(63) }, { publishedAt: '2026-02-31T17:00:00Z' }, { artifact: base + 'main/saquettoos/dist/saquettoOS.js' }, { artifact: base + 'a'.repeat(40) + '/stories-hunter/dist/Hunter.js' }, { artifact: base + 'a'.repeat(40) + '/saquettoos/dist/saquettoOS.js?x=1' }]) {
    const h = harness(); Object.assign(h.state.manifest, patch); const result = await h.api.install(h.app); assert.ok(['invalid_manifest', 'incompatible'].includes(result.status), JSON.stringify(patch)); assert.equal(h.files.get(h.target), h.previous); assert.equal(h.state.writes.length, 0);
  }
  const h = harness('uol'); h.state.manifest.size = 2097153; assert.equal((await h.api.check(h.app)).status, 'invalid_manifest');
});
test('network, redirect, type, oversized manifest and malformed JSON fail without installation', async () => {
  for (const change of [h => h.state.failNetwork = true, h => h.state.manifestResponse = { statusCode: 429 }, h => h.state.manifestResponse = { url: 'https://example.org' }, h => h.state.manifestResponse = { headers: { 'content-type': 'text/html' } }, h => h.state.manifest = 'x'.repeat(8193), h => h.state.manifest = '{']) {
    const h = harness(); change(h); assert.ok(['network_error', 'invalid_manifest'].includes((await h.api.install(h.app)).status)); assert.equal(h.state.writes.length, 0);
  }
});
test('artifact hash, UTF-8, size, envelope and syntax failures preserve script', async () => {
  for (const change of [h => h.state.manifest.sha256 = '0'.repeat(64), h => h.state.next += '!', h => { h.state.next = Buffer.from([0xc0, 0xaf]); h.state.manifest = h.publication(h.state.next); }, h => { h.state.next = h.next.replace('await saquettoMain();', 'void saquettoMain();'); h.state.manifest = h.publication(h.state.next); }, h => { h.state.next = h.next.replace('async function', 'const broken = ;\nasync function'); h.state.manifest = h.publication(h.state.next); }]) {
    const h = harness(); change(h); assert.equal((await h.api.install(h.app)).status, 'invalid_artifact'); assert.equal(h.files.get(h.target), h.previous); assert.equal(h.state.writes.length, 0);
  }
});
test('verified UTF-8 artifacts install only into their own target, back up exact bytes and leave data intact', async () => {
  for (const kind of ['os', 'uol']) {
    const h = harness(kind); const originalLauncher = h.files.get('/cloud/saquettoOS.js');
    const result = await h.api.install(h.app); assert.equal(result.status, 'installed'); assert.equal(result.version, '1.1.0'); assert.equal(hash(h.files.get(h.target)), h.state.manifest.sha256);
    assert.equal([...h.files].find(([p]) => p.includes('backup-'))[1], h.previous);
    assert.equal(JSON.parse(h.journal()[1]).phase, 'installed'); h.preserved();
    if (kind === 'uol') assert.equal(h.files.get('/cloud/saquettoOS.js'), originalLauncher);
  }
});
test('candidate syntax validation never invokes downloaded code', async () => {
  const h = harness(); h.state.next = h.next.replace('// Português', 'throw new Error("must not execute");\n// Português'); h.state.manifest = h.publication(h.state.next);
  assert.equal((await h.api.install(h.app)).status, 'installed');
});
test('backup/staging/journal write faults preserve existing script', async () => {
  for (const part of ['backup-', '.ota-', 'saquettoos_ota_']) {
    const h = harness(); h.state.writeHook = path => { if (path.includes(part)) throw new Error('disk'); };
    assert.equal((await h.api.install(h.app)).status, 'installation_failed'); assert.equal(h.files.get(h.target), h.previous); h.preserved();
  }
});
test('failed move rolls back a missing target from verified local backup', async () => {
  const h = harness(); h.state.moveHook = (from, to) => { if (from.includes('.ota-')) { h.files.delete(to); throw new Error('interrupted move'); } };
  assert.equal((await h.api.install(h.app)).status, 'rolled_back'); assert.equal(h.files.get(h.target), h.previous); assert.equal(JSON.parse(h.journal()[1]).phase, 'rolled_back'); h.preserved();
});
test('unknown readback stays explicit recovery_required and does not overwrite third-party bytes', async () => {
  const h = harness(); h.state.moveHook = (from, to) => { h.files.set(to, 'unrecognized concurrent content'); return false; };
  assert.equal((await h.api.install(h.app)).status, 'recovery_required'); assert.equal(h.files.get(h.target), 'unrecognized concurrent content');
  assert.equal((await h.api.recover(h.app)).status, 'recovery_required'); h.preserved();
});
test('changed installation during download and during staging remains untouched', async () => {
  for (const timing of ['download', 'stage']) {
    const h = harness(), other = code('1.2.0');
    if (timing === 'download') h.state.requestHook = url => { if (url.endsWith('.js')) h.files.set(h.target, other); };
    else h.state.writeHook = path => { if (path.includes('.ota-')) h.files.set(h.target, other); };
    assert.equal((await h.api.install(h.app)).status, 'concurrent_change'); assert.equal(h.files.get(h.target), other); h.preserved();
  }
});
test('competing journal ownership prevents rollback', async () => {
  const h = harness(); h.state.moveHook = () => { const [p, encoded] = h.journal(), j = JSON.parse(encoded); j.operationId = '99999999-abcd-abcd-abcd-123456789012'; h.files.set(p, JSON.stringify(j)); throw new Error('race'); };
  assert.equal((await h.api.install(h.app)).status, 'concurrent_change'); assert.equal(h.files.get(h.target), h.previous);
});
test('recovery validates journal paths, recognizes readback, and restores after interruption', async () => {
  const h = harness(); await h.api.install(h.app); const [p, encoded] = h.journal(), original = JSON.parse(encoded);
  h.files.set(p, JSON.stringify({ ...original, phase: 'replacing', backupPath: '/local/session.json' }));
  assert.equal((await h.api.recover(h.app)).status, 'recovery_required'); h.preserved();
  h.files.set(p, JSON.stringify({ ...original, phase: 'replacing' })); assert.equal((await h.api.recover(h.app)).status, 'installed');
  h.files.set(p, JSON.stringify({ ...original, phase: 'replacing' })); h.files.delete(h.target);
  assert.equal((await h.api.recover(h.app)).status, 'rolled_back'); assert.equal(h.files.get(h.target), h.previous); h.preserved();
});
test('same instance serializes installs and rejects recovery while installing', async () => {
  const h = harness(); let release; const gate = new Promise(resolve => { release = resolve; });
  h.state.requestHook = async () => gate;
  const first = h.api.install(h.app); await new Promise(resolve => setImmediate(resolve));
  assert.equal((await h.api.install(h.app)).status, 'busy'); assert.equal((await h.api.recover(h.app)).status, 'busy');
  release(); assert.equal((await first).status, 'installed');
});
