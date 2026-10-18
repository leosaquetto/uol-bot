const { test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, 'native/updater.js'), 'utf8');
const hash = data => crypto.createHash('sha256').update(data).digest('hex');
const artifact = version => '// Variables used by Scriptable.\n// These must be at the very top of the file.\n// Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter\nconst SCRIPT_VERSION = "' + version + '";\nconst HunterCollector = (() => { return {}; })();\nfunction hunterCollectionPausedForUpdate(action) {}\nasync function main() {}\nfunction buildDashboardHTML() {}\n// Português, ações e 🎟️\nawait main();\nScript.complete();\n';
const artifactURL = 'https://raw.githubusercontent.com/leosaquetto/uol-bot/' + 'a'.repeat(40) + '/stories-hunter/dist/Hunter.js';
function publication(code = artifact('5.8.0'), channel = 'stable') {
  return { schema: 1, updaterProtocol: 1, version: '5.8.0', channel, artifact: artifactURL,
    size: Buffer.byteLength(code), sha256: hash(code), publishedAt: '2026-10-18T17:00:00.000Z' };
}
function harness(options = {}) {
  const previous = artifact('5.7.9'), next = artifact('5.8.0');
  const files = new Map([['/cloud/Hunter.js', previous], ['/cloud/vouchers.json', 'private-vouchers'], ['/local/session.json', 'private-session']]);
  const writes = [], moves = [], calls = [], alerts = [], opened = [];
  const state = { code: next, manifest: publication(next), fail: null, onRequest: null, onWrite: null, onRead: null, onMove: null, confirm: 0,
    response: {}, manifestResponse: {}, networkFailure: false, journalFailure: false };
  function manager(root) { return {
    documentsDirectory: () => root, joinPath: path.posix.join, fileExists: p => files.has(p),
    readString(p) { if (state.onRead) { const override = state.onRead(p); if (override !== undefined) return override; } return files.get(p); },
    writeString(p, value) {
      writes.push(p);
      if (state.onWrite && state.onWrite(p, value) === false) return;
      files.set(p, value);
    },
    move(from, to) {
      moves.push([from, to]);
      if (state.onMove && state.onMove(from, to) === false) return;
      files.set(to, files.get(from)); files.delete(from);
    }
  }; }
  const cloud = manager('/cloud'), local = manager('/local');
  if (options.noMove) delete cloud.move;
  const context = vm.createContext({ SCRIPT_VERSION: '5.7.9', getFileManager: () => cloud, FileManager: { local: () => local },
    Script: { name: () => 'Hunter' }, UUID: { string: () => '12345678-abcd-1234-abcd-123456789012' },
    Request: class {
      constructor(url) { this.url = url; }
      async load() {
        calls.push(this);
        if (state.networkFailure) throw new Error('network');
        if (state.onRequest) await state.onRequest(this.url);
        const isManifest = this.url.endsWith('.json');
        this.response = { statusCode: 200, url: this.url, headers: { 'Content-Type': 'text/plain;charset=utf-8' },
          ...(isManifest ? state.manifestResponse : state.response) };
        const input = isManifest ? typeof state.manifest === 'string' ? state.manifest : JSON.stringify(state.manifest) : state.code;
        const data = Buffer.isBuffer(input) ? input : Buffer.from(input, 'utf8');
        return { getBytes: () => Array.from(data), toRawString: () => data.toString('utf8') };
      }
    },
    Alert: class { addAction() {} addCancelAction() {} async presentAlert() { alerts.push({ title: this.title, message: this.message }); return this.title === 'Atualização disponível' ? state.confirm : 0; } },
    Safari: { open: url => opened.push(url) }
  });
  vm.runInContext(source, context);
  return { context, files, writes, moves, calls, alerts, opened, state, previous, next, local, cloud,
    result: () => vm.runInContext('hunterLastUpdateResult', context),
    journal: () => Array.from(files).find(([p]) => /^\/local\/uol_hunter_ota_[a-f0-9]+\.json$/.test(p)),
    unchanged() { assert.equal(files.get('/cloud/Hunter.js'), previous); assert.equal(files.get('/cloud/vouchers.json'), 'private-vouchers'); assert.equal(files.get('/local/session.json'), 'private-session'); }
  };
}

test('native SHA-256 matches node crypto across UTF-8, block boundaries and the full artifact', () => {
  const h = harness();
  for (const input of ['', 'abc', 'Português 🎟️\0', ...[55,56,63,64,65,127,128,129,10000].map(n => 'x'.repeat(n)), fs.readFileSync(path.join(__dirname, 'dist/Hunter.js'), 'utf8')]) {
    assert.equal(h.context.hunterUpdateSha256(Array.from(Buffer.from(input))), hash(input));
    assert.equal(h.context.hunterUpdateHashCode(input), hash(input));
  }
  assert.equal(h.context.hunterUpdateSha256(Array.from({ length: 256 }, (_, i) => i)), hash(Buffer.from(Array.from({ length: 256 }, (_, i) => i))));
});

test('metadata discovery downloads no code and writes no installed or private file', async () => {
  const h = harness(); const result = await h.context.checkHunterUpdate();
  assert.equal(result.status, 'available'); assert.equal(result.manifest.version, '5.8.0');
  assert.equal(h.calls.length, 1); assert.match(h.calls[0].url, /\/main\/stories-hunter\/releases\/stable\.json$/);
  assert.equal(h.calls[0].onRedirect(), null); assert.equal(h.calls[0].timeoutInterval, 20); assert.equal(h.writes.length, 0); h.unchanged();
});

test('same/older versions and unavailable network never download executable code', async () => {
  for (const version of ['5.7.9', '5.7.8']) {
    const h = harness(); h.state.manifest.version = version;
    assert.equal((await h.context.checkHunterUpdate()).status, 'up_to_date');
    assert.equal(await h.context.performSelfUpdate(true), false); assert.equal(h.calls.length, 2); assert.equal(h.writes.length, 0); h.unchanged();
  }
  const h = harness(); h.state.networkFailure = true;
  assert.equal((await h.context.checkHunterUpdate()).status, 'network_error'); h.unchanged();
});

test('channel is device-local and beta/stable reference identical immutable artifact bytes', async () => {
  const h = harness(); h.files.set('/local/uol_hunter_update_channel.json', JSON.stringify({ channel: 'beta' }));
  h.state.manifest.channel = 'beta'; const beta = await h.context.checkHunterUpdate();
  assert.equal(beta.channel, 'beta'); assert.match(h.calls[0].url, /\/beta\.json$/);
  h.files.set('/local/uol_hunter_update_channel.json', JSON.stringify({ channel: 'stable' })); h.state.manifest.channel = 'stable';
  const stable = await h.context.checkHunterUpdate(); assert.equal(beta.manifest.sha256, stable.manifest.sha256); assert.equal(beta.manifest.artifact, stable.manifest.artifact);
  h.files.set('/local/uol_hunter_update_channel.json', '{broken'); assert.equal(h.context.hunterUpdateChannel(), 'stable');
});

test('invalid schema, protocol, channel, mutable URL and malformed manifest fail closed', async () => {
  const cases = [
    [{ schema: 2 }, 'incompatible'], [{ updaterProtocol: 2 }, 'incompatible'], [{ version: '5.8.0-beta' }, 'invalid_manifest'],
    [{ version: '05.8.0' }, 'invalid_manifest'], [{ channel: 'beta' }, 'invalid_manifest'], [{ size: 0 }, 'invalid_manifest'],
    [{ size: 2097153 }, 'invalid_manifest'], [{ size: 20.5 }, 'invalid_manifest'], [{ sha256: 'x'.repeat(64) }, 'invalid_manifest'],
    [{ artifact: artifactURL.replace('a'.repeat(40), 'main') }, 'invalid_manifest'], [{ artifact: artifactURL + '?redirect=1' }, 'invalid_manifest'],
    [{ artifact: artifactURL.replace('raw.githubusercontent.com', 'example.org') }, 'invalid_manifest'],
    [{ publishedAt: '2026-02-31T17:00:00.000Z' }, 'invalid_manifest'], [{ publishedAt: 'yesterday' }, 'invalid_manifest']
  ];
  for (const [patch, expected] of cases) {
    const h = harness(); Object.assign(h.state.manifest, patch); assert.equal((await h.context.checkHunterUpdate()).status, expected, JSON.stringify(patch));
    assert.equal(h.writes.length, 0); h.unchanged();
  }
  for (const text of ['{broken', '[]', 'null', 'x'.repeat(8193)]) {
    const h = harness(); h.state.manifest = text; assert.equal((await h.context.checkHunterUpdate()).status, 'invalid_manifest');
  }
});

test('manifest accepts exact legacy commit URL and ISO seconds without milliseconds', () => {
  const h = harness(), manifest = publication();
  manifest.artifact = artifactURL.replace('stories-hunter/dist/Hunter.js', 'scriptable_uol_stories_hunter.js');
  manifest.publishedAt = '2026-10-18T17:00:00Z';
  assert.equal(h.context.hunterValidateUpdateManifest(manifest, 'stable').status, 'valid');
});

test('HTTP/redirect/MIME and oversized responses preserve current installation', async () => {
  for (const response of [{ statusCode: 500 }, { statusCode: 302 }, { statusCode: 404 }, { url: 'https://example.org/Hunter.js' },
    { headers: { 'Content-Type': 'text/html' } }, { headers: { 'Content-Type': 'text/plain', 'Content-Length': '2097153' } }]) {
    const h = harness(); h.state.response = response;
    assert.equal(await h.context.performSelfUpdate(true), false); assert.equal(h.writes.length, 0); h.unchanged();
  }
  const h = harness(); h.state.manifestResponse = { statusCode: 429 }; assert.equal((await h.context.checkHunterUpdate()).status, 'network_error');
});

test('truncated, changed same-size, invalid UTF-8 and version-mismatched downloads are rejected', async () => {
  for (const make of [h => { h.state.code = h.next.slice(0, -1); },
    h => { h.state.code = h.next.replace('Hunter', 'Huntes'); },
    h => { h.state.code = Buffer.from([0xc0, 0xaf]); h.state.manifest.size = 2; h.state.manifest.sha256 = hash(h.state.code); },
    h => { h.state.code = artifact('5.9.0'); h.state.manifest.size = Buffer.byteLength(h.state.code); h.state.manifest.sha256 = hash(h.state.code); },
    h => { h.state.code = h.next.replace('await main();', 'void main();'); h.state.manifest.size = Buffer.byteLength(h.state.code); h.state.manifest.sha256 = hash(h.state.code); }]) {
    const h = harness(); make(h); assert.equal(await h.context.performSelfUpdate(true), false); assert.equal(h.result().status, 'invalid_artifact'); assert.equal(h.writes.length, 0); h.unchanged();
  }
});

test('verified code-only install stages beside target, saves verified backup/journal and relaunches', async () => {
  const h = harness(); assert.equal(await h.context.performSelfUpdate(true), true);
  assert.equal(h.calls.length, 2); assert.equal(h.files.get('/cloud/Hunter.js'), h.next);
  const backup = Array.from(h.files).find(([p]) => p.includes('uol_hunter_ota_backup-'));
  assert.equal(backup[1], h.previous); assert.equal(JSON.parse(h.journal()[1]).phase, 'installed');
  assert.equal(h.moves.length, 1); assert.match(h.moves[0][0], /^\/cloud\/\.Hunter\.ota-/); assert.equal(h.moves[0][1], '/cloud/Hunter.js');
  assert.equal(h.opened[0], 'scriptable:///run?scriptName=Hunter');
  assert.ok(!h.writes.some(p => /voucher|session|cookie|database|settings/.test(p)));
});

test('cancelled confirmation leaves installed code and files untouched', async () => {
  const h = harness(); h.state.confirm = -1;
  assert.equal(await h.context.performSelfUpdate(true), false); assert.equal(h.result().status, 'cancelled'); assert.equal(h.writes.length, 0); h.unchanged();
});

test('backup, staging and journal faults never touch installed script', async () => {
  for (const phase of ['backup', 'stage', 'journal']) {
    const h = harness(); h.state.onWrite = (p, value) => {
      if (phase === 'backup' && p.includes('backup-') || phase === 'stage' && p.includes('.ota-') || phase === 'journal' && /uol_hunter_ota_[a-f0-9]+\.json$/.test(p)) {
        h.files.set(p, 'truncated'); return false;
      }
    };
    assert.equal(await h.context.performSelfUpdate(true), false, phase); assert.equal(h.moves.length, 0); h.unchanged();
  }
});

test('installed file changed during download or preparation is never overwritten', async () => {
  for (const when of ['download', 'stage']) {
    const h = harness(), different = artifact('5.9.0');
    if (when === 'download') h.state.onRequest = url => { if (url.endsWith('.js')) h.files.set('/cloud/Hunter.js', different); };
    else h.state.onWrite = p => { if (p.includes('.ota-')) h.files.set('/cloud/Hunter.js', different); };
    assert.equal(await h.context.performSelfUpdate(true), false); assert.equal(h.files.get('/cloud/Hunter.js'), different); assert.equal(h.result().status, 'concurrent_change'); assert.equal(h.moves.length, 0);
  }
});

test('partial replacement restores only the verified previous script', async () => {
  const h = harness(); h.state.onMove = (from, to) => {
    if (from.includes('.ota-')) { h.files.set(to, 'truncated'); throw new Error('partial move'); }
  };
  assert.equal(await h.context.performSelfUpdate(true), false); assert.equal(h.result().status, 'restored'); h.unchanged();
  assert.equal(JSON.parse(h.journal()[1]).phase, 'rolled_back'); assert.equal(h.opened.length, 0);
});

test('lost successful move response is reconciled by exact installed hash', async () => {
  const h = harness(); h.state.onMove = (from, to) => { h.files.set(to, h.files.get(from)); throw new Error('response lost'); };
  assert.equal(await h.context.performSelfUpdate(true), true); assert.equal(h.files.get('/cloud/Hunter.js'), h.next); assert.equal(h.moves.length, 1);
  assert.equal(h.writes.filter(target => target === '/cloud/Hunter.js').length, 0);
});

test('unattended update falls back once when move throws or silently preserves the exact prior script', async () => {
  for (const mode of ['throws', 'no-op']) {
    const runtime = harness();
    runtime.state.onMove = () => { if (mode === 'throws') throw new Error('destination exists'); return false; };
    assert.equal(await runtime.context.performSelfUpdate(false, true), true, mode);
    assert.equal(runtime.result().status, 'installed');
    assert.equal(runtime.files.get('/cloud/Hunter.js'), runtime.next);
    assert.equal(runtime.moves.length, 1);
    assert.deepEqual(runtime.writes.filter(target => target === '/cloud/Hunter.js'), ['/cloud/Hunter.js']);
    assert.equal(JSON.parse(runtime.journal()[1]).phase, 'installed');
    assert.equal(runtime.alerts.length, 0); assert.equal(runtime.opened.length, 0);
    assert.equal(runtime.files.get('/cloud/vouchers.json'), 'private-vouchers');
    assert.equal(runtime.files.get('/local/session.json'), 'private-session');
    assert.ok(!runtime.writes.some(target => /voucher|session|cookie|database|settings/.test(target)));
  }
});

test('lost successful fallback response is reconciled without a second write or recovery move', async () => {
  const runtime = harness();
  runtime.state.onMove = () => { throw new Error('destination exists'); };
  runtime.state.onWrite = (target, code) => {
    if (target === '/cloud/Hunter.js') { runtime.files.set(target, code); throw new Error('response lost'); }
  };
  assert.equal(await runtime.context.performSelfUpdate(false, true), true);
  assert.equal(runtime.result().status, 'installed'); assert.equal(runtime.files.get('/cloud/Hunter.js'), runtime.next);
  assert.equal(runtime.moves.length, 1);
  assert.equal(runtime.writes.filter(target => target === '/cloud/Hunter.js').length, 1);
  assert.equal(runtime.alerts.length, 0); assert.equal(runtime.opened.length, 0);
});

test('move refusal never permits fallback after target, journal, backup or stage changes', async () => {
  for (const changed of ['target', 'owner', 'phase', 'newHash', 'backup', 'stage']) {
    const runtime = harness(), concurrent = artifact('5.9.0');
    runtime.state.onMove = (from, target) => {
      const [journalPath, encoded] = runtime.journal(), journal = JSON.parse(encoded);
      if (changed === 'target') runtime.files.set(target, concurrent);
      if (changed === 'owner') journal.operationId = '99999999-abcd-1234-abcd-123456789012';
      if (changed === 'phase') journal.phase = 'superseded';
      if (changed === 'newHash') journal.newHash = '0'.repeat(64);
      if (['owner', 'phase', 'newHash'].includes(changed)) runtime.files.set(journalPath, JSON.stringify(journal));
      if (changed === 'backup') runtime.files.set(journal.backupPath, 'changed backup');
      if (changed === 'stage') runtime.files.set(from, 'changed staging file');
      throw new Error('replacement refused');
    };
    assert.equal(await runtime.context.performSelfUpdate(false, true), false, changed);
    assert.equal(runtime.moves.length, 1, changed);
    assert.equal(runtime.writes.filter(target => target === '/cloud/Hunter.js').length, 0, changed);
    assert.equal(runtime.files.get('/cloud/Hunter.js'), changed === 'target' ? concurrent : runtime.previous, changed);
    if (['target', 'owner', 'phase', 'newHash'].includes(changed)) assert.equal(runtime.result().status, 'concurrent_change', changed);
    if (['phase', 'newHash'].includes(changed)) {
      const journal = JSON.parse(runtime.journal()[1]);
      assert.equal(journal[changed], changed === 'phase' ? 'superseded' : '0'.repeat(64));
    }
    assert.equal(runtime.files.get('/cloud/vouchers.json'), 'private-vouchers');
    assert.equal(runtime.files.get('/local/session.json'), 'private-session');
    assert.equal(runtime.alerts.length, 0); assert.equal(runtime.opened.length, 0);
  }
});

test('failed fallback that leaves the previous script intact is preserved without retrying', async () => {
  for (const mode of ['throws', 'no-op']) {
    const runtime = harness();
    runtime.state.onMove = () => { throw new Error('destination exists'); };
    runtime.state.onWrite = target => {
      if (target !== '/cloud/Hunter.js') return;
      if (mode === 'throws') throw new Error('write refused');
      return false;
    };
    assert.equal(await runtime.context.performSelfUpdate(false, true), false, mode);
    assert.equal(runtime.result().status, 'preserved'); runtime.unchanged();
    assert.equal(runtime.moves.length, 1);
    assert.equal(runtime.writes.filter(target => target === '/cloud/Hunter.js').length, 1);
    assert.equal(JSON.parse(runtime.journal()[1]).phase, 'rolled_back');
    assert.equal(runtime.alerts.length, 0); assert.equal(runtime.opened.length, 0);
  }
});

test('partial fallback is restored from backup, including when the restore move is refused', async () => {
  for (const refuseRestore of [false, true]) {
    const runtime = harness();
    runtime.state.onMove = from => {
      if (from.includes('.ota-') || refuseRestore) throw new Error('destination exists');
    };
    runtime.state.onWrite = (target, code) => {
      if (target === '/cloud/Hunter.js' && code === runtime.next) {
        runtime.files.set(target, 'partial fallback'); throw new Error('partial write');
      }
    };
    assert.equal(await runtime.context.performSelfUpdate(false, true), false);
    assert.equal(runtime.result().status, 'restored'); runtime.unchanged();
    assert.equal(runtime.moves.length, 2);
    assert.equal(runtime.writes.filter(target => target === '/cloud/Hunter.js').length, refuseRestore ? 2 : 1);
    assert.equal(JSON.parse(runtime.journal()[1]).phase, 'rolled_back');
    assert.equal(runtime.alerts.length, 0); assert.equal(runtime.opened.length, 0);
  }
});

test('failed install and restore fallbacks retain explicit recovery_required and the verified backup', async () => {
  const runtime = harness();
  runtime.state.onMove = () => { throw new Error('destination exists'); };
  runtime.state.onWrite = (target, code) => {
    if (target !== '/cloud/Hunter.js') return;
    if (code === runtime.next) runtime.files.set(target, 'partial fallback');
    throw new Error('write unavailable');
  };
  assert.equal(await runtime.context.performSelfUpdate(false, true), false);
  assert.equal(runtime.result().status, 'recovery_required');
  assert.equal(runtime.moves.length, 2);
  assert.equal(runtime.writes.filter(target => target === '/cloud/Hunter.js').length, 2);
  assert.equal(runtime.files.get(JSON.parse(runtime.journal()[1]).backupPath), runtime.previous);
  assert.equal(JSON.parse(runtime.journal()[1]).phase, 'replacing');
  assert.equal(runtime.files.get('/cloud/vouchers.json'), 'private-vouchers');
  assert.equal(runtime.files.get('/local/session.json'), 'private-session');
  assert.equal(runtime.alerts.length, 0); assert.equal(runtime.opened.length, 0);
});

test('readback mismatch restores invalid content but preserves unrelated valid concurrent install', async () => {
  for (const replacement of ['truncated', artifact('5.9.0')]) {
    const h = harness(); h.state.onMove = (from, to) => { if (from.includes('.ota-')) { h.files.set(to, replacement); return false; } };
    assert.equal(await h.context.performSelfUpdate(true), false);
    assert.equal(h.files.get('/cloud/Hunter.js'), replacement === 'truncated' ? h.previous : replacement);
    assert.equal(h.result().status, replacement === 'truncated' ? 'restored' : 'concurrent_change');
  }
});

test('damaged backup or failed restore remains explicit recovery_required', async () => {
  for (const mode of ['backup', 'restore']) {
    const h = harness(); h.state.onMove = (from, to) => {
      if (from.includes('.ota-')) {
        h.files.set(to, 'truncated');
        if (mode === 'backup') for (const key of h.files.keys()) if (key.includes('backup-')) h.files.set(key, 'damaged backup');
        throw new Error('partial');
      }
      if (mode === 'restore') throw new Error('restore unavailable');
    };
    h.state.onWrite = target => { if (target === '/cloud/Hunter.js') throw new Error('restore write unavailable'); };
    assert.equal(await h.context.performSelfUpdate(true), false); assert.equal(h.result().status, 'recovery_required'); assert.equal(h.opened.length, 0);
    assert.equal(h.files.get('/cloud/vouchers.json'), 'private-vouchers');
  }
});

test('journal recovery validates identity, backup and target before any restoration', async () => {
  const h = harness(); h.state.onMove = (from, to) => { h.files.set(to, 'truncated'); throw new Error('write unavailable'); };
  h.state.onWrite = target => { if (target === '/cloud/Hunter.js') throw new Error('restore write unavailable'); };
  await h.context.performSelfUpdate(true); assert.equal(h.result().status, 'recovery_required');
  h.state.onMove = null; h.state.onWrite = null; assert.equal((await h.context.recoverHunterUpdate()).status, 'restored'); h.unchanged();
  const journalPath = h.journal()[0], journal = JSON.parse(h.journal()[1]);
  journal.phase = 'replacing'; journal.backupPath = '/local/session.json'; h.files.set(journalPath, JSON.stringify(journal));
  const count = h.writes.length; assert.equal((await h.context.recoverHunterUpdate()).status, 'recovery_required'); assert.equal(h.writes.length, count); h.unchanged();
});

test('manual write fallback preserves prior installation on failed replacement', async () => {
  const h = harness({ noMove: true }); let failed = false;
  h.state.onWrite = (p, value) => { if (p === '/cloud/Hunter.js' && value === h.next && !failed) { failed = true; h.files.set(p, 'truncated'); throw new Error('write failure'); } };
  assert.equal(await h.context.performSelfUpdate(true), false); assert.equal(h.result().status, 'restored'); h.unchanged();
});

test('same-process overlapping update attempts perform one installation', async () => {
  const h = harness(); let unblock; const blocked = new Promise(resolve => { unblock = resolve; });
  h.state.onRequest = async url => { if (url.endsWith('.json')) await blocked; };
  const first = h.context.performSelfUpdate(true);
  assert.equal(await h.context.performSelfUpdate(true), false);
  unblock(); assert.equal(await first, true); assert.equal(h.moves.length, 1);
});

test('post-load discovery sends only safe metadata to existing UI and tolerates a closed view', async () => {
  const h = harness(), evaluations = [];
  const result = await h.context.discoverHunterUpdate({ evaluateJavaScript: async code => evaluations.push(code) });
  assert.equal(result.status, 'available'); assert.equal(h.calls.length, 1); assert.equal(h.writes.length, 0);
  assert.match(evaluations[0], /handleNativeUpdateDiscovery/); assert.ok(!evaluations[0].includes('artifact')); assert.ok(!evaluations[0].includes('sha256'));
  await h.context.discoverHunterUpdate({ evaluateJavaScript: async () => { throw new Error('view dismissed'); } }); h.unchanged();
});

test('a competing journal owner is not overwritten by rollback', async () => {
  const h = harness(), concurrent = artifact('5.9.0');
  h.state.onMove = (from, to) => {
    if (!from.includes('.ota-')) return;
    h.files.set(to, concurrent);
    const [journalPath, encoded] = h.journal(), journal = JSON.parse(encoded);
    journal.operationId = '99999999-abcd-1234-abcd-123456789012';
    h.files.set(journalPath, JSON.stringify(journal));
    throw new Error('competing updater');
  };
  assert.equal(await h.context.performSelfUpdate(true), false);
  assert.equal(h.result().status, 'concurrent_change'); assert.equal(h.files.get('/cloud/Hunter.js'), concurrent);
  assert.equal(h.moves.length, 1); assert.equal(h.opened.length, 0);
});

test('journal recovery recognizes already-installed bytes without reinstalling or changing data', async () => {
  const h = harness(); assert.equal(await h.context.performSelfUpdate(true), true);
  const [journalPath, encoded] = h.journal(), journal = JSON.parse(encoded);
  journal.phase = 'replacing'; h.files.set(journalPath, JSON.stringify(journal));
  const moves = h.moves.length;
  assert.equal((await h.context.recoverHunterUpdate()).status, 'installed');
  assert.equal(h.moves.length, moves); assert.equal(h.files.get('/cloud/Hunter.js'), h.next);
  assert.equal(JSON.parse(h.journal()[1]).phase, 'installed');
});

test('matching manifest hash cannot authorize syntactically invalid middle code', async () => {
  const h = harness();
  h.state.code = h.next.replace('await main();', 'const broken = ;\nawait main();');
  h.state.manifest = publication(h.state.code);
  assert.equal(h.context.hunterUpdateVersion(h.state.code), null);
  assert.equal(await h.context.performSelfUpdate(true), false);
  assert.equal(h.result().status, 'invalid_artifact'); assert.equal(h.writes.length, 0); h.unchanged();
});

test('syntax validation compiles but never executes any candidate top-level code', () => {
  const h = harness();
  h.context.probeExecuted = false;
  const code = h.next.replace('await main();', 'probeExecuted = true;\nthrow new Error("must not execute");\nawait main();');
  assert.equal(h.context.hunterUpdateVersion(code), '5.8.0');
  assert.equal(h.context.probeExecuted, false);
});

test('replacement corruption retaining envelope markers restores the verified backup', async () => {
  const h = harness();
  const corrupted = h.next.replace('await main();', 'const broken = ;\nawait main();');
  h.state.onMove = (from, to) => {
    if (from.includes('.ota-')) { h.files.set(to, corrupted); return false; }
  };
  assert.equal(await h.context.performSelfUpdate(true), false);
  assert.equal(h.result().status, 'restored'); h.unchanged();
  assert.equal(JSON.parse(h.journal()[1]).phase, 'rolled_back'); assert.equal(h.opened.length, 0);
});

test('documented limitation: final-check-to-move race is not a cross-process compare-and-swap', async () => {
  const h = harness(), concurrent = artifact('5.9.0'); let raced = false;
  h.state.onMove = (from, to) => {
    if (!from.includes('.ota-')) return;
    // Simulate another process after the updater's final read, immediately
    // before the FileManager implementation performs unconditional replacement.
    h.files.set(to, concurrent); raced = true;
  };
  assert.equal(await h.context.performSelfUpdate(true), true);
  assert.equal(raced, true);
  // The API exposes no conditional move. This test records that limitation;
  // it must not be described as locking or proof of cross-process atomicity.
  assert.equal(h.files.get('/cloud/Hunter.js'), h.next);
});

test('discovery reuses dormant banner when no UI callback exists and leaves other states untouched', async () => {
  const h = harness(), nodes = { otaUpdateBanner: { style: { display: 'none' } }, otaVersionText: { textContent: 'Original label' } };
  const dom = vm.createContext({ document: { getElementById: id => nodes[id] } });
  const view = { evaluateJavaScript: async code => vm.runInContext(code, dom) };
  await h.context.discoverHunterUpdate(view);
  assert.equal(nodes.otaUpdateBanner.style.display, 'flex');
  assert.equal(nodes.otaVersionText.textContent, 'Versão 5.8.0 disponível');
  nodes.otaUpdateBanner.style.display = 'none'; nodes.otaVersionText.textContent = 'Original label';
  h.state.manifest.version = '5.7.9'; await h.context.discoverHunterUpdate(view);
  assert.equal(nodes.otaUpdateBanner.style.display, 'none'); assert.equal(nodes.otaVersionText.textContent, 'Original label');
  h.state.networkFailure = true; await h.context.discoverHunterUpdate(view);
  assert.equal(nodes.otaUpdateBanner.style.display, 'none'); assert.equal(h.writes.length, 0); h.unchanged();
});

test('explicit unattended update verifies identical gates without alerts, relaunch or private writes',async()=>{
  const h=harness();assert.equal(await h.context.performSelfUpdate(false,true),true);
  assert.equal(h.result().status,'installed');assert.equal(h.alerts.length,0);assert.equal(h.opened.length,0);
  assert.equal(h.files.get('/cloud/Hunter.js'),h.next);assert.equal(h.files.get('/cloud/vouchers.json'),'private-vouchers');
  const bad=harness();bad.state.manifest.sha256='0'.repeat(64);assert.equal(await bad.context.performSelfUpdate(false,true),false);bad.unchanged();
});

test('channel opt-in validates enum and writes only the updater preference',()=>{
  const h=harness();h.context.configureHunterUpdateChannel(undefined);assert.equal(h.writes.length,0);
  assert.throws(()=>h.context.configureHunterUpdateChannel('other'));assert.equal(h.writes.length,0);
  h.context.configureHunterUpdateChannel('beta');assert.equal(h.context.hunterUpdateChannel(),'beta');
  assert.deepEqual(h.writes,['/local/uol_hunter_update_channel.json']);
});
