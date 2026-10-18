const { test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, 'native/updater.js'), 'utf8');
const hash = code => crypto.createHash('sha256').update(code).digest('hex');
const artifact = version => '// Variables used by Scriptable.\n// These must be at the very top of the file.\n// Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter\nconst SCRIPT_VERSION = "' + version + '";\nconst HunterCollector = (() => { return {}; })();\nfunction hunterCollectionPausedForUpdate(action) {}\nasync function main() {}\nfunction buildDashboardHTML() {}\nawait main();\nScript.complete();\n';

function harness(version = '5.8.3') {
  const previous = artifact('5.8.3'), next = artifact('5.8.4');
  const files = new Map([['/cloud/Clube UOL Premium.js', artifact(version)], ['/cloud/vouchers.json', 'private-vouchers'], ['/local/session.json', 'private-session']]);
  const writes = [], updates = [], opened = [], toasts = [], requests = [];
  const state = { install: false, result: { status: 'up_to_date' }, updateFailure: false, openFailure: false };
  const manager = root => ({
    documentsDirectory: () => root, joinPath: path.posix.join, fileExists: target => files.has(target),
    readString: target => files.get(target),
    writeString(target, code) { writes.push(target); files.set(target, code); }
  });
  const cloud = manager('/cloud'), local = manager('/local');
  const context = vm.createContext({
    SCRIPT_VERSION: version, getFileManager: () => cloud, FileManager: { local: () => local },
    Script: { name: () => 'Clube UOL Premium' },
    Safari: { open(url) { opened.push(url); if (state.openFailure) throw new Error('open unavailable'); } },
    Request: class { constructor(url) { this.url = url; requests.push(this); } async load() { throw new Error('offline'); } }
  });
  vm.runInContext(source, context);
  const paths = context.hunterUpdatePaths();
  const journal = {
    schema: 1, operationId: '12345678-abcd-1234-abcd-123456789012', phase: 'installed', scriptPath: paths.scriptPath,
    backupPath: '/local/uol_hunter_ota_backup-' + paths.key + '-12345678-abcd-1234-abcd-123456789012.js',
    oldVersion: '5.8.3', oldHash: hash(previous), newVersion: '5.8.4', newHash: hash(next)
  };
  const setJournal = patch => files.set(paths.journalPath, JSON.stringify({ ...journal, ...patch }));
  const readJournal = () => JSON.parse(files.get(paths.journalPath));
  context.performSelfUpdate = async (...args) => {
    updates.push(args);
    if (state.updateFailure) throw new Error('update unavailable');
    context.testUpdateResult = state.result;
    vm.runInContext('hunterLastUpdateResult = testUpdateResult', context);
    if (state.install) { files.set(paths.scriptPath, next); setJournal(); }
    return state.install;
  };
  const bannerText = { textContent: 'Versão disponível' }, bannerButton = { style: { display: '' } };
  const banner = { style: { display: 'none' }, querySelector: selector => selector === '.ota-banner-text' ? bannerText : selector === 'button' ? bannerButton : null };
  const ui = vm.createContext({ showToast: (...args) => toasts.push(args), document: { getElementById: id => id === 'otaUpdateBanner' ? banner : null } });
  const view = { evaluateJavaScript: async script => vm.runInContext(script, ui) };
  return { context, files, writes, updates, opened, toasts, requests, state, paths, previous, next, setJournal, readJournal, view, banner, bannerText, bannerButton,
    privateUntouched() {
      assert.equal(files.get('/cloud/vouchers.json'), 'private-vouchers');
      assert.equal(files.get('/local/session.json'), 'private-session');
      assert.ok(!writes.some(target => /voucher|session|cookie|database|settings/.test(target)));
    }
  };
}

test('opening checks updates unattended with a three-second metadata timeout and no success message when unchanged', async () => {
  const runtime = harness();
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false);
  assert.deepEqual(runtime.updates, [[false, true, 3]]);
  await runtime.context.showHunterStartupUpdateNotice(runtime.view);
  assert.equal(runtime.toasts.length, 0); assert.equal(runtime.opened.length, 0); assert.equal(runtime.writes.length, 0);
  runtime.privateUntouched();
});

test('startup timeout configures the native request instead of racing an uncancelled installation', async () => {
  const runtime = harness();
  assert.equal((await runtime.context.checkHunterUpdate(3)).status, 'network_error');
  assert.equal(runtime.requests.length, 1); assert.equal(runtime.requests[0].timeoutInterval, 3);
  assert.doesNotMatch(source, /Promise\.race\s*\(/);
});

test('failed automatic checks preserve installed code and permit the cached dashboard without relaunching', async () => {
  for (const status of ['network_error', 'invalid_artifact', 'preserved', 'recovery_required']) {
    const runtime = harness(); runtime.state.result = { status };
    assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false, status);
    assert.equal(runtime.files.get(runtime.paths.scriptPath), runtime.previous);
    assert.equal(runtime.opened.length, 0);
    await runtime.context.showHunterStartupUpdateNotice(runtime.view);
    assert.equal(runtime.toasts.length, 0);
    if (status === 'network_error') assert.equal(runtime.banner.style.display, 'none');
    else {
      assert.equal(runtime.banner.style.display, 'flex');
      assert.equal(runtime.bannerButton.style.display, 'none');
      assert.match(runtime.bannerText.textContent, /atualização/i);
      assert.doesNotMatch(runtime.bannerText.textContent, /Versão disponível|Atualizado para/);
    }
    runtime.privateUntouched();
  }
});

test('unexpected startup update failures cannot prevent the existing application from opening', async () => {
  const runtime = harness(); runtime.state.updateFailure = true;
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false);
  assert.equal(runtime.files.get(runtime.paths.scriptPath), runtime.previous); assert.equal(runtime.opened.length, 0);
  runtime.privateUntouched();
});

test('verified automatic install relaunches only once and the old runtime never announces success', async () => {
  const runtime = harness(); runtime.state.install = true; runtime.state.result = { status: 'installed', version: '5.8.4' };
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), true);
  assert.equal(runtime.files.get(runtime.paths.scriptPath), runtime.next);
  assert.equal(runtime.readJournal().relaunchAttempted, true);
  assert.deepEqual(runtime.opened, ['scriptable:///run?scriptName=Clube%20UOL%20Premium']);
  await runtime.context.showHunterStartupUpdateNotice(runtime.view);
  assert.equal(runtime.toasts.length, 0);
  runtime.state.install = false; runtime.state.result = { status: 'concurrent_change' };
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false);
  assert.equal(runtime.opened.length, 1);
  await runtime.context.showHunterStartupUpdateNotice(runtime.view);
  assert.equal(runtime.toasts.length, 0);
  runtime.privateUntouched();
});

test('failed relaunch is not retried indefinitely and does not claim the new runtime is active', async () => {
  const runtime = harness(); runtime.state.install = true; runtime.state.openFailure = true;
  runtime.state.result = { status: 'installed', version: '5.8.4' };
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false);
  assert.equal(runtime.readJournal().relaunchAttempted, true);
  runtime.state.install = false; runtime.state.result = { status: 'concurrent_change' };
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false);
  assert.equal(runtime.opened.length, 1);
  await runtime.context.showHunterStartupUpdateNotice(runtime.view);
  assert.equal(runtime.toasts.length, 0);
});

test('only the exact installed version running from its matching journal announces success once', async () => {
  const runtime = harness('5.8.4'); runtime.setJournal({ relaunchAttempted: true });
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false);
  await runtime.context.showHunterStartupUpdateNotice(runtime.view);
  assert.equal(runtime.toasts.length, 1); assert.match(JSON.stringify(runtime.toasts), /5\.8\.4/);
  assert.match(JSON.stringify(runtime.toasts), /atualizad/i);
  assert.equal(runtime.banner.style.display, 'none');
  await runtime.context.showHunterStartupUpdateNotice(runtime.view);
  assert.equal(runtime.toasts.length, 1);
  assert.equal(runtime.readJournal().announced, true);
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false);
  await runtime.context.showHunterStartupUpdateNotice(runtime.view);
  assert.equal(runtime.toasts.length, 1); assert.equal(runtime.opened.length, 0);
  runtime.privateUntouched();
});

test('journal path, phase, hash or version mismatches never create a successful startup notice', async () => {
  for (const mismatch of ['scriptPath', 'phase', 'newHash', 'newVersion', 'target', 'runtime']) {
    const runtime = harness(mismatch === 'runtime' ? '5.8.3' : '5.8.4');
    const patch = { relaunchAttempted: true };
    if (mismatch === 'scriptPath') patch.scriptPath = '/cloud/Another Script.js';
    if (mismatch === 'phase') patch.phase = 'rolled_back';
    if (mismatch === 'newHash') patch.newHash = '0'.repeat(64);
    if (mismatch === 'newVersion') patch.newVersion = '5.9.0';
    if (mismatch === 'target') runtime.files.set(runtime.paths.scriptPath, artifact('5.9.0'));
    if (mismatch === 'runtime') runtime.files.set(runtime.paths.scriptPath, runtime.next);
    runtime.setJournal(patch);
    assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false, mismatch);
    await runtime.context.showHunterStartupUpdateNotice(runtime.view);
    assert.equal(runtime.toasts.length, 0, mismatch);
    assert.notEqual(runtime.readJournal().announced, true, mismatch);
    runtime.privateUntouched();
  }
});

test('an already-announced installation stays quiet and a closed WebView cannot reject startup', async () => {
  const runtime = harness('5.8.4'); runtime.setJournal({ announced: true, relaunchAttempted: true });
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false);
  await runtime.context.showHunterStartupUpdateNotice(runtime.view);
  assert.equal(runtime.toasts.length, 0);
  runtime.setJournal({ relaunchAttempted: true });
  assert.equal(await runtime.context.autoUpdateHunterOnOpen(), false);
  await assert.doesNotReject(runtime.context.showHunterStartupUpdateNotice({ evaluateJavaScript: async () => { throw new Error('view closed'); } }));
  runtime.privateUntouched();
});
