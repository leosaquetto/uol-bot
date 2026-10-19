const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');

const host = fs.readFileSync(path.join(__dirname, 'native/host.js'), 'utf8');
const migration = host.slice(host.indexOf('const HUNTER_LEGACY_MAX_KEYS ='),
  host.indexOf('\nfunction readHunterLinkCheckCache('));
const voucher = host.slice(host.indexOf('async function nativeVoucherDataUrl('),
  host.indexOf('\nconst HUNTER_LEGACY_MAX_KEYS ='));
const browser = host.slice(host.indexOf('function installHunterBrowser('),
  host.indexOf('\nfunction nativeRedemptionKey('));

function makeHarness(initial, options = {}) {
  const storage = new Map(Object.entries(initial));
  const files = new Map();
  const acknowledged = [];
  const statuses = [];
  const evaluations = [];
  const fm = {
    documentsDirectory: () => '/private',
    joinPath: (root, name) => root + '/' + name,
    fileExists: name => files.has(name),
    readString(name) {
      const value = files.get(name);
      if (name.endsWith('uol_webview_private_legacy_v1.json') && options.corruptReadback && value) return value.slice(0, -1);
      return value;
    },
    writeString(name, value) {
      if (options.failWrite && name.endsWith('uol_webview_private_legacy_v1.json')) throw Error('synthetic write failure');
      files.set(name, value);
    }
  };
  const window = {
    hunterLegacyPrivateCacheManifest: () => Array.from(storage, ([key, value]) => ({ key, length: value.length })),
    hunterLegacyPrivateCacheChunk: (key, offset, limit) => {
      const value = storage.get(key);
      return value === undefined ? null : { key, offset, length: value.length, data: value.slice(offset, offset + limit) };
    },
    hunterLegacyPrivateCacheCommitted: keys => {
      acknowledged.push(keys);
      for (const key of keys) storage.delete(key);
    },
    hunterLegacyPrivateCacheMigrationResult: result => statuses.push(result)
  };
  const view = {
    async evaluateJavaScript(script, completion) {
      evaluations.push({ script, completion });
      return vm.runInNewContext(script, { window, completion: result => result });
    }
  };
  const context = vm.createContext({
    LEGACY_PRIVATE_ARCHIVE_FILE_NAME: 'uol_webview_private_legacy_v1.json',
    FileManager: { local: () => fm },
    Promise, Date, JSON
  });
  vm.runInContext(migration + '\n' + browser, context);
  return { context, view, storage, files, acknowledged, statuses, evaluations };
}

test('legacy migration archives exact private values in chunks and acknowledges only after readback', async () => {
  const secret = 'synthetic-password-and-cookie';
  const voucher = 'data:application/pdf;base64,' + 'JVBERi0'.repeat(7000);
  const key = 'uol_vc_https://clube.uol.com.br/perfil/beneficios/pR1';
  const harness = makeHarness({ uol_hub_settings: JSON.stringify({ password: secret }), [key]: voucher });
  harness.context.installHunterBrowser(harness.view);
  assert.equal(harness.view.shouldAllowRequest({ url: 'hunter-legacy://migrate' }), false);
  await harness.view.hunterLegacyMigrationTask;
  const archive = JSON.parse(harness.files.get('/private/uol_webview_private_legacy_v1.json'));
  assert.equal(archive.version, 1);
  assert.equal(archive.entries.find(entry => entry.key === 'uol_hub_settings').value, JSON.stringify({ password: secret }));
  assert.equal(archive.entries.find(entry => entry.key === key).value, voucher);
  assert.equal(harness.acknowledged.length, 1);
  assert.equal(harness.acknowledged[0].length, 2);
  assert.equal(harness.storage.size, 0);
  assert.equal(harness.statuses[0].status, 'archived');
  assert.equal(harness.statuses[0].acknowledged, true);
  assert.ok(harness.evaluations.filter(entry => entry.script.includes('hunterLegacyPrivateCacheChunk')).length > 1);
  assert.ok(harness.evaluations.every(entry => !entry.script.includes(secret) && !entry.script.includes(voucher)));
  assert.doesNotMatch(JSON.stringify(harness.statuses), /synthetic-password|JVBERi0/);
});

test('a long signed voucher key stays private and can be archived', async () => {
  const key = 'uol_vc_https://clube.uol.com.br/download?token=' + 'x'.repeat(700);
  const harness = makeHarness({ [key]: 'synthetic-private-data' });
  const result = await harness.context.runNativeLegacyPrivateMigration(harness.view);
  assert.equal(result.status, 'archived');
  assert.equal(harness.storage.size, 0);
  assert.equal(JSON.parse(harness.files.get('/private/uol_webview_private_legacy_v1.json')).entries[0].key, key);
});

for (const failure of ['failWrite', 'corruptReadback']) {
  test('legacy migration retains WebView values if archive ' + failure, async () => {
    const key = 'uol_vc_https://clube.uol.com.br/cupom';
    const value = 'synthetic-private-voucher';
    const harness = makeHarness({ [key]: value }, { [failure]: true });
    harness.context.installHunterBrowser(harness.view);
    assert.equal(harness.view.shouldAllowRequest({ url: 'hunter-legacy://migrate' }), false);
    await harness.view.hunterLegacyMigrationTask;
    assert.equal(harness.storage.get(key), value);
    assert.equal(harness.acknowledged.length, 0);
    assert.equal(harness.statuses[0].status, 'retry');
    assert.equal(harness.statuses[0].reason, 'archive_write_failed');
  });
}

test('legacy migration rejects a mismatched chunk without writing or scrubbing', async () => {
  const harness = makeHarness({ uol_real_redemptions: 'private-cache' });
  const original = harness.view.evaluateJavaScript;
  harness.view.evaluateJavaScript = function(script, completion) {
    if (script.includes('hunterLegacyPrivateCacheChunk')) return JSON.stringify({
      key: 'uol_real_redemptions', offset: 1, length: 13, data: 'private-cache'
    });
    return original.call(this, script, completion);
  };
  const result = await harness.context.runNativeLegacyPrivateMigration(harness.view);
  assert.equal(result.reason, 'chunk_mismatch');
  assert.equal(harness.storage.get('uol_real_redemptions'), 'private-cache');
  assert.equal(harness.files.size, 0);
  assert.equal(harness.acknowledged.length, 0);
});

function archivedVoucherHarness(entries) {
  const archivePath = '/private/uol_webview_private_legacy_v1.json';
  const archive = JSON.stringify({ version: 1, entries });
  const fm = {
    documentsDirectory: () => '/private', joinPath: (root, name) => root + '/' + name,
    fileExists: file => file === archivePath, readString: () => archive
  };
  const context = vm.createContext({
    LEGACY_PRIVATE_ARCHIVE_FILE_NAME: 'uol_webview_private_legacy_v1.json',
    FileManager: { local: () => fm }, getFileManager: () => fm,
    Date, JSON, Promise
  });
  vm.runInContext(voucher, context);
  return context;
}

const verifiedReceipt = {
  user: 'gaoriel', id: 'pR1', url: 'https://clube.uol.com.br/perfil/beneficios/pR1',
  receiptVerifiedAt: '2026-10-19T10:00:00.000Z'
};
const pdfUrl = 'data:application/pdf;base64,JVBERi0AA==';
const exactVoucherKey = 'uol_vc_v2_gaoriel_pR1';

test('exact v2 archive voucher is reusable only for the verified account and receipt', async () => {
  const context = archivedVoucherHarness([{ key: exactVoucherKey, value: JSON.stringify({
    dataUrl: pdfUrl, mimeType: 'application/pdf', title: 'untrusted cache title'
  }) }]);
  const result = await context.nativeArchivedVoucherDataUrl(verifiedReceipt);
  assert.equal(result.status, 'available');
  assert.equal(result.dataUrl, pdfUrl);
  assert.equal(result.mime, 'application/pdf');
  assert.equal(result.title, undefined);
  for (const mismatch of [
    { user: 'leosaquetto' },
    { id: 'pR2', url: 'https://clube.uol.com.br/perfil/beneficios/pR2' },
    { receiptVerifiedAt: '' },
    { url: 'https://clube.uol.com.br/perfil/beneficios/pR2' }
  ]) {
    assert.equal((await context.nativeArchivedVoucherDataUrl({ ...verifiedReceipt, ...mismatch })).status, 'missing');
  }
});

test('ambiguous v1 key, unverified receipt and invalid signature never supply a voucher', async () => {
  const v1 = archivedVoucherHarness([{ key: 'uol_vc_https://clube.uol.com.br/perfil/beneficios/pR1',
    value: JSON.stringify({ dataUrl: pdfUrl, mimeType: 'application/pdf' }) }]);
  assert.equal((await v1.nativeArchivedVoucherDataUrl(verifiedReceipt)).status, 'missing');
  const invalid = archivedVoucherHarness([{ key: exactVoucherKey,
    value: JSON.stringify({ dataUrl: 'data:application/pdf;base64,iVBORw0KGgo=', mimeType: 'application/pdf' }) }]);
  assert.equal((await invalid.nativeArchivedVoucherDataUrl(verifiedReceipt)).status, 'missing');
  assert.equal((await invalid.nativeArchivedVoucherDataUrl({ ...verifiedReceipt, receiptVerifiedAt: undefined })).status, 'missing');
});

test('voucher route returns archived bytes for exact verified row without mutating its metadata', async () => {
  const context = archivedVoucherHarness([{ key: exactVoucherKey,
    value: JSON.stringify({ dataUrl: pdfUrl, mimeType: 'application/pdf', title: 'cache title' }) }]);
  const calls = [];
  const row = { ...verifiedReceipt, title: 'verified title' };
  const view = { hunterPrivateRedemptions: [row], async evaluateJavaScript(code) { calls.push(code); } };
  await context.deliverNativeVoucher(view, 'gaoriel', 0, 'pR1', 7);
  assert.match(calls[0], /"status":"available"/);
  assert.match(calls[0], /"request":7/);
  assert.equal(row.title, 'verified title');
  assert.equal(row.voucherImageDataUrl, undefined);
  calls.length = 0;
  await context.deliverNativeVoucher(view, 'leosaquetto', 0, 'pR1', 8);
  assert.match(calls[0], /"status":"missing"/);
});
