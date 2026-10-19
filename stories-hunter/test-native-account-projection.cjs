const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');

const host = fs.readFileSync(path.join(__dirname, 'native/host.js'), 'utf8');
const projection = host.slice(host.indexOf('function publicHunterAccountResult('), host.indexOf('\n__BUILD_UPDATER__'));
const publicRows = host.slice(host.indexOf('function publicHunterRedemptions('), host.indexOf('\nfunction buildDashboardHTML('));
const identity = host.slice(host.indexOf('function nativeRedemptionKey('), host.indexOf('\nfunction loadRedemptionsDatabase('));
const voucher = host.slice(host.indexOf('async function nativeVoucherDataUrl('), host.indexOf('\nfunction readHunterLinkCheckCache('));
const credentials = host.slice(host.indexOf('function publicHunterCredentials('), host.indexOf('\nfunction publicHunterRedemptions('));
const secretAction = host.slice(host.indexOf('async function performNativeSecretAction('), host.indexOf('\nasync function nativeVoucherDataUrl('));

test('public account metadata masks login and native actions return no secret value', async () => {
  const copied = [], callbacks = [];
  const privateCredentials = { source: 'local', accounts: {
    leosaquetto: { login: 'synthetic@example.test', senha: 'synthetic-password' }
  } };
  const context = vm.createContext({
    loadCredentials: () => privateCredentials,
    Alert: class { addAction() {} addCancelAction() {} async presentAlert() { return 0; } },
    Pasteboard: { copyString: value => copied.push(value) }
  });
  vm.runInContext(credentials + '\n' + secretAction, context);
  const display = context.publicHunterCredentials(privateCredentials);
  assert.equal(display.accounts.leosaquetto.hasLogin, true);
  assert.equal(display.accounts.leosaquetto.hasPassword, true);
  assert.doesNotMatch(JSON.stringify(display), /synthetic@example|synthetic-password/);
  const view = { async evaluateJavaScript(code) { callbacks.push(code); } };
  await context.performNativeSecretAction(view, 'copy-login', 'leosaquetto');
  assert.deepEqual(copied, ['synthetic@example.test']);
  assert.match(callbacks[0], /"copy-login","leosaquetto",\{"status":"copied"\}/);
  assert.doesNotMatch(callbacks[0], /synthetic@example|synthetic-password/);
});

test('verified native receipt replaces stale metadata and persists without exposing voucher bytes', () => {
  const old = { user: 'gaoriel', id: 'receipt-1', url: 'https://clube.uol.com.br/perfil/beneficios/receipt-1',
    title: 'Título antigo', imageUrl: 'https://old.test/image', voucherImageDataUrl: 'data:image/png;base64,iVBORw0KGgo=',
    redeemedAt: '2026-10-01T10:00:00.000Z' };
  let saved;
  const context = vm.createContext({
    loadRedemptionsDatabase: () => [old],
    saveRedemptionsDatabase: rows => { saved = rows; }
  });
  vm.runInContext(identity + '\n' + publicRows + '\n' + projection, context);
  const result = context.publicHunterAccountResult({}, 'gaoriel', { status: 'authenticated', account: 'gaoriel',
    records: [{ id: 'receipt-1', url: old.url, title: 'Título confirmado', imageUrl: 'https://clube.uol.com.br/image',
      category: 'campanhasdeingresso' }] });
  assert.equal(saved[0].title, 'Título confirmado');
  assert.equal(saved[0].redeemedAt, old.redeemedAt);
  assert.equal(saved[0].voucherImageDataUrl, old.voucherImageDataUrl);
  assert.ok(Number.isFinite(Date.parse(saved[0].receiptVerifiedAt)));
  assert.equal(result.records[0].title, 'Título confirmado');
  assert.equal(result.records[0].nativeVoucherIndex, 0);
  assert.equal(result.records[0].hasNativeVoucher, true);
  assert.equal(result.records[0].receiptVerified, true);
  assert.equal(result.records[0].voucherImageDataUrl, undefined);
  assert.doesNotMatch(JSON.stringify(result), /iVBORw0KGgo/);
});

test('unverified receipt cannot enter the private database', () => {
  let writes = 0;
  const context = vm.createContext({
    loadRedemptionsDatabase: () => [], saveRedemptionsDatabase: () => { writes++; }
  });
  vm.runInContext(identity + '\n' + publicRows + '\n' + projection, context);
  const result = context.publicHunterAccountResult({}, 'leosaquetto', { status: 'authenticated', account: 'leosaquetto',
    records: [{ id: 'bad', url: 'https://evil.test/perfil/beneficios/bad', title: 'Forged' }] });
  assert.equal(writes, 0);
  assert.equal(result.records.length, 0);
});

test('native voucher bridge checks actual PDF, PNG and JPEG signatures', async () => {
  const context = vm.createContext({ FileManager: { local: () => ({ documentsDirectory: () => '/private',
    fileExists: () => false }) }, getFileManager: () => ({ documentsDirectory: () => '/private', fileExists: () => false }) });
  vm.runInContext(voucher, context);
  for (const [mime, signature] of [['application/pdf', 'JVBERi0'], ['image/png', 'iVBORw0KGgo'],
    ['image/jpeg', '/9j/']]) {
    const result = await context.nativeVoucherDataUrl({ voucherImageDataUrl: 'data:' + mime + ';base64,' + signature + 'AA==' });
    assert.equal(result.status, 'available');
    assert.equal(result.mime, mime);
  }
  assert.equal((await context.nativeVoucherDataUrl({ voucherImageDataUrl: 'data:image/png;base64,JVBERi0AA==' })).status, 'missing');
  assert.equal((await context.nativeVoucherDataUrl({ voucherImageDataUrl: 'data:application/pdf;base64,AAAA' })).status, 'missing');
});

test('native voucher lookup binds the account, index and receipt identity', async () => {
  const evaluations = [];
  const context = vm.createContext({ FileManager: { local: () => ({ documentsDirectory: () => '/private', fileExists: () => false }) },
    getFileManager: () => ({ documentsDirectory: () => '/private', fileExists: () => false }) });
  vm.runInContext(voucher, context);
  const view = { hunterPrivateRedemptions: [{ id: 'receipt-1', user: 'gaoriel',
    voucherImageDataUrl: 'data:application/pdf;base64,JVBERi0AA==' }],
  async evaluateJavaScript(code) { evaluations.push(code); } };
  await context.deliverNativeVoucher(view, 'gaoriel', 0, 'receipt-2', 8);
  assert.match(evaluations.pop(), /"status":"missing","id":"receipt-2","request":8/);
  await context.deliverNativeVoucher(view, 'leosaquetto', 0, 'receipt-1', 8);
  assert.match(evaluations.pop(), /"status":"missing","id":"receipt-1","request":8/);
  await context.deliverNativeVoucher(view, 'gaoriel', 0, 'receipt-1', 9);
  assert.match(evaluations.pop(), /"status":"available".*"id":"receipt-1","request":9/);
});

test('saved iCloud PDF downloads before reading and traversal paths are refused', async () => {
  let downloaded = false, downloads = 0, reads = 0;
  const cloud = { documentsDirectory: () => '/cloud', fileExists: () => true,
    isFileDownloaded: () => downloaded, async downloadFileFromiCloud() { downloaded = true; downloads++; },
    read() { reads++; return { toBase64String: () => 'JVBERi0AA==' }; } };
  const context = vm.createContext({ FileManager: { local: () => ({ documentsDirectory: () => '/local', fileExists: () => false }) },
    getFileManager: () => cloud });
  vm.runInContext(voucher, context);
  const result = await context.nativeVoucherDataUrl({ localVoucherFile: '/cloud/voucher.pdf' });
  assert.equal(result.status, 'available');
  assert.equal(result.mime, 'application/pdf');
  assert.equal(downloads, 1); assert.equal(reads, 1);
  assert.equal((await context.nativeVoucherDataUrl({ localVoucherFile: '/cloud/../secret.pdf' })).status, 'missing');
  assert.equal(reads, 1);
});
