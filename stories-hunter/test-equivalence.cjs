const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const {baseline, literal, render} = require('./test-support/artifact.cjs');
const root = path.join(__dirname, '..');
const manifest = require('./baseline/manifest.json');
const reference = baseline();
const current = fs.readFileSync(path.join(__dirname, 'dist/Hunter.js'), 'utf8');
const sha = data => crypto.createHash('sha256').update(data).digest('hex');

test('frozen public release has exact original size and hash', () => {
  const expected = manifest.files['scriptable_uol_stories_hunter.js'];
  assert.equal(Buffer.byteLength(reference), expected.bytes);
  assert.equal(sha(reference), expected.sha256);
  for (const name of ['BUNDLED_COOKIES_LEO', 'BUNDLED_COOKIES_GAORIEL', 'BUNDLED_REDEMPTIONS'])
    assert.ok(reference.includes('const ' + name + ' = [];'));
});

test('unchanged redemption module and receipt parser remain byte-identical', () => {
  // Collector/access now include explicit recovery, covered by their behavioral
  // suites. Redemption and receipt parsing still have the frozen contract.
  for (const name of ['redemption']) {
    const file = 'stories-hunter/' + name + '.js';
    assert.equal(sha(fs.readFileSync(path.join(root, file))), manifest.files[file].sha256);
  }
  assert.equal(literal(current, 'const UOL_RECEIPT_PARSER = '), literal(reference, 'const UOL_RECEIPT_PARSER = '));
});

test('current startup and failure pages preserve distinct status and resolved assets', () => {
  const {startup, failure} = render(current);
  for (const page of [startup, failure]) {
    assert.match(page, /<title>Clube UOL Premium<\/title>/);
    assert.match(page, /<main role="status" aria-live="polite">/);
    assert.match(page, /data:image\/png;base64,/);
    assert.doesNotMatch(page, /__(?:LOGO_BASE64|STARTUP_TITLE|STARTUP_DESCRIPTION)__/);
  }
  assert.match(startup, /Preparando suas ofertas/);
  assert.match(startup, /<progress aria-label="Carregando o Clube UOL Premium">/);
  assert.match(failure, /Não foi possível abrir/);
  assert.match(failure, /Seus dados foram preservados/);
  assert.doesNotMatch(failure, /<progress/);
});

test('dashboard preserves five-area shell while projecting native state without private values', () => {
  const secret = 'SYNTHETIC_PRIVATE_SENTINEL';
  const fixture = {
    user: 'gaoriel', theme: 'purple',
    history: { active: [{id: 'pSynthetic', title: 'A < B & "C"', image: 'https://example.invalid/poster.png'}], history: [] },
    settings: { userSessions: { gaoriel: {cookie: secret} }, myRedemptions: [{id: 'personal', user: 'gaoriel', localVoucherFile: secret}] },
    credentials: { source: 'native', accounts: { gaoriel: {login: 'gabriel@example.test', senha: secret} } },
    cookies: [{name: 'session', value: secret}],
    receipts: [{id: 'receipt', title: 'Recibo', user: 'gaoriel', voucherImageDataUrl: 'data:image/png;base64,' + secret,
      localVoucherFile: secret, receiptVerifiedAt: '2026-10-19T10:00:00.000Z'}],
    collection: {status: 'offline', pending: 1}
  };
  const {dashboard} = render(current, fixture);
  assert.doesNotMatch(dashboard, new RegExp(secret));
  assert.doesNotMatch(dashboard, /__(?:INITIAL_DATA_JSON|LOGO_BASE64|ICON_[A-Za-z0-9_]+)__/);
  for (const id of ['viewMain', 'viewHistory', 'viewRedemptions', 'settingsModal', 'bottomNav'])
    assert.match(dashboard, new RegExp('id="' + id + '"'));
  const initial = literal(dashboard, '    const INITIAL = ');
  assert.equal(initial.settings.currentUser, 'gaoriel');
  assert.equal(initial.settings.userSessions, undefined);
  assert.equal(initial.credentials.accounts.gaoriel.hasPassword, true);
  assert.equal(initial.credentials.accounts.gaoriel.senha, undefined);
  assert.equal(initial.realRedemptions[0].receiptVerified, true);
  assert.equal(initial.realRedemptions[0].hasNativeVoucher, true);
  assert.equal(initial.realRedemptions[0].voucherImageDataUrl, undefined);
  assert.equal(initial.realRedemptions[0].localVoucherFile, undefined);
  assert.equal(initial.historyData.active[0].title, 'A < B & "C"');
});
