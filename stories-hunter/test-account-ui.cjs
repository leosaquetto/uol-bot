const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = require('./assemble-ui.cjs').assembleUI();
const functions = source.slice(source.indexOf('    let nativeRedemptionPending'), source.indexOf('    function updateSessionStatusUI()'));
function harness() {
  const nodes = new Map(), writes = [];
  const context = vm.createContext({
    URL, INITIAL: { nativeAccounts: true }, settings: { currentUser: 'leosaquetto', realRedemptions: [] },
    activeOffer: { link: 'https://clube.uol.com.br/campanhasdeingresso/test' }, currentTab: 'redemptions',
    ICONS: { tickets: '<svg/>' }, window: { location: { href: '' } }, showToast() {},
    document: { getElementById(id) { if (!nodes.has(id)) nodes.set(id, { style: {} }); return nodes.get(id); } },
    redemptionIdentity: value => value.user && value.url ? value.user + '|' + value.url : '',
    isOfferUnavailableForRedeem: () => false,
    enterOwnedModal() {}, leaveOwnedModal() {},
    parseRedemptionMoment: value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) ? { timestamp: Date.parse(value) } : null,
    saveSettingsToStorage() { writes.push('save'); }, localStorage: { setItem() {} },
    updateTimelineBadgeDot() {}, renderRedemptionsScreen() {}, updateSessionStatusUI() {},
    fetch: () => assert.fail('account transport must not use ambient WebView cookies'),
  });
  vm.runInContext(functions, context);
  return { context, nodes, writes };
}
test('redeem captures the chosen account, blocks duplicate clicks and accepts no fabricated success', async () => {
  const { context: c, nodes } = harness();
  await c.executeRedemption();
  assert.match(c.window.location.href, /^hunter-account:\/\/redeem\?account=leosaquetto&url=/);
  c.settings.currentUser = 'gaoriel';
  await c.executeRedemption();
  assert.match(c.window.location.href, /account=leosaquetto/);
  c.handleNativeAccountResult('redeem', 'gaoriel', { account: 'gaoriel', status: 'confirmed' });
  assert.equal(nodes.get('btnConfirmAction').disabled, true);
  c.handleNativeAccountResult('redeem', 'leosaquetto', { account: 'leosaquetto', status: 'uncertain' });
  assert.match(nodes.get('cfFeedbackArea').textContent, /incerto/);
  assert.equal(c.settings.realRedemptions.length, 0);
  assert.equal(c.settings.myRedemptions, undefined);
});
test('verified receipts keep account ownership and original dates across profile switches', () => {
  const { context: c } = harness();
  const url = 'https://clube.uol.com.br/perfil/beneficios/one';
  c.settings.realRedemptions = [{ user: 'leosaquetto', url, redeemedAt: '2026-10-01', cupomCode: 'preserved' }];
  c.handleNativeAccountResult('history', 'gaoriel', { account: 'gaoriel', status: 'authenticated',
    records: [{ id: 'one', title: 'Same campaign', url, imageUrl: 'https://example.test/poster.jpg', redemptionDate: null }] });
  assert.equal(c.settings.realRedemptions.length, 2);
  assert.equal(c.settings.realRedemptions[0].redeemedAt, '2026-10-01');
  assert.equal(c.settings.realRedemptions[0].cupomCode, 'preserved');
  const gab = c.settings.realRedemptions[1];
  assert.equal(gab.user, 'gaoriel'); assert.equal(gab.redeemedAt, ''); assert.equal(gab.date, '');
  assert.equal(gab.thumb, 'https://example.test/poster.jpg');
  c.handleNativeAccountResult('history', 'leosaquetto', { account: 'gaoriel', status: 'authenticated', records: [] });
  assert.equal(c.settings.realRedemptions.length, 2);
});
test('a new native receipt overrides stale local metadata without borrowing its date or voucher bytes', () => {
  const { context: c } = harness();
  const url = 'https://clube.uol.com.br/perfil/beneficios/one';
  c.settings.realRedemptions = [{ user: 'leosaquetto', id: 'one', url, title: 'Old title',
    redeemedAt: '2026-10-01', voucherImageDataUrl: 'data:image/png;base64,aGVsbG8=',
    localVoucherFile: '/private/path', cupomCode: 'old-code' }];
  c.handleNativeAccountResult('history', 'leosaquetto', { account: 'leosaquetto', status: 'authenticated',
    records: [{ id: 'one', user: 'leosaquetto', title: 'Verified title', url, hasNativeVoucher: true,
      nativeVoucherIndex: 7 }] });
  const row = c.settings.realRedemptions[0];
  assert.equal(row.title, 'Verified title');
  assert.equal(row.redeemedAt, '');
  assert.equal(row.cupomCode, 'old-code');
  assert.equal(row.voucherImageDataUrl, undefined);
  assert.equal(row.localVoucherFile, undefined);
  const verified = c.window.hunterVerifiedRedemptions.get('leosaquetto|' + url);
  assert.equal(verified.title, 'Verified title');
  assert.equal(verified.nativeVoucherIndex, 7);
  assert.equal(verified.cupomCode, undefined);

  c.handleNativeAccountResult('history', 'leosaquetto', { account: 'leosaquetto', status: 'authenticated',
    records: [{ id: 'one', user: 'leosaquetto', title: 'Verified title', url, redemptionDate: '2026-10-18' }] });
  assert.equal(c.settings.realRedemptions[0].redeemedAt, '2026-10-18');
  c.handleNativeAccountResult('history', 'leosaquetto', { account: 'leosaquetto', status: 'authenticated',
    records: [{ id: 'one', user: 'leosaquetto', title: 'Verified title', url }] });
  assert.equal(c.settings.realRedemptions[0].redeemedAt, '2026-10-18');
});
