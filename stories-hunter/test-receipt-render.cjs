const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const template = require('./assemble-ui.cjs').assembleUI();
function fn(name) {
  const starts = ['    function ' + name + '(', '    async function ' + name + '('];
  const start = starts.map(value => template.indexOf(value)).find(value => value >= 0);
  assert.ok(start >= 0, name);
  return template.slice(start, template.indexOf('\n    }', start) + '\n    }'.length);
}
function harness(records = []) {
  const nodes = new Map(), copies = [];
  const context = vm.createContext({
    URL, Blob, atob, Uint8Array,
    settings: { currentUser: 'leosaquetto' }, currentRedemptionsFilter: 'all', currentRedemptionsList: records,
    uolSessionPresentation: () => ({ label: 'Conectado' }), currentSessionStatus: 'valid', ICONS: { globe: '<svg/>', tickets: '<svg/>' },
    getAllRedemptionsList: () => records, getAvatarUrl: () => 'https://example.test/avatar.jpg', parseRedemptionDate: () => 0,
    localStorage: { setItem() {} }, document: { getElementById(id) {
      if (!nodes.has(id)) nodes.set(id, { style: {}, innerHTML: '', textContent: '' });
      return nodes.get(id);
    } },
    copyPromoCode: code => copies.push(code), fetch: () => assert.fail('rendering must not fetch an unsafe URL'),
    getStoredVoucher: () => null, trustedVoucherDownloadUrl: () => '',
    isVerifiedReceipt: () => false,
    enterOwnedModal() {}, leaveOwnedModal() {}, closeRedemptionDetailsModal() {},
    console: { log() {} },
  });
  vm.runInContext(['escapeReceiptHtml', 'receiptSafeUrl', 'formatRedemptionTitle', 'formatRedemptionDateTime',
    'base64ToUint8Array', 'voucherFileName', 'voucherMimeFromBytes', 'voucherBlobFromDataUrl',
    'renderRedemptionsScreen', 'openRedemptionDetailsModal', 'loadVoucherContent'].map(fn).join('\n'), context);
  return { context, nodes, copies };
}

test('receipt text and image attributes cannot turn decoded history fields into executable markup', () => {
  const record = { user: 'leosaquetto', title: '<img src=x onerror=alert(1)>', partner: 'Campanhas de Ingressos',
    thumb: 'https://example.test/" onerror="alert(1)', redeemedAt: '<img src=x onerror=alert(2)>' };
  const { context: c, nodes } = harness([record]);
  c.renderRedemptionsScreen();
  const html = nodes.get('viewRedemptions').innerHTML;
  assert.ok(html.includes(c.escapeReceiptHtml(c.formatRedemptionTitle(record.title))));
  assert.ok(html.includes(c.escapeReceiptHtml(record.redeemedAt)));
  assert.doesNotMatch(html, /<img src=x|<Img Src=x|src="https:\/\/example\.test\/" onerror=/);
  record.partner = '<img src=x onerror=alert(3)>';
  c.openRedemptionDetailsModal(0);
  assert.equal(nodes.get('rdmTitle').textContent, c.formatRedemptionTitle(record.title));
  assert.equal(nodes.get('rdmImg').style.display, 'none');
  assert.ok(nodes.get('rdmTagRow').innerHTML.includes(c.escapeReceiptHtml(record.partner)));
  assert.ok(nodes.get('rdmTimeBadge').innerHTML.includes(c.escapeReceiptHtml(record.redeemedAt)));
});

test('receipt URL validation preserves HTTPS and base64 images but rejects credentials and executable schemes', () => {
  const { context: c } = harness();
  assert.equal(c.receiptSafeUrl('https://example.test/poster.jpg?a=1&b=2'), 'https://example.test/poster.jpg?a=1&b=2');
  assert.equal(c.receiptSafeUrl('data:image/png;base64,aGVsbG8='), 'data:image/png;base64,aGVsbG8=');
  assert.equal(c.receiptSafeUrl('data:application/pdf;base64,aGVsbG8=', 'pdf'), 'data:application/pdf;base64,aGVsbG8=');
  for (const url of ['javascript:alert(1)', 'data:text/html;base64,aGVsbG8=', 'data:image/svg+xml;base64,aGVsbG8=',
    'http://example.test/poster.jpg', 'https://user:password@example.test/image', 'https://example.test/" onerror="alert(1)',
    'https://example.test/with\\path', 'https://example.test/with\nnewline']) {
    assert.equal(c.receiptSafeUrl(url), '', url.split(':')[0]);
  }
  assert.equal(c.receiptSafeUrl('data:application/pdf;base64,aGVsbG8='), '');
  assert.equal(c.receiptSafeUrl('data:image/png;base64,aGVsbG8=', 'download'), '');
});

test('voucher display keeps image data and binds coupon copying without interpolating JavaScript', async () => {
  const { context: c, nodes, copies } = harness();
  const coupon = "x');alert(1);//<img src=x>";
  await c.loadVoucherContent({ cupomCode: coupon, voucherImageDataUrl: 'data:text/html;base64,aGVsbG8=', downloadUrl: 'javascript:alert(1)' });
  const html = nodes.get('voucherViewerContainer').innerHTML;
  assert.ok(html.includes(c.escapeReceiptHtml(coupon)));
  assert.doesNotMatch(html, /onclick=|data:text\/html|javascript:|<img src=x>/);
  nodes.get('btnCopyPromo').onclick(); assert.deepEqual(copies, [coupon]);
  const image = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9YfNpS8AAAAASUVORK5CYII=';
  await c.loadVoucherContent({ voucherImageDataUrl: image });
  assert.ok(nodes.get('voucherViewerContainer').innerHTML.includes(image));
  assert.equal(nodes.get('btnShareVoucher').style.display, 'flex');
});
