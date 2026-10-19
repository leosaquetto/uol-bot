const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const source = require('./assemble-ui.cjs').assembleUI();

function fn(name) {
  const start = source.search(new RegExp('    (?:async )?function ' + name + '\\('));
  assert.ok(start > 0, name);
  return source.slice(start, source.indexOf('\n    }', start) + '\n    }'.length);
}

function nodes() {
  const map = new Map();
  return { map, document: {
    getElementById(id) {
      if (!map.has(id)) map.set(id, { innerHTML: '', textContent: '', style: {} });
      return map.get(id);
    },
    querySelector() { return null; }
  } };
}

test('late offer details cannot replace the subsequently opened offer or execute markup', async () => {
  const { map, document } = nodes();
  const pending = new Map();
  const offers = new Map(['A', 'B'].map(id => [id, { id, title: id, link: 'https://clube.uol.com.br/campanhasdeingresso/' + id,
    category: 'OUTROS', imageUrl: '', description: '' }]));
  const context = vm.createContext({
    URL, Date, document, activeOffer: null, currentTab: 'home', historyData: { active: [], history: [] },
    CATEGORY_MAP: { OUTROS: { id: 'OUTROS', icon: '', label: 'Outros' } }, ICONS: {}, currentRenderedGroups: new Map(),
    findOffer: id => offers.get(id), isStoriesOnlyOffer: () => false, isEditorsChoice: () => false,
    getRedeemedUsersForOffer: () => [], renderRedeemedFriendsPill: () => '', getGroupKey: offer => offer.id,
    normalizeEventInfo: () => ({}), receiptSafeUrl: () => '', getCachedDescription: () => '',
    captureTime: () => 0,
    enterOwnedModal() {}, leaveOwnedModal() {},
    catalogCampaign: link => ({ link }),
    setCachedDescription() {}, saveHistoryToStorage() {}, classifyCategory: () => 'OUTROS',
    fetch: url => new Promise(resolve => pending.set(url, resolve)),
  });
  vm.runInContext('let detailsGeneration = 0;\n' + [
    'escapeReceiptHtml', 'isOfferUnavailableForRedeem', 'openDetails', 'renderModalDescription', 'closeDetailsModal'
  ].map(fn).join('\n'), context);
  const a = context.openDetails('A');
  const b = context.openDetails('B');
  const page = label => ({ ok: true, text: async () => `<p><p><strong>Assinante UOL:</strong> Show ${label}</p></p>` });
  pending.get(offers.get('B').link)(page('B'));
  await b;
  pending.get(offers.get('A').link)(page('A'));
  await a;
  assert.match(map.get('dtDesc').innerHTML, /Show B/);
  assert.doesNotMatch(map.get('dtDesc').innerHTML, /Show A/);
  context.renderModalDescription('<img src=x onerror=alert(1)>');
  assert.match(map.get('dtDesc').innerHTML, /&lt;img/);
  assert.doesNotMatch(map.get('dtDesc').innerHTML, /<img/);
});

test('voucher cache keys are account scoped and downloaded bytes determine PDF JPEG PNG', () => {
  const context = vm.createContext({ URL, Uint8Array });
  vm.runInContext(['receiptSafeUrl', 'getVoucherCacheKey', 'voucherMimeFromBytes', 'trustedVoucherDownloadUrl'].map(fn).join('\n'), context);
  const record = { id: 'same', cupomCode: 'same-code' };
  assert.notEqual(context.getVoucherCacheKey({ ...record, user: 'leosaquetto' }),
    context.getVoucherCacheKey({ ...record, user: 'gaoriel' }));
  assert.equal(context.getVoucherCacheKey(record), '');
  assert.equal(context.voucherMimeFromBytes(Uint8Array.of(37, 80, 68, 70, 45).buffer), 'application/pdf');
  assert.equal(context.voucherMimeFromBytes(Uint8Array.of(255, 216, 255, 1).buffer), 'image/jpeg');
  assert.equal(context.voucherMimeFromBytes(Uint8Array.of(137, 80, 78, 71, 13, 10, 26, 10).buffer), 'image/png');
  assert.equal(context.voucherMimeFromBytes(Uint8Array.of(60, 104, 116, 109, 108).buffer), '');
  assert.equal(context.trustedVoucherDownloadUrl('https://clube.uol.com.br/voucher/one'), 'https://clube.uol.com.br/voucher/one');
  assert.equal(context.trustedVoucherDownloadUrl('https://uol.com.br.evil.test/voucher'), '');
});

test('local-only receipts remain unverified even with forged voucher bridge metadata', () => {
  const context = vm.createContext({
    INITIAL: { realRedemptions: [{ id: 'verified', user: 'leosaquetto', receiptVerified: true },
      { id: 'legacy', user: 'leosaquetto' }] },
    window: {},
    redemptionIdentity: record => record.user + '|' + record.id
  });
  vm.runInContext("window.hunterVerifiedRedemptions = new Map([['gaoriel|live', { id: 'live', user: 'gaoriel' }]])", context);
  vm.runInContext(fn('isVerifiedReceipt'), context);
  assert.equal(context.isVerifiedReceipt({ id: 'verified', user: 'leosaquetto' }), true);
  assert.equal(context.isVerifiedReceipt({ id: 'legacy', user: 'leosaquetto', receiptVerified: true }), false);
  assert.equal(context.isVerifiedReceipt({ id: 'live', user: 'gaoriel' }), true);
  assert.equal(context.isVerifiedReceipt({ id: 'local', user: 'leosaquetto', hasNativeVoucher: true, nativeVoucherIndex: 0 }), false);
});

test('a stale voucher response cannot overwrite a newer PDF; cached PDF has a valid format', async () => {
  const { map, document } = nodes();
  const pending = new Map();
  let selected = 'A';
  const context = vm.createContext({
    URL, Uint8Array, Blob, btoa, atob, document, currentVoucherBlob: null, currentVoucherFileName: '',
    getStoredVoucher: () => null, storeVoucherInCache() {}, extractVoucherImage: async () => null,
    trustedVoucherDownloadUrl: url => url, console: { log() {} },
    fetch: url => new Promise(resolve => pending.set(url, resolve))
  });
  vm.runInContext(['escapeReceiptHtml', 'receiptSafeUrl', 'arrayBufferToBase64', 'voucherFileName',
    'voucherMimeFromBytes', 'loadVoucherContent'].map(fn).join('\n'), context);
  const a = context.loadVoucherContent({ user: 'leosaquetto', downloadUrl: 'https://clube.uol.com.br/A' }, () => selected === 'A');
  selected = 'B';
  const b = context.loadVoucherContent({ user: 'leosaquetto', downloadUrl: 'https://clube.uol.com.br/B' }, () => selected === 'B');
  const response = (url, bytes) => ({ ok: true, url, arrayBuffer: async () => bytes.buffer });
  pending.get('https://clube.uol.com.br/B')(response('https://clube.uol.com.br/B', Uint8Array.of(37, 80, 68, 70, 45, 49)));
  await b;
  pending.get('https://clube.uol.com.br/A')(response('https://clube.uol.com.br/A', Uint8Array.of(255, 216, 255, 1)));
  await a;
  assert.match(map.get('voucherViewerContainer').innerHTML, /voucher-pdf-frame/);
  assert.doesNotMatch(map.get('voucherViewerContainer').innerHTML, /voucher-preview-img/);
  assert.equal(vm.runInContext('currentVoucherBlob.type', context), 'application/pdf');
  assert.equal(vm.runInContext('currentVoucherFileName', context), 'voucher-clube-uol.pdf');
});

test('verified native voucher request binds account, index, and generation before showing bytes', async () => {
  const { map, document } = nodes();
  const record = { id: 'receipt-one', user: 'leosaquetto', title: 'Voucher', nativeVoucherIndex: 3,
    receiptVerified: true, hasNativeVoucher: false };
  const context = vm.createContext({
    URL, Blob, Uint8Array, atob, document, currentRedemptionsList: [record], activeRedemptionIndex: -1,
    INITIAL: { nativeAccounts: true }, settings: { currentUser: 'leosaquetto' },
    window: { location: { href: '' } }, formatRedemptionTitle: title => title,
    formatRedemptionDateTime: date => date, loadVoucherContent: () => assert.fail('native voucher should be first'),
    isVerifiedReceipt: () => true,
    enterOwnedModal() {}, leaveOwnedModal() {}, closeVoucherModal() {},
  });
  vm.runInContext('let voucherGeneration = 0; let pendingNativeVoucher = null; let currentVoucherBlob = null; let currentVoucherFileName = "";\n' +
    ['receiptSafeUrl', 'escapeReceiptHtml', 'base64ToUint8Array', 'voucherMimeFromBytes', 'voucherBlobFromDataUrl', 'voucherFileName',
      'openVoucherModal', 'handleNativeVoucherResult'].map(fn).join('\n'), context);
  await context.openVoucherModal(0);
  assert.equal(context.window.location.href, 'hunter-voucher://get?index=3&account=leosaquetto&id=receipt-one&request=1');
  const result = { status: 'available',
    dataUrl: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9YfNpS8AAAAASUVORK5CYII=',
    mime: 'image/png' };
  context.handleNativeVoucherResult('leosaquetto', 3, { ...result, id: 'receipt-one', request: 0 });
  assert.doesNotMatch(map.get('voucherViewerContainer').innerHTML, /voucher-preview-img/);
  context.handleNativeVoucherResult('gaoriel', 3, { ...result, id: 'receipt-one', request: 1 });
  assert.doesNotMatch(map.get('voucherViewerContainer').innerHTML, /voucher-preview-img/);
  context.handleNativeVoucherResult('leosaquetto', 3, { ...result, id: 'wrong-receipt', request: 1 });
  assert.doesNotMatch(map.get('voucherViewerContainer').innerHTML, /voucher-preview-img/);
  context.handleNativeVoucherResult('leosaquetto', 3, { ...result, id: 'receipt-one', request: 1 });
  assert.match(map.get('voucherViewerContainer').innerHTML, /voucher-preview-img/);

  // “Todos” can open a verified receipt from the other profile without switching
  // the account selected for future redemptions.
  context.currentRedemptionsList.push({ ...record, user: 'gaoriel', nativeVoucherIndex: 4 });
  await context.openVoucherModal(1);
  assert.equal(context.settings.currentUser, 'leosaquetto');
  assert.equal(context.window.location.href, 'hunter-voucher://get?index=4&account=gaoriel&id=receipt-one&request=2');
  context.handleNativeVoucherResult('gaoriel', 4, {
    status: 'available', dataUrl: 'data:application/pdf;base64,JVBERi0xLjQK', mime: 'application/pdf',
    id: 'receipt-one', request: 2
  });
  assert.match(map.get('voucherViewerContainer').innerHTML, /voucher-pdf-frame/);
  assert.equal(vm.runInContext('currentVoucherBlob.type', context), 'application/pdf');
  assert.equal(vm.runInContext('currentVoucherFileName', context), 'voucher-clube-uol.pdf');
});

test('password actions use a native gesture route and never put the password in the WebView', () => {
  const { map, document } = nodes();
  const messages = [];
  const context = vm.createContext({
    document, INITIAL: { nativeAccounts: true }, credentials: { accounts: { leosaquetto: {
      hasLogin: true, maskedLogin: 'acc••••st', hasPassword: true } } }, settings: { currentUser: 'leosaquetto' },
    window: { location: { href: '' } }, getAvatarUrl: () => '', updateSessionStatusUI() {},
    showToast: message => messages.push(message), enterOwnedModal() {}, leaveOwnedModal() {},
    closeAuthAssistantModal() {}
  });
  vm.runInContext('let pendingNativeSecret = null;\n' + ['openAuthAssistantModal', 'requestNativeSecret',
    'showAuthLogin', 'toggleAuthPassVisibility', 'copyAuthLogin', 'copyAuthPassword', 'handleNativeSecretResult'].map(fn).join('\n'), context);
  context.openAuthAssistantModal();
  assert.equal(map.get('authModalLoginVal').textContent, 'acc••••st');
  assert.equal(map.get('authModalPassVal').textContent, '••••••••');
  context.showAuthLogin();
  assert.equal(context.window.location.href, 'hunter-secret://show-login?account=leosaquetto');
  context.handleNativeSecretResult('show-login', 'leosaquetto', { status: 'shown' });
  context.copyAuthLogin();
  assert.equal(context.window.location.href, 'hunter-secret://copy-login?account=leosaquetto');
  context.handleNativeSecretResult('copy-login', 'leosaquetto', { status: 'copied' });
  context.toggleAuthPassVisibility();
  assert.equal(context.window.location.href, 'hunter-secret://show?account=leosaquetto');
  assert.equal(map.get('authModalPassVal').textContent, '••••••••');
  context.handleNativeSecretResult('show', 'leosaquetto', { status: 'shown' });
  context.copyAuthPassword();
  assert.equal(context.window.location.href, 'hunter-secret://copy?account=leosaquetto');
  context.handleNativeSecretResult('copy', 'leosaquetto', { status: 'copied' });
  assert.match(messages.at(-1), /copiada/);
});

test('login navigation waits for the matching native copy result', () => {
  const scheduled = [], messages = [];
  const context = vm.createContext({
    INITIAL: { nativeAccounts: true }, settings: { currentUser: 'leosaquetto' },
    window: { location: { href: '' } }, showToast: message => messages.push(message),
    setTimeout: callback => scheduled.push(callback)
  });
  vm.runInContext('let pendingNativeSecret = null;\n' + ['requestNativeSecret', 'handleNativeSecretResult',
    'openUolLogoutAndLogin', 'openUolDirectLogin'].map(fn).join('\n'), context);
  context.openUolDirectLogin();
  assert.equal(context.window.location.href, 'hunter-secret://copy-login?account=leosaquetto');
  context.handleNativeSecretResult('copy-login', 'gaoriel', { status: 'copied' });
  assert.equal(scheduled.length, 0);
  context.handleNativeSecretResult('copy-login', 'leosaquetto', { status: 'missing' });
  assert.equal(scheduled.length, 0);
  assert.match(messages.at(-1), /não configurado/);
  context.openUolLogoutAndLogin();
  context.handleNativeSecretResult('copy-login', 'leosaquetto', { status: 'copied' });
  assert.equal(scheduled.length, 1);
  scheduled.shift()();
  assert.match(context.window.location.href, /^https:\/\/conta\.uol\.com\.br\/logout\?/);
  context.openUolDirectLogin();
  context.handleNativeSecretResult('copy-login', 'leosaquetto', { status: 'copied' });
  scheduled.shift()();
  assert.match(context.window.location.href, /^https:\/\/conta\.uol\.com\.br\/login\?/);
});

test('native voucher downloads stay in session memory without localStorage bytes', () => {
  const writes = [];
  const context = vm.createContext({
    URL, Blob, Uint8Array, atob, INITIAL: { nativeAccounts: true },
    localStorage: { getItem: () => assert.fail('native mode must not read voucher bytes'),
      setItem: (...args) => writes.push(args) }, console: { log() {} }
  });
  vm.runInContext('const voucherSessionCache = new Map();\n' + [
    'receiptSafeUrl', 'getVoucherCacheKey', 'voucherFileName', 'voucherMimeFromBytes',
    'base64ToUint8Array', 'voucherBlobFromDataUrl', 'getStoredVoucher', 'storeVoucherInCache'
  ].map(fn).join('\n'), context);
  const record = { user: 'leosaquetto', id: 'one' };
  const dataUrl = 'data:application/pdf;base64,JVBERi0xLjQK';
  const blob = new Blob([Buffer.from('%PDF-1.4\n')], { type: 'application/pdf' });
  context.storeVoucherInCache(record, dataUrl, blob);
  assert.equal(writes.length, 0);
  assert.equal(context.getStoredVoucher(record).blob.type, 'application/pdf');
  assert.equal(context.getStoredVoucher({ user: 'gaoriel', id: 'one' }), null);
});

test('owned dialogs move focus in, close on Escape, and restore the invoking control', () => {
  const focused = [];
  const trigger = { isConnected: true, focus: () => focused.push('trigger') };
  const closeButton = { focus: () => focused.push('close') };
  const modal = { style: { display: 'flex' }, setAttribute() {}, contains: () => false,
    querySelector: () => closeButton, focus: () => focused.push('modal') };
  const scheduled = [];
  const context = vm.createContext({ document: { activeElement: trigger, body: {} },
    setTimeout: fn => scheduled.push(fn) });
  vm.runInContext('const ownedModalOrigins = new WeakMap();\n' + ['enterOwnedModal', 'leaveOwnedModal'].map(fn).join('\n'), context);
  context.enterOwnedModal(modal, () => { modal.style.display = 'none'; context.leaveOwnedModal(modal); });
  scheduled.shift()();
  assert.deepEqual(focused, ['close']);
  modal.onkeydown({ key: 'Escape', preventDefault() {} });
  assert.deepEqual(focused, ['close', 'trigger']);
  assert.equal(modal.style.display, 'none');
});

test('timeline includes distinct future restock transitions and escapes external text', () => {
  const offer = { id: 'one', title: 'Show', category: 'OUTROS', captureSource: 'worker',
    capturedAt: Date.parse('2026-10-18T10:00:00Z') };
  const context = vm.createContext({
    Date, window: { hunterAvailabilityEvents: [
      { offerId: 'one', eventType: 'sold_out', occurredAt: '2026-10-18T11:00:00Z' },
      { offerId: 'one', eventType: 'restocked', occurredAt: '2026-10-18T12:00:00Z' }
    ] },
    historyData: { active: [offer], history: [] }, CATEGORY_MAP: { OUTROS: { label: 'Outros' } },
    getGroupKey: value => value.id, offerImage: () => '', isStoriesOnlyOffer: () => false,
    catalogTime: value => Number.isFinite(Date.parse(value)) ? Date.parse(value) : Number(value) || null,
    getAllRedemptionsList: () => [], currentTimelineFilter: 'all', ICONS: { tickets: '<svg></svg>' },
  });
  vm.runInContext(['plausibleCaptureTime', 'captureTime', 'getTimelineEvents'].map(fn).join('\n'), context);
  const transitions = context.getTimelineEvents().filter(event => event.id.startsWith('transition-'));
  assert.deepEqual(Array.from(transitions, event => event.type).sort(), ['esgotada', 'reaberta']);
  context.historyData.active.push({ ...offer, id: 'legacy', captureSource: 'legacy', capturedAt: 0,
    firstSeen: Date.parse('2026-10-19T10:00:00Z') });
  assert.equal(context.getTimelineEvents().find(event => event.id === 'nova-legacy').timestamp, null);

  const { map, document } = nodes();
  for (const id of ['tlFilterAll', 'tlFilterNew', 'tlFilterExpired', 'tlFilterRedemption']) {
    document.getElementById(id).classList = { toggle() {} };
    document.getElementById(id).setAttribute = () => {};
  }
  Object.assign(context, {
    document, getTimelineEvents: () => [{ type: 'nova', offerId: "x');alert(1)//", title: '<img src=x onerror=alert(1)>',
      desc: '<svg onload=alert(2)>', badgeText: '<b>NOVA</b>', timestamp: Date.now(), imageUrl: '' }],
    validOfferImage: () => '', timelineTimeLabel: () => '<script>bad</script>',
    htmlAttribute: value => String(value || '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch])),
  });
  vm.runInContext(fn('escapeReceiptHtml') + '\n' + fn('renderTimelineList'), context);
  context.renderTimelineList();
  const html = map.get('timelineList').innerHTML;
  assert.match(html, /&lt;img/);
  assert.match(html, /&lt;svg/);
  assert.doesNotMatch(html, /onclick=|<img src=x|<svg onload|<script>/);
});
