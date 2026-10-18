const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { webcrypto } = require('node:crypto');
const source = fs.readFileSync(path.join(__dirname, 'build.js'), 'utf8');
const hostCode = source.slice(source.indexOf('let nativeAccountManager = null;'), source.indexOf('function isNewerVersion('));
const routeCode = source.slice(source.indexOf('function installHunterBrowser('), source.indexOf('function nativeRedemptionKey('));
const parser = fs.readFileSync(path.join(__dirname, '../cloudflare-workers/uol-redemption-sentinel/src/offers.js'), 'utf8').replace(/^export /gm, '');

test('native receipt parser treats response HTML as data in an unpresented, network-blocked document', async () => {
  const loads = [], scripts = []; let options, presents = 0;
  const context = vm.createContext({
    UOL_RECEIPT_PARSER: parser, Date, Keychain: {}, UUID: {}, Request: class {},
    HunterRedemption: { create: value => { options = value; return {}; } },
    WebView: class {
      async loadHTML(html) { loads.push(html); }
      async evaluateJavaScript(code, useCompletion) {
        scripts.push(code); assert.equal(this.shouldAllowRequest({ url: 'https://example.test/' }), false);
        if (useCompletion) return new Promise(resolve => {
          vm.runInNewContext(code, { URL, crypto: webcrypto, TextEncoder, Uint8Array, completion: resolve });
        });
        return vm.runInNewContext(code, { URL });
      }
      present() { presents++; assert.fail('receipt parser must never present response HTML'); }
    },
    loadSessionCookies: user => [{ account: user }],
  });
  vm.runInContext(hostCode, context); context.createNativeAccountManager();
  const html = '<html><body>Olá, Alice Teste!<h1>Meus Resgates</h1><div id="beneficios">' +
    '<article class="beneficio"><h2 class="titulo">&lt;img onerror=fetch(1)&gt;</h2><a href="/perfil/beneficios/receipt-1">Ver</a></article></div>' +
    '<script>throw new Error("HTML executed")</script><img src="https://example.test/tracker"></body></html>';
  const history = await options.parseHistory(html);
  assert.equal(history.signedInName, 'Alice Teste');
  assert.equal(history.entries[0].title, '<img onerror=fetch(1)>');
  assert.equal(history.entries[0].redemptionDate, null);
  assert.equal(presents, 0); assert.equal(loads.length, 1);
  assert.doesNotMatch(loads[0], /Alice|tracker|HTML executed|receipt-1/);
  assert.match(loads[0], /Content-Security-Policy/);
  assert.ok(scripts[0].includes('\\u003cscript>'), 'HTML markup stays inside an escaped JSON string');
});

test('native account bridge captures the account and refuses unsafe redeem destinations before dispatch', async () => {
  const view = { async evaluateJavaScript(code) { results.push(code); } }, calls = [], results = [];
  let release;
  const context = vm.createContext({
    manageNativeAccount: async (...args) => { calls.push(args); await new Promise(resolve => { release = resolve; }); },
    manageNativeHunterAccess: () => assert.fail('account routes do not activate collector devices'),
    Safari: { open: () => assert.fail('account route must not use the browser cookie jar'), openInApp: () => assert.fail('account route must not open a sheet') },
  });
  vm.runInContext(routeCode, context); context.installHunterBrowser(view);
  const prefix = 'hunter-account://redeem?account=gaoriel&url=';
  for (const url of ['http://clube.uol.com.br/campanhasdeingresso/one', 'https://example.test/campanhasdeingresso/one',
    'https://clube.uol.com.br@evil.test/campanhasdeingresso/one', 'https://clube.uol.com.br:444/campanhasdeingresso/one',
    'https://clube.uol.com.br/campanhasdeingresso/one/resgatar', 'https://clube.uol.com.br/campanhasdeingresso/one?x=1',
    'https://clube.uol.com.br/campanhasdeingresso/%2e%2e', 'https://clube.uol.com.br/campanhasdeingresso/one#x']) {
    assert.equal(view.shouldAllowRequest({ url: prefix + encodeURIComponent(url) }), false);
  }
  await Promise.resolve(); assert.equal(calls.length, 0);
  const url = 'https://clube.uol.com.br/campanhasdeingresso/one';
  assert.equal(view.shouldAllowRequest({ url: prefix + encodeURIComponent(url) }), false);
  await Promise.resolve();
  assert.equal(calls.length, 1); assert.deepEqual(calls[0].slice(1), ['redeem', 'gaoriel', url]);
  assert.equal(view.shouldAllowRequest({ url: 'hunter-account://history?account=leosaquetto' }), false);
  await Promise.resolve(); assert.equal(calls.length, 1);
  assert.match(results[0], /"history","leosaquetto".*status:'busy',account:"leosaquetto"/);
  release(); await view.hunterActionTask;
  assert.equal(view.shouldAllowRequest({ url: 'hunter-account://history?account=leosaquetto' }), false);
  await Promise.resolve(); assert.deepEqual(calls[1].slice(1), ['history', 'leosaquetto', '']);
  release(); await view.hunterActionTask;
});

test('native redeem cancellation never calls transport and result JSON cannot escape the callback', async () => {
  const calls = [], evaluations = []; let confirm = -1;
  const context = vm.createContext({
    HunterRedemption: { create: () => ({
      redeem: async (account, url) => { calls.push({ account, url }); return { account, status: 'uncertain', reason: '</script><script>private' }; },
    }) },
    Keychain: {}, Date, UUID: {}, Request: class {},
    Alert: class { addAction() {} addCancelAction() {} async presentAlert() { return confirm; } },
  });
  vm.runInContext(hostCode, context);
  const view = { async evaluateJavaScript(code) { evaluations.push(code); } };
  const url = 'https://clube.uol.com.br/campanhasdeingresso/one';
  await context.manageNativeAccount(view, 'redeem', 'gaoriel', url);
  assert.equal(calls.length, 0); assert.match(evaluations[0], /"status":"cancelled","account":"gaoriel"/);
  confirm = 0; await context.manageNativeAccount(view, 'redeem', 'gaoriel', url);
  assert.deepEqual(calls, [{ account: 'gaoriel', url }]);
  assert.doesNotMatch(evaluations[1], /<\/script>/);
  assert.match(evaluations[1], /\\u003c\/script>/);
});
