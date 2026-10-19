const test = require('node:test');
const assert = require('node:assert/strict');
const webviewApps = require('./adapters/webview-apps.js');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const dashboardSource = `
function buildDashboardHTML(historyData, settings, credentials, sessionCookies, realRedemptions, collectionPausedForUpdate = false, collectionResult = null) {
  const initialData = {
    settings: settings,
    credentials: credentials,
    sessionCookies: (sessionCookies && sessionCookies.length > 0) ? sessionCookies : cookiesForActiveUser,
    sessionCookiesLeo: settings.userSessions.leosaquetto,
    sessionCookiesGaoriel: settings.userSessions.gaoriel,
    nativeAccounts: true
  };
  const jsonStr = JSON.stringify(initialData).replace(/</g, "\\\\u003c");
  const rawHtml = '<html><head></head><body>Dashboard<script>const INITIAL = __INITIAL_DATA_JSON__;<\\/script>Boolean(targetCreds.login && targetCreds.senha);Boolean(userCreds.login && userCreds.senha)</body></html>';
  return resolveHunterAssets(rawHtml).replace("__INITIAL_DATA_JSON__", () => jsonStr);
}
function resolveHunterAssets(html) { return html; }
`;

const uolSource = `
async function main() {
  fixtures.started++;
  const parser = new WebView();
  await parser.loadHTML('<html>hidden parser</html>', 'https://clube.uol.com.br');
  parser.shouldAllowRequest = () => false;
  fixtures.parsed = await parser.evaluateJavaScript('parse-receipt', false);
  const ui = new WebView(); fixtures.ui = ui;
  ui.shouldAllowRequest = request => {
    if (request.url === 'hunter-theme://set?value=white') { fixtures.themes++; ui.evaluateJavaScript('theme-saved', true).catch(()=>{}); return false; }
    if (request.url === 'hunter-account://login') {
      fixtures.loginTask = (async()=>{const login = new WebView(); fixtures.login = login; await login.loadURL('https://clube.uol.com.br/login'); await login.present(); await login.loadHTML('<p>Auth received</p>', 'https://clube.uol.com.br');})();
      return false;
    }
    return true;
  };
  await ui.loadHTML('<html><head></head><body>Startup</body></html>', 'https://clube.uol.com.br');
  const presentation = ui.present(true);
  await fixtures.ready;
  await ui.loadHTML(buildDashboardHTML({}, fixtures.settings, fixtures.credentials, fixtures.cookies, []), 'https://clube.uol.com.br');
  await presentation;fixtures.finished++;
}
${dashboardSource}
await main();
Script.complete();
`;
const bilheteTail = `
try{
  cleanupRetiredInfrastructureKeys();
  if(config.runsInWidget)Script.setWidget(await widget());
  else await panel(args.queryParameters?.mode||'painel');
}catch(e){
  if(config.runsInWidget){recordAutomaticError(e);const w=new ListWidget();w.addText('Abra Bilhete Único no Scriptable');w.url=open('painel');Script.setWidget(w);}
  else await errorDialog(e);
}
Script.complete();`;
const bilheteSource = `
async function panel(mode) {
  fixtures.modes.push({mode,params:{...args.queryParameters}});
  if(mode==='recarga'){fixtures.changes++;if(fixtures.nativeWait)await fixtures.nativeWait;return;}
  if(mode==='corrigir')throw new Error('private native message');
  if(mode==='auth'){const login=new WebView();await login.loadURL('https://example.invalid/login');await login.present();return;}
  if(mode==='close')return;
  const view=new WebView();fixtures.ui=view;
  await view.loadHTML('<html><head></head><body>'+mode+'</body></html>','https://bilheteunico.sptrans.local');
  await view.present(true);
}
` + bilheteTail;
function harness(options = {}) {
  let readyResolve; const fixtures = { started: 0, finished: 0, parsed: null, themes: 0, modes: [], changes: 0, ready: new Promise(resolve => { readyResolve = resolve; }) };
  fixtures.settings = { currentUser: 'leosaquetto', groupedView: true, userSessions: { leosaquetto: [{ value: 'session-leo-secret' }], gaoriel: [{ value: 'session-guest-secret' }] }, password: 'settings-secret', unrelatedToken: 'unrelated-secret' };
  fixtures.credentials = { email: 'raw-email-secret@example.invalid', senha: 'top-password-secret', accounts: { leosaquetto: { login: 'native-login@example.invalid', senha: 'leo-password-secret', cookies: ['account-cookie-secret'] }, gaoriel: { login: 'guest-login@example.invalid', senha: 'guest-password-secret' } } };
  fixtures.cookies = [{ name: 'SESSION', value: 'active-cookie-secret' }];
  const renders = [], evaluations = [], nativeCalls = [], openings = [], sources = { 'clube-uol': uolSource, 'bilhete-unico': bilheteSource };
  let closeCount = 0, currentTab = 'history';
  class NativeWebView {
    async loadHTML(html, baseURL) { nativeCalls.push(['html', html, baseURL]); }
    async loadURL(url) { nativeCalls.push(['url', url]); }
    async loadRequest(request) { nativeCalls.push(['request', request]); }
    async evaluateJavaScript(code, callback) { nativeCalls.push(['eval', code, callback]); return 'native-parser-result'; }
    async present(fullscreen) { nativeCalls.push(['present', fullscreen]); if (fixtures.nativeSheetWait) await fixtures.nativeSheetWait; }
    async getHTML() { return 'native-html'; }
  }
  const loader = { async load(id, exportsString, transform, overrides) {
    const transformed = transform(sources[id]);
    const globals = { fixtures, config: { runsInWidget: false }, Script: { complete() { throw new Error('standalone_complete_must_not_run'); } }, ...overrides };
    return new AsyncFunction(...Object.keys(globals), transformed + '\nreturn (' + exportsString + ');')(...Object.values(globals));
  } };
  const api = webviewApps.create({ loader, WebView: NativeWebView, theme: 'light', render: async (html, base) => { renders.push({ html, base }); }, evalFrame: async (code, callback) => { evaluations.push({ code, callback }); return code.includes('.tab-content.active') ? currentTab : 'frame-result'; }, openURL: async url => { openings.push(url); if (options.externalWait) await options.externalWait; }, closeRequested: async () => { closeCount++; if (options.close) await options.close(); } });
  return { fixtures, renders, evaluations, nativeCalls, openings, sources, api, readyResolve, closeCount: () => closeCount, setTab: value => { currentTab = value; } };
}
const route = (mode, params = {}) => 'scriptable:///run?scriptName=' + encodeURIComponent('Bilhete Único') + '&mode=' + encodeURIComponent(mode) + Object.entries(params).map(([k, v]) => '&' + encodeURIComponent(k) + '=' + encodeURIComponent(v)).join('');
const tick = () => new Promise(resolve => setImmediate(resolve));

test('UOL startup embeds immediately while main awaits final render and keeps parser native', async () => {
  const h = harness(), panel = await h.api.open('clube-uol');
  assert.equal(h.fixtures.started, 1); assert.equal(h.fixtures.finished, 0); assert.match(panel.html, /Startup/); assert.match(panel.html, /data-saquetto-app-theme/);
  assert.equal(h.fixtures.parsed, 'native-parser-result'); assert.deepEqual(h.nativeCalls.map(c => c[0]), ['html', 'eval']);
  assert.equal(h.renders[0].base, 'https://clube.uol.com.br'); h.readyResolve(); await tick();
  assert.match(panel.html, /Dashboard/); assert.equal(h.renders.length, 2); panel.dispose(); await tick(); assert.equal(h.fixtures.finished, 1);
});
test('UOL original navigation handler receives actions and iframe evaluation preserves callback flag', async () => {
  const h = harness(), panel = await h.api.open('clube-uol');
  const result = await panel.handle('webview.navigate', { url: 'hunter-theme://set?value=white' });
  assert.deepEqual(result, { status: 'ok', allow: false }); await tick(); assert.equal(h.fixtures.themes, 1); assert.deepEqual(h.evaluations[0], { code: 'theme-saved', callback: true });
  h.readyResolve(); panel.dispose();
});
test('login loadURL and subsequent confirmation HTML stay in real native WebView', async () => {
  const h = harness(), panel = await h.api.open('clube-uol');
  await panel.handle('webview.navigate', { url: 'hunter-account://login' }); await h.fixtures.loginTask;
  assert.ok(h.nativeCalls.some(c => c[0] === 'url')); assert.ok(h.nativeCalls.some(c => c[0] === 'present')); assert.ok(h.nativeCalls.some(c => c[0] === 'html' && c[1].includes('Auth received')));
  assert.equal(h.renders.length, 1); h.readyResolve(); panel.dispose();
});
test('ordinary external links use host opener without replacing central document', async () => {
  const h = harness(), panel = await h.api.open('clube-uol');
  assert.deepEqual(await panel.handle('webview.navigate', { url: 'https://example.invalid/offer' }), { status: 'ok', allow: false }); assert.deepEqual(h.openings, ['https://example.invalid/offer']);
  assert.equal(panel.shouldAllowRequest({ url: 'https://example.invalid/second' }), false); await tick(); assert.equal(h.openings.length, 2);
  assert.equal((await panel.handle('webview.navigate', { url: 'javascript:alert(1)' })).status, 'error'); h.readyResolve(); panel.dispose();
});
test('Bilhete executes routes inside existing core and refreshes current tab after native actions', async () => {
  const h = harness(), panel = await h.api.open('bilhete-unico');
  assert.equal(h.fixtures.modes[0].mode, 'painel'); assert.equal(h.renders[0].base, 'https://bilheteunico.sptrans.local');
  let result = await panel.handle('webview.navigate', { url: route('metro', { destino: 'Estação de teste' }) });
  assert.equal(result.status, 'ok'); assert.match(panel.html, /metro/); assert.equal(h.fixtures.modes[1].params.destino, 'Estação de teste');
  result = await panel.handle('webview.navigate', { url: route('recarga') }); assert.equal(result.status, 'ok'); assert.equal(h.fixtures.changes, 1);
  assert.deepEqual(h.fixtures.modes.at(-1), { mode: 'painel', params: { mode: 'painel', tab: 'history' } }); assert.equal(h.openings.length, 0); panel.dispose();
});
test('Bilhete route identity, modes, query duplication and controls fail closed', async () => {
  const h = harness(), panel = await h.api.open('bilhete-unico');
  for (const url of [route('painel') + '&mode=recarga', route('invalid'), route('metro', { tab: 'invalid' }), route('metro', { unknown: 'x' }), route('metro', { destino: '\u0000' }), route('painel').replace('Bilhete%20%C3%9Anico', 'Other'), 'scriptable:///run?scriptName=%ZZ&mode=painel']) {
    assert.equal((await panel.handle('webview.navigate', { url })).reason, 'invalid_route');
  }
  assert.equal(h.fixtures.modes.length, 1); panel.dispose();
});
test('Bilhete accepts Unicode normalization, closes only its host panel and never starts another script', async () => {
  const h = harness(), panel = await h.api.open('bilhete-unico');
  const decomposed = route('metro').replace(encodeURIComponent('Bilhete Único'), encodeURIComponent('Bilhete Único'.normalize('NFD')));
  assert.equal((await panel.handle('webview.navigate', { url: decomposed })).status, 'ok');
  assert.equal((await panel.handle('webview.navigate', { url: route('close') })).status, 'ok'); assert.equal(h.closeCount(), 1); assert.equal(h.openings.length, 0); panel.dispose();
});
test('Bilhete duplicate native actions are blocked while previous confirmation is running', async () => {
  const h = harness(), panel = await h.api.open('bilhete-unico'); let resolve;
  h.fixtures.nativeWait = new Promise(r => { resolve = r; });
  const task = panel.handle('webview.navigate', { url: route('recarga') }); await tick();
  assert.equal((await panel.handle('webview.navigate', { url: route('recarga') })).reason, 'busy'); resolve(); assert.equal((await task).status, 'ok'); assert.equal(h.fixtures.changes, 1); panel.dispose();
});
test('native failure returns generic error without leaking message or automatically retrying mutations', async () => {
  const h = harness(), panel = await h.api.open('bilhete-unico');
  const result = await panel.handle('webview.navigate', { url: route('corrigir') }); assert.equal(result.reason, 'native_action_failed'); assert.doesNotMatch(JSON.stringify(result), /private/); assert.equal(h.fixtures.modes.length, 2); panel.dispose();
});
test('dispose prevents delayed render and evaluation while releasing embedded presentation', async () => {
  const h = harness(), panel = await h.api.open('clube-uol'); panel.dispose(); h.readyResolve(); await tick();
  assert.equal(h.renders.length, 1); assert.equal(h.fixtures.finished, 1); await assert.rejects(h.fixtures.ui.evaluateJavaScript('private-code'), /app_closed/);
  assert.equal((await panel.handle('webview.navigate', { url: 'https://example.invalid' })).reason, 'disposed'); assert.equal(panel.shouldAllowRequest({ url: 'https://example.invalid' }), false);
});
test('UOL dashboard DTO removes passwords, raw logins and all session sources without mutating native state', async () => {
  const h = harness(), snapshot = JSON.stringify({ settings: h.fixtures.settings, credentials: h.fixtures.credentials, cookies: h.fixtures.cookies });
  const panel = await h.api.open('clube-uol'); h.readyResolve(); await tick();
  for (const value of ['session-leo-secret', 'session-guest-secret', 'settings-secret', 'unrelated-secret', 'raw-email-secret@example.invalid', 'top-password-secret', 'native-login@example.invalid', 'leo-password-secret', 'account-cookie-secret', 'guest-login@example.invalid', 'guest-password-secret', 'active-cookie-secret']) assert.ok(!panel.html.includes(value), 'secret must remain native');
  const initial = JSON.parse(panel.html.match(/const INITIAL = (.*?);<\/script>/)[1]);
  assert.deepEqual(initial.settings, { currentUser: 'leosaquetto', groupedView: true });
  assert.deepEqual(initial.credentials, { accounts: { leosaquetto: { login: 'nat••••id', hasCredentials: true }, gaoriel: { login: 'gue••••id', hasCredentials: true } }, source: 'Login nativo' });
  assert.deepEqual(initial.sessionCookies, []); assert.deepEqual(initial.sessionCookiesLeo, []); assert.deepEqual(initial.sessionCookiesGaoriel, []);
  assert.match(panel.html, /Boolean\(targetCreds.hasCredentials\)/); assert.match(panel.html, /openAuthAssistantModal = reconnectUolSession/);
  assert.equal(JSON.stringify({ settings: h.fixtures.settings, credentials: h.fixtures.credentials, cookies: h.fixtures.cookies }), snapshot);
  await panel.dispose();
});
test('UOL close waits for main and the latest native action and is idempotent', async () => {
  const h = harness(), panel = await h.api.open('clube-uol'); let finishAction;
  h.fixtures.ui.hunterActionTask = new Promise(resolve => { finishAction = resolve; });
  let closed = false; const closing = panel.dispose(); assert.equal(panel.dispose(), closing); closing.then(() => { closed = true; });
  await tick(); assert.equal(closed, false); assert.equal(h.fixtures.finished, 0);
  h.readyResolve(); await tick(); assert.equal(h.fixtures.finished, 1); assert.equal(closed, false);
  finishAction(); await closing; assert.equal(closed, true); assert.equal(h.renders.length, 1);
});
test('Bilhete close drains an in-flight native operation without refreshing or retrying it', async () => {
  const h = harness(), panel = await h.api.open('bilhete-unico'); let finish;
  h.fixtures.nativeWait = new Promise(resolve => { finish = resolve; });
  const operation = panel.handle('webview.navigate', { url: route('recarga') }); await tick();
  let closed = false; const closing = panel.dispose().then(() => { closed = true; }); await tick(); assert.equal(closed, false);
  finish(); assert.equal((await operation).reason, 'disposed'); await closing;
  assert.equal(h.fixtures.changes, 1); assert.equal(h.renders.length, 1); assert.equal(h.fixtures.modes.length, 2);
});
test('native sheet and external navigation settle before dispose completes', async () => {
  let finishNavigation; const externalWait = new Promise(resolve => { finishNavigation = resolve; });
  const h = harness({ externalWait }), panel = await h.api.open('bilhete-unico'); let finishSheet;
  h.fixtures.nativeSheetWait = new Promise(resolve => { finishSheet = resolve; });
  const auth = panel.handle('webview.navigate', { url: route('auth') });
  const navigation = panel.handle('webview.navigate', { url: 'https://example.invalid/offer' }); await tick();
  let closed = false; const closing = panel.dispose().then(() => { closed = true; }); await tick(); assert.equal(closed, false);
  finishSheet(); await auth; await tick(); assert.equal(closed, false); finishNavigation(); await navigation; await closing; assert.equal(closed, true);
});
test('Bilhete close route can await host disposal without a self-wait cycle', async () => {
  let panel, hostClosed = false;
  const h = harness({ close: async () => { await panel.dispose(); hostClosed = true; } }); panel = await h.api.open('bilhete-unico');
  assert.equal((await panel.handle('webview.navigate', { url: route('close') })).status, 'ok');
  await panel.dispose(); await tick(); assert.equal(hostClosed, true); assert.equal(h.closeCount(), 1);
});
test('unknown IDs and changed entrypoints reject without executing installed code', async () => {
  const h = harness(); await assert.rejects(h.api.open('other'), /unsupported_webview_app/);
  h.sources['clube-uol'] = uolSource.replace('\nawait main();\nScript.complete();', '\nvoid main();\nScript.complete();'); await assert.rejects(h.api.open('clube-uol'), /adapter_entrypoint_changed/); assert.equal(h.fixtures.started, 0);
  h.sources['bilhete-unico'] = bilheteSource.replace('cleanupRetiredInfrastructureKeys();', 'unexpectedCleanup();'); await assert.rejects(h.api.open('bilhete-unico'), /adapter_entrypoint_changed/); assert.equal(h.fixtures.modes.length, 0);
  h.sources['clube-uol'] = uolSource.replace('    credentials: credentials,', '    credentials: loadCredentials(),'); await assert.rejects(h.api.open('clube-uol'), /adapter_dashboard_changed/); assert.equal(h.fixtures.started, 0);
});
