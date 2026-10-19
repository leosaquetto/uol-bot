const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const template = require('./assemble-ui.cjs').assembleUI();
function fn(name) {
  const start = template.indexOf('    function ' + name + '(');
  assert.ok(start > 0, name);
  return template.slice(start, template.indexOf('\n    }', start) + '\n    }'.length);
}
function harness(initial) {
  const elements = new Map(), toasts = [];
  const context = vm.createContext({
    INITIAL: initial, hunterInstagramAction: null, window: { location: { href: 'hunter-dashboard' } },
    document: { getElementById: id => {
      if (!elements.has(id)) elements.set(id, { textContent: '', innerHTML: '', setAttribute(key, value) { this[key] = value; }, getAttribute(key) { return this[key] ?? null; } });
      return elements.get(id);
    } },
    showToast: text => toasts.push(text),
  });
  vm.runInContext(['hunterAccessState', 'renderHunterAccessUI', 'manageHunterAccess', 'hunterStatusPresentation', 'hunterStatusIcon', 'setHunterCollectionStatus'].map(fn).join('\n'), context);
  return { context, elements, toasts };
}

test('member access states provide the correct action without importing private data into the page', () => {
  const cases = [
    ['activation_required', 'Ativação necessária', 'Solicitar acesso'],
    ['access_pending', 'Aguardando aprovação', 'Verificar aprovação'],
    ['approved', 'Acesso aprovado', 'Verificar acesso'],
    ['access_revoked', 'Acesso suspenso', 'Solicitar acesso'],
    ['access_expired', 'Acesso expirado', 'Solicitar acesso'],
    ['setup_unavailable', 'Verificação indisponível', 'Solicitar acesso'],
  ];
  for (const [status, expected, action] of cases) {
    const h = harness({ nativeBrowser: true, hunterAccess: { status, role: 'member', token: 'never-display', code: '<script>untrusted</script>' } });
    h.context.renderHunterAccessUI();
    assert.equal(h.elements.get('hunterAccessStatus').textContent, expected);
    assert.equal(h.elements.get('btnHunterAccess').textContent, action);
    assert.equal(h.elements.get('btnHunterAccess').disabled, false);
    const pageText = [...h.elements.values()].map(element => element.textContent).join(' ');
    assert.doesNotMatch(pageText, /never-display|untrusted|iCloud/);
    h.context.manageHunterAccess();
    assert.equal(h.context.window.location.href, 'hunter-access://manage');
  }
});

test('owner management and nested collection access use only whitelisted status and role', () => {
  const owner = harness({ nativeBrowser: true, hunterCollection: { access: { role: 'owner', status: 'active' } } });
  owner.context.renderHunterAccessUI();
  assert.equal(owner.elements.get('hunterAccessStatus').textContent, 'Acesso aprovado');
  assert.equal(owner.elements.get('btnHunterAccess').textContent, 'Gerenciar aparelhos');
  owner.context.manageHunterAccess();
  assert.equal(owner.context.window.location.href, 'hunter-access://manage');
  for (const status of ['<img onerror=alert(1)>', 'constructor', '__proto__']) {
    const unexpected = harness({ nativeBrowser: true, hunterAccess: { role: '<owner>', status } });
    unexpected.context.renderHunterAccessUI();
    assert.equal(unexpected.elements.get('hunterAccessStatus').textContent, 'Ativação necessária');
    assert.equal(unexpected.elements.get('btnHunterAccess').textContent, 'Solicitar acesso');
  }
  const preview = harness({ hunterCollection: { status: 'access_pending' } });
  preview.context.renderHunterAccessUI();
  assert.equal(preview.elements.get('btnHunterAccess').disabled, true);
  preview.context.manageHunterAccess();
  assert.equal(preview.context.window.location.href, 'hunter-dashboard');
  assert.equal(preview.toasts.length, 1);
});

test('collection status distinguishes approval, suspension and unavailable verification without an iCloud dependency', () => {
  const h = harness({});
  for (const status of ['activation_required', 'access_pending', 'access_revoked', 'access_expired', 'setup_unavailable']) {
    h.context.setHunterCollectionStatus({ status, reason: 'bootstrap_missing' });
    const text = h.elements.get('hunterCollectionDiagnostics').innerHTML.replace(/<[^>]*>/g, '');
    assert.doesNotMatch(text, /iCloud|coleta concluída/);
    assert.match(text, /Ajustes|aprovação|suspenso|expirado/);
  }
  assert.match(template, /id="btnHunterAccess" onclick="manageHunterAccess\(\)"/);
  assert.match(fn('openSettingsModal'), /renderHunterAccessUI\(\)/);
});

function instagramHarness() {
  const h = harness({ nativeBrowser: true, hunterAccess: { status: 'approved', role: 'member' }, hunterCollection: { sources: { portal: { status: 'found', found: 3 } } } });
  h.refreshes = 0;
  h.context.streamLiveOffers = force => { assert.equal(force, true); h.refreshes++; };
  vm.runInContext(['reconnectHunterInstagram', 'collectHunterInstagram', 'runHunterInstagramAction', 'handleNativeInstagramResult', 'showHunterInstagramToast'].map(fn).join('\n'), h.context);
  h.context.renderHunterAccessUI();
  return h;
}

test('Instagram connection and manual collection require native approval and serialize both controls', () => {
  const h = instagramHarness();
  assert.equal(h.context.window.location.href, 'hunter-dashboard');
  assert.equal(h.elements.get('btnReconnectInstagram').hidden, false);
  assert.equal(h.elements.get('btnCollectInstagram').hidden, false);
  assert.equal(h.elements.get('btnReconnectInstagram').textContent, 'Conectar Instagram neste iPhone');
  h.context.reconnectHunterInstagram();
  assert.equal(h.context.window.location.href, 'hunter-instagram://connect');
  assert.equal(h.elements.get('btnReconnectInstagram').disabled, true);
  assert.equal(h.elements.get('btnCollectInstagram').disabled, true);
  h.context.collectHunterInstagram();
  assert.equal(h.context.window.location.href, 'hunter-instagram://connect');
  h.context.renderHunterAccessUI();
  assert.equal(h.elements.get('btnCollectInstagram').disabled, true, 'rerender cannot unlock an active operation');
  h.context.handleNativeInstagramResult('connect', { status: 'cancelled' });
  assert.equal(h.elements.get('btnReconnectInstagram').disabled, false);
  assert.equal(h.elements.get('btnCollectInstagram').disabled, false);
  h.context.collectHunterInstagram();
  assert.equal(h.context.window.location.href, 'hunter-instagram://collect');
  h.context.handleNativeInstagramResult('collect', { status: 'access_revoked' });
  assert.equal(h.elements.get('btnReconnectInstagram').hidden, true);
  assert.equal(h.elements.get('btnCollectInstagram').hidden, true);
  h.context.reconnectHunterInstagram();
  assert.equal(h.context.window.location.href, 'hunter-instagram://collect');
  const preview = instagramHarness();
  preview.context.INITIAL.nativeBrowser = false; preview.context.renderHunterAccessUI();
  assert.equal(preview.elements.get('btnReconnectInstagram').hidden, true);
  assert.equal(preview.elements.get('btnCollectInstagram').hidden, true);
  preview.context.collectHunterInstagram(); assert.equal(preview.context.window.location.href, 'hunter-dashboard');
  assert.match(template, /id="btnCollectInstagram" onclick="collectHunterInstagram\(\)"/);
});

test('only a completed Instagram query refreshes the catalog and preserves Portal results', () => {
  const h = instagramHarness();
  for (const status of ['ready', 'cancelled', 'unknown', 'auth_required', 'backoff', 'rate_limited', 'daily_budget']) {
    h.context.handleNativeInstagramResult('connect', { status: 'complete', sources: { instagram: { transport: 'webview', status, found: 0 } } });
    assert.equal(h.refreshes, 0);
    assert.doesNotMatch(h.toasts.at(-1), /reconectado|Consulta ao Instagram concluída/);
    if (['unknown', 'ready', 'cancelled'].includes(status)) assert.doesNotMatch(h.toasts.at(-1), /limite|aguardando intervalo/);
    if (status === 'auth_required') assert.match(h.toasts.at(-1), /entre no Instagram neste iPhone/);
  }
  for (const status of ['found', 'empty']) {
    h.context.handleNativeInstagramResult('collect', { status: 'complete', sources: { instagram: { transport: 'webview', status, found: status === 'found' ? 1 : 0 } } });
    assert.match(h.toasts.at(-1), /Consulta ao Instagram concluída neste iPhone/);
    assert.equal(h.context.INITIAL.hunterCollection.sources.portal.found, 3);
  }
  assert.equal(h.refreshes, 2);
});

test('WebView diagnostics distinguish retry causes, show allowed retry dates, and never render raw reasons', () => {
  const h = instagramHarness();
  const nextAttemptAt = '2026-10-20T12:30:00.000Z';
  for (const [status, reason, expected] of [
    ['rate_limited', 'http_429', /HTTP 429/],
    ['daily_budget', 'daily_budget', /limite diário de consultas deste iPhone/],
    ['backoff', 'network_error', /falha de rede/],
  ]) {
    h.context.handleNativeInstagramResult('collect', { status: 'complete', sources: { instagram: { transport: 'webview', status, reason, nextAttemptAt } } });
    const text = h.elements.get('hunterCollectionDiagnostics').innerHTML;
    assert.match(text, expected); assert.match(text, /próxima tentativa em/); assert.match(text, /20\/10/);
  }
  h.context.handleNativeInstagramResult('collect', { status: 'complete', sources: { instagram: { transport: 'webview', status: 'backoff', reason: '<img src=x onerror=secret()>', nextAttemptAt: 'secret' } } });
  const text = h.elements.get('hunterCollectionDiagnostics').innerHTML;
  assert.doesNotMatch(text, /secret|onerror|próxima tentativa/);
  assert.match(text, /após falha de consulta/);
  h.context.handleNativeInstagramResult('collect', { status: 'complete', sources: { instagram: { transport: 'webview', status: 'unknown', reason: 'network_error' } } });
  assert.match(h.toasts.at(-1), /falha de rede/); assert.doesNotMatch(h.toasts.at(-1), /limite/);
});


test('Settings retains safe Instagram failure details and guides an unauthenticated manual capture', () => {
  const h = instagramHarness();
  h.context.handleNativeInstagramResult('collect', { status: 'complete', sources: { instagram: {
    transport: 'webview', status: 'unknown', reason: 'http_status', stage: 'profile', statusCode: 400
  } } });
  assert.match(h.elements.get('hunterInstagramStatus').textContent, /não aceitou.*consulta do perfil.*HTTP 400/);
  assert.match(h.toasts.at(-1), /consulta do perfil.*HTTP 400/);
  assert.equal(h.elements.get('hunterInstagramStatus').hidden, false);
  h.context.handleNativeInstagramResult('collect', { status: 'complete', sources: { instagram: {
    transport: 'webview', status: 'unknown', reason: '<img private-secret>', stage: '<img private-secret>', statusCode: 'private-secret'
  } } });
  assert.doesNotMatch(h.elements.get('hunterInstagramStatus').textContent + h.toasts.at(-1), /private-secret|<img|HTTP/);
  h.context.handleNativeInstagramResult('collect', { status: 'complete', sources: { instagram: { transport: 'webview', status: 'auth_required' } } });
  h.context.collectHunterInstagram();
  assert.equal(h.context.window.location.href, 'hunter-dashboard');
  assert.match(h.toasts.at(-1), /Conectar Instagram neste iPhone/);
  h.context.reconnectHunterInstagram();
  assert.equal(h.context.window.location.href, 'hunter-instagram://connect');
});

test('bridge, page, response and timeout failures are distinct and use warning toast icons', () => {
  const h = instagramHarness();
  const icon = { innerHTML: '<polyline/>' };
  h.context.document.getElementById('hubToast').querySelector = () => icon;
  for (const [reason, expected] of [
    ['browser_bridge_unsupported', /devolver o resultado/],
    ['browser_evaluation_failed', /executar a consulta/],
    ['browser_response_invalid', /não reconheceu/],
    ['browser_runtime_unsupported', /recurso necessário/],
    ['browser_timeout', /excedeu o tempo/],
    ['profile_identity_mismatch', /validar o perfil/],
  ]) {
    h.context.handleNativeInstagramResult('collect', { status: 'complete', sources: { instagram: { transport: 'webview', status: 'unknown', reason, stage: 'stories' } } });
    assert.match(h.elements.get('hunterInstagramStatus').textContent, expected);
    assert.match(h.toasts.at(-1), expected);
    assert.equal(icon.innerHTML, '<path d="M12 5v9M12 18v1"/>');
  }
});
