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
    INITIAL: initial, window: { location: { href: 'hunter-dashboard' } },
    document: { getElementById: id => { if (!elements.has(id)) elements.set(id, { textContent: '' }); return elements.get(id); } },
    showToast: text => toasts.push(text),
  });
  vm.runInContext(['hunterAccessState', 'renderHunterAccessUI', 'manageHunterAccess', 'setHunterCollectionStatus'].map(fn).join('\n'), context);
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
    const text = h.elements.get('hunterCollectionStatus').textContent;
    assert.doesNotMatch(text, /iCloud|coleta concluída/);
    assert.match(text, /Ajustes|aprovação|suspenso|expirado/);
  }
  assert.match(template, /id="btnHunterAccess" onclick="manageHunterAccess\(\)"/);
  assert.match(fn('openSettingsModal'), /renderHunterAccessUI\(\)/);
});
