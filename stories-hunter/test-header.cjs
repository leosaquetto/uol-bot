const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const template = require('./assemble-ui.cjs').assembleUI();
const layout = template.slice(template.indexOf('// HEADER_LAYOUT_START'), template.indexOf('// HEADER_LAYOUT_END'));
const collectionFunctions = ['hunterStatusPresentation', 'hunterStatusIcon', 'setHunterCollectionStatus'].map(name => {
  const start = template.indexOf('    function ' + name + '(');
  assert.ok(start > 0, name);
  return template.slice(start, template.indexOf('\n    }', start) + '\n    }'.length);
}).join('\n');
function headerHarness({ resizeObserver = true } = {}) {
  let height = 110, resizeCallback, mutationCallback, mutationOptions;
  const values = new Map(), handlers = new Map(), classes = new Set();
  const header = { getBoundingClientRect: () => ({ height }), classList: { add: value => classes.add(value), remove: value => classes.delete(value) } };
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, { hidden: true, open: false, innerHTML: '', setAttribute(key, value) { this[key] = value; }, getAttribute(key) { return this[key] ?? null; } });
    return elements.get(id);
  };
  const status = element('hunterCollectionStatus');
  const context = vm.createContext({
    document: {
      documentElement: { style: { setProperty: (key, value) => values.set(key, value) } },
      querySelector: selector => { assert.equal(selector, '.top-header'); return header; },
      getElementById: element,
    },
    window: { scrollY: 0, addEventListener: (name, callback) => handlers.set(name, callback) },
    ...(resizeObserver ? { ResizeObserver: class { constructor(callback) { resizeCallback = callback; } observe(target) { assert.equal(target, header); } } } : {}),
    MutationObserver: class { constructor(callback) { mutationCallback = callback; } observe(target, options) { assert.equal(target, header); mutationOptions = options; } },
  });
  vm.runInContext(layout + '\n' + collectionFunctions, context);
  return { context, values, handlers, classes, status, element, mutationOptions: () => mutationOptions,
    overview: () => element('hunterCollectionOverview').innerHTML,
    diagnosis: () => element('hunterCollectionDiagnostics').innerHTML.replace(/<[^>]*>/g, ''),
    setHeight: value => { height = value; },
    resized: () => resizeCallback(), mutated: () => mutationCallback() };
}

test('header offset follows its measured border box, wrapped status and viewport changes without losing scroll fade', () => {
  const h = headerHarness();
  h.context.installHeaderLayout();
  assert.equal(h.values.get('--hunter-header-height'), '110px');
  h.setHeight(143.4); h.resized();
  assert.equal(h.values.get('--hunter-header-height'), '144px', 'wrapped status cannot overlap the content');
  h.setHeight(122); h.handlers.get('resize')();
  assert.equal(h.values.get('--hunter-header-height'), '122px');
  h.setHeight(156); h.handlers.get('orientationchange')();
  assert.equal(h.values.get('--hunter-header-height'), '156px');
  h.context.window.scrollY = 30; h.context.handleHeaderScroll();
  assert.equal(h.classes.has('header-scrolled'), true);
  h.context.window.scrollY = 0; h.context.handleHeaderScroll();
  assert.equal(h.classes.has('header-scrolled'), false);
  h.setHeight(0); h.resized();
  assert.equal(h.values.get('--hunter-header-height'), '156px', 'transient zero does not collapse the reserved space');
});

test('older WebView status and disclosure changes retain the diagnosis and update measured offsets', () => {
  const h = headerHarness({ resizeObserver: false });
  h.context.installHeaderLayout();
  assert.equal(h.mutationOptions().subtree, true);
  assert.equal(h.mutationOptions().characterData, true);
  assert.equal(h.mutationOptions().attributes, true);
  assert.deepEqual(Array.from(h.mutationOptions().attributeFilter), ['hidden', 'open']);
  h.context.setHunterCollectionStatus({ status: 'not_configured', reason: 'bootstrap_missing' });
  assert.equal(h.status.hidden, false);
  assert.equal(h.element('hunterCollectionDetails').open, true);
  assert.match(h.diagnosis(), /aguardando arquivo privado no iCloud/);
  h.setHeight(152); h.mutated();
  assert.equal(h.values.get('--hunter-header-height'), '152px');
  h.element('hunterCollectionDetails').open = false;
  h.setHeight(112); h.mutated();
  assert.equal(h.values.get('--hunter-header-height'), '112px');
  h.context.setHunterCollectionStatus({ status: 'disabled' });
  assert.equal(h.status.hidden, true);
  h.setHeight(98); h.mutated();
  assert.equal(h.values.get('--hunter-header-height'), '98px');
});

test('all tabs reserve measured header height plus a gap and the refresh SVG has explicit nonzero dimensions', () => {
  const body = template.match(/\n    body \{([^}]+)\}/)[1];
  assert.match(body, /padding-top: calc\(var\(--hunter-header-height\) \+ 12px\)/);
  assert.match(template, /window\.addEventListener\('load', \(\) => \{\s*installHeaderLayout\(\)/);
  const button = template.match(/<button[^>]*id="btnRefreshCatalog"[^>]*>([\s\S]*?)<\/button>/)[1];
  assert.match(button, /<svg[^>]*width="20"[^>]*height="20"[^>]*flex-shrink: 0/);
});

test('collection states keep distinct labels, semantics and icons independent of color', () => {
  const h = headerHarness();
  const cases = [
    ['starting', 'Buscando novidades', 'buscando novidades', 'activity', 'refresh'],
    ['updated', 'Reabra para coletar', 'atualizado; abra novamente para coletar', 'neutral', 'pause'],
    ['complete', 'Coleta concluída', 'coleta concluída', 'neutral', 'check'],
    ['not_configured', 'Configuração pendente', 'configuração privada pendente', 'attention', 'alert'],
    ['activation_required', 'Ative a coleta', 'ative a coleta em Ajustes', 'attention', 'lock'],
    ['access_pending', 'Aguardando aprovação', 'acesso aguardando aprovação', 'attention', 'clock'],
    ['access_revoked', 'Acesso suspenso', 'acesso suspenso; coleta pausada', 'attention', 'lock'],
    ['access_expired', 'Acesso expirado', 'acesso expirado; solicite aprovação', 'attention', 'lock'],
    ['setup_unavailable', 'Verifique o acesso', 'não foi possível verificar o acesso; tente novamente em Ajustes', 'attention', 'alert'],
    ['unavailable', 'Coleta indisponível', 'coleta indisponível', 'attention', 'alert'],
    ['failed', 'Coleta indisponível', 'coleta indisponível', 'attention', 'alert'],
    ['error', 'Coleta indisponível', 'coleta indisponível', 'attention', 'alert'],
    ['busy', 'Coleta em andamento', 'outra execução em andamento', 'activity', 'refresh'],
    ['auth_required', 'Autenticação necessária', 'autenticação necessária; coleta suspensa', 'attention', 'lock'],
    ['rate_limited', 'Consulta limitada', 'consulta limitada pelo serviço', 'waiting', 'pause'],
    ['backoff', 'Aguardando', 'aguardando intervalo de segurança', 'waiting', 'pause'],
    ['cooldown', 'Pausa breve', 'aguardando intervalo mínimo', 'waiting', 'clock'],
    ['daily_budget', 'Limite diário', 'limite diário atingido', 'waiting', 'pause'],
    ['unknown', 'A confirmar', 'consulta não confirmada', 'neutral', 'alert'],
    ['empty', 'Sem novidades', 'sem novidades', 'neutral', 'check'],
    ['found', '+3', '3 novidades encontradas', 'positive', 'plus'],
  ];
  for (const [status, label, detail, tone, icon] of cases) {
    const state = h.context.hunterStatusPresentation(status, 3);
    assert.equal(state.label, label, status);
    assert.ok(state.detail.startsWith(detail), status);
    assert.equal(state.tone, tone, status);
    assert.equal(state.icon, icon, status);
    h.context.setHunterCollectionStatus({ status, found: 3 });
    assert.ok(h.overview().includes(label), status);
    assert.ok(h.diagnosis().includes(detail), status);
    assert.doesNotMatch(h.overview(), /iPhone| • /, status);
    assert.equal(h.element('hunterCollectionDetails').open, tone === 'attention', status);
  }
});

test('source waiting and empty states stay compact while full source diagnoses remain available', () => {
  const h = headerHarness();
  for (const [status, short, detail] of [
    ['cooldown', 'Pausa breve', 'aguardando intervalo mínimo'],
    ['backoff', 'Aguardando', 'aguardando intervalo de segurança'],
    ['daily_budget', 'Limite diário', 'limite diário atingido'],
    ['rate_limited', 'Consulta limitada', 'consulta limitada pelo serviço'],
  ]) {
    h.context.setHunterCollectionStatus({ status: 'complete', received: 6, sources: { portal: { status: 'found', found: 0 }, instagram: { status } } });
    assert.equal(h.element('hunterCollectionDetails').open, false, status);
    assert.match(h.overview(), /<strong>6<\/strong> recebidas/);
    assert.match(h.overview(), /Portal.*class="visually-hidden">Sem novidades/);
    assert.ok(h.overview().includes(short), status);
    assert.ok(h.diagnosis().includes('Portal: 0 novidades encontradas'), status);
    assert.ok(h.diagnosis().includes('Instagram: ' + detail), status);
    assert.match(h.diagnosis(), /6 recebidas pelo servidor/);
    assert.doesNotMatch(h.diagnosis(), /entregues|coleta concluída/);
  }
  h.context.setHunterCollectionStatus({ sources: { portal: { status: 'empty' }, instagram: { status: 'found', found: 4 } } });
  assert.match(h.overview(), /Instagram.*\+4/);
  assert.match(h.diagnosis(), /Portal: sem novidades.*Instagram: 4 novidades encontradas/);
});

test('new primary or source attention expands details once and resolution returns to compact state', () => {
  const h = headerHarness();
  const first = { status: 'complete', sources: { portal: { status: 'empty' }, instagram: { status: 'auth_required' } } };
  h.context.setHunterCollectionStatus(first);
  assert.equal(h.element('hunterCollectionDetails').open, true);
  assert.match(h.diagnosis(), /Instagram: autenticação necessária; coleta suspensa/);
  h.element('hunterCollectionDetails').open = false;
  h.context.setHunterCollectionStatus(first);
  assert.equal(h.element('hunterCollectionDetails').open, false, 'same attention respects user dismissal');
  h.context.setHunterCollectionStatus({ status: 'access_pending', sources: first.sources });
  assert.equal(h.element('hunterCollectionDetails').open, true, 'new primary attention opens');
  h.element('hunterCollectionDetails').open = false;
  h.context.setHunterCollectionStatus({ status: 'access_revoked', sources: first.sources });
  assert.equal(h.element('hunterCollectionDetails').open, true, 'revocation differs from pending');
  h.context.setHunterCollectionStatus({ sources: { portal: { status: 'empty' }, instagram: { status: 'cooldown' } } });
  assert.equal(h.element('hunterCollectionDetails').open, false, 'resolved attention collapses');
  h.element('hunterCollectionDetails').open = true;
  h.context.setHunterCollectionStatus({ status: 'empty' });
  assert.equal(h.element('hunterCollectionDetails').open, true, 'normal updates preserve manually opened diagnostics');
});

test('private setup diagnoses remain specific and arbitrary status strings cannot enter markup', () => {
  const h = headerHarness();
  for (const [reason, detail] of [
    ['bootstrap_missing', 'aguardando arquivo privado no iCloud'],
    ['bootstrap_unavailable', 'arquivo privado indisponível; tentando novamente na próxima abertura'],
    ['bootstrap_invalid', 'arquivo privado inválido; configuração pendente'],
    ['device_mismatch', 'configuração privada pertence a outro aparelho'],
  ]) {
    h.context.setHunterCollectionStatus({ status: 'not_configured', reason });
    assert.ok(h.diagnosis().includes(detail), reason);
    assert.equal(h.element('hunterCollectionDetails').open, true, reason);
  }
  for (const untrusted of ['constructor', '__proto__', '<img src=x onerror=alert(1)>', 'x'.repeat(5000)]) {
    h.context.setHunterCollectionStatus({ status: untrusted, reason: untrusted, received: untrusted,
      sources: { portal: { status: untrusted, reason: untrusted, found: untrusted }, instagram: { status: 'not_configured', reason: untrusted } } });
    assert.ok(!h.overview().includes(untrusted));
    assert.ok(!h.diagnosis().includes(untrusted));
    assert.match(h.diagnosis(), /consulta não confirmada.*Portal: consulta não confirmada.*Instagram: configuração privada pendente/);
  }
});

test('counts remain distinct, unsafe counts are omitted, and disabled collection clears every status surface', () => {
  const h = headerHarness();
  h.context.setHunterCollectionStatus({ received: 2, delivered: 1, pending: 3 });
  assert.match(h.diagnosis(), /2 recebidas pelo servidor.*1 entregues.*3 pendentes no iPhone/);
  for (const value of [-1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, '7', '<script>']) {
    h.context.setHunterCollectionStatus({ received: value, delivered: value, pending: value, sources: { portal: { status: 'found', found: value } } });
    assert.doesNotMatch(h.overview(), /collection-metric/);
    assert.match(h.diagnosis(), /Portal: 0 novidades encontradas/);
  }
  for (const summary of [null, undefined, { status: 'disabled', received: 6 }]) {
    h.context.setHunterCollectionStatus({ status: 'auth_required' });
    h.context.setHunterCollectionStatus(summary);
    assert.equal(h.status.hidden, true);
    assert.equal(h.overview(), '');
    assert.equal(h.diagnosis(), '');
    assert.equal(h.element('hunterCollectionDetails').open, false);
  }
});

test('header structure preserves accessible actions, live status, glass and responsive disclosure', () => {
  const header = template.slice(template.indexOf('  <div class="top-header">'), template.indexOf('  <!-- BANNER FLUTUANTE'));
  assert.ok(header.indexOf('header-main') < header.indexOf('header-status'));
  assert.match(header, /id="catalogOfferCount"/);
  assert.match(header, /id="liveStatusText" role="status" aria-live="polite"/);
  assert.match(header, /<details[^>]*id="hunterCollectionDetails"[^>]*ontoggle="measureHeaderHeight\(\)"/);
  assert.match(header, /<summary class="collection-summary">/);
  assert.match(header, /id="btnRefreshCatalog" onclick="refreshHunterCatalog\(\)"/);
  assert.match(header, /id="btnTimelineModal" onclick="openActivityTimelineModal\(\)"/);
  assert.match(header, /id="timelineBadgeDot"/);
  assert.match(header, /<button[^>]*class="avatar-wrapper"[^>]*onclick="openSettingsModal\(\)"/);
  const glass = template.match(/\.top-header \{([^}]+)\}/)[1];
  for (const property of ['position: fixed', 'background: rgba(0, 0, 0, 0.68)', 'backdrop-filter: blur(20px) saturate(180%)', 'border-radius: 0 0 24px 24px', 'safe-area-inset-top', 'box-shadow: 0 10px 30px']) assert.ok(glass.includes(property), property);
  assert.doesNotMatch(glass, /(?:^|;)\s*(?:height|max-height):/);
  assert.match(template, /\.header-main \{[^}]*grid-template-columns: minmax\(100px, 1fr\) auto/);
  assert.match(template, /\.header-actions \.btn-circle \{[^}]*width: 44px; height: 44px/);
  assert.match(template, /\.collection-overview \{[^}]*flex-wrap: wrap/);
  assert.match(template, /\.collection-diagnostics \{[^}]*overflow-wrap: anywhere/);
  assert.match(template, /@media \(prefers-reduced-motion: reduce\) \{[^}]*\.collection-chevron[^}]*transition: none/);
  assert.match(template, /\.top-header \.status-dot,[^}]*\.top-header \.timeline-badge-dot \{ animation: none/);
});
