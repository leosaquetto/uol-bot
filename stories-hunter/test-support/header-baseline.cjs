const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const { baseline, literal } = require('./artifact.cjs');
const manifest = require('../baseline/manifest.json');

function frozenTemplate() {
  const source = baseline();
  const icons = literal(source, 'const ICONS = ');
  let template = literal(source.slice(source.indexOf('function buildDashboardHTML(')), 'const rawHtml = ');
  const logo = template.match(/<img src="data:image\/png;base64,([^"]+)" alt="Clube UOL" class="brand-logo-img"/)[1];
  template = template.replaceAll(logo, '__LOGO_BASE64__');
  for (const name of ['bell', 'globe', 'link', 'navHistory', 'navHome', 'navRedemptions', 'navTickets', 'notes', 'settings', 'ticketCheck', 'ticketText', 'tickets', 'undo']) {
    template = template.replaceAll(icons[name], '__ICON_' + name + '__');
  }
  const expected = manifest.files['stories-hunter/template.html'];
  assert.equal(Buffer.byteLength(template), expected.bytes);
  assert.equal(createHash('sha256').update(template).digest('hex'), expected.sha256);
  return template;
}

function restoreProductLabels(template) {
  return template
    .replaceAll('Carregando o Clube UOL Premium', 'Carregando o Stories Hunter')
    .replaceAll('Abra o Clube UOL Premium no Scriptable para atualizar.', 'Abra o Stories Hunter no Scriptable para atualizar.')
    .replaceAll('content="Clube UOL Premium"', 'content="Clube UOL"')
    .replaceAll('<title>Clube UOL Premium</title>', '<title>Clube UOL • Hub de Ingressos</title>')
    .replaceAll('Conectar conta ao Clube UOL Premium', 'Conectar conta ao Hunter')
    .replaceAll('O Clube UOL Premium só salva a sessão', 'O Hunter só salva a sessão')
    .replaceAll('Abra o Clube UOL Premium no Scriptable para gerenciar o acesso.', 'Abra o Hunter no Scriptable para gerenciar o acesso.');
}

function region(source, start, end) {
  const startIndex = source.indexOf(start);
  const endIndex = source.indexOf(end, startIndex);
  assert.ok(startIndex >= 0 && endIndex > startIndex, start);
  assert.equal(source.indexOf(start, startIndex + start.length), -1, 'unique region: ' + start);
  return source.slice(startIndex, endIndex);
}

function restoreHeaderPresentation(current, reference) {
  let output = current;
  const replaceRegion = (start, end, referenceStart = start) => {
    output = output.replace(region(output, start, end), () => region(reference, referenceStart, end));
  };
  replaceRegion('    /* TOP HEADER FIXO', '    .thumb-code-badge {');
  replaceRegion('  <div class="top-header">', '\n  <!-- BANNER FLUTUANTE');
  output = output.replace(/\n        <button type="button" id="catalogStatusTrigger"[^\n]+<\/button>/, '');
  replaceRegion('    function hunterStatusPresentation(', '\n    // UI_DATA_HELPERS_END', '    function setHunterCollectionStatus(');
  output = output.replace(region(output, '    function setCatalogHeaderStatus(', '    function renderCatalogStatus('), '');
  output = output.replace(/--hunter-header-height: [^;]+;/, reference.match(/--hunter-header-height: [^;]+;/)[0]);
  output = output.replace("attributeFilter: ['hidden', 'open']", "attributeFilter: ['hidden']");
  output = output.replace("setCatalogHeaderStatus('Atualizando…', 'loading', visible.activeItems.length + visible.soldOutItems.length);", "document.getElementById('liveStatusText').textContent = 'Atualizando novidades ao vivo...';");
  output = output.replace("setCatalogHeaderStatus(fresh ? 'Catálogo atualizado' : 'Catálogo em cache', fresh ? 'fresh' : 'cache', visibleCount);", "document.getElementById('liveStatusText').textContent = (fresh ? 'Catálogo atualizado • ' : 'Catálogo em cache • ') + visibleCount + ' ofertas';\n      document.getElementById('liveDot').style.backgroundColor = fresh ? '#00e676' : '#ffb300';");
  output = output.replace("setCatalogHeaderStatus('Atualizando…', 'loading');", "if (status) { status.textContent = 'Atualizando ofertas…'; status.setAttribute('aria-busy', 'true'); }");
  output = output.replace("const button = document.getElementById('btnRefreshCatalog');\n      if (button)", "const button = document.getElementById('btnRefreshCatalog');\n      const status = document.getElementById('liveStatusText');\n      if (button)");
  for (const tag of ['style', 'script']) {
    const boundary = new RegExp('\\s*(?=</' + tag + '>)');
    output = output.replace(boundary, reference.match(boundary)[0]);
  }
  return output;
}

// Explicit 5.8.6 additions. Recovery behavior has dedicated access/collector/UI
// tests; retain the frozen comparison for every unrelated part of the screen.
function restoreInstagramPresentation(current, reference) {
  let output = current;
  const replaceOnce = (before, after) => {
    assert.equal(output.split(before).length, 2, 'unique Instagram presentation delta');
    output = output.replace(before, () => after);
  };
  replaceOnce('    .source-mini.overlap {', '    .source-mini.mini-insta {\n      border-radius: 2.3px;\n    }\n    .source-mini.overlap {');
  replaceOnce('    .source-mini.mini-insta,\n    .source-mini-circle.mini-insta {', '    .source-mini-circle.mini-insta {');
  replaceOnce('    }\n    .source-mini-circle.mini-insta {\n      box-shadow:', '      box-shadow:');
  replaceOnce('    .source-mini.mini-insta svg,\n', '');
  replaceOnce('    .source-mini.mini-insta svg {\n      width: 61.111111%;\n      height: 61.111111%;\n    }\n', '');
  replaceOnce('          <button class="btn-modal-secondary" id="btnReconnectInstagram" onclick="reconnectHunterInstagram()" hidden>Reconectar Instagram</button>\n', '');
  replaceOnce("      const instagram = document.getElementById('btnReconnectInstagram');\n      if (instagram) {\n        instagram.hidden = access.state !== 'approved';\n        instagram.disabled = !INITIAL.nativeBrowser;\n      }\n", '');
  assert.ok(!reference.includes('    function reconnectHunterInstagram()'));
  output = output.replace(region(output, '    function reconnectHunterInstagram()', '    // NAVIGATION_PERSONALIZATION_START'), '');
  return output;
}

module.exports = { frozenTemplate, restoreProductLabels, restoreHeaderPresentation, restoreInstagramPresentation };
