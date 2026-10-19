const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const vm = require('node:vm');

function literal(source, marker) {
  const offset = source.indexOf(marker);
  if (offset < 0) throw new Error('Missing artifact marker: ' + marker);
  const start = offset + marker.length;
  let i = start, quoted = false, escaped = false, depth = 0;
  for (; i < source.length; i++) {
    const c = source[i];
    if (quoted) {
      if (escaped) escaped = false;
      else if (c === '\\') escaped = true;
      else if (c === '"') { quoted = false; if (!depth) { i++; break; } }
    } else if (c === '"') quoted = true;
    else if (c === '{' || c === '[') depth++;
    else if (c === '}' || c === ']') { if (!--depth) { i++; break; } }
  }
  return JSON.parse(source.slice(start, i));
}

function baseline() {
  return zlib.gunzipSync(fs.readFileSync(path.join(__dirname, '../baseline/hunter-5.7.9.js.gz'))).toString('utf8');
}

function render(source, fixture = {}) {
  const context = vm.createContext({
    STARTUP_HTML: literal(source, 'const STARTUP_HTML = '),
    ICONS: literal(source, 'const ICONS = '),
    BUNDLED_COOKIES_LEO: [], BUNDLED_COOKIES_GAORIEL: [], BUNDLED_REDEMPTIONS: [],
    SCRIPT_VERSION: 'comparison-version', NTFY_TOPIC: 'fixture-topic',
    loadNavigationTheme: () => fixture.theme || 'default',
    Script: { name: () => 'Fixture Hunter' }
  });
  if (source.includes('const HUNTER_LOGO_BASE64 = ')) {
    context.HUNTER_LOGO_BASE64 = literal(source, 'const HUNTER_LOGO_BASE64 = ');
    const start = source.indexOf('function resolveHunterAssets(');
    vm.runInContext(source.slice(start, source.indexOf('\n}', start) + 2), context);
  }
  const startup = source.indexOf('function buildHunterStartupHTML(');
  const dashboard = source.indexOf('function buildDashboardHTML(');
  const publicProjection = source.indexOf('function publicHunterCredentials(');
  const dashboardStart = publicProjection >= 0 && publicProjection < dashboard ? publicProjection : dashboard;
  vm.runInContext(source.slice(startup, source.indexOf('\nfunction installHunterBrowser(', startup)) + '\n' +
    source.slice(dashboardStart, source.indexOf('\nawait main();', dashboard)), context);
  return {
    startup: context.buildHunterStartupHTML(false),
    failure: context.buildHunterStartupHTML(true),
    dashboard: context.buildDashboardHTML(fixture.history || {active: [], history: []},
      {currentUser: fixture.user || 'leosaquetto', userSessions: {}, ...(fixture.settings || {})},
      fixture.credentials || {}, fixture.cookies || [], fixture.receipts || [], !!fixture.updated, fixture.collection || null)
  };
}

module.exports = { baseline, literal, render };
