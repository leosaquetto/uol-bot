const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const template = fs.readFileSync(path.join(__dirname, 'template.html'), 'utf8');
const code = template.slice(template.indexOf('// NAVIGATION_PERSONALIZATION_START'), template.indexOf('// NAVIGATION_PERSONALIZATION_END'));

function harness({ native = true, saved, cached = 'default' } = {}) {
  const elements = new Map();
  function element(id) {
    if (!elements.has(id)) elements.set(id, { children: [], attributes: {}, hidden: false,
      setAttribute(key, value) { this.attributes[key] = value; },
      getAttribute(key) { return this.attributes[key]; },
    });
    return elements.get(id);
  }
  const buttons = ['default', 'purple', 'orange', 'white'].map(theme => {
    const button = element(theme); button.setAttribute('data-theme-option', theme); return button;
  });
  const writes = [], settings = { navigationBarTheme: cached, currentUser: 'gaoriel', priorizarWords: ['museu'] };
  const context = vm.createContext({
    INITIAL: { nativeBrowser: native, navigationBarTheme: saved }, settings,
    ICONS: Object.fromEntries(['navHome', 'navTickets', 'navHistory', 'navRedemptions'].map(key => [key, '<svg></svg>'])),
    document: { getElementById: element, querySelectorAll: () => buttons },
    window: { location: { href: 'dashboard' } },
    localStorage: { setItem: (key, value) => writes.push([key, value]) },
    saveSettingsToStorage: () => writes.push(['settings', JSON.stringify(settings)]),
  });
  vm.runInContext(code, context);
  context.initializeNavigationTheme();
  return { context, settings, elements, element, writes };
}

test('first install preserves default and native device choice supersedes stale page cache', () => {
  const fresh = harness({ cached: 'purple' });
  assert.equal(fresh.element('bottomNav').attributes['data-navigation-theme'], 'default');
  const h = harness({ saved: 'white', cached: 'orange' });
  assert.equal(h.settings.navigationBarTheme, 'white');
  assert.equal(h.element('white').attributes['aria-pressed'], 'true');
  assert.equal(h.settings.currentUser, 'gaoriel');
  assert.deepEqual(h.settings.priorizarWords, ['museu']);
  assert.equal(h.writes.length, 0);
});

test('choosing a theme applies immediately, waits for durable receipt, and restores the last saved value on failure', () => {
  const h = harness({ saved: 'default' });
  h.context.chooseNavigationTheme('purple');
  assert.equal(h.element('bottomNav').attributes['data-navigation-theme'], 'purple');
  assert.equal(h.context.window.location.href, 'hunter-theme://set?value=purple');
  assert.equal(h.writes.length, 0);
  h.context.handleNavigationThemeSaveResult('purple', true);
  assert.equal(h.writes.length, 1);
  h.context.chooseNavigationTheme('orange');
  h.context.handleNavigationThemeSaveResult('orange', false);
  assert.equal(h.settings.navigationBarTheme, 'purple');
  assert.match(h.element('navigationThemeStatus').textContent, /anterior foi mantida/);
  h.context.chooseNavigationTheme('default');
  h.context.handleNavigationThemeSaveResult('default', true);
  assert.equal(h.settings.navigationBarTheme, 'default');
});

test('rapid repeated choices cannot overlap native saves or consume an unrelated acknowledgement', () => {
  const h = harness({ saved: 'default' });
  h.context.chooseNavigationTheme('purple');
  assert.equal(h.element('white').disabled, true);
  h.context.chooseNavigationTheme('orange');
  h.context.chooseNavigationTheme('purple');
  assert.equal(h.settings.navigationBarTheme, 'purple');
  h.context.handleNavigationThemeSaveResult('orange', true);
  assert.equal(h.element('white').disabled, true);
  h.context.handleNavigationThemeSaveResult('purple', false);
  assert.equal(h.settings.navigationBarTheme, 'default');
  assert.equal(h.element('white').disabled, false);
  h.context.chooseNavigationTheme('purple');
  h.context.handleNavigationThemeSaveResult('purple', true);
  assert.equal(h.settings.navigationBarTheme, 'purple');
});

test('personalization tab exposes all four real-icon previews without affecting general settings', () => {
  const h = harness({ saved: 'orange' });
  h.context.selectSettingsTab('personalization');
  assert.equal(h.element('settingsGeneralPanel').hidden, true);
  assert.equal(h.element('settingsPersonalizationPanel').hidden, false);
  assert.equal(h.element('settingsPersonalizationTab').attributes['aria-selected'], 'true');
  const html = h.element('navigationThemeOptions').innerHTML;
  assert.equal((html.match(/data-theme-option=/g) || []).length, 4);
  assert.equal((html.match(/<svg>/g) || []).length, 16);
  h.context.selectSettingsTab('general');
  assert.equal(h.element('settingsGeneralPanel').hidden, false);
  assert.equal(h.element('settingsPersonalizationPanel').hidden, true);
  assert.equal(h.settings.navigationBarTheme, 'orange');
});

test('browser preview persists the choice, rejects unknown themes and reports storage failure', () => {
  const h = harness({ native: false, cached: 'white' });
  h.context.chooseNavigationTheme('orange');
  assert.equal(JSON.parse(h.writes[0][1]).navigationBarTheme, 'orange');
  h.context.chooseNavigationTheme('invalid');
  assert.equal(h.settings.navigationBarTheme, 'orange');
  assert.equal(h.writes.length, 1);
  h.context.localStorage.setItem = () => { throw Error('storage unavailable'); };
  h.context.chooseNavigationTheme('purple');
  assert.equal(h.settings.navigationBarTheme, 'orange');
  assert.equal(h.context.window.location.href, 'dashboard');
});

test('registered default CSS remains byte-for-byte intact', () => {
  const baseline = fs.readFileSync(path.join(__dirname, 'NAVIGATION_BAR_BASELINE.md'), 'utf8').match(/```css\n([\s\S]*?)\n```/)[1];
  const start = template.indexOf('    .bottom-nav {');
  const actual = template.slice(start, template.indexOf('    /* The default bar above', start)).trim();
  assert.equal(actual, baseline);
});
