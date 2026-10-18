const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'build.js'), 'utf8');
const themeFile = '/local/uol_navigation_theme.json';
const themeCode = source.slice(source.indexOf('function loadNavigationTheme()'), source.indexOf('// Private installs seed local files once.'));
const bridgeCode = source.slice(source.indexOf('function installHunterBrowser('), source.indexOf('function nativeRedemptionKey('));

function harness(files = new Map()) {
  const reads = [], writes = [], acknowledgements = [], browserCalls = [];
  let failure = '', dismiss;
  const context = vm.createContext({
    FileManager: {
      iCloud: () => assert.fail('theme must not access iCloud'),
      local: () => {
        if (failure === 'manager') throw Error('unavailable');
        return {
          documentsDirectory: () => '/local', joinPath: path.join,
          fileExists: file => files.has(file),
          readString: file => { reads.push(file); if (failure === 'read') throw Error('unavailable'); return files.get(file); },
          writeString: (file, value) => { if (failure === 'write') throw Error('unavailable'); writes.push(file); files.set(file, value); },
        };
      },
    },
    Safari: {
      openInApp: url => { browserCalls.push(url); return new Promise(resolve => { dismiss = resolve; }); },
    },
    console: { warn() {} },
    handleNavigationThemeSaveResult: (theme, saved) => acknowledgements.push([theme, saved]),
  });
  vm.runInContext(themeCode + bridgeCode, context);
  const sensitiveTask = Promise.resolve('account-task');
  const view = { hunterActionTask: sensitiveTask, evaluateJavaScript: async code => vm.runInContext(code, context) };
  context.installHunterBrowser(view);
  return { context, view, files, reads, writes, acknowledgements, browserCalls, sensitiveTask,
    setFailure: value => { failure = value; }, dismiss: () => dismiss() };
}

test('theme roundtrip uses only its device-local file and survives a fresh context', () => {
  const files = new Map([
    ['/local/uol_hub_settings.json', '{"synthetic":"settings"}'],
    ['/cloud/uol_hub_settings.json', '{"synthetic":"icloud-settings"}'],
    ['/local/uol_redemptions_database.json', '["synthetic-redemption"]'],
  ]);
  const preserved = [...files];
  const h = harness(files);
  assert.equal(h.context.loadNavigationTheme(), 'default');
  for (const theme of ['default', 'purple', 'orange', 'white']) {
    assert.equal(h.context.saveNavigationTheme(theme), true);
    assert.deepEqual(JSON.parse(files.get(themeFile)), { theme });
    assert.equal(harness(files).context.loadNavigationTheme(), theme);
  }
  for (const [file, value] of preserved) assert.equal(files.get(file), value);
  assert.deepEqual(h.writes, Array(4).fill(themeFile));
  assert.deepEqual(h.reads, []);
});

test('absent, malformed, or unknown themes fall back without rewriting storage', () => {
  const h = harness();
  for (const stored of [undefined, '{', 'null', '[]', '{}', '"purple"', '{"theme":null}', '{"theme":"blue"}', '{"theme":"PURPLE"}']) {
    if (stored === undefined) h.files.delete(themeFile);
    else h.files.set(themeFile, stored);
    assert.equal(h.context.loadNavigationTheme(), 'default');
    assert.equal(h.files.get(themeFile), stored);
  }
  assert.deepEqual(h.writes, []);
  assert.ok(h.reads.every(file => file === themeFile));
});

test('save rejects values outside the exact enum and reports local failures', () => {
  const h = harness(new Map([[themeFile, '{"theme":"purple"}']]));
  for (const theme of ['', 'blue', 'PURPLE', 'purple ', null, undefined, {}, ['purple'], new String('purple')]) {
    assert.equal(h.context.saveNavigationTheme(theme), false);
  }
  assert.equal(h.files.get(themeFile), '{"theme":"purple"}');
  assert.deepEqual(h.reads, []);
  assert.deepEqual(h.writes, []);
  h.setFailure('write');
  assert.equal(h.context.saveNavigationTheme('white'), false);
  assert.equal(h.files.get(themeFile), '{"theme":"purple"}');
  h.setFailure('read');
  assert.equal(h.context.loadNavigationTheme(), 'default');
  h.setFailure('manager');
  assert.equal(h.context.loadNavigationTheme(), 'default');
  assert.equal(h.context.saveNavigationTheme('orange'), false);
});

test('theme bridge saves synchronously, acknowledges outcome, and preserves the active task', async () => {
  const h = harness();
  assert.equal(h.view.shouldAllowRequest({ url: 'hunter-theme://set?value=purple' }), false);
  assert.equal(h.files.get(themeFile), '{"theme":"purple"}', 'save completes before route returns');
  assert.deepEqual(h.acknowledgements, []);
  assert.equal(h.view.hunterActionTask, h.sensitiveTask);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(h.acknowledgements, [['purple', true]]);
  h.setFailure('write');
  assert.equal(h.view.shouldAllowRequest({ url: 'hunter-theme://set?value=white' }), false);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(h.acknowledgements, [['purple', true], ['white', false]]);
  assert.equal(h.files.get(themeFile), '{"theme":"purple"}');
  assert.equal(h.view.hunterActionTask, h.sensitiveTask);
});

test('theme bridge rejects malformed routes without saving and tolerates a closed WebView', async () => {
  const h = harness();
  for (const url of ['hunter-theme://set?value=blue', 'hunter-theme://set?value=PURPLE', 'hunter-theme://set?value=%70urple',
    'hunter-theme://set?value=purple&extra=1', 'hunter-theme://set?value=purple/', 'hunter-theme://set?value=purple#x',
    'hunter-theme://other?value=purple', 'HUNTER-THEME://set?value=purple', 'hunter-theme://set?value=purple\n', 'hunter-theme:']) {
    assert.equal(h.view.shouldAllowRequest({ url }), false);
  }
  assert.equal(h.view.shouldAllowRequest({ url: 'https://example.test' }), true);
  assert.equal(h.view.shouldAllowRequest({}), true);
  assert.deepEqual(h.writes, []);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(h.acknowledgements, []);
  h.view.evaluateJavaScript = async () => { throw Error('closed'); };
  assert.equal(h.view.shouldAllowRequest({ url: 'hunter-theme://set?value=orange' }), false);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(h.context.loadNavigationTheme(), 'orange');
});

test('theme saves remain available while the offer browser lock is held', async () => {
  const h = harness();
  const route = 'hunter-browser://open?url=' + encodeURIComponent('https://clube.uol.com.br/campanhasdeingresso/show');
  assert.equal(h.view.shouldAllowRequest({ url: route }), false);
  await Promise.resolve();
  assert.equal(h.browserCalls.length, 1);
  assert.equal(h.view.shouldAllowRequest({ url: 'hunter-theme://set?value=white' }), false);
  assert.equal(h.context.loadNavigationTheme(), 'white');
  assert.equal(h.view.shouldAllowRequest({ url: route }), false);
  await Promise.resolve();
  assert.equal(h.browserCalls.length, 1, 'theme saving does not release the browser lock');
  assert.equal(h.view.hunterActionTask, h.sensitiveTask);
  h.dismiss();
  await new Promise(resolve => setImmediate(resolve));
});

test('dashboard receives the stored device-local theme on each opening', () => {
  const h = harness(new Map([[themeFile, '{"theme":"orange"}']]));
  Object.assign(h.context, { BUNDLED_COOKIES_LEO: [], BUNDLED_COOKIES_GAORIEL: [], BUNDLED_REDEMPTIONS: [],
    ICONS: {}, NTFY_TOPIC: 'synthetic', SCRIPT_VERSION: '5.7.9' });
  const dashboard = source.slice(source.indexOf('function buildDashboardHTML('), source.indexOf('\nawait main();'))
    .replace('${JSON.stringify(htmlTemplate)}', JSON.stringify('__INITIAL_DATA_JSON__'));
  vm.runInContext(dashboard, h.context);
  const settings = { userSessions: { leosaquetto: [], gaoriel: [] }, navigationBarTheme: 'purple' };
  const html = h.context.buildDashboardHTML({}, settings, {}, [], []);
  assert.equal(JSON.parse(html).navigationBarTheme, 'orange');
  assert.equal(settings.navigationBarTheme, 'purple', 'device preference does not mutate shared settings');
  h.files.set(themeFile, '{"theme":"white"}');
  assert.equal(JSON.parse(h.context.buildDashboardHTML({}, settings, {}, [], [])).navigationBarTheme, 'white');
});
