'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const fingerprint = source => crypto.createHash('sha256').update(source).digest('hex');
const tick = () => new Promise(resolve => setImmediate(resolve));

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

function fixture({ id = 'clube-uol', name = 'Clube UOL Premium', source, changedHash = false } = {}) {
  const events = [], captured = {}, visible = deferred();
  let parentCompletions = 0;
  class WebView {
    async loadHTML(html, baseURL) { events.push(['html', html, baseURL]); }
    present(fullScreen) { events.push(['webview', fullScreen]); return visible.promise; }
  }
  class UITable {
    present() { events.push(['table']); return visible.promise; }
  }
  const fm = {
    documentsDirectory: () => '/fixture/scripts',
    listContents: () => [name + '.js'],
    joinPath: (a, b) => a + '/' + b,
    readString: () => source
  };
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, 'native/runtime.js'), 'utf8'), { module }, { filename: 'saquettoos/native/runtime.js' });
  const forbidden = () => { throw new Error('Native sessions must not use the iframe adapter'); };
  const runtime = module.exports.create({
    fm,
    local: new Proxy({}, { get: forbidden }),
    catalog: [{ id, name, scriptName: name, mode: 'native' }],
    compatibility: { [id]: { sha256: [changedHash ? 'unreviewed' : fingerprint(source)] } },
    hash: fingerprint,
    script: { complete() { parentCompletions++; } },
    globals: { WebView, UITable, events, captured, expectedWebView: WebView, expectedTable: UITable },
    shell: forbidden, uuid: forbidden, timer: forbidden, request: forbidden, openURL: forbidden,
    nativeWebView: WebView
  });
  return { runtime, events, captured, visible, get parentCompletions() { return parentCompletions; } };
}

test('native WebView keeps the original HTML and awaits dismissal before returning to the central', async () => {
  const f = fixture({ source: `
    captured.originalWebView = WebView === expectedWebView;
    const view = new WebView();
    await view.loadHTML('<main>Original app</main>', 'https://example.test/app/');
    await view.present(true);
    events.push(['after-dismiss']);
  ` });
  let returned = false;
  const opening = f.runtime.open('clube-uol').then(result => { returned = true; return result; });
  await tick();
  assert.equal(f.captured.originalWebView, true);
  assert.equal(f.runtime.nativeActive, true);
  assert.equal(f.runtime.activeId, 'clube-uol');
  assert.equal(returned, false);
  assert.deepEqual(f.events, [['html', '<main>Original app</main>', 'https://example.test/app/'], ['webview', true]]);
  f.visible.resolve();
  const result = await opening;
  assert.equal(result.status, 'returned');
  assert.equal(result.mode, 'native');
  assert.equal(result.appId, 'clube-uol');
  assert.equal(f.runtime.nativeActive, false);
  assert.equal(f.runtime.activeId, null);
  assert.equal(f.events.at(-1)[0], 'after-dismiss');
});

test('native app receives its own Script, module and URLScheme identity without completing the parent', async () => {
  const name = 'Bilhete U\u0301nico';
  const f = fixture({ id: 'bilhete-unico', name, source: `
    captured.name = Script.name();
    captured.filename = module.filename;
    captured.run = URLScheme.forRunningScript();
    captured.open = URLScheme.forOpeningScript();
    captured.settings = URLScheme.forOpeningScriptSettings();
    captured.parameters = URLScheme.allParameters();
    captured.missing = URLScheme.parameter('private');
    Script.complete();
    events.push(['still-running']);
    await new WebView().present();
  ` });
  const opening = f.runtime.open('bilhete-unico');
  await tick();
  assert.equal(f.parentCompletions, 0);
  assert.equal(f.runtime.nativeActive, true);
  assert.equal(f.captured.name, name);
  assert.equal(f.captured.filename, '/fixture/scripts/' + name + '.js');
  assert.equal(f.captured.run, 'scriptable:///run?scriptName=' + encodeURIComponent(name));
  assert.equal(f.captured.open, 'scriptable:///open?scriptName=' + encodeURIComponent(name));
  assert.equal(f.captured.settings, 'scriptable:///open?scriptName=' + encodeURIComponent(name) + '&openSettings=true');
  assert.equal(Object.keys(f.captured.parameters).length, 0);
  assert.equal(f.captured.missing, undefined);
  assert.equal(f.events[0][0], 'still-running');
  f.visible.resolve();
  await opening;
  assert.equal(f.parentCompletions, 0);
});

test('concurrent opens are denied and shutdown waits for the visible native session', async () => {
  const f = fixture({ source: 'await new WebView().present();' });
  const opening = f.runtime.open('clube-uol');
  await tick();
  await assert.rejects(f.runtime.open('clube-uol'), /app_busy/);
  await assert.rejects(f.runtime.close(), /app_busy/);
  let drained = false, stopped = false;
  const draining = f.runtime.waitForNative().then(() => { drained = true; });
  const stopping = f.runtime.shutdown().then(() => { stopped = true; });
  await tick();
  assert.equal(drained, false);
  assert.equal(stopped, false);
  assert.equal(f.runtime.nativeActive, true);
  await assert.rejects(f.runtime.open('clube-uol'), /app_closed/);
  f.visible.resolve();
  await Promise.all([opening, draining, stopping]);
  assert.equal(drained, true);
  assert.equal(stopped, true);
  assert.equal(f.runtime.activeId, null);
});

test('unreviewed native source never executes and releases the session', async () => {
  const f = fixture({ source: "events.push(['executed']); await new WebView().present();", changedHash: true });
  await assert.rejects(f.runtime.open('clube-uol'), /adapter_source_changed/);
  assert.equal(f.events.length, 0);
  assert.equal(f.runtime.activeId, null);
  assert.equal(f.runtime.nativeActive, false);
  await f.runtime.shutdown();
});

test('calendar keeps native UITable and awaits its previously unawaited presentation', async () => {
  const f = fixture({ id: 'super-calendario', name: 'Super Calendário', source: `
    async function main() {
      captured.originalTable = UITable === expectedTable;
      const t = new UITable();
  t.present();
      events.push(['calendar-finished']);
    }
    await main();
  ` });
  let returned = false;
  const opening = f.runtime.open('super-calendario').then(result => { returned = true; return result; });
  await tick();
  assert.equal(f.captured.originalTable, true);
  assert.deepEqual(f.events, [['table']]);
  assert.equal(returned, false);
  f.visible.resolve();
  assert.equal((await opening).status, 'returned');
  assert.equal(f.events.at(-1)[0], 'calendar-finished');
});

test('media downloader awaits its async entrypoint through native UI dismissal', async () => {
  const f = fixture({ id: 'media-downloader', name: 'media-downloader', source: `
    (async function () {
      await new WebView().present();
      events.push(['media-finished']);
    })();
  ` });
  let returned = false;
  const opening = f.runtime.open('media-downloader').then(result => { returned = true; return result; });
  await tick();
  assert.equal(returned, false);
  assert.equal(f.runtime.nativeActive, true);
  assert.deepEqual(f.events, [['webview', undefined]]);
  f.visible.resolve();
  assert.equal((await opening).status, 'returned');
  assert.equal(f.events.at(-1)[0], 'media-finished');
});

test('missing or ambiguous calendar and media entrypoints fail before execution', async () => {
  for (const [id, marker] of [['super-calendario', '  t.present();'], ['media-downloader', '(async function () {']]) {
    for (const repetitions of [0, 2]) {
      const source = "events.push(['executed']);\n" + (marker + '\n').repeat(repetitions);
      const f = fixture({ id, source });
      await assert.rejects(f.runtime.open(id), /adapter_entrypoint_changed/);
      assert.equal(f.events.length, 0);
      assert.equal(f.runtime.activeId, null);
    }
  }
});
