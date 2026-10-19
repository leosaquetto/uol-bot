'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const adapterSource = fs.readFileSync(path.join(__dirname, 'adapters/media-app.js'), 'utf8');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const entrypoint = '\n// ── DISPARO AUTOMÁTICO SE EM AMBIENTE SCRIPTABLE ──\n';
// Synthetic fixture only: no source, keys or configuration from an installed script.
const tail = entrypoint + `if (typeof Script !== "undefined") {
  (async function () {
    try {
      await main();
    } catch (err) {
      throw err;
    }
  })();
}

// ── EXPORTAÇÃO PARA TESTES UNITÁRIOS EM NODE.JS ──
if (typeof module !== "undefined" && module.exports) {
  module.exports = {
    main: main
  };
}
`;
function fixture(body, globals = {}) {
  const context = vm.createContext({ module: { exports: {} }, ...globals });
  vm.runInContext(adapterSource, context);
  const calls = [], capture = {};
  const loader = { async load(id, exportsString, transform, overrides) {
    calls.push({ id, exportsString, transform, overrides });
    const source = 'function extractPostTarget(value) { return value; }\nasync function main() { ' + body + '\n}\n' + tail;
    const changed = transform(source);
    const keys = Object.keys(overrides);
    return new AsyncFunction(...keys, 'capture', 'Script', changed + '\nreturn ' + exportsString)(...keys.map(key => overrides[key]), capture, { complete() {}, setShortcutOutput() {} });
  } };
  return { factory: context.module.exports.create({ loader, theme: 'glass' }), calls, capture };
}
const tick = () => new Promise(resolve => setImmediate(resolve));
async function stateUntil(app, phase) {
  for (let i = 0; i < 30; i++) {
    const result = await app.handle('media.state');
    if (result.state.phase === phase) return result.state;
    await tick();
  }
  assert.fail('Expected phase ' + phase);
}
const start = app => app.handle('media.start', { url: 'https://example.test/post/123' });

test('opens URL entry without loader, clipboard or native work; themes and client script parse', async () => {
  let reads = 0;
  const f = fixture('return {};', { Pasteboard: { paste() { reads++; } } });
  const app = await f.factory.open();
  assert.equal(f.calls.length, 0);
  assert.equal(reads, 0);
  assert.match(app.html, /data-theme="glass"/);
  assert.match(app.html, /id="mediaUrl"/);
  assert.match(app.html, /SaquettoApp\.request/);
  new vm.Script(app.html.match(/<script>([\s\S]*?)<\/script>/)[1]);
  for (const url of ['javascript:alert(1)', 'https://user:secret@example.test', 'file:///tmp/file', 'https://good.test\\@bad.test', '', 'https://a.test/a\nb']) {
    assert.equal((await app.handle('media.start', { url })).reason, 'invalid_url');
  }
  assert.equal(f.calls.length, 0);
});

test('strict transform suppresses automatic execution and preserves original callable main', async () => {
  const f = fixture('capture.runs = (capture.runs || 0) + 1; capture.args = args; return {cancelled:true};');
  const app = await f.factory.open(); await start(app); await stateUntil(app, 'cancelled');
  assert.equal(f.capture.runs, 1);
  assert.equal(f.calls[0].id, 'media-downloader');
  assert.equal(f.capture.args.shortcutParameter, 'https://example.test/post/123');
  const transform = f.calls[0].transform;
  assert.throws(() => transform('async function main() {}'), /media_entrypoint_changed/);
  assert.throws(() => transform('async function main() {}' + tail + entrypoint), /media_entrypoint_changed/);
  assert.throws(() => transform('async function main() {}' + tail.replace('await main();', 'await changedMain();')), /media_entrypoint_changed/);
});

test('table selection calls original callback once and keeps untrusted labels inert', async () => {
  const f = fixture(`const table = new UITable(); const heading = new UITableRow(); heading.isHeader=true; heading.addText('<img onerror=alert(1)>', 'Subtitle'); table.addRow(heading);
    const row=new UITableRow(); row.addText('Download'); row.onSelect=()=>{capture.selected=(capture.selected||0)+1;}; table.addRow(row); await table.present(false); return {cancelled:true};`);
  const app = await f.factory.open(); await start(app); const state = await stateUntil(app, 'table');
  assert.equal(state.table.rows[0].selectable, false);
  assert.equal(state.table.rows[0].cells[0].title, '<img onerror=alert(1)>');
  assert.equal((await app.handle('media.row', { id: state.table.id, row: 0 })).reason, 'invalid_selection');
  await app.handle('media.row', { id: state.table.id, row: 1 });
  await stateUntil(app, 'cancelled');
  assert.equal(f.capture.selected, 1);
  assert.equal((await app.handle('media.row', { id: state.table.id, row: 1 })).reason, 'invalid_selection');
  assert.match(app.html, /n\.textContent=value/);
  assert.ok(!app.html.includes('innerHTML'));
});

test('cell onTap and non-dismiss rows maintain native dismissal semantics', async () => {
  const f = fixture(`const table=new UITable(); const row=new UITableRow(); row.dismissOnSelect=false; row.onSelect=()=>{capture.row=true;}; const cell=row.addButton('Save'); cell.onTap=()=>{capture.cell=true;}; table.addRow(row); await table.present(); return {cancelled:true};`);
  const app = await f.factory.open(); await start(app); const state = await stateUntil(app, 'table');
  await app.handle('media.row', { id: state.table.id, row: 0 });
  assert.equal((await app.handle('media.state')).state.phase, 'table');
  assert.equal(f.capture.row, true);
  await app.handle('media.cell', { id: state.table.id, row: 0, cell: 0 });
  assert.equal(f.capture.cell, true);
  assert.equal((await app.handle('media.state')).state.phase, 'table');
  await app.handle('media.cancel', { id: state.table.id });
  await stateUntil(app, 'cancelled');
});

test('Alert keeps secure defaults private and returns original button index and values', async () => {
  const secret = 'synthetic-private-value';
  const f = fixture(`const alert=new Alert(); alert.title='Continue'; alert.addTextField('Name','Initial'); alert.addSecureTextField('Password','${secret}'); alert.addAction('Continue'); alert.addDestructiveAction('Remove'); alert.addCancelAction('Cancel'); capture.choice=await alert.presentAlert(); capture.fields=[alert.textFieldValue(0),alert.textFieldValue(1)]; return {cancelled:true};`);
  const app = await f.factory.open(); await start(app); const state = await stateUntil(app, 'dialog');
  assert.ok(!JSON.stringify(state).includes(secret));
  assert.equal(state.dialog.fields[1].secure, true);
  assert.equal((await app.handle('media.dialog', { id: state.dialog.id, choice: 9, fields: [] })).reason, 'invalid_dialog');
  await app.handle('media.dialog', { id: state.dialog.id, choice: 1, fields: ['New name', ''] });
  await stateUntil(app, 'cancelled');
  assert.equal(f.capture.choice, 1);
  assert.deepEqual(f.capture.fields, ['New name', secret]);
});

test('table cancel resolves presentation without invoking save callbacks', async () => {
  const f = fixture(`const table=new UITable();const row=new UITableRow();row.addText('Save');row.onSelect=()=>{capture.saved=true;};table.addRow(row);await table.present();return {cancelled:true};`);
  const app = await f.factory.open(); await start(app); const state = await stateUntil(app, 'table');
  await app.handle('media.cancel', { id: state.table.id });
  await stateUntil(app, 'cancelled');
  assert.equal(f.capture.saved, undefined);
});

test('reports only verified native save calls, not original optimistic counters or paths', async () => {
  const optimistic = fixture('return {photosSaved:99,savedPaths:["/private/secret/file"]};');
  const app = await optimistic.factory.open(); await start(app); const state = await stateUntil(app, 'error');
  assert.ok(!JSON.stringify(state).includes('/private/'));
  assert.match(state.message, /Nenhum salvamento confirmado/);
  let saved = 0;
  const real = fixture('await Photos.saveImage({image:true});return {photosSaved:99};', { Photos: { async saveImage() { saved++; } } });
  const realApp = await real.factory.open(); await start(realApp); const done = await stateUntil(realApp, 'done');
  assert.equal(saved, 1);
  assert.match(done.message, /^1 foto/);
});

test('native failures are visible without leaking raw exception credentials', async () => {
  const f = fixture('throw new Error("https://private.test/?api_key=synthetic-secret");');
  const app = await f.factory.open(); await start(app); const state = await stateUntil(app, 'error');
  assert.ok(!JSON.stringify(state).includes('synthetic-secret'));
  assert.match(state.message, /Não foi possível/);
});

test('notifications require observed native evidence and use the same accurate summary', async () => {
  const notifications = [];
  class Notification { async schedule() { notifications.push(this.body); } }
  const optimistic = fixture('const n=new Notification();n.body="99 photos saved";await n.schedule();return {};', { Notification });
  const first = await optimistic.factory.open(); await start(first); await stateUntil(first, 'error');
  assert.equal(notifications.length, 0);
  const real = fixture('await ShareSheet.present(["/private/file"]);const n=new Notification();n.body="Saved video";await n.schedule();return {};', { Notification, ShareSheet: { async present() {} } });
  const second = await real.factory.open(); await start(second); const state = await stateUntil(second, 'done');
  assert.equal(notifications.length, 1);
  assert.equal(notifications[0], state.message);
  assert.equal(state.message, 'Menu de compartilhamento encerrado.');
});

test('operation start is nonblocking, duplicate runs denied, dispose cancels pending UI', async () => {
  const f = fixture('const alert=new Alert();alert.addCancelAction("Cancel");capture.choice=await alert.presentAlert();return {cancelled:true};');
  const app = await f.factory.open(); const started = await start(app);
  assert.equal(started.state.phase, 'working');
  await stateUntil(app, 'dialog');
  assert.equal((await start(app)).reason, 'busy');
  app.dispose(); await tick();
  assert.equal(f.capture.choice, -1);
  assert.equal((await app.handle('media.state')).reason, 'disposed');
});

test('dispose guards native save after an in-flight download finishes', async () => {
  let release, saves = 0;
  class NativeRequest { load() { return new Promise(resolve => { release=resolve; }); } }
  const f = fixture('const request=new Request("https://example.test/file");await request.load();await Photos.saveImage({});return {};', { Request: NativeRequest, Photos: { async saveImage() { saves++; } } });
  const app = await f.factory.open(); await start(app); await tick(); assert.equal(typeof release, 'function');
  app.dispose(); release({}); await tick(); await tick();
  assert.equal(saves, 0);
});

test('native raster bytes render but thumbnail URLs and signed parameters stay out of shell', async () => {
  const f = fixture('const table=new UITable();const row=new UITableRow();row.addImage({});row.addImageAtURL("https://example.test/image?token=private");table.addRow(row);await table.present();return {cancelled:true};', { Data: { fromPNG() { return { toBase64String() { return 'YWJj'; } }; } } });
  const app = await f.factory.open(); await start(app); const state = await stateUntil(app, 'table');
  assert.equal(state.table.rows[0].cells[0].image, 'data:image/png;base64,YWJj');
  assert.ok(!JSON.stringify(state).includes('token=private'));
  app.dispose();
});

test('installed source entrypoint transforms and compiles without executing private source', async t => {
  const file = path.join(os.homedir(), 'Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents/media-downloader.js');
  if (!fs.existsSync(file)) return t.skip('Installed source unavailable; synthetic transform covered above.');
  const f = fixture('return {cancelled:true};'); const app = await f.factory.open(); await start(app); await stateUntil(app, 'cancelled');
  const transformed = f.calls[0].transform(fs.readFileSync(file, 'utf8'));
  assert.ok(!transformed.includes(entrypoint));
  assert.doesNotThrow(() => new AsyncFunction(transformed + '\nreturn ({main,extractPostTarget});'));
});
