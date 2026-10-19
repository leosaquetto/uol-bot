#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const root = __dirname;
const read = name => fs.readFileSync(path.join(root, name), 'utf8');
const dataURL = name => 'data:image/png;base64,' + fs.readFileSync(path.join(root, 'assets', name)).toString('base64');
const safe = value => JSON.stringify(value).replace(/</g, '\\u003c');
const catalog = JSON.parse(read('catalog.json'));
const assets = { logoLight: dataURL('saquettoos-white.png'), logoDark: dataURL('saquettoos-violet.png'), uolIcon: dataURL('clube-uol.png') };
const html = read('ui/index.html').replace('__SAQUETTO_STYLES__', () => read('ui/styles.css'))
  .replace('__SAQUETTO_APP__', () => read('ui/app.js'));
if (!html.includes('__SAQUETTO_INITIAL_JSON__')) throw new Error('Missing initial state marker');
const code = read('native/host.js').replace('__SAQUETTO_CATALOG__', () => safe(catalog))
  .replace('__SAQUETTO_HTML__', () => safe(html))
  .replace('__SAQUETTO_ASSETS__', () => safe(assets))
  .replace('__SAQUETTO_UPDATER__', () => read('native/updater.js'))
  .replace('__SAQUETTO_COMPATIBILITY__', () => safe(JSON.parse(read('compatibility.json'))))
  .replace('__SAQUETTO_ADAPTERS__', () => ['native-apps', 'media-app', 'webview-apps'].map(name => read('adapters/' + name + '.js')).join('\n'))
  .replace('__SAQUETTO_RUNTIME__', () => read('native/runtime.js'));
new Function('return async function(){\n' + code + '\n}');
new Function(read('ui/app.js'));
if (code.includes('__SAQUETTO_UPDATER__') || code.includes('__SAQUETTO_CATALOG__')) throw new Error('Incomplete assembly');
fs.mkdirSync(path.join(root, 'dist'), { recursive: true });
const file = path.join(root, 'dist/saquettoOS.js');
fs.writeFileSync(file, code);
if (process.argv.includes('--preview')) {
  const demo = { version: '1.0.2', profile: { name: 'Leo', role: 'admin', status: 'approved', appIds: catalog.map(app => app.id) },
    apps: catalog.map(app => ({ ...app, installed: true, allowed: true })), assets,
    preferences: { appearance: 'system', material: 'glass', channel: 'stable', favorites: catalog.slice(0, 4).map(app => app.id), notifications: { enabled: false, updates: true, access: true } },
    notifications: [], accessConfigured: true, update: { items: [] } };
  const preview = html.replace('__SAQUETTO_INITIAL_JSON__', () => safe(demo))
    .replace('<head>', '<head><script>window.__SAQUETTO_PREVIEW__=true;</script>');
  fs.mkdirSync(path.join(root, 'preview'), { recursive: true });
  fs.writeFileSync(path.join(root, 'preview/index.html'), preview);
}
console.log(JSON.stringify({ file: 'saquettoos/dist/saquettoOS.js', bytes: Buffer.byteLength(code), sha256: crypto.createHash('sha256').update(code).digest('hex') }));
