'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { assembleUI, assembleUIFromParts } = require('./assemble-ui.cjs');
const { frozenTemplate } = require('./test-support/header-baseline.cjs');

test('UI assembly inserts exact CSS and JavaScript sources once into the shell', () => {
  const read = name => fs.readFileSync(path.join(__dirname, 'ui', name), 'utf8');
  const shell = read('shell.html');
  const styles = read('styles.css');
  const app = read('app.js');
  const assembled = assembleUI();
  assert.equal(assembled, shell.replace('__HUNTER_UI_STYLES__', () => styles)
    .replace('__HUNTER_UI_SCRIPT__', () => app));
  assert.doesNotMatch(assembled, /__HUNTER_UI_(?:STYLES|SCRIPT)__/);
  assert.equal(assembled.split(styles).length, 2, 'CSS must appear exactly once');
  assert.equal(assembled.split(app).length, 2, 'JavaScript must appear exactly once');
});

test('all established navigation and content mounts survive UI redesign', () => {
  const original = frozenTemplate();
  const current = assembleUI();
  for (const id of ['viewMain', 'viewHistory', 'viewRedemptions', 'settingsModal', 'bottomNav', 'cardList']) {
    const marker = 'id="' + id + '"';
    assert.ok(original.includes(marker), 'baseline mount: ' + id);
    assert.equal(current.split(marker).length, 2, 'unique current mount: ' + id);
  }
  for (const tab of ['home', 'tickets', 'history', 'redemptions'])
    assert.equal(current.split('data-tab="' + tab + '"').length, 2, 'unique navigation tab: ' + tab);
});

test('UI assembly preserves literal replacement sequences and whitespace', () => {
  const shell = '<style>__HUNTER_UI_STYLES__</style>\n<script>__HUNTER_UI_SCRIPT__</script>\n';
  const styles = '\n  /* $& $$ $` */\n  ';
  const app = '\n  const text = "$& $$";\n  ';
  assert.equal(assembleUIFromParts({ shell, styles, app }), `<style>${styles}</style>\n<script>${app}</script>\n`);
});

test('UI assembly rejects missing or duplicated source markers', () => {
  assert.throws(() => assembleUIFromParts({ shell: '', styles: '', app: '' }), /exactly one UI marker/);
  assert.throws(() => assembleUIFromParts({
    shell: '__HUNTER_UI_STYLES____HUNTER_UI_STYLES____HUNTER_UI_SCRIPT__',
    styles: '',
    app: '',
  }), /exactly one UI marker/);
});
