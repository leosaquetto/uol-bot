'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const { assembleUI, assembleUIFromParts } = require('./assemble-ui.cjs');
const baseline = require('./baseline/manifest.json');

test('mechanical UI extraction reconstructs the reference template byte for byte', () => {
  const output = Buffer.from(assembleUI(), 'utf8');
  const reference = baseline.files['stories-hunter/template.html'];
  assert.equal(output.byteLength, reference.bytes);
  assert.equal(createHash('sha256').update(output).digest('hex'), reference.sha256);
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
