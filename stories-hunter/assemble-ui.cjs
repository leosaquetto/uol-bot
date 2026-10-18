'use strict';

const fs = require('node:fs');
const path = require('node:path');

const STYLES_MARKER = '__HUNTER_UI_STYLES__';
const SCRIPT_MARKER = '__HUNTER_UI_SCRIPT__';

// Preserve whitespace, script scope and CSS order from the original template.
function assembleUIFromParts({ shell, styles, app }) {
  for (const marker of [STYLES_MARKER, SCRIPT_MARKER]) {
    if (shell.split(marker).length !== 2) {
      throw new Error(`Expected exactly one UI marker: ${marker}`);
    }
  }
  return shell
    .replace(STYLES_MARKER, () => styles)
    .replace(SCRIPT_MARKER, () => app);
}

function assembleUI() {
  const read = (name) => fs.readFileSync(path.join(__dirname, 'ui', name), 'utf8');
  return assembleUIFromParts({
    shell: read('shell.html'),
    styles: read('styles.css'),
    app: read('app.js'),
  });
}

module.exports = { assembleUI, assembleUIFromParts };
