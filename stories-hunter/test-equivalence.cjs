const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const {baseline, literal, render} = require('./test-support/artifact.cjs');
const {restoreHeaderPresentation} = require('./test-support/header-baseline.cjs');
const root = path.join(__dirname, '..');
const manifest = require('./baseline/manifest.json');
const reference = baseline();
const current = fs.readFileSync(path.join(__dirname, 'dist/Hunter.js'), 'utf8');
const sha = data => crypto.createHash('sha256').update(data).digest('hex');

test('frozen public release has exact original size and hash', () => {
  const expected = manifest.files['scriptable_uol_stories_hunter.js'];
  assert.equal(Buffer.byteLength(reference), expected.bytes);
  assert.equal(sha(reference), expected.sha256);
  for (const name of ['BUNDLED_COOKIES_LEO', 'BUNDLED_COOKIES_GAORIEL', 'BUNDLED_REDEMPTIONS'])
    assert.ok(reference.includes('const ' + name + ' = [];'));
});

test('native domain modules and receipt parser remain byte-identical', () => {
  for (const name of ['collector', 'access', 'redemption']) {
    const file = 'stories-hunter/' + name + '.js';
    assert.equal(sha(fs.readFileSync(path.join(root, file))), manifest.files[file].sha256);
  }
  assert.equal(literal(current, 'const UOL_RECEIPT_PARSER = '), literal(reference, 'const UOL_RECEIPT_PARSER = '));
});

test('startup, failure and dashboard outside the header presentation match frozen release for synthetic states', () => {
  const fixtures = [ {}, {updated: true}, {user: 'gaoriel', theme: 'white'}, {theme: 'purple'}, {theme: 'orange'},
    {history: {active: [{id:'pSynthetic', title:'A < B & "C"', image:'https://example.invalid/poster.png'}], history: []},
      collection: {status:'offline',pending:1}, receipts:[{id:'fixture',date:'2026-10-18',user:'gaoriel'}]} ];
  for (const fixture of fixtures) {
    const expected = render(reference, fixture);
    for (const key of Object.keys(expected)) expected[key] = expected[key].replaceAll('Carregando o Stories Hunter', 'Carregando o Clube UOL Premium').replaceAll('Abra o Stories Hunter no Scriptable para atualizar.', 'Abra o Clube UOL Premium no Scriptable para atualizar.').replaceAll("content=\"Clube UOL\"", "content=\"Clube UOL Premium\"").replaceAll("<title>Clube UOL • Hub de Ingressos</title>", "<title>Clube UOL Premium</title>").replaceAll("<title>Clube UOL</title>", "<title>Clube UOL Premium</title>").replaceAll("Conectar conta ao Hunter", "Conectar conta ao Clube UOL Premium").replaceAll("O Hunter só salva a sessão", "O Clube UOL Premium só salva a sessão").replaceAll("Abra o Hunter no Scriptable para gerenciar o acesso.", "Abra o Clube UOL Premium no Scriptable para gerenciar o acesso.");
    const actual = render(current, fixture);
    actual.dashboard = restoreHeaderPresentation(actual.dashboard, expected.dashboard);
    assert.deepEqual(actual, expected);
  }
});
