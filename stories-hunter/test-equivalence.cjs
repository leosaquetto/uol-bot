const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const {baseline, literal, render} = require('./test-support/artifact.cjs');
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

test('startup, failure and complete dashboard HTML match frozen release for synthetic states', () => {
  const fixtures = [ {}, {updated: true}, {user: 'gaoriel', theme: 'white'}, {theme: 'purple'}, {theme: 'orange'},
    {history: {active: [{id:'pSynthetic', title:'A < B & "C"', image:'https://example.invalid/poster.png'}], history: []},
      collection: {status:'offline',pending:1}, receipts:[{id:'fixture',date:'2026-10-18',user:'gaoriel'}]} ];
  for (const fixture of fixtures) assert.deepEqual(render(current, fixture), render(reference, fixture));
});
