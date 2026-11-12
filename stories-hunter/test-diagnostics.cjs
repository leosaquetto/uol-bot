const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const host = fs.readFileSync(path.join(__dirname, 'native/host.js'), 'utf8');
const source = host.slice(host.indexOf('function createHunterDiagnostics()'), host.indexOf('\nfunction createNativeInstagramBrowser()'));

function fixture(enabled, failWrite = false) {
  const writes = new Map();
  const fm = {
    documentsDirectory: () => '/icloud',
    joinPath: (root, name) => root + '/' + name,
    fileExists: name => enabled && name === '/icloud/uol_hunter_diagnostics.enabled',
    writeString(name, value) { if (failWrite) throw Error('iCloud unavailable'); writes.set(name, value); }
  };
  const context = vm.createContext({ FileManager: { iCloud: () => fm }, UUID: { string: () => 'fixture-id' },
    SCRIPT_VERSION: '5.9.9', Date, Set, JSON });
  vm.runInContext(source, context);
  return { record: context.createHunterDiagnostics(), writes };
}

test('diagnostics are disabled without a private iCloud marker', () => {
  const { record, writes } = fixture(false);
  assert.equal(record.enabled, false);
  record('launch');
  assert.equal(writes.size, 0);
});

test('opted-in diagnostics record only bounded technical stages and no arbitrary text', () => {
  const { record, writes } = fixture(true);
  assert.equal(record.enabled, true);
  record('launch');
  record('https://example.com/private?token=never');
  for (let i = 0; i < 40; i++) record('home_loaded');
  assert.equal(writes.size, 1);
  const [name, raw] = [...writes][0];
  assert.match(name, /^\/icloud\/uol_hunter_diagnostic_/);
  assert.equal(raw.includes('token=never'), false);
  const saved = JSON.parse(raw);
  assert.equal(saved.version, '5.9.9');
  assert.equal(saved.events.length, 32);
  assert.equal(saved.events.every(event => event.stage === 'home_loaded'), true);
});

test('iCloud write failures never interrupt the app', () => {
  const { record } = fixture(true, true);
  assert.doesNotThrow(() => record('launch'));
});
