import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { importAccountFile, parseAccountSource, parseUolCredentials } from '../scripts/import-account.mjs';

const credentials = 'Gabriel\nLogin: gab-example\nSenha: GAB-FAKE\n\nLeo\nLogin: leo@example.invalid\nSenha: LEO-FAKE:with=punctuation\n';
const script = fileURLToPath(new URL('../scripts/import-account.mjs', import.meta.url));
const fixture = (callback) => {
  const dir = mkdtempSync(join(tmpdir(), 'uol-import-test-'));
  try { return callback(dir); } finally { rmSync(dir, { recursive: true, force: true }); }
};

test('text parser selects Leo, preserving password punctuation and excluding Gab', () => {
  const result = parseUolCredentials(credentials, { expectedName: 'LEONARDO' });
  assert.deepEqual(Object.keys(result), ['leo']);
  assert.equal(result.leo.login, 'leo@example.invalid');
  assert.equal(result.leo.password, 'LEO-FAKE:with=punctuation');
  assert.equal(result.leo.expectedName, 'LEONARDO');
  assert.equal(result.leo.enabled, true);
});

test('text parser supports separate value lines and rejects missing or duplicate credentials', () => {
  const result = parseUolCredentials('Leo:\nLogin:\nleo@example.invalid\nSenha:\nfake\nGabriel\nLogin: elsewhere\nSenha: fake2');
  assert.equal(result.leo.password, 'fake');
  assert.throws(() => parseUolCredentials('Gabriel\nLogin: gab\nSenha: fake'), /account_section_not_found/);
  assert.throws(() => parseUolCredentials('Leo\nLogin: leo\nLogin: duplicate\nSenha: fake'), /duplicate_credential_field/);
  assert.throws(() => parseUolCredentials('Leo\nLogin: leo'), /invalid_account_password/);
});

test('JSON imports all accounts or selects one; malformed entries fail closed', () => {
  const registry = { leo: { login: 'leo', password: 'fake', expectedName: 'LEONARDO', enabled: true }, gui: { login: 'gui', password: 'fake2', expectedName: 'GUILHERME', enabled: false } };
  assert.deepEqual(parseAccountSource(JSON.stringify(registry)), registry);
  assert.deepEqual(Object.keys(parseAccountSource(JSON.stringify(registry), { id: 'gui' })), ['gui']);
  assert.throws(() => parseAccountSource(JSON.stringify(registry), { id: 'missing' }), /account_not_in_json/);
  assert.throws(() => parseAccountSource('{broken'), /invalid_accounts_json/);
  assert.throws(() => parseAccountSource(JSON.stringify({ leo: { ...registry.leo, enabled: 'true' } })), /invalid_account_enabled/);
});

test('atomic import merges account IDs, fixes output permissions and never changes the input', () => fixture((dir) => {
  const inputPath = join(dir, 'credentials.txt');
  const outputPath = join(dir, 'accounts.json');
  const oldGab = { login: 'gab', password: 'existing-fake', expectedName: 'GABRIEL', enabled: false };
  writeFileSync(inputPath, credentials);
  writeFileSync(outputPath, JSON.stringify({ gab: oldGab }));
  chmodSync(outputPath, 0o644);
  assert.deepEqual(importAccountFile({ inputPath, outputPath, id: 'leo', expectedName: 'LEONARDO' }), { importedCount: 1, totalAccounts: 2 });
  assert.equal(statSync(outputPath).mode & 0o777, 0o600);
  const result = JSON.parse(readFileSync(outputPath, 'utf8'));
  assert.deepEqual(result.gab, oldGab);
  assert.equal(result.leo.password, 'LEO-FAKE:with=punctuation');
  assert.equal(readFileSync(inputPath, 'utf8'), credentials);
}));

test('output inside a checkout or symlink into a checkout is refused', () => fixture((dir) => {
  const repo = join(dir, 'repo');
  mkdirSync(repo);
  mkdirSync(join(repo, '.git'));
  const inputPath = join(dir, 'credentials.txt');
  writeFileSync(inputPath, credentials);
  assert.throws(() => importAccountFile({ inputPath, outputPath: join(repo, 'accounts.json') }), /outside_git/);
  symlinkSync(repo, join(dir, 'alias'));
  assert.throws(() => importAccountFile({ inputPath, outputPath: join(dir, 'alias', 'accounts.json') }), /outside_git/);
  assert.throws(() => importAccountFile({ inputPath, outputPath: 'relative.json' }), /absolute_output/);
}));

test('CLI outputs counts only, and parse errors never echo secret input', () => fixture((dir) => {
  const inputPath = join(dir, 'credentials.txt');
  const outputPath = join(dir, 'accounts.json');
  writeFileSync(inputPath, credentials);
  const stdout = execFileSync(process.execPath, [script, '--input', inputPath, '--output', outputPath, '--account', 'leo'], { encoding: 'utf8' });
  assert.match(stdout, /1 conta\(s\)/);
  assert.doesNotMatch(stdout, /FAKE|example\.invalid/);
  writeFileSync(inputPath, '{"secret":"DO-NOT-PRINT"');
  const failed = spawnSync(process.execPath, [script, '--input', inputPath, '--output', outputPath], { encoding: 'utf8' });
  assert.equal(failed.status, 1);
  assert.doesNotMatch(failed.stderr + failed.stdout, /DO-NOT-PRINT/);
  assert.equal(JSON.parse(readFileSync(outputPath, 'utf8')).leo.login, 'leo@example.invalid');
}));
