#!/usr/bin/env node
import { chmodSync, existsSync, lstatSync, readFileSync, realpathSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';

function fail(code) { throw new Error(code); }
function accountId(value) {
  if (!/^[a-z][a-z0-9_-]{0,63}$/.test(value ?? '')) fail('invalid_account_id');
  return value;
}
function normalizeSection(value) {
  return value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[*#]/g, '').replace(/:\s*$/, '').trim().toLowerCase();
}

export function validateRegistry(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail('invalid_accounts_json');
  const entries = Object.entries(value);
  if (!entries.length) fail('empty_accounts_json');
  for (const [id, account] of entries) {
    accountId(id);
    if (!account || typeof account !== 'object' || Array.isArray(account)) fail('invalid_account_record');
    if (typeof account.login !== 'string' || !account.login.trim() || account.login.length > 512) fail('invalid_account_login');
    if (typeof account.password !== 'string' || !account.password || account.password.length > 4096) fail('invalid_account_password');
    if (typeof account.expectedName !== 'string' || !account.expectedName.trim() || account.expectedName.length > 200) fail('invalid_account_name');
    if (typeof account.enabled !== 'boolean') fail('invalid_account_enabled');
  }
  return value;
}

export function parseUolCredentials(text, { id = 'leo', section = id, expectedName = id === 'leo' ? 'LEONARDO' : id.toUpperCase() } = {}) {
  accountId(id);
  const lines = text.replace(/^\uFEFF/, '').split(/\r?\n/);
  const index = lines.findIndex((line) => normalizeSection(line) === normalizeSection(section));
  if (index < 0) fail('account_section_not_found');
  const fields = {};
  for (let i = index + 1; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line) continue;
    const match = /^(login|usu[aá]rio|e-?mail|senha|password)\s*[:=]\s*(.*)$/i.exec(line);
    if (!match) break;
    const key = /^(senha|password)$/i.test(match[1]) ? 'password' : 'login';
    if (fields[key] !== undefined) fail('duplicate_credential_field');
    let value = match[2];
    if (!value) {
      while (i + 1 < lines.length && !lines[i + 1].trim()) i += 1;
      i += 1;
      value = lines[i]?.trim();
      if (!value || /^(login|usu[aá]rio|e-?mail|senha|password)\s*[:=]/i.test(value)) fail('missing_credential_value');
    }
    fields[key] = value;
  }
  return validateRegistry({ [id]: { ...fields, expectedName, enabled: true } });
}

export function parseAccountSource(text, options = {}) {
  if (!text.trimStart().startsWith('{')) return parseUolCredentials(text, options);
  let registry;
  try { registry = JSON.parse(text); } catch { fail('invalid_accounts_json'); }
  validateRegistry(registry);
  if (options.id) {
    accountId(options.id);
    if (!Object.hasOwn(registry, options.id)) fail('account_not_in_json');
    registry = { [options.id]: registry[options.id] };
  }
  if (options.expectedName) {
    const ids = Object.keys(registry);
    if (ids.length !== 1) fail('name_override_requires_one_account');
    registry[ids[0]] = { ...registry[ids[0]], expectedName: options.expectedName };
  }
  return validateRegistry(registry);
}

function safeOutputPath(outputPath) {
  if (!isAbsolute(outputPath ?? '')) fail('absolute_output_path_required');
  let parent;
  try { parent = realpathSync(dirname(outputPath)); } catch { fail('output_directory_missing'); }
  let ancestor = parent;
  while (true) {
    if (existsSync(join(ancestor, '.git'))) fail('output_must_be_outside_git');
    const next = dirname(ancestor);
    if (next === ancestor) break;
    ancestor = next;
  }
  const destination = join(parent, outputPath.split(/[\\/]/).at(-1));
  if (existsSync(destination) && (!lstatSync(destination).isFile() || lstatSync(destination).isSymbolicLink())) fail('unsafe_output_file');
  return destination;
}

export function importAccountFile({ inputPath, outputPath, id, section, expectedName }) {
  const destination = safeOutputPath(outputPath);
  if (resolve(inputPath) === destination) fail('input_output_must_differ');
  let source;
  try { source = readFileSync(inputPath, 'utf8'); } catch { fail('input_read_failed'); }
  const imported = parseAccountSource(source, { id, section, expectedName });
  let previous = {};
  if (existsSync(destination)) {
    try { previous = JSON.parse(readFileSync(destination, 'utf8')); } catch { fail('existing_registry_invalid'); }
    validateRegistry(previous);
  }
  const merged = validateRegistry({ ...previous, ...imported });
  const temporary = `${destination}.${randomUUID()}.tmp`;
  try {
    writeFileSync(temporary, `${JSON.stringify(merged, null, 2)}\n`, { flag: 'wx', mode: 0o600 });
    chmodSync(temporary, 0o600);
    renameSync(temporary, destination);
  } catch {
    fail('registry_write_failed');
  } finally {
    rmSync(temporary, { force: true });
  }
  return { importedCount: Object.keys(imported).length, totalAccounts: Object.keys(merged).length };
}

function parseArgs(argv) {
  const flags = { '--input': 'inputPath', '--output': 'outputPath', '--account': 'id', '--section': 'section', '--expected-name': 'expectedName' };
  const options = {};
  for (let i = 0; i < argv.length; i += 2) {
    const key = flags[argv[i]];
    if (!key || options[key] !== undefined || !argv[i + 1] || argv[i + 1].startsWith('--')) fail('invalid_arguments');
    options[key] = argv[i + 1];
  }
  if (!options.inputPath || !options.outputPath) fail('input_and_output_required');
  return options;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const result = importAccountFile(parseArgs(process.argv.slice(2)));
    process.stdout.write(`Registro privado atualizado: ${result.importedCount} conta(s) importada(s), ${result.totalAccounts} no total. Permissão 0600.\n`);
  } catch (error) {
    const code = /^[a-z_]+$/.test(error.message) ? error.message : 'import_failed';
    process.stderr.write(`Importação falhou: ${code}. Nenhuma credencial exibida.\n`);
    process.exitCode = 1;
  }
}
