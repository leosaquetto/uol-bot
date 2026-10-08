#!/usr/bin/env node
import { readFileSync, statSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const DEFAULT_BASE_URL = 'https://uol-redemption-sentinel.leosaquetto.workers.dev';
const COMMANDS = new Set(['status', 'bootstrap', 'probe', 'activate', 'pause', 'retry-notifications']);
const OMIT_KEYS = /password|senha|secret|token|cookie|^session$|sessionjar|sessiondata|login|email|headers|^identity$|^body$|html|voucher|baseline/i;

function privateFile(path) {
  const stat = statSync(path);
  if (!stat.isFile() || (stat.mode & 0o077) !== 0) throw new Error('private_file_permissions_required');
  return readFileSync(path, 'utf8');
}

export function safeStatus(value) {
  if (Array.isArray(value)) return value.map(safeStatus);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.entries(value).filter(([key]) => !OMIT_KEYS.test(key)).map(([key, item]) => [key, safeStatus(item)]));
}

export async function adminRequest({ command, account, baseUrl = DEFAULT_BASE_URL, token, data, campaign, fetchImpl = globalThis.fetch }) {
  if (!COMMANDS.has(command)) throw new Error('invalid_command');
  if (!/^[a-z][a-z0-9_-]{0,63}$/.test(account ?? '')) throw new Error('invalid_account_id');
  if (!token || typeof token !== 'string' || /[\r\n]/.test(token)) throw new Error('admin_token_required');
  let base;
  try { base = new URL(baseUrl); } catch { throw new Error('invalid_base_url'); }
  if (base.protocol !== 'https:' || base.username || base.password || base.search || base.hash || base.pathname !== '/') throw new Error('invalid_base_url');
  if (command === 'bootstrap' && (!data || typeof data !== 'object' || Array.isArray(data))) throw new Error('bootstrap_data_required');
  if (command === 'activate' && !/^[a-z0-9][a-z0-9_-]{0,127}$/.test(campaign ?? '')) throw new Error('campaign_required');
  if (command !== 'bootstrap' && data !== undefined) throw new Error('data_only_for_bootstrap');
  const payload = command === 'bootstrap' ? data : command === 'activate' ? { campaignId: campaign } : command === 'probe' ? { readOnly: true } : {};
  let response;
  try {
    response = await fetchImpl(new URL(`/admin/accounts/${account}/${command}`, base), {
      method: command === 'status' ? 'GET' : 'POST',
      headers: { Authorization: `Bearer ${token}`, Accept: 'application/json', ...(command === 'status' ? {} : { 'Content-Type': 'application/json' }) },
      ...(command === 'status' ? {} : { body: JSON.stringify(payload) }),
      redirect: 'error', signal: AbortSignal.timeout(60_000),
    });
  } catch { throw new Error('admin_request_failed'); }
  if (!response.ok) throw new Error(`admin_http_${response.status}`);
  try { return safeStatus(await response.json()); } catch { throw new Error('admin_invalid_json'); }
}

function parseArgs(argv) {
  const [command, ...flags] = argv;
  if (!COMMANDS.has(command)) throw new Error('invalid_command');
  const names = { '--account': 'account', '--base-url': 'baseUrl', '--token-file': 'tokenFile', '--data': 'dataFile', '--campaign': 'campaign' };
  const options = { command };
  for (let i = 0; i < flags.length; i += 2) {
    const key = names[flags[i]];
    if (!key || options[key] !== undefined || !flags[i + 1] || flags[i + 1].startsWith('--')) throw new Error('invalid_arguments');
    options[key] = flags[i + 1];
  }
  return options;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const options = parseArgs(process.argv.slice(2));
    options.token = options.tokenFile ? privateFile(options.tokenFile).trim() : process.env.SENTINEL_ADMIN_TOKEN;
    if (options.dataFile) {
      try { options.data = JSON.parse(privateFile(options.dataFile)); } catch { throw new Error('bootstrap_private_json_invalid'); }
    }
    const result = await adminRequest(options);
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } catch (error) {
    const code = /^[a-z_0-9]+$/.test(error.message) ? error.message : 'admin_failed';
    process.stderr.write(`Operação falhou: ${code}.\n`);
    process.exitCode = 1;
  }
}
