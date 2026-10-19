#!/usr/bin/env node
'use strict';
// Operator-only provisioning. Tokens stay in a private directory and HTTPS headers.
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const base = path.join(os.homedir(), '.config/saquettoos');
const file = path.join(base, 'owner.json');
const endpoint = 'https://saquettoos-access.leosaquetto.workers.dev';
async function main() {
  fs.mkdirSync(base, { recursive: true, mode: 0o700 });
  let owner;
  if (fs.existsSync(file)) owner = JSON.parse(fs.readFileSync(file, 'utf8'));
  else {
    owner = { api: endpoint, name: 'Leo', deviceId: crypto.randomUUID(), token: crypto.randomBytes(32).toString('hex') };
    fs.writeFileSync(file, JSON.stringify(owner), { mode: 0o600, flag: 'wx' });
  }
  if (owner.api !== endpoint || !/^[a-f0-9]{64}$/.test(owner.token) || !/^[a-f0-9-]{36}$/.test(owner.deviceId)) throw new Error('owner_configuration_invalid');
  const bootstrap = fs.readFileSync(path.join(base, 'bootstrap-token'), 'utf8').trim();
  if (!/^[a-f0-9]{64}$/.test(bootstrap)) throw new Error('bootstrap_invalid');
  const response = await fetch(endpoint + '/v1/bootstrap', { method: 'POST', redirect: 'error',
    headers: { Authorization: 'Bearer ' + bootstrap, 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: owner.name, deviceId: owner.deviceId, token: owner.token }), signal: AbortSignal.timeout(20000) });
  if (!response.ok) throw new Error('owner_provisioning_http_' + response.status);
  const verify = await fetch(endpoint + '/v1/me', { redirect: 'error', headers: { Authorization: 'Bearer ' + owner.token }, signal: AbortSignal.timeout(20000) });
  const result = await verify.json();
  if (!verify.ok || result.profile?.role !== 'admin' || result.profile?.status !== 'approved' || result.profile?.id !== owner.deviceId) throw new Error('owner_not_verified');
  fs.writeFileSync(path.join(base, 'owner-verified.json'), JSON.stringify({ id: owner.deviceId, verifiedAt: new Date().toISOString() }), { mode: 0o600 });
  console.log('Owner verified: Leo · admin · approved. Credentials retained privately.');
}
main().catch(error => { console.error(/^[a-z_0-9]+$/.test(error.message) ? error.message : 'owner_provisioning_failed'); process.exitCode = 1; });
