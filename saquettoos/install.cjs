#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');
const hash = value => crypto.createHash('sha256').update(value).digest('hex');
function main() {
const source = fs.readFileSync(path.join(__dirname, 'dist/saquettoOS.js'));
const cloud = path.join(os.homedir(), 'Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents');
const target = path.join(cloud, 'saquettoOS.js');
const privateDir = path.join(os.homedir(), '.config/saquettoos');
if (!fs.statSync(cloud).isDirectory()) throw new Error('Scriptable directory unavailable');
const manifest = JSON.parse(fs.readFileSync(path.join(__dirname, 'releases/stable.json')));
if (source.length !== manifest.size || hash(source) !== manifest.sha256) throw new Error('Artifact differs from stable');
const previous = fs.existsSync(target) ? fs.readFileSync(target) : null;
if (previous && !previous.equals(source)) {
  const backupDir = path.join(privateDir, 'backups');
  fs.mkdirSync(backupDir, { recursive: true, mode: 0o700 });
  const backup = path.join(backupDir, 'saquettoOS-' + hash(previous) + '.js');
  if (!fs.existsSync(backup)) fs.writeFileSync(backup, previous, { flag: 'wx', mode: 0o600 });
  if (!fs.readFileSync(backup).equals(previous)) throw new Error('Backup not verified');
}
if (!previous || !previous.equals(source)) {
  const staged = target + '.' + crypto.randomUUID() + '.tmp';
  fs.writeFileSync(staged, source, { flag: 'wx', mode: 0o600 });
  if (!fs.readFileSync(staged).equals(source)) throw new Error('Stage not verified');
  if (previous ? !fs.readFileSync(target).equals(previous) : fs.existsSync(target)) throw new Error('Concurrent modification');
  fs.renameSync(staged, target);
}
if (hash(fs.readFileSync(target)) !== manifest.sha256) throw new Error('Installed hash mismatch');
if (process.argv.includes('--owner-bootstrap')) {
  const owner = JSON.parse(fs.readFileSync(path.join(privateDir, 'owner.json')));
  const verified = JSON.parse(fs.readFileSync(path.join(privateDir, 'owner-verified.json')));
  if (verified.id !== owner.deviceId || owner.api !== 'https://saquettoos-access.leosaquetto.workers.dev') throw new Error('Owner not verified');
  const bootstrap = path.join(cloud, 'saquettoos-owner-bootstrap.json');
  const bytes = JSON.stringify({ api: owner.api, deviceId: owner.deviceId, token: owner.token });
  if (fs.existsSync(bootstrap) && fs.readFileSync(bootstrap, 'utf8') !== bytes) throw new Error('Bootstrap conflict');
  if (!fs.existsSync(bootstrap)) fs.writeFileSync(bootstrap, bytes, { flag: 'wx', mode: 0o600 });
}
console.log(JSON.stringify({ installed: target, version: manifest.version, bytes: source.length, sha256: manifest.sha256 }));
}
try { main(); } catch (_) { console.error('Installation failed; no private configuration was printed. Check stable artifact and private owner verification.'); process.exitCode = 1; }
