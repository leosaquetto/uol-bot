#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const cp = require('node:child_process');
const crypto = require('node:crypto');
const [command, commit] = process.argv.slice(2);
const root = path.resolve(__dirname, '..');
const dir = path.join(__dirname, 'releases');
const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
function artifact(revision) {
  if (!/^[a-f0-9]{40}$/.test(revision || '')) throw new Error('Provide a full committed artifact revision');
  return cp.execFileSync('git', ['show', revision + ':saquettoos/dist/saquettoOS.js'], { cwd: root, maxBuffer: 16 * 1024 * 1024 });
}
function verify(bytes, manifest) {
  if (bytes.length !== manifest.size || hash(bytes) !== manifest.sha256) throw new Error('Artifact mismatch');
  const text = bytes.toString('utf8');
  if (!bytes.equals(Buffer.from(text, 'utf8'))) throw new Error('Invalid UTF-8');
  new Function('return async function(){\n' + text + '\n}');
  return text;
}
fs.mkdirSync(dir, { recursive: true });
if (command === 'prepare') {
  const bytes = artifact(commit);
  if (!bytes.equals(fs.readFileSync(path.join(__dirname, 'dist/saquettoOS.js')))) throw new Error('Commit differs from local artifact');
  const version = /^const SAQUETTO_VERSION = '([0-9]+\.[0-9]+\.[0-9]+)';$/m.exec(bytes.toString('utf8'))?.[1];
  if (!version) throw new Error('Missing version');
  const manifest = { schema: 1, updaterProtocol: 1, version, channel: 'beta', artifact: 'https://raw.githubusercontent.com/leosaquetto/uol-bot/' + commit + '/saquettoos/dist/saquettoOS.js',
    size: bytes.length, sha256: hash(bytes), publishedAt: new Date().toISOString() };
  verify(bytes, manifest);
  fs.writeFileSync(path.join(dir, 'beta.json'), JSON.stringify(manifest, null, 2) + '\n');
  console.log('Beta manifest prepared; committed bytes preserved.');
} else if (command === 'promote') {
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, 'beta.json')));
  const revision = /^https:\/\/raw\.githubusercontent\.com\/leosaquetto\/uol-bot\/([a-f0-9]{40})\/saquettoos\/dist\/saquettoOS\.js$/.exec(manifest.artifact)?.[1];
  verify(artifact(revision), manifest);
  fs.writeFileSync(path.join(dir, 'stable.json'), JSON.stringify({ ...manifest, channel: 'stable' }, null, 2) + '\n');
  console.log('Stable manifest promoted from identical beta bytes.');
} else throw new Error('Usage: node saquettoos/release.cjs prepare COMMIT | promote');
