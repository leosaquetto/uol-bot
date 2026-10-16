import { readFile, writeFile, mkdir, copyFile, chmod, realpath, lstat } from 'node:fs/promises';
import { resolve, dirname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { validateConfig } from '../src/monitor.js';

// An operator-only local bundle. Never package source control, tests or session data.
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repository = resolve(root, '../..');
const [destinationArg, tokenPath] = process.argv.slice(2);
if (!destinationArg || !tokenPath) throw new Error('Usage: node scripts/package.mjs OUTPUT_DIRECTORY DEDICATED_TOKEN_FILE');
const destination = resolve(destinationArg);
const outside = path => path === '..' || path.startsWith(`..${sep}`);
function assertSeparate(repo, dest) {
  if (!outside(relative(repo, dest)) || !outside(relative(dest, repo))) throw new Error('Private package must be outside the repository and its ancestors');
}
assertSeparate(repository, destination);
await mkdir(destination, { recursive: true, mode: 0o700 });
assertSeparate(await realpath(repository), await realpath(destination));
// Check every destination before reading the token or updating the bundle.
for (const file of ['src', 'icons', 'manifest.json', 'src/background.js', 'src/monitor.js', 'src/stories.js',
  'popup.html', 'popup.css', 'popup.js', 'options.html', 'options.css', 'options.js',
  'icons/icon48.png', 'icons/icon128.png', 'README.md', 'provisioning.json', 'package-checksums.json']) {
  try { if ((await lstat(join(destination, file))).isSymbolicLink()) throw new Error('Package destination contains a symlink'); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
}
const token = (await readFile(resolve(tokenPath), 'utf8')).trim();
const config = validateConfig({ token, profiles: ['clubeuol'], intervalMinutes: 5, enabled: true, notifications: true });
const files = ['manifest.json', 'src/background.js', 'src/monitor.js', 'src/stories.js',
  'popup.html', 'popup.css', 'popup.js', 'options.html', 'options.css', 'options.js',
  'icons/icon48.png', 'icons/icon128.png', 'README.md'];
await mkdir(destination, { recursive: true, mode: 0o700 });
await chmod(destination, 0o700);
const checksums = {};
for (const file of files) {
  const target = join(destination, file);
  await mkdir(dirname(target), { recursive: true, mode: 0o700 });
  await copyFile(join(root, file), target); await chmod(target, 0o600);
  checksums[file] = createHash('sha256').update(await readFile(target)).digest('hex');
}
await writeFile(join(destination, 'provisioning.json'), JSON.stringify(config), { mode: 0o600 });
await chmod(join(destination, 'provisioning.json'), 0o600);
await writeFile(join(destination, 'package-checksums.json'), JSON.stringify(checksums, null, 2)+'\n', { mode: 0o600 });
console.log(JSON.stringify({ packaged: true, destination, files: files.length, configuredProfiles: config.profiles,
  intervalMinutes: config.intervalMinutes, sessionExported: false }));
