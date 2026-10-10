import { lstatSync, openSync, readFileSync, closeSync, mkdirSync, renameSync, writeFileSync, fstatSync, fsyncSync, constants } from 'node:fs';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';

export function privateDirectory(path) {
  mkdirSync(path, { recursive: true, mode: 0o700 });
  const info = lstatSync(path);
  if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) || info.uid !== process.getuid())
    throw new Error('private_directory_required');
}

export function privateRead(path, limit = 262144) {
  privateDirectory(dirname(path));
  const info = lstatSync(path);
  if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o777) !== 0o600 || info.uid !== process.getuid())
    throw new Error('private_file_required');
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const opened = fstatSync(fd);
    if (opened.ino !== info.ino || opened.dev !== info.dev) throw new Error('private_file_changed');
    const raw = readFileSync(fd);
    if (raw.length > limit) throw new Error('private_file_limit');
    return JSON.parse(raw.toString('utf8'));
  } finally { closeSync(fd); }
}

export function privateWrite(path, value) {
  privateDirectory(dirname(path));
  const temporary = `${path}.${randomUUID()}.tmp`;
  const fd = openSync(temporary, 'wx', 0o600);
  try { writeFileSync(fd, JSON.stringify(value)); fsyncSync(fd); } finally { closeSync(fd); }
  renameSync(temporary, path);
  const directory = openSync(dirname(path), 'r');
  try { fsyncSync(directory); } finally { closeSync(directory); }
}

const uuid = /^[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}$/i;
export function validateRegistration(value) {
  const r = structuredClone(value);
  if (r.origin !== 'https://www.instagram.com' || !uuid.test(r.uaid) || !uuid.test(r.channelID))
    throw new Error('invalid_registration');
  const url = new URL(r.endpoint);
  if (url.protocol !== 'https:' || url.hostname !== 'updates.push.services.mozilla.com' || url.port ||
      url.username || url.password || !url.pathname.startsWith('/wpush/v2/')) throw new Error('invalid_endpoint');
  for (const [name, size] of [['privateKey',32],['publicKey',65],['auth',16],['applicationServerKey',65]]) {
    if (typeof r[name] !== 'string' || !/^[A-Za-z0-9_-]+$/.test(r[name]) || Buffer.from(r[name],'base64url').length !== size)
      throw new Error('invalid_registration_key');
  }
  return r;
}
