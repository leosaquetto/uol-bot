import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

test('private packager rejects repository paths and symlinks before reading a credential', async () => {
  const script = fileURLToPath(new URL('../scripts/package.mjs', import.meta.url));
  const repo = fileURLToPath(new URL('../../../', import.meta.url));
  const temp = await mkdtemp(join(tmpdir(), 'uol-package-test-'));
  try {
    const linked = join(temp, 'linked-repository');
    await symlink(repo, linked, 'dir');
    for (const destination of [repo, join(repo, '..private'), linked]) {
      const result = spawnSync(process.execPath, [script, destination, join(temp, 'missing-credential')], { encoding: 'utf8' });
      assert.equal(result.status, 1);
      assert.match(result.stderr, /Private package must be outside/);
      assert.doesNotMatch(result.stderr, /missing-credential/);
    }
  } finally { await rm(temp, { recursive: true, force: true }); }
});
