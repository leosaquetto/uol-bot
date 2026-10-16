import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync,writeFileSync,chmodSync,symlinkSync,rmSync } from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {releaseSecretFileArgs} from '../scripts/release.mjs';
test('release adds only a protected notification secret file to the same deployment',()=>{
 const root=mkdtempSync(join(tmpdir(),'wake-secret-test-'));
 try{
  const file=join(root,'secrets.json');
  writeFileSync(file,JSON.stringify({INSTAGRAM_WAKE_CLIENTS:'{}',INSTAGRAM_WAKE_RELAY_URL:'https://example.test/instagram-wake',INSTAGRAM_WAKE_RELAY_TOKEN:'test'}),{mode:0o600});
  assert.deepEqual(releaseSecretFileArgs(file),['--secrets-file',file]);
  assert.deepEqual(releaseSecretFileArgs(''),[]);
  chmodSync(file,0o644);assert.throws(()=>releaseSecretFileArgs(file),/not_private/);chmodSync(file,0o600);
  symlinkSync(file,join(root,'link'));assert.throws(()=>releaseSecretFileArgs(join(root,'link')),/not_private/);
  writeFileSync(file,JSON.stringify({ADMIN_TOKEN:'must_not_replace'}));assert.throws(()=>releaseSecretFileArgs(file),/scope_invalid/);
 }finally{rmSync(root,{recursive:true,force:true});}
});
