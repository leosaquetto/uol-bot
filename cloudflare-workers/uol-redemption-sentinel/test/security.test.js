import test from 'node:test';
import assert from 'node:assert/strict';
import {seal,unseal,digest,matchesToken,readAccounts,normalizeLogin} from '../src/security.js';
const key=Buffer.alloc(32,5).toString('base64');
test('encrypted sessions bind ciphertext to account; wrong keys, tampering and other accounts fail',async()=>{
  const a=await digest('account-a');const b=await digest('account-b');
  const value={cookies:[{name:'SESS',value:'fixture-private'}]};
  const encrypted=await seal(value,key,a);
  assert.equal(JSON.stringify(encrypted).includes('fixture-private'),false);
  assert.deepEqual(await unseal(encrypted,key,a),value);
  await assert.rejects(()=>unseal(encrypted,key,b));
  await assert.rejects(()=>unseal({...encrypted,data:encrypted.data.slice(0,-5)+'AAAAA'},key,a));
  await assert.rejects(()=>unseal(encrypted,Buffer.alloc(32,6).toString('base64'),a));
  assert.notEqual((await seal(value,key,a)).iv,encrypted.iv);
});
test('admin token and account configuration fail closed',async()=>{
  assert.equal(await matchesToken('test-token','test-token'),true);
  assert.equal(await matchesToken('test-token','other'),false);
  assert.equal(await matchesToken(undefined,'other'),false);
  assert.equal(normalizeLogin(' USER@example.invalid '),'user@example.invalid');
  assert.equal(normalizeLogin('123.456.789-00'),'12345678900');
  assert.throws(()=>readAccounts({ACCOUNTS_JSON:'[]'}));
  assert.throws(()=>readAccounts({ACCOUNTS_JSON:JSON.stringify({leo:{login:'x'}})}));
});
