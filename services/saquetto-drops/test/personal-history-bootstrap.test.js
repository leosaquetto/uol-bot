import test from 'node:test';
import assert from 'node:assert/strict';
import {phoneFromCredentialsValue} from '../src/personal-history-bootstrap.js';

test('history bootstrap derives only the internal phone digits from primary credentials',()=>{
  assert.equal(phoneFromCredentialsValue(JSON.stringify({me:{id:'551199887766:12@s.whatsapp.net'}})),'551199887766');
  assert.equal(phoneFromCredentialsValue(JSON.stringify({me:{id:'551199887766@s.whatsapp.net'}})),'551199887766');
});

test('history bootstrap rejects missing or malformed primary identity',()=>{
  for(const value of ['{}',JSON.stringify({me:{id:'not-a-phone@s.whatsapp.net'}}),JSON.stringify({me:{id:'123@s.whatsapp.net'}})]){
    assert.throws(()=>phoneFromCredentialsValue(value),/primary_identity_unavailable/);
  }
});
