import test from 'node:test';
import assert from 'node:assert/strict';
import {customPairingCode,phoneFromCredentialsValue,waitForSocketOpen} from '../src/personal-history-bootstrap.js';

test('history bootstrap derives only the internal phone digits from primary credentials',()=>{
  assert.equal(phoneFromCredentialsValue(JSON.stringify({me:{id:'551199887766:12@s.whatsapp.net'}})),'551199887766');
  assert.equal(phoneFromCredentialsValue(JSON.stringify({me:{id:'551199887766@s.whatsapp.net'}})),'551199887766');
});

test('history bootstrap rejects missing or malformed primary identity',()=>{
  for(const value of ['{}',JSON.stringify({me:{id:'not-a-phone@s.whatsapp.net'}}),JSON.stringify({me:{id:'123@s.whatsapp.net'}})]){
    assert.throws(()=>phoneFromCredentialsValue(value),/primary_identity_unavailable/);
  }
});


test('history bootstrap waits until websocket is open before requesting pairing code',async()=>{
  const socket={ws:{isOpen:false}};
  setTimeout(()=>{socket.ws.isOpen=true;},25);
  await waitForSocketOpen(socket,{timeoutMs:250,pollMs:5});
  assert.equal(socket.ws.isOpen,true);
});

test('history bootstrap accepts only an explicit eight digit pairing code',()=>{
  const valid='12'.repeat(4);
  assert.equal(customPairingCode(undefined),undefined);
  assert.equal(customPairingCode(valid),valid);
  for(const value of ['1234567','123456789','abcd1234'])assert.throws(()=>customPairingCode(value),/invalid_pairing_code/);
});
