import test from 'node:test';
import assert from 'node:assert/strict';
import { createECDH, randomBytes, randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { decryptPush } from '../src/webpush-crypto.js';
import { decodePushEvent, observeWebPush, pushEnvelope } from '../src/webpush.js';
import { openStore } from '../src/store.js';
import { postFromPush } from '../src/push.js';
import ece from 'http_ece';

const makePush = (version, override) => {
  const receiver=createECDH('prime256v1');receiver.generateKeys();
  const sender=createECDH('prime256v1');sender.generateKeys();
  const auth=randomBytes(16),salt=randomBytes(16);
  const registration={uaid:randomUUID().replaceAll('-',''),channelID:randomUUID(),
    endpoint:'https://updates.push.services.mozilla.com/wpush/v2/test',applicationServerKey:sender.getPublicKey().toString('base64url'),
    privateKey:receiver.getPrivateKey().toString('base64url'),
    publicKey:receiver.getPublicKey().toString('base64url'),auth:auth.toString('base64url')};
  const payload=override || {title:'New post',body:'Novo álbum ♥',data:{type:'tweet',uri:'/taylorswift13/status/2102952373022851247'}};
  const ciphertext=ece.encrypt(Buffer.from(JSON.stringify(payload)),{
    version,privateKey:sender,dh:receiver.getPublicKey(),authSecret:auth,salt,
  });
  return {registration,payload,message:{messageType:'notification',channelID:registration.channelID,version:'one',data:ciphertext.toString('base64url'),headers:{encoding:version,
    encryption:`salt=${salt.toString('base64url')}`,crypto_key:`dh=${sender.getPublicKey().toString('base64url')}`}}};
};

for (const version of ['aes128gcm','aesgcm']) test(`Web Push ${version} authenticates and decrypts without accepting tampered content`,()=>{
  const {registration,payload,message}=makePush(version);
  assert.deepEqual(decryptPush(message,registration),payload);
  const corrupt=Buffer.from(message.data,'base64url');corrupt[corrupt.length-1]^=1;
  assert.throws(()=>decryptPush({...message,data:corrupt.toString('base64url')},registration));
  assert.throws(()=>decryptPush(message,{...registration,auth:randomBytes(16).toString('base64url')}));
  assert.throws(()=>decryptPush({...message,headers:{encoding:'unknown'}},registration));
});

class FakeWebSocket extends EventTarget {
  static instances=[];
  sent=[];
  constructor(url){super();assert.equal(url,'wss://push.services.mozilla.com/');FakeWebSocket.instances.push(this);}
  send(text){this.sent.push(JSON.parse(text));}
  close(){this.dispatchEvent(new Event('close'));}
  message(data){this.dispatchEvent(new MessageEvent('message',{data:JSON.stringify(data)}));}
}
const quiet={log(){}};
const hello=(socket,registration)=>{
  socket.dispatchEvent(new Event('open'));
  socket.message({messageType:'hello',status:200,uaid:registration.uaid,use_webpush:true});
};

test('push transport persists before ACK, deduplicates redelivery and recovers after restart',async()=>{
  const {registration,message}=makePush('aesgcm');
  const dir=mkdtempSync(join(tmpdir(),'drops-webpush-')),path=join(dir,'state.sqlite');
  let store=openStore(path),observer,callbacks=0;
  try {
    observer=observeWebPush({registration,store,onEvent:()=>callbacks++,WebSocketImpl:FakeWebSocket,logger:quiet});
    let socket=FakeWebSocket.instances.at(-1);hello(socket,registration);
    const send=socket.send.bind(socket);
    socket.send=data=>{if(JSON.parse(data).messageType==='ack')assert.equal(store.snapshot().events[0].count,1);send(data);};
    socket.message(message);socket.message(message);await Promise.resolve();
    assert.equal(callbacks,1);assert.equal(socket.sent.filter(m=>m.messageType==='ack').length,2);
    const persisted=JSON.parse(store.nextEvent().payload);
    assert.equal(postFromPush(decodePushEvent(persisted,registration)).author,'taylorswift13');
    await observer.stop();store.close();store=openStore(path);
    observer=observeWebPush({registration,store,WebSocketImpl:FakeWebSocket,logger:quiet});
    socket=FakeWebSocket.instances.at(-1);hello(socket,registration);socket.message(message);
    assert.equal(observer.isReady(),true);assert.equal(store.snapshot().events[0].count,1);
    assert.equal(socket.sent[0].uaid,registration.uaid);
  } finally {await observer?.stop();store.close();rmSync(dir,{recursive:true,force:true});}
});

test('storage failure halts transport without ACK or losing the upstream notification',async()=>{
  const {registration,message}=makePush('aesgcm');
  const observer=observeWebPush({registration,store:{recordPush(){throw new Error('disk_full');}},WebSocketImpl:FakeWebSocket,logger:quiet});
  try {
    const socket=FakeWebSocket.instances.at(-1);hello(socket,registration);socket.message(message);
    assert.equal(observer.state(),'push_storage_unavailable');assert.equal(observer.isReady(),false);
    assert.equal(socket.sent.filter(m=>m.messageType==='ack').length,0);
  }finally{await observer.stop();}
});

test('changed upstream identity requires review and never silently creates a subscription',async()=>{
  const {registration}=makePush('aes128gcm');
  const observer=observeWebPush({registration,store:{},WebSocketImpl:FakeWebSocket,logger:quiet});
  try {
    const socket=FakeWebSocket.instances.at(-1);hello(socket,{...registration,uaid:'changed'});
    assert.equal(observer.state(),'registration_requires_review');assert.equal(observer.isReady(),false);
    assert.equal(socket.sent.some(m=>m.messageType==='register'),false);
  }finally{await observer.stop();}
});

test('WebSocket outage reconnects with the same subscription and resumes reception',async()=>{
  const {registration,message}=makePush('aes128gcm');let persisted=0;
  const observer=observeWebPush({registration,store:{recordPush(){persisted++;return {added:true};}},WebSocketImpl:FakeWebSocket,logger:quiet,retryMs:5});
  try {
    const first=FakeWebSocket.instances.at(-1);hello(first,registration);first.close();
    assert.equal(observer.isReady(),false);
    await delay(20);const second=FakeWebSocket.instances.at(-1);assert.notEqual(second,first);
    hello(second,registration);second.message(message);
    assert.equal(second.sent[0].uaid,registration.uaid);assert.deepEqual(second.sent[0].channelIDs,[registration.channelID]);
    assert.equal(observer.isReady(),true);assert.equal(persisted,1);
  }finally{await observer.stop();}
});

test('identity comes only from tweet notification URI, not links in the notification text',()=>{
  const {registration,message}=makePush('aes128gcm',{body:'https://x.com/other/status/2102952373022851248',data:{type:'tweet',uri:'/taylorswift13/status/2102952373022851247'}});
  assert.equal(postFromPush(decodePushEvent(pushEnvelope(message,registration),registration)).author,'taylorswift13');
  for(const data of [{type:'dm',uri:'/taylorswift13/status/2102952373022851247'},{type:'tweet',uri:'https://evil.test/taylorswift13/status/2102952373022851247'}]){
    const f=makePush('aesgcm',{data});assert.throws(()=>decodePushEvent(pushEnvelope(f.message,f.registration),f.registration));
  }
});

test('Web Push rejects malformed framing before cryptographic processing',()=>{
  const {registration,message}=makePush('aes128gcm');
  const malformed=Buffer.from(message.data,'base64url');malformed.writeUInt32BE(0,16);
  assert.throws(()=>decryptPush({...message,data:malformed.toString('base64url')},registration),/record_header/);
  assert.throws(()=>decryptPush({...message,data:'not base64'},registration),/base64url/);
  assert.throws(()=>decryptPush({...message,data:'A'.repeat(100000)},registration),/push_size/);
});
