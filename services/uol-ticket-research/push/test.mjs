import test from 'node:test';
import assert from 'node:assert/strict';
import { createECDH, randomBytes, randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createRequire } from 'node:module';
import { openPushStore } from './store.mjs';
import { observeInstagramPush, processPending } from './receiver.mjs';
import { classifyInstagramPush } from './classify.mjs';
import { privateWrite } from './private.mjs';
import { registerInstagram } from './register.mjs';
import { bootstrap } from './bootstrap.mjs';
const ece=createRequire(new URL('../../saquetto-drops/src/webpush-crypto.js',import.meta.url))('http_ece');

function fixture(payload={data:{type:'story',uri:'/stories/clubeuol/4004185955500703427/'}},version='one') {
  const receiver=createECDH('prime256v1');receiver.generateKeys();
  const sender=createECDH('prime256v1');sender.generateKeys();
  const auth=randomBytes(16),salt=randomBytes(16);
  const registration={origin:'https://www.instagram.com',uaid:randomUUID(),channelID:randomUUID(),
    endpoint:'https://updates.push.services.mozilla.com/wpush/v2/fixture',applicationServerKey:sender.getPublicKey().toString('base64url'),
    publicKey:receiver.getPublicKey().toString('base64url'),privateKey:receiver.getPrivateKey().toString('base64url'),
    auth:auth.toString('base64url'),instagramRegistration:{status:'accepted'}};
  const encrypted=ece.encrypt(Buffer.from(JSON.stringify(payload)),{version:'aes128gcm',privateKey:sender,
    dh:receiver.getPublicKey(),authSecret:auth,salt});
  return {registration,packet:{messageType:'notification',channelID:registration.channelID,version,
    data:encrypted.toString('base64url'),headers:{encoding:'aes128gcm'}}};
}
class Socket extends EventTarget {
  static instances=[];
  sent=[];
  constructor(url){super();assert.equal(url,'wss://push.services.mozilla.com/');Socket.instances.push(this);}
  send(data){this.sent.push(JSON.parse(data));}
  close(){this.dispatchEvent(new Event('close'));}
  message(data){this.dispatchEvent(new MessageEvent('message',{data:JSON.stringify(data)}));}
}
const hello=(socket,r)=>{socket.dispatchEvent(new Event('open'));socket.message({messageType:'hello',status:200,uaid:r.uaid,use_webpush:true});};
const logger={log(){}};
function directory(t) { const dir=mkdtempSync(join(tmpdir(),'uol-ig-push-'));t.after(()=>rmSync(dir,{recursive:true,force:true}));return dir; }

test('private WAL/FULL store commits packet before ACK, then persists one signal across replay and restart',t=>{
  const {registration,packet}=fixture(),dir=directory(t),path=join(dir,'push.sqlite');
  let store=openPushStore(path),observer=observeInstagramPush({registration,store,WebSocketImpl:Socket,logger});
  const socket=Socket.instances.at(-1);hello(socket,registration);
  const original=socket.send.bind(socket);
  socket.send=data=>{
    if(JSON.parse(data).messageType==='ack')assert.equal(store.db.prepare('SELECT count(*) AS n FROM push_events').get().n,1);
    original(data);
  };
  socket.message(packet);socket.message({...packet,headers:{...packet.headers}});
  assert.equal(store.db.prepare('SELECT count(*) AS n FROM signals').get().n,1);
  assert.equal(store.db.prepare('PRAGMA synchronous').get().synchronous,2);
  assert.equal(store.db.prepare('PRAGMA journal_mode').get().journal_mode,'wal');
  assert.equal(statSync(path).mode&0o777,0o600);
  observer.stop();store.close();store=openPushStore(path);
  observer=observeInstagramPush({registration,store,WebSocketImpl:Socket,logger});
  const again=Socket.instances.at(-1);hello(again,registration);again.message(packet);
  assert.equal(store.db.prepare('SELECT count(*) AS n FROM signals').get().n,1);
  observer.stop();store.close();
});

test('packet committed before a crash is processed at startup; corrupt push never creates a signal',t=>{
  const {registration,packet}=fixture(),path=join(directory(t),'push.sqlite');
  let store=openPushStore(path);store.persist(packet);store.close();store=openPushStore(path);
  processPending(store,registration);
  const corrupt={...packet,version:'corrupt',data:packet.data.slice(0,-8)+'AAAAAAAA'};
  store.persist(corrupt);processPending(store,registration);
  assert.equal(store.db.prepare('SELECT count(*) AS n FROM signals').get().n,1);
  assert.equal(store.db.prepare("SELECT reason FROM push_events WHERE state='ignored'").get().reason,'push_decryption_rejected');
  store.close();
});

test('unmatched, text-only, wrong profile and off-origin notifications do not trigger reads',()=>{
  for(const payload of [{body:'clubeuol added a story'}, {data:{type:'story',uri:'/stories/other/4004185955500703427/'}},
    {data:{type:'story',uri:'https://evil.test/stories/clubeuol/4004185955500703427/'}},
    {data:{type:'dm',username:'clubeuol'}}, {data:{type:'story',uri:'/direct/inbox/',body:'clubeuol'}}])
    assert.equal(classifyInstagramPush(payload),null);
  assert.equal(classifyInstagramPush({data:{type:'story',username:'clubeuol',story_id:'4004185955500703427'}}).profile,'clubeuol');
});

test('storage failure sends no ACK; changed UAID stops without creating a subscription',()=>{
  const {registration,packet}=fixture();let status;
  const store={pending:()=>[],health:value=>status=value,persist(){throw new Error('disk_full');}};
  let observer=observeInstagramPush({registration,store,WebSocketImpl:Socket,logger});
  let socket=Socket.instances.at(-1);hello(socket,registration);socket.message(packet);
  assert.equal(socket.sent.some(x=>x.messageType==='ack'),false);assert.equal(status,'push_storage_unavailable');observer.stop();
  observer=observeInstagramPush({registration,store,WebSocketImpl:Socket,logger});socket=Socket.instances.at(-1);
  hello(socket,{...registration,uaid:randomUUID()});
  assert.equal(status,'registration_requires_review');assert.equal(socket.sent.some(x=>x.messageType==='register'),false);observer.stop();
});

test('outage reconnects the same UAID/channel and resumes reception',async t=>{
  const {registration,packet}=fixture(),store=openPushStore(join(directory(t),'push.sqlite'));
  const observer=observeInstagramPush({registration,store,WebSocketImpl:Socket,logger,retryMs:5});
  const first=Socket.instances.at(-1);hello(first,registration);first.close();await delay(20);
  const second=Socket.instances.at(-1);hello(second,registration);second.message(packet);
  assert.equal(second.sent[0].uaid,registration.uaid);assert.deepEqual(second.sent[0].channelIDs,[registration.channelID]);
  assert.equal(observer.isReady(),true);observer.stop();store.close();
});

test('bootstrap checkpoints a new private identity without replacing an existing registration',async t=>{
  const {registration}=fixture(),path=join(directory(t),'registration.json');
  const pending=bootstrap({applicationServerKey:registration.applicationServerKey,output:path,WebSocketImpl:Socket});
  const socket=Socket.instances.at(-1);hello(socket,registration);
  const channel=socket.sent.at(-1).channelID;
  assert.equal(socket.sent.at(-1).messageType,'register');assert.equal(statSync(path).mode&0o777,0o600);
  socket.message({messageType:'register',status:200,channelID:channel,pushEndpoint:registration.endpoint});
  const result=await pending;assert.equal(result.uaid,registration.uaid);assert.equal(result.origin,'https://www.instagram.com');
  await assert.rejects(bootstrap({applicationServerKey:registration.applicationServerKey,output:path,WebSocketImpl:Socket}),/already_exists/);
});

test('registration sends the observed web_vapid shape once; ambiguous responses never prove Story delivery',async t=>{
  const {registration}=fixture(),dir=directory(t),registrationPath=join(dir,'registration.json');
  registration.instagramRegistration={status:'not_attempted'};privateWrite(registrationPath,registration);
  const sessionPath=join(dir,'session.json'),fieldsPath=join(dir,'fields.json');
  privateWrite(sessionPath,{user_agent:'fixture',cookies:['sessionid','csrftoken','mid'].map(name=>({name,value:'fixture',domain:'.instagram.com',path:'/'}))});
  privateWrite(fieldsPath,{fb_dtsg:'fixture',jazoest:'12345'});
  let calls=0;
  const result=await registerInstagram({registrationPath,sessionPath,fieldsPath,fetchImpl:async(url,options)=>{
    calls++;assert.equal(url,'https://www.instagram.com/api/v1/web/push/register/');
    const body=new URLSearchParams(options.body);assert.equal(body.get('device_type'),'web_vapid');
    assert.deepEqual(Object.keys(JSON.parse(body.get('subscription_keys'))),['p256dh','auth']);
    return new Response(JSON.stringify({status:'ok'}),{status:200});
  }});
  assert.equal(result.status,'instagram_registration_accepted');assert.equal(result.storyPushProven,false);
  await assert.rejects(registerInstagram({registrationPath,sessionPath,fieldsPath,fetchImpl:()=>{calls++;}}),/requires_review/);
  assert.equal(calls,1);
});
