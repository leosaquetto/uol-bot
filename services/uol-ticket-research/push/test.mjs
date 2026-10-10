import test from 'node:test';
import assert from 'node:assert/strict';
import { createECDH, randomBytes, randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync, statSync, openSync, closeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createRequire } from 'node:module';
import { spawnSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { openPushStore, PUSH_STORE_LIMITS } from './store.mjs';
import { observeInstagramPush, processPending } from './receiver.mjs';
import { classifyInstagramPush } from './classify.mjs';
import { privateWrite } from './private.mjs';
import { registerInstagram } from './register.mjs';
import { bootstrap } from './bootstrap.mjs';
const ece=createRequire(new URL('../../saquetto-drops/src/webpush-crypto.js',import.meta.url))('http_ece');

function fixture(payload={data:{type:'story',uri:'/stories/clubeuol/4004185955500703427/'}},version='one') {
  const receiver=createECDH('prime256v1');
  do { receiver.generateKeys(); } while(receiver.getPrivateKey().length!==32);
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
function seed(store,count,{prefix='seed',state='pending',packet='{}',receivedAt=0,completedAt=null,compactedAt=null}={}) {
  const insert=store.db.prepare(`INSERT INTO push_events(id,received_at,packet,packet_hash,state,reason,
    packet_bytes,completed_at,compacted_at) VALUES(?,?,?,?,?,?,?,?,?)`);
  store.db.exec('BEGIN IMMEDIATE');
  try {
    for(let i=0;i<count;i++)insert.run(`${prefix}-${i}`,receivedAt,packet,'fixture-hash',state,
      state==='ignored'?'fixture_reason':null,Buffer.byteLength(packet,'utf8'),completedAt,compactedAt);
    store.db.exec('COMMIT');
  } catch(error) { store.db.exec('ROLLBACK');throw error; }
}

test('terminal packet retains diagnostics seven days after completion; compact identity preserves replay and proof sequence',t=>{
  const {packet}=fixture(),path=join(directory(t),'push.sqlite'),start=1700000000000;
  let store=openPushStore(path);
  store.persist(packet,start);
  const event=store.pending()[0],completed=start+86400000;
  store.finish(event,{profile:'clubeuol',storyId:'4004185955500703427',evidence:'story_uri'},null,completed);
  const proof=store.db.prepare('SELECT * FROM signals').get();
  const identity=store.db.prepare('SELECT id,packet_hash,state,reason FROM push_events').get();
  assert.equal(store.maintain(completed+PUSH_STORE_LIMITS.retentionMs-1).compacted,0);
  assert.deepEqual(JSON.parse(store.db.prepare('SELECT packet FROM push_events').get().packet),packet);
  assert.equal(store.maintain(completed+PUSH_STORE_LIMITS.retentionMs).compacted,1);
  assert.equal(store.db.prepare('SELECT packet FROM push_events').get().packet,'{}');
  assert.deepEqual(store.db.prepare('SELECT id,packet_hash,state,reason FROM push_events').get(),identity);
  store.close();store=openPushStore(path);
  assert.deepEqual(store.persist(packet),{id:event.id,added:false});
  assert.throws(()=>store.persist({...packet,data:packet.data+'changed'}),/push_version_conflict/);
  assert.equal(store.finish(event,null,'changed'),false);
  assert.deepEqual(store.db.prepare('SELECT * FROM signals').get(),proof);
  assert.equal(store.capacity().compactedCount,1);
  store.close();
});

test('legacy terminal compaction migrates at most fifty packets per commit and resumes after restart',t=>{
  const path=join(directory(t),'push.sqlite');closeSync(openSync(path,'wx',0o600));
  let db=new DatabaseSync(path);
  db.exec(`CREATE TABLE push_events(id TEXT PRIMARY KEY,received_at INTEGER NOT NULL,packet TEXT NOT NULL,
    packet_hash TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',reason TEXT)`);
  const insert=db.prepare('INSERT INTO push_events VALUES(?,?,?,?,?,?)');
  db.exec('BEGIN IMMEDIATE');
  for(let i=0;i<121;i++)insert.run(`legacy-${i}`,0,'{"encrypted":"fixture"}','fixture-hash','ignored','legacy_reason');
  insert.run('pending',0,'{"encrypted":"pending"}','pending-hash','pending',null);
  db.exec('COMMIT');db.close();
  let store=openPushStore(path),now=PUSH_STORE_LIMITS.retentionMs+1;
  assert.equal(store.maintain(now).compacted,50);store.close();store=openPushStore(path);
  assert.equal(store.maintain(now).compacted,50);
  assert.equal(store.maintain(now).compacted,21);
  assert.equal(store.maintain(now).compacted,0);
  assert.equal(store.capacity().eventCount,122);assert.equal(store.capacity().pendingCount,1);
  assert.equal(store.db.prepare("SELECT count(*) AS n FROM push_events WHERE state='ignored' AND reason='legacy_reason' AND packet_hash='fixture-hash'").get().n,121);
  assert.equal(store.db.prepare("SELECT packet FROM push_events WHERE id='pending'").get().packet,'{"encrypted":"pending"}');
  store.close();
});

test('byte pressure compacts recent terminal packets in bounded batches but preserves every pending packet',t=>{
  const store=openPushStore(join(directory(t),'push.sqlite')),now=1700000000000;
  const packet=JSON.stringify({data:'x'.repeat(131061)});assert.equal(Buffer.byteLength(packet),131072);
  seed(store,244,{prefix:'terminal',state:'ignored',packet,receivedAt:now,completedAt:now});
  seed(store,12,{prefix:'pending',packet});
  assert.equal(store.capacity().payloadBytes,PUSH_STORE_LIMITS.payloadBytes);
  assert.deepEqual(store.maintain(now),{compacted:50,remaining:true});
  assert.equal(store.maintain(now).compacted,2);
  assert.equal(store.capacity().pressure,false);
  assert.equal(store.db.prepare("SELECT count(*) AS n FROM push_events WHERE state='pending' AND packet=?").get(packet).n,12);
  assert.equal(store.db.prepare("SELECT count(*) AS n FROM push_events WHERE reason='fixture_reason'").get().n,244);
  store.close();
});

test('more than five thousand compact identities still accept new pending packets',t=>{
  const store=openPushStore(join(directory(t),'push.sqlite'));
  seed(store,5001,{state:'ignored',completedAt:1,compactedAt:2});
  assert.equal(store.persist(fixture().packet).added,true);
  assert.equal(store.capacity().eventCount,5002);assert.equal(store.capacity().pendingCount,1);
  store.close();
});

test('pending, payload and identity capacities reject unique packets while existing replay remains accepted',t=>{
  const {packet}=fixture();
  for(const kind of ['pending','payload','identity']) {
    const store=openPushStore(join(directory(t),`${kind}.sqlite`));
    const saved=store.persist(packet);
    if(kind==='pending')seed(store,5000);
    if(kind==='payload')seed(store,128,{packet:'x'.repeat(PUSH_STORE_LIMITS.packetBytes)});
    if(kind==='identity')seed(store,100000,{state:'ignored',compactedAt:1});
    const before=store.capacity();
    assert.deepEqual(store.persist(packet),{id:saved.id,added:false});
    assert.throws(()=>store.persist({...packet,data:packet.data+'changed'}),/push_version_conflict/);
    assert.throws(()=>store.persist({...packet,version:'unique'}),new RegExp(`push_${kind==='identity'?'identity':kind}_limit`));
    assert.equal(store.capacity().eventCount,before.eventCount);
    assert.equal(store.capacity().pendingCount,before.pendingCount);
    assert.equal(store.capacity().payloadBytes,before.payloadBytes);
    store.close();
  }
});

test('packet and receiver frame budgets measure UTF8 bytes, and capacity status contains only sanitized reasons',t=>{
  const {registration,packet}=fixture(),store=openPushStore(join(directory(t),'push.sqlite'));
  assert.throws(()=>store.persist({...packet,data:'é'.repeat(140000)}),/push_packet_limit/);
  assert.equal(store.capacity().eventCount,0);
  const statuses=[];
  let observer=observeInstagramPush({registration,store,WebSocketImpl:Socket,
    logger:{log:value=>statuses.push(JSON.parse(value).event)}});
  t.after(()=>observer.stop());
  let socket=Socket.instances.at(-1);hello(socket,registration);
  const oversized={...packet,data:'é'.repeat(90000),headers:{extra:'🙂'.repeat(21000)}};
  assert.ok(JSON.stringify(oversized).length<PUSH_STORE_LIMITS.packetBytes);
  socket.message(oversized);
  assert.ok(statuses.includes('frame_rejected'));
  assert.equal(socket.sent.some(message=>message.messageType==='ack'),false);observer.stop();store.close();
  for(const reason of ['push_pending_limit','push_payload_limit','push_identity_limit']) {
    let status;
    observer=observeInstagramPush({registration,store:{pending:()=>[],health:value=>status=value,
      persist(){throw new Error(reason);}},WebSocketImpl:Socket,logger});
    socket=Socket.instances.at(-1);hello(socket,registration);socket.message(packet);
    assert.equal(status,reason);assert.equal(socket.sent.some(message=>message.messageType==='ack'),false);
    observer.stop();assert.equal(status,reason);
  }
});

test('finish failure and abrupt unfinished compaction both roll back without losing pending data',t=>{
  const path=join(directory(t),'push.sqlite'),{packet}=fixture();let store=openPushStore(path);
  store.persist(packet);const event=store.pending()[0];
  store.db.exec("CREATE TRIGGER reject_signal BEFORE INSERT ON signals BEGIN SELECT RAISE(ABORT,'fixture_signal_failure'); END");
  assert.throws(()=>store.finish(event,{profile:'clubeuol',storyId:'4004185955500703427',evidence:'story_uri'},null),/fixture_signal_failure/);
  assert.equal(store.pending().length,1);assert.equal(store.db.prepare('SELECT completed_at FROM push_events').get().completed_at,null);
  assert.equal(store.db.prepare('SELECT count(*) AS n FROM signals').get().n,0);
  seed(store,2,{state:'ignored',packet:'{"encrypted":"fixture"}',completedAt:0});
  store.db.exec("CREATE TRIGGER reject_compaction BEFORE UPDATE ON push_events WHEN NEW.id='seed-1' AND NEW.packet='{}' BEGIN SELECT RAISE(ABORT,'fixture_compaction_failure'); END");
  assert.throws(()=>store.maintain(Date.now()),/fixture_compaction_failure/);
  assert.equal(store.capacity().compactedCount,0);
  store.db.exec('DROP TRIGGER reject_compaction');store.close();
  const crashed=spawnSync(process.execPath,['--input-type=module','-e',
    "import {DatabaseSync} from 'node:sqlite';const db=new DatabaseSync(process.argv[1]);db.exec(\"BEGIN IMMEDIATE; UPDATE push_events SET packet='{}',packet_bytes=2,compacted_at=1 WHERE id='seed-0'\");process.exit(0)",path],{encoding:'utf8'});
  assert.equal(crashed.status,0);store=openPushStore(path);
  assert.equal(store.pending().length,1);
  assert.equal(store.db.prepare("SELECT count(*) AS n FROM push_events WHERE state='ignored' AND packet='{\"encrypted\":\"fixture\"}'").get().n,2);
  assert.equal(store.maintain(Date.now()).compacted,2);
  store.close();
});

test('receiver resumes a fifty-packet crash backlog through bounded maintenance',{timeout:2000},async t=>{
  const {registration}=fixture(),store=openPushStore(join(directory(t),'push.sqlite'));
  seed(store,121);let drained;
  const completed=new Promise(resolve=>drained=resolve),finish=store.finish.bind(store);
  let finished=0;store.finish=(...args)=>{const result=finish(...args);if(++finished===121)drained();return result;};
  const observer=observeInstagramPush({registration,store,WebSocketImpl:Socket,logger,
    maintenanceMs:5,retentionMs:1000,decrypt:()=>({})});
  t.after(()=>{observer.stop();store.close();});
  assert.equal(store.capacity().pendingCount,71);
  await completed;
  assert.equal(store.capacity().pendingCount,0);
  assert.equal(store.capacity().eventCount,121);
});

test('receiver drains legacy payloads above32MiB before connecting, then stops all maintenance timers',{timeout:12000},async t=>{
  const {registration,packet}=fixture(),store=openPushStore(join(directory(t),'push.sqlite'));
  const legacy=JSON.stringify({data:'x'.repeat(131061)});
  seed(store,320,{state:'ignored',packet:legacy,receivedAt:Date.now(),completedAt:Date.now()});
  store.db.exec('UPDATE push_events SET packet_bytes=NULL');
  assert.ok(store.capacity().payloadBytes>PUSH_STORE_LIMITS.payloadBytes);
  const batches=[],maintain=store.maintain.bind(store);
  store.maintain=(...args)=>{const result=maintain(...args);batches.push(result.compacted);return result;};
  let connected;const connection=new Promise(resolve=>connected=resolve);
  class RecoverySocket extends Socket { constructor(url){super(url);connected(this);} }
  const sockets=Socket.instances.length;
  const observer=observeInstagramPush({registration,store,WebSocketImpl:RecoverySocket,logger,maintenanceMs:5});
  t.after(()=>{observer.stop();store.close();});
  assert.equal(Socket.instances.length,sockets);assert.equal(batches[0],50);
  assert.equal(store.db.prepare('SELECT status FROM receiver_state').get().status,'push_payload_limit');
  const socket=await connection;
  assert.equal(store.capacity().pressure,false);assert.ok(batches.every(count=>count<=50));
  assert.ok(batches.length>=3);
  hello(socket,registration);socket.message(packet);
  assert.equal(observer.isReady(),true);assert.equal(socket.sent.filter(message=>message.messageType==='ack').length,1);
  assert.equal(store.db.prepare('SELECT count(*) AS n FROM signals').get().n,1);
  observer.stop();const completedBatches=batches.length;await delay(20);
  assert.equal(batches.length,completedBatches);assert.equal(observer.isReady(),false);
});

test('reclaimable payload limit disconnects without ACK and recovers the same subscription through bounded batches',{timeout:12000},async t=>{
  const {registration,packet}=fixture(),store=openPushStore(join(directory(t),'push.sqlite'));
  let connected;
  class RecoverySocket extends Socket { constructor(url){super(url);connected?.(this);} }
  const observer=observeInstagramPush({registration,store,WebSocketImpl:RecoverySocket,logger,maintenanceMs:5});
  t.after(()=>{observer.stop();store.close();});
  const first=Socket.instances.at(-1);hello(first,registration);
  seed(store,320,{state:'ignored',packet:JSON.stringify({data:'x'.repeat(131061)}),receivedAt:Date.now(),completedAt:Date.now()});
  const connection=new Promise(resolve=>connected=resolve);
  first.message(packet);
  assert.equal(first.sent.some(message=>message.messageType==='ack'),false);
  assert.equal(store.capacity().eventCount,320);assert.equal(store.capacity().pendingCount,0);
  assert.equal(store.db.prepare('SELECT status FROM receiver_state').get().status,'push_payload_limit');
  assert.equal(observer.isReady(),false);
  const second=await connection;assert.equal(store.capacity().pressure,false);
  hello(second,registration);second.message(packet);
  assert.equal(second.sent[0].uaid,registration.uaid);assert.deepEqual(second.sent[0].channelIDs,[registration.channelID]);
  assert.equal(observer.isReady(),true);assert.equal(second.sent.filter(message=>message.messageType==='ack').length,1);
  assert.equal(store.capacity().eventCount,321);
  assert.equal(store.db.prepare('SELECT count(*) AS n FROM signals').get().n,1);
});

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
