import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { proto } from '@whiskeysockets/baileys';
import { openStore } from '../src/store.js';
import { createSender } from '../src/sender.js';
import { startWhatsApp } from '../src/whatsapp.js';

const destination='12345@g.us';
const config={operation:{paused:false,dryRun:false,minDelayMs:0},destinations:{uol:{type:'group',jid:destination,verified:true}}};
const binary=Buffer.from([0,255,128,1,42]);
const generateMessage=async(jid,content,options)=>({key:{id:options.messageId,remoteJid:jid},
  message:proto.Message.create({extendedTextMessage:{text:content.text,jpegThumbnail:binary}})});
const fixture=()=>{
  const dir=mkdtempSync(join(tmpdir(),'drops-wire-'));const path=join(dir,'state.sqlite');
  const store=openStore(path);
  const job=store.enqueue({key:'uol:wire:test',destination,payload:{destinationAlias:'uol',text:'one stored message'}});
  return {dir,path,store,job,close(){this.store.close();rmSync(dir,{recursive:true,force:true});}};
};
const sender=(f,relay,overrides={})=>createSender({store:f.store,getConfig:()=>config,
  whatsapp:{isReady:()=>true,verifyDestination:async()=>{},socket:()=>({relayMessage:relay})},
  canSend:()=>true,prepare:async payload=>({text:payload.text}),generateMessage,...overrides});

test('protobuf is durable before relay, retains binary data after restart and never republishes unknown job',async()=>{
  const f=fixture();let relays=0,originalId;
  try {
    await sender(f,async(jid,wire,options)=>{
      relays++;originalId=options.messageId;
      const current=f.store.job(f.job.id);
      assert.equal(current.state,'dispatching');assert.equal(current.destination,jid);assert.equal(current.message_id,options.messageId);
      const stored=f.store.wireMessage(options.messageId,jid);
      assert.deepEqual(stored,Buffer.from(proto.Message.encode(wire).finish()));
      assert.deepEqual(proto.Message.decode(stored).extendedTextMessage.jpegThumbnail,binary);
      throw new Error('response lost after possible acceptance');
    })();
    assert.equal(relays,1);assert.equal(f.store.job(f.job.id).state,'unknown');
    f.store.close();f.store=openStore(f.path);
    assert.equal(f.store.job(f.job.id).message_id,originalId);assert.equal(f.store.claim(),null);
    let socketOptions;
    const whatsapp=startWhatsApp({store:f.store,socketFactory:options=>{
      socketOptions=options;
      return {ev:new EventEmitter(),ws:new EventEmitter(),end:async()=>{}};
    }});
    try {
      const recovered=await socketOptions.getMessage({id:originalId,remoteJid:destination});
      assert.equal(recovered.extendedTextMessage.text,'one stored message');
      assert.deepEqual(recovered.extendedTextMessage.jpegThumbnail,binary);
      assert.equal(await socketOptions.getMessage({id:originalId,remoteJid:'wrong@g.us'}),undefined);
      assert.equal(await socketOptions.getMessage({id:'wrong',remoteJid:destination}),undefined);
    } finally {await whatsapp.stop();}
    await sender(f,async()=>{relays++;})();assert.equal(relays,1);
    f.store.receipt(originalId,destination,'accepted','server_ack');
    assert.equal(f.store.job(f.job.id).state,'accepted');
  } finally {f.close();}
});

test('failed durable write cannot relay a message and only the same active claim can retry',async()=>{
  for(const lost of [false,true]){
    const f=fixture();let relays=0;
    try {
      f.store.saveWireMessage=()=>{
        if(lost)f.store.updateJob(f.job.id,'unknown','externally_reserved');
        throw new Error('storage write failed');
      };
      await sender(f,async()=>{relays++;})();
      assert.equal(relays,0);assert.equal(f.store.job(f.job.id).state,lost?'unknown':'queued');
    } finally {f.close();}
  }
});

test('store refuses wire data for a different attempt or destination',()=>{
  const f=fixture();
  try {
    const claim=f.store.claim();const bytes=proto.Message.encode({conversation:'hello'}).finish();
    for(const invalid of [{...claim,attempts:claim.attempts+1},{...claim,destination:'wrong@g.us'},{...claim,message_id:'wrong'}]){
      assert.throws(()=>f.store.saveWireMessage(invalid,bytes),/reservation_lost/);
    }
    assert.equal(f.store.wireMessage(claim.message_id,claim.destination),null);
    f.store.saveWireMessage(claim,bytes);assert.deepEqual(f.store.wireMessage(claim.message_id,claim.destination),Buffer.from(bytes));
  } finally {f.close();}
});

test('pause during asynchronous protobuf generation prevents relay',async()=>{
  const f=fixture();let relays=0;
  try {
    await sender(f,async()=>{relays++;},{generateMessage:async(...args)=>{
      const wire=await generateMessage(...args);config.operation.paused=true;return wire;
    }})();
    assert.equal(relays,0);assert.equal(f.store.job(f.job.id).state,'queued');
  } finally {config.operation.paused=false;f.close();}
});

test('process interruption after durable preparation retains protobuf and recovers as unknown',()=>{
  const f=fixture();
  try {
    const claim=f.store.claim();const bytes=proto.Message.encode({extendedTextMessage:{text:'crash',jpegThumbnail:binary}}).finish();
    f.store.saveWireMessage(claim,bytes);
    f.store.close();f.store=openStore(f.path);
    const recovered=f.store.job(claim.id);
    assert.equal(recovered.state,'unknown');assert.equal(recovered.code,'interrupted_dispatch');
    assert.equal(recovered.message_id,claim.message_id);assert.equal(recovered.destination,claim.destination);
    assert.deepEqual(f.store.wireMessage(claim.message_id,claim.destination),Buffer.from(bytes));
    assert.equal(f.store.claim(),null);
  } finally {f.close();}
});
