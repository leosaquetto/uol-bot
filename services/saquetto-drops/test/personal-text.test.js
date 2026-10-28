import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {openStore} from '../src/store.js';
import {createPersonalTextApi,createPersonalTextSender} from '../src/personal-text.js';
import {createPersonalInbox} from '../src/personal-inbox.js';

const requestId='123e4567-e89b-42d3-a456-426614174000';
const jid='5511999999999@s.whatsapp.net';
function fixture(){
  const dir=mkdtempSync(join(tmpdir(),'personal-text-'));
  const store=openStore(join(dir,'db.sqlite'));
  const config={operation:{paused:false,dryRun:false,minDelayMs:0},destinations:{self:{type:'contact',jid,verified:true}}};
  const getConfig=()=>config;
  const inbox=createPersonalInbox({store,now:()=>1000});
  const api=createPersonalTextApi({store,getConfig,canSend:()=>true,inbox,now:()=>1000});
  return {dir,store,config,getConfig,inbox,api,close(){store.close();rmSync(dir,{recursive:true,force:true});}};
}

test('personal text API exposes only aliases and keeps JIDs private',()=>{
  const f=fixture();try{
    assert.deepEqual(f.api.destinations(),[{id:'self',name:'self',type:'contact'}]);
    assert.equal(JSON.stringify(f.api.destinations()).includes('551199'),false);
  }finally{f.close();}
});

test('personal text enqueue is exact, idempotent and self-only',()=>{
  const f=fixture();try{
    const input={requestId,destinations:['self'],text:'  oi\nleo  ',mediaIds:[]};
    const first=f.api.enqueue(input);
    assert.equal(first.requestId,requestId);assert.equal(first.jobs[0].destination,'self');assert.equal(first.jobs[0].state,'queued');
    assert.equal(JSON.stringify(first).includes('551199'),false);
    assert.deepEqual(f.api.enqueue(input),first);
    assert.throws(()=>f.api.enqueue({...input,text:'outra'}),/idempotency_conflict/);
    for(const bad of [
      {...input,destinations:['5511999999999']},
      {...input,destinations:['self','self']},
      {...input,mediaIds:['x']},
      {...input,text:'   '},
      {...input,text:'oi\u0000'},
    ]) assert.throws(()=>f.api.enqueue(bad));
  }finally{f.close();}
});

test('personal text sender dispatches once and uncertainty is never replayed',async()=>{
  const f=fixture();try{
    f.api.enqueue({requestId,destinations:['self'],text:'mensagem privada',mediaIds:[]});
    const relays=[];
    const whatsapp={
      isReady:()=>true,ownDestination:()=>f.config.destinations.self,verifyDestination:async d=>({writable:d.jid===jid}),
      socket:()=>({user:{id:jid},waUploadToServer:async()=>{},relayMessage:async(...args)=>{relays.push(args);}}),
    };
    const generateMessage=async(remoteJid,content,options)=>({key:{id:options.messageId,remoteJid},message:{conversation:content.text}});
    const sender=createPersonalTextSender({store:f.store,getConfig:f.getConfig,whatsapp,canSend:()=>true,generateMessage,now:()=>2000});
    await sender();
    assert.equal(relays.length,1);
    const job=f.store.jobByKey('personal-mcp:'+requestId);
    assert.equal(job.state,'unknown');assert.equal(job.code,'awaiting_whatsapp_ack');
    await sender();
    assert.equal(relays.length,1,'unknown dispatch must never be replayed');
    f.store.receipt(job.message_id,jid,'accepted','server_ack');
    assert.equal(f.store.job(job.id).state,'accepted');
  }finally{f.close();}
});

test('personal text can enqueue and dispatch only an indexed exact chatRef',async()=>{
  const f=fixture();try{
    const targetJid='5511888888888@s.whatsapp.net';
    f.inbox.ingestChats([{id:targetJid,name:'Diego Acaki Arquiteto'}]);
    const chat=f.inbox.listChats({query:'Diego'})[0];
    const chatRequest='123e4567-e89b-42d3-a456-426614174001';
    const queued=f.api.enqueueChat({requestId:chatRequest,chatRef:chat.chatRef,text:'oi diego'});
    assert.equal(queued.jobs[0].destination,chat.chatRef);
    assert.equal(JSON.stringify(queued).includes('551188'),false);
    const stored=f.store.jobByKey('personal-mcp:'+chatRequest);
    assert.equal(stored.destination,targetJid,'JID may exist only inside the local job');
    assert.throws(()=>f.api.enqueueChat({requestId:'123e4567-e89b-42d3-a456-426614174002',chatRef:'11111111-1111-4111-8111-111111111111',text:'x'}),/chat_not_found/);

    const relays=[];
    const whatsapp={
      isReady:()=>true,
      verifyDestination:async d=>({writable:d.jid===targetJid}),
      socket:()=>({user:{id:jid},waUploadToServer:async()=>{},relayMessage:async(...args)=>relays.push(args)}),
      ownDestination:()=>f.config.destinations.self,
    };
    const generateMessage=async(remoteJid,content,options)=>({key:{id:options.messageId,remoteJid},message:{conversation:content.text}});
    const sender=createPersonalTextSender({store:f.store,getConfig:f.getConfig,inbox:f.inbox,whatsapp,canSend:()=>true,generateMessage,now:()=>2000});
    await sender();
    assert.equal(relays.length,1);
    assert.equal(relays[0][0],targetJid);
    assert.equal(f.store.job(stored.id).state,'unknown');
  }finally{f.close();}
});
