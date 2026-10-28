import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {openStore} from '../src/store.js';
import {createPersonalInbox} from '../src/personal-inbox.js';
import {createPersonalInboxApi} from '../src/personal-inbox-api.js';

const jid='5511999999999@s.whatsapp.net';
const msg=(id,ts,text)=>({key:{remoteJid:jid,id,fromMe:false},messageTimestamp:ts,message:{conversation:text}});
function fixture(){
  const dir=mkdtempSync(join(tmpdir(),'personal-inbox-api-'));
  const store=openStore(join(dir,'state.sqlite'));
  const inbox=createPersonalInbox({store,now:()=>1700000000000});
  inbox.ingestHistory({chats:[{id:jid,name:'Diego Acaki Arquiteto'}],contacts:[],messages:[msg('M1',100,'orçamento'),msg('M2',200,'site')]});
  const calls=[];
  const whatsapp={isReady:()=>true,socket:()=>({fetchMessageHistory:async(...args)=>calls.push(args)})};
  const api=createPersonalInboxApi({inbox,whatsapp});
  return {dir,store,inbox,api,calls,close(){store.close();rmSync(dir,{recursive:true,force:true});}};
}

test('lists bounded chats and messages using opaque refs only',()=>{
  const f=fixture();try{
    const chats=f.api.chats({query:'Diego',limit:10});
    assert.equal(chats.length,1);
    assert.equal(JSON.stringify(chats).includes('551199'),false);
    const messages=f.api.messages({chatRef:chats[0].chatRef,limit:10});
    assert.deepEqual(messages.map(x=>x.text),['site','orçamento']);
    assert.equal(JSON.stringify(messages).includes('551199'),false);
    assert.throws(()=>f.api.chats({limit:101}),/invalid_limit/);
    assert.throws(()=>f.api.messages({chatRef:'not-a-uuid',limit:10}),/invalid_chat_ref/);
  }finally{f.close();}
});

test('search is bounded and can be scoped to one exact chatRef',()=>{
  const f=fixture();try{
    const [chat]=f.api.chats({limit:10});
    assert.equal(f.api.search({query:'orça',chatRef:chat.chatRef,limit:5})[0].text,'orçamento');
    assert.throws(()=>f.api.search({query:'',limit:5}),/invalid_query/);
    assert.throws(()=>f.api.search({query:'x',limit:0}),/invalid_limit/);
  }finally{f.close();}
});

test('history_more uses exact oldest persisted key and never returns jid',async()=>{
  const f=fixture();try{
    const [chat]=f.api.chats({limit:10});
    const result=await f.api.historyMore({chatRef:chat.chatRef,count:25});
    assert.deepEqual(result,{accepted:true,chatRef:chat.chatRef,count:25});
    assert.equal(f.calls.length,1);
    assert.equal(f.calls[0][0],25);
    assert.equal(f.calls[0][1].id,'M1');
    assert.equal(f.calls[0][2],100);
    assert.equal(JSON.stringify(result).includes('551199'),false);
  }finally{f.close();}
});

test('history_more fails closed without ready socket or an oldest message',async()=>{
  const f=fixture();try{
    const emptyJid='empty@s.whatsapp.net';
    f.inbox.ingestChats([{id:emptyJid,name:'Empty'}]);
    const empty=f.api.chats({query:'Empty'})[0];
    await assert.rejects(f.api.historyMore({chatRef:empty.chatRef,count:10}),/history_anchor_unavailable/);
    const offline=createPersonalInboxApi({inbox:f.inbox,whatsapp:{isReady:()=>false}});
    const [chat]=f.api.chats({query:'Diego'});
    await assert.rejects(offline.historyMore({chatRef:chat.chatRef,count:10}),/whatsapp_not_ready/);
    await assert.rejects(f.api.historyMore({chatRef:chat.chatRef,count:51}),/invalid_count/);
  }finally{f.close();}
});

test('HTTP route exposes read-only chat/search surfaces with the same bounded schema',async()=>{
  const f=fixture();try{
    const url=new URL('http://localhost/v1/whatsapp/chats?query=Diego&limit=5');
    assert.equal(f.api.matches('GET',url.pathname),true);
    const result=await f.api.route({method:'GET',headers:{}},url);
    assert.equal(result.status,200);
    assert.equal(result.body.chats.length,1);
    assert.equal(JSON.stringify(result.body).includes('551199'),false);
  }finally{f.close();}
});
