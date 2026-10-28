import test from 'node:test';
import assert from 'node:assert/strict';
import {Readable} from 'node:stream';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {openStore} from '../src/store.js';
import {createPersonalInbox} from '../src/personal-inbox.js';
import {createPersonalChatActions} from '../src/personal-chat-actions.js';

const message=(jid,id,ts,text,fromMe=false)=>({key:{remoteJid:jid,id,fromMe},messageTimestamp:ts,message:{conversation:text}});
function fixture(){
  const dir=mkdtempSync(join(tmpdir(),'personal-chat-actions-'));
  const store=openStore(join(dir,'state.sqlite'));
  const inbox=createPersonalInbox({store,now:()=>1700000000000});
  const jids={a:'a@s.whatsapp.net',b:'b@s.whatsapp.net',c:'c@s.whatsapp.net',empty:'empty@s.whatsapp.net'};
  inbox.ingestHistory({chats:[
    {id:jids.a,name:'Chat A'},{id:jids.b,name:'Chat B'},{id:jids.c,name:'Chat C'},{id:jids.empty,name:'Empty'},
  ],contacts:[],messages:[
    message(jids.a,'A1',100,'a'),
    message(jids.b,'B1',150,'b1'),message(jids.b,'B2',200,'b2'),
    message(jids.c,'C1',9999,'c'),
  ]});
  const calls=[];
  const socket={
    chatModify:async(...args)=>{calls.push(['chatModify',...args]);},
    sendMessage:async(...args)=>{calls.push(['sendMessage',...args]);},
  };
  const whatsapp={isReady:()=>true,socket:()=>socket};
  const actions=createPersonalChatActions({store,inbox,whatsapp,now:()=>1700000000000});
  return {dir,store,inbox,jids,calls,actions,close(){store.close();rmSync(dir,{recursive:true,force:true});}};
}

test('deleteChat requires the exact current lastMessageRef and sends complete lastMessages data',async()=>{
  const f=fixture();try{
    const chat=f.inbox.listChats({query:'Chat A'})[0];
    assert.match(chat.lastMessageRef,/^[0-9a-f-]{36}$/i);
    const result=await f.actions.deleteChat({chatRef:chat.chatRef,lastMessageRef:chat.lastMessageRef});
    assert.deepEqual(result,{state:'deleted',chatRef:chat.chatRef});
    assert.equal(f.calls.length,1);
    const [,patch,jid]=f.calls[0];
    assert.equal(jid,f.jids.a);
    assert.equal(patch.delete,true);
    assert.equal(patch.lastMessages[0].key.id,'A1');
    assert.equal(patch.lastMessages[0].messageTimestamp,100);
    assert.equal(f.inbox.listChats({query:'Chat A'}).length,0);
  }finally{f.close();}
});

test('deleteChat fails closed when last message is missing or changed',async()=>{
  const f=fixture();try{
    const empty=f.inbox.listChats({query:'Empty'})[0];
    await assert.rejects(f.actions.deleteChat({chatRef:empty.chatRef,lastMessageRef:'11111111-1111-4111-8111-111111111111'}),/last_message_unavailable/);
    const chat=f.inbox.listChats({query:'Chat B'})[0];
    f.inbox.ingestMessages([message(f.jids.b,'B3',300,'new')]);
    await assert.rejects(f.actions.deleteChat({chatRef:chat.chatRef,lastMessageRef:chat.lastMessageRef}),/last_message_changed/);
    assert.equal(f.calls.length,0,'chatModify must not run on stale state');
  }finally{f.close();}
});

test('deleteMessage for_me targets only an exact messageRef and tombstones after success',async()=>{
  const f=fixture();try{
    const chat=f.inbox.listChats({query:'Chat B'})[0];
    const target=f.inbox.listMessages(chat.chatRef,{limit:10}).find(x=>x.text==='b1');
    const result=await f.actions.deleteMessage({messageRef:target.messageRef,mode:'for_me'});
    assert.deepEqual(result,{state:'deleted',messageRef:target.messageRef,mode:'for_me'});
    const [,patch,jid]=f.calls[0];
    assert.equal(jid,f.jids.b);
    assert.deepEqual(patch.deleteForMe,{deleteMedia:false,key:{remoteJid:f.jids.b,id:'B1',fromMe:false},timestamp:150});
    assert.equal(f.inbox.listMessages(chat.chatRef,{limit:10}).find(x=>x.messageRef===target.messageRef).deleted,true);
    await assert.rejects(f.actions.deleteMessage({messageRef:'11111111-1111-4111-8111-111111111111',mode:'for_me'}),/message_not_found/);
  }finally{f.close();}
});


test('archiveChat uses the current indexed last message and never archives an unknown chat',async()=>{
  const f=fixture();try{
    const chat=f.inbox.listChats({query:'Chat B'})[0];
    const result=await f.actions.archiveChat({chatRef:chat.chatRef});
    assert.deepEqual(result,{state:'archived',chatRef:chat.chatRef});
    const [,patch,jid]=f.calls[0];
    assert.equal(jid,f.jids.b);
    assert.equal(patch.archive,true);
    assert.equal(patch.lastMessages[0].key.id,'B2');
    assert.equal(patch.lastMessages[0].messageTimestamp,200);
    await assert.rejects(f.actions.archiveChat({chatRef:'11111111-1111-4111-8111-111111111111'}),/chat_not_found/);
  }finally{f.close();}
});

test('deleteMessage everyone sends an exact delete key only for the indexed message',async()=>{
  const f=fixture();try{
    f.inbox.ingestMessages([message(f.jids.a,'ME1',120,'mine',true)]);
    const chat=f.inbox.listChats({query:'Chat A'})[0];
    const target=f.inbox.listMessages(chat.chatRef,{limit:10}).find(x=>x.text==='mine');
    const result=await f.actions.deleteMessage({messageRef:target.messageRef,mode:'everyone'});
    assert.deepEqual(result,{state:'deleted',messageRef:target.messageRef,mode:'everyone'});
    const [,jid,payload]=f.calls[0];
    assert.equal(jid,f.jids.a);
    assert.deepEqual(payload.delete,{remoteJid:f.jids.a,id:'ME1',fromMe:true});
    assert.equal(f.inbox.listMessages(chat.chatRef,{limit:10}).find(x=>x.messageRef===target.messageRef).deleted,true);
  }finally{f.close();}
});

test('cleanup preview snapshots exact refs and apply never expands beyond the approved set',async()=>{
  const f=fixture();try{
    const preview=f.actions.previewCleanup({inactiveBefore:500,maxIndexedMessages:2,limit:10});
    assert.deepEqual(preview.chats.map(x=>x.name).sort(),['Chat A','Chat B','Empty']);
    assert.ok(preview.chats.every(x=>x.chatRef&&('lastMessageRef' in x)));
    const outsider=f.inbox.listChats({query:'Chat C'})[0];
    await assert.rejects(f.actions.applyCleanup({snapshotId:preview.snapshotId,chatRefs:[outsider.chatRef]}),/snapshot_mismatch/);
    assert.equal(f.calls.length,0);

    const a=preview.chats.find(x=>x.name==='Chat A');
    const b=preview.chats.find(x=>x.name==='Chat B');
    f.inbox.ingestMessages([message(f.jids.b,'B3',300,'arrived after preview')]);
    const receipts=await f.actions.applyCleanup({snapshotId:preview.snapshotId,chatRefs:[a.chatRef,b.chatRef]});
    assert.equal(receipts.find(x=>x.chatRef===a.chatRef).state,'deleted');
    assert.equal(receipts.find(x=>x.chatRef===b.chatRef).state,'failed');
    assert.equal(receipts.find(x=>x.chatRef===b.chatRef).code,'last_message_changed');
    assert.equal(f.calls.length,1,'only the still-matching chat may be mutated');
    await assert.rejects(f.actions.applyCleanup({snapshotId:preview.snapshotId,chatRefs:[b.chatRef]}),/snapshot_consumed/);
    assert.equal(f.calls.length,1,'approved cleanup snapshot is single-use and cannot replay mutations');
  }finally{f.close();}
});

test('cleanup HTTP preview route is exact and does not expose jid',async()=>{
  const f=fixture();try{
    const input=JSON.stringify({inactiveBefore:500,maxIndexedMessages:1,limit:10});
    const req=Readable.from([Buffer.from(input)]);req.method='POST';req.headers={'content-type':'application/json'};
    const url=new URL('http://localhost/v1/whatsapp/cleanup/preview');
    assert.equal(f.actions.matches(req.method,url.pathname),true);
    const result=await f.actions.route(req,url);
    assert.equal(result.status,200);
    assert.equal(JSON.stringify(result.body).includes('@s.whatsapp.net'),false);
    assert.ok(result.body.chats.some(x=>x.name==='Chat A'));
  }finally{f.close();}
});
