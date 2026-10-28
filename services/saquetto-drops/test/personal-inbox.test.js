import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {openStore} from '../src/store.js';
import {createPersonalInbox} from '../src/personal-inbox.js';

function fixture(){
  const dir=mkdtempSync(join(tmpdir(),'personal-inbox-'));
  const store=openStore(join(dir,'state.sqlite'));
  const inbox=createPersonalInbox({store,now:()=>1700000000000});
  return {store,inbox};
}
const msg=(jid,id,ts,text,fromMe=false)=>({key:{remoteJid:jid,id,fromMe},messageTimestamp:ts,message:{conversation:text}});

test('history creates opaque chat/message refs and never exposes jid',()=>{
  const {store,inbox}=fixture();
  inbox.ingestHistory({
    chats:[{id:'5511999999999@s.whatsapp.net',name:'Diego Acaki Arquiteto'}],
    contacts:[{id:'5511999999999@s.whatsapp.net',name:'Diego Acaki'}],
    messages:[msg('5511999999999@s.whatsapp.net','M1',100,'oi')],
  });
  const [chat]=inbox.listChats({query:'Diego'});
  assert.match(chat.chatRef,/^[0-9a-f-]{36}$/i);
  assert.equal(chat.name,'Diego Acaki Arquiteto');
  assert.equal(chat.indexedMessageCount,1);
  assert.equal(JSON.stringify(chat).includes('5511999999999'),false);
  const [message]=inbox.listMessages(chat.chatRef,{limit:10});
  assert.match(message.messageRef,/^[0-9a-f-]{36}$/i);
  assert.equal(message.text,'oi');
  assert.equal(JSON.stringify(message).includes('5511999999999'),false);
  store.close();
});

test('duplicate upserts are idempotent and newer message wins chat ordering',()=>{
  const {store,inbox}=fixture();
  inbox.ingestMessages([msg('a@s.whatsapp.net','A1',100,'old')]);
  inbox.ingestMessages([msg('a@s.whatsapp.net','A1',100,'old')]);
  inbox.ingestMessages([msg('a@s.whatsapp.net','A2',200,'new')]);
  const [chat]=inbox.listChats({limit:10});
  assert.equal(chat.indexedMessageCount,2);
  assert.equal(chat.lastMessageAt,200);
  assert.equal(inbox.listMessages(chat.chatRef,{limit:10}).map(x=>x.text).join(','),'new,old');
  store.close();
});

test('duplicate display names return multiple candidates rather than selecting one',()=>{
  const {store,inbox}=fixture();
  inbox.ingestHistory({chats:[
    {id:'a@s.whatsapp.net',name:'Diego'},
    {id:'b@s.whatsapp.net',name:'Diego'},
  ],contacts:[],messages:[]});
  const chats=inbox.listChats({query:'Diego',limit:10});
  assert.equal(chats.length,2);
  assert.notEqual(chats[0].chatRef,chats[1].chatRef);
  store.close();
});

test('extracts safe text/captions and does not turn protocol messages into user text',()=>{
  const {store,inbox}=fixture();
  const jid='a@s.whatsapp.net';
  inbox.ingestMessages([
    {key:{remoteJid:jid,id:'T1',fromMe:false},messageTimestamp:100,message:{extendedTextMessage:{text:'texto'}}},
    {key:{remoteJid:jid,id:'T2',fromMe:false},messageTimestamp:101,message:{imageMessage:{caption:'legenda'}}},
    {key:{remoteJid:jid,id:'T3',fromMe:false},messageTimestamp:102,message:{protocolMessage:{type:0}}},
  ]);
  const [chat]=inbox.listChats({limit:10});
  const rows=inbox.listMessages(chat.chatRef,{limit:10});
  assert.deepEqual(rows.map(x=>[x.contentType,x.text]),[
    ['protocolMessage',null],['imageMessage','legenda'],['extendedTextMessage','texto'],
  ]);
  store.close();
});

test('message updates change persisted content without changing its opaque ref',()=>{
  const {store,inbox}=fixture();
  const jid='a@s.whatsapp.net';
  inbox.ingestMessages([msg(jid,'M1',100,'antes')]);
  const [chat]=inbox.listChats({limit:10});
  const before=inbox.listMessages(chat.chatRef,{limit:10})[0];
  inbox.ingestUpdates([{key:{remoteJid:jid,id:'M1',fromMe:false},update:{message:{conversation:'depois'}}}]);
  const after=inbox.listMessages(chat.chatRef,{limit:10})[0];
  assert.equal(after.messageRef,before.messageRef);
  assert.equal(after.text,'depois');
  store.close();
});

test('delete event tombstones exact message and removes its text',()=>{
  const {store,inbox}=fixture();
  const jid='a@s.whatsapp.net';
  inbox.ingestMessages([msg(jid,'M1',100,'segredo')]);
  const [chat]=inbox.listChats({limit:10});
  const before=inbox.listMessages(chat.chatRef,{limit:10})[0];
  inbox.ingestDeletes({keys:[{remoteJid:jid,id:'M1',fromMe:false}]});
  const after=inbox.listMessages(chat.chatRef,{limit:10})[0];
  assert.equal(after.messageRef,before.messageRef);
  assert.equal(after.deleted,true);
  assert.equal(after.text,null);
  store.close();
});

test('chat/contact updates refresh names without exposing contact ids',()=>{
  const {store,inbox}=fixture();
  inbox.ingestMessages([msg('a@s.whatsapp.net','M1',100,'oi')]);
  inbox.ingestContacts([{id:'a@s.whatsapp.net',notify:'Diego'}]);
  assert.equal(inbox.listChats({query:'Diego'})[0].name,'Diego');
  inbox.ingestChats([{id:'a@s.whatsapp.net',name:'Diego Acaki Arquiteto'}]);
  assert.equal(inbox.listChats({query:'Acaki'})[0].name,'Diego Acaki Arquiteto');
  store.close();
});
