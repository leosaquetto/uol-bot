import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {openStore} from '../src/store.js';
import {startWhatsApp} from '../src/whatsapp.js';

test('startWhatsApp forwards inbox events while preserving message receipt handling',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'personal-inbox-events-'));
  const store=openStore(join(dir,'state.sqlite'));
  const calls=[];
  const personalInbox={
    ingestHistory:value=>calls.push(['history',value]),
    ingestMessages:value=>calls.push(['messages',value]),
    ingestUpdates:value=>calls.push(['updates',value]),
    ingestDeletes:value=>calls.push(['deletes',value]),
    ingestChats:value=>calls.push(['chats',value]),
    ingestContacts:value=>calls.push(['contacts',value]),
  };
  const socket={user:{id:'111:1@s.whatsapp.net'},ev:new EventEmitter(),ws:new EventEmitter(),end(){}};
  const whatsapp=startWhatsApp({store,personalInbox,socketFactory:()=>socket,logger:{log(){}}});
  try{
    socket.ev.emit('messaging-history.set',{chats:[{id:'a'}],contacts:[],messages:[]});
    socket.ev.emit('messages.upsert',{messages:[{key:{id:'1'}}],type:'notify'});
    socket.ev.emit('messages.update',[{key:{id:'1'},update:{}}]);
    socket.ev.emit('messages.delete',{keys:[{id:'1'}]});
    socket.ev.emit('chats.upsert',[{id:'a'}]);
    socket.ev.emit('contacts.upsert',[{id:'a'}]);
    assert.deepEqual(calls.map(x=>x[0]),['history','messages','updates','deletes','chats','contacts']);
  }finally{await whatsapp.stop();store.close();rmSync(dir,{recursive:true,force:true});}
});


test('contact LID is resolved to PN before forwarding to the personal inbox',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'personal-contact-lid-'));
  const store=openStore(join(dir,'state.sqlite'));
  const calls=[];
  const socket={
    user:{id:'111:1@s.whatsapp.net'},ev:new EventEmitter(),ws:new EventEmitter(),end(){},
    signalRepository:{lidMapping:{async getPNForLID(id){
      assert.equal(id,'12345@lid');
      return '551199887766@s.whatsapp.net';
    }}},
  };
  const personalInbox={ingestContacts:value=>calls.push(value)};
  const whatsapp=startWhatsApp({store,personalInbox,socketFactory:()=>socket,logger:{log(){},warn(){}}});
  try{
    socket.ev.emit('contacts.upsert',[{id:'12345@lid',name:'Diego Acaki'}]);
    await new Promise(resolve=>setImmediate(resolve));
    assert.deepEqual(calls,[[{id:'551199887766@s.whatsapp.net',name:'Diego Acaki'}]]);
  }finally{await whatsapp.stop();store.close();rmSync(dir,{recursive:true,force:true});}
});

test('primary socket keeps the existing paired identity and limited sync',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'personal-primary-identity-'));
  const store=openStore(join(dir,'state.sqlite'));
  const socket={user:{id:'111:1@s.whatsapp.net'},ev:new EventEmitter(),ws:new EventEmitter(),end(){}};
  let options;
  const whatsapp=startWhatsApp({store,personalInbox:{},socketFactory:value=>{options=value;return socket;},logger:{log(){}}});
  try{
    assert.equal(options.syncFullHistory,false);
    assert.equal(Object.hasOwn(options,'browser'),false);
    assert.equal(Object.hasOwn(options,'shouldSyncHistoryMessage'),false);
  }finally{await whatsapp.stop();store.close();rmSync(dir,{recursive:true,force:true});}
});

test('history bootstrap requests full history without overriding pairing browser identity',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'personal-history-bootstrap-'));
  const store=openStore(join(dir,'state.sqlite'));
  const socket={user:{id:'111:1@s.whatsapp.net'},ev:new EventEmitter(),ws:new EventEmitter(),end(){}};
  let options;
  const whatsapp=startWhatsApp({store,personalInbox:{},historyBootstrap:true,socketFactory:value=>{options=value;return socket;},logger:{log(){}}});
  try{
    assert.equal(options.syncFullHistory,true);
    assert.equal(Object.hasOwn(options,'browser'),false);
    assert.equal(options.shouldSyncHistoryMessage?.({syncType:999}),true);
  }finally{await whatsapp.stop();store.close();rmSync(dir,{recursive:true,force:true});}
});
