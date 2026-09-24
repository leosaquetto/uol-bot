import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openStore } from '../src/store.js';
import { startWhatsApp } from '../src/whatsapp.js';

test('destination verification checks exact membership, posting rights and registered contacts',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'drops-destinations-'));
  const store=openStore(join(dir,'state.sqlite'));
  let group={subject:'Requested group',participants:[{id:'222@s.whatsapp.net'}]};
  const socket={user:{id:'111:3@s.whatsapp.net'},ev:new EventEmitter(),ws:new EventEmitter(),end(){},
    groupMetadata:async()=>group,onWhatsApp:async jid=>[{jid,exists:jid==='333@s.whatsapp.net'}]};
  const whatsapp=startWhatsApp({store,socketFactory:()=>socket,logger:{log(){}}});
  try {
    socket.ev.emit('connection.update',{connection:'open'});
    const destination={type:'group',jid:'444@g.us'};
    await assert.rejects(whatsapp.verifyDestination(destination),/destination_not_writable/);
    socket.user.lid='999:3@lid';
    group={...group,participants:[{id:'999@lid'}]};
    assert.equal((await whatsapp.verifyDestination(destination)).writable,true);
    group.announce=true;
    await assert.rejects(whatsapp.verifyDestination(destination),/destination_not_writable/);
    group.participants[0].admin='admin';group.isCommunityAnnounce=true;
    assert.equal((await whatsapp.verifyDestination(destination)).communityAnnouncements,true);
    assert.equal((await whatsapp.verifyDestination(whatsapp.ownDestination())).self,true);
    assert.equal((await whatsapp.verifyDestination({type:'contact',jid:'333@s.whatsapp.net'})).writable,true);
    await assert.rejects(whatsapp.verifyDestination({type:'contact',jid:'555@s.whatsapp.net'}),/contact_unverified/);
  } finally {await whatsapp.stop();store.close();rmSync(dir,{recursive:true,force:true});}
});
