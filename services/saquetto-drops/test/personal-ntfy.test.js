import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {openStore} from '../src/store.js';
import {applyReceipt} from '../src/whatsapp.js';
import {personalWhatsAppLink,sendPersonalNtfyNotification} from '../src/personal-ntfy.js';
import {createSender} from '../src/sender.js';

const personalJid='1234567@s.whatsapp.net';
const groupJid='987654@g.us';
function fixture(){
  const dir=mkdtempSync(join(tmpdir(),'drops-personal-ntfy-'));
  const store=openStore(join(dir,'state.sqlite'));
  return {dir,store,close(){store.close();rmSync(dir,{recursive:true,force:true});}};
}
function dispatch(store,destination,payload){
  const job=store.enqueue({key:randomUUID(),destination,payload});
  const claimed=store.claim();
  assert.equal(claimed.id,job.id);
  return claimed;
}
const flush=()=>new Promise(resolve=>setImmediate(resolve));

test('only the first personal acceptance notifies across manual, gateway and automatic jobs',()=>{
  const f=fixture(),notices=[];
  try{
    for(const [index,source] of [
      {manual:true,requestId:randomUUID()},
      {gateway:true,gatewayRequestHash:'a'.repeat(64)},
      {destinationAlias:'eu-mesmo'},
    ].entries()){
      const job=dispatch(f.store,personalJid,{...source,text:'same message'});
      const receipt={store:f.store,messageId:job.message_id,jid:personalJid,state:'accepted',level:'server_ack',
        personalJids:[personalJid],onPersonalMessageAccepted:text=>notices.push(text)};
      assert.equal(applyReceipt(receipt).length,1);
      assert.equal(applyReceipt({...receipt,state:'confirmed',level:'recipient_receipt'}).length,0);
      assert.equal(applyReceipt(receipt).length,0);
      assert.equal(f.store.job(job.id).state,'confirmed');
      assert.equal(index,notices.length-1);
    }
    assert.deepEqual(notices,['same message','same message','same message']);
  }finally{f.close();}
});

test('direct confirmation notifies once; enqueue, nonpersonal, failed and uncertain jobs do not',()=>{
  const f=fixture(),notices=[];
  try{
    const queued=dispatch(f.store,personalJid,{manual:true,text:'queued'});
    assert.equal(notices.length,0); // HTTP 202 only queued the job.
    const direct=applyReceipt({store:f.store,messageId:queued.message_id,jid:personalJid,state:'confirmed',
      level:'recipient_receipt',personalJids:[personalJid],onPersonalMessageAccepted:text=>notices.push(text)});
    assert.equal(direct.length,1);
    applyReceipt({store:f.store,messageId:queued.message_id,jid:personalJid,state:'confirmed',
      level:'participant_receipt',personalJids:[personalJid],onPersonalMessageAccepted:text=>notices.push(text)});

    const other=dispatch(f.store,groupJid,{destinationAlias:'group',text:'group'});
    applyReceipt({store:f.store,messageId:other.message_id,jid:groupJid,state:'accepted',level:'server_ack',
      personalJids:[personalJid],onPersonalMessageAccepted:text=>notices.push(text)});

    const uncertain=dispatch(f.store,personalJid,{manual:true,text:'uncertain'});
    f.store.updateJob(uncertain.id,'unknown','dispatch_unconfirmed');
    applyReceipt({store:f.store,messageId:uncertain.message_id,jid:personalJid,state:'unknown',level:'unknown',
      personalJids:[personalJid],onPersonalMessageAccepted:text=>notices.push(text)});

    const failed=dispatch(f.store,personalJid,{manual:true,text:'failed'});
    f.store.updateJob(failed.id,'failed','pre_dispatch_failure');
    applyReceipt({store:f.store,messageId:failed.message_id,jid:personalJid,state:'accepted',level:'server_ack',
      personalJids:[personalJid],onPersonalMessageAccepted:text=>notices.push(text)});
    assert.deepEqual(notices,['queued']);
  }finally{f.close();}
});

test('personal chat link is derived from the private WhatsApp JID',()=>{
  assert.equal(personalWhatsAppLink(personalJid),'https://wa.me/1234567');
  assert.equal(personalWhatsAppLink('1234567:4@s.whatsapp.net'),'https://wa.me/1234567');
  assert.throws(()=>personalWhatsAppLink('123456@lid'),/invalid_personal_jid/);
});

test('ntfy request uses the personal topic, body, click link and open-conversation action',async()=>{
  const calls=[];
  const ok=await sendPersonalNtfyNotification('**Oi** com `formatação`',{jid:personalJid,fetchImpl:async(url,options)=>{
    calls.push({url,options});return {ok:true,status:200};
  }});
  assert.equal(ok,true);
  assert.equal(calls.length,1);
  const {url,options}=calls[0];
  assert.equal(url,'https://ntfy.sh/leo-saquetto-wpp-3054');
  assert.equal(options.method,'POST');
  assert.equal(options.body,'Oi com formatação');
  assert.equal(Buffer.from(options.headers.Title.match(/^=\?utf-8\?B\?([^?]+)\?=$/)[1],'base64').toString(),
    'WhatsApp · Conversa Pessoal');
  assert.equal(options.headers.Click,personalWhatsAppLink(personalJid));
  assert.equal(options.headers.Actions,`view, Abrir Conversa, ${options.headers.Click}, clear=true`);
  assert.equal(options.headers.Tags,'speech_balloon,calling');
});

test('media-only personal messages still produce a readable ntfy body',async()=>{
  let body;
  const ok=await sendPersonalNtfyNotification('',{jid:personalJid,fetchImpl:async(_url,options)=>{
    body=options.body;return {ok:true,status:200};
  }});
  assert.equal(ok,true);
  assert.equal(body,'Mensagem com mídia enviada ao WhatsApp.');
});

test('ntfy failure leaves the accepted WhatsApp job accepted and cannot resend it',async()=>{
  const f=fixture();let sends=0;const warnings=[];
  const config={destinations:{'eu-mesmo':{type:'contact',jid:personalJid,verified:true}},
    operation:{paused:false,dryRun:false,minDelayMs:0}};
  const sender=createSender({store:f.store,getConfig:()=>config,whatsapp:{isReady:()=>true,
    verifyDestination:async()=>{},socket:()=>({sendMessage:async()=>{sends++;}})},canSend:()=>true,
    prepare:async payload=>({text:payload.text})});
  const oldWarn=console.warn;console.warn=value=>warnings.push(value);
  try{
    const job=f.store.enqueue({key:randomUUID(),destination:personalJid,
      payload:{destinationAlias:'eu-mesmo',text:'once'}});
    await sender();
    assert.equal(sends,1);
    const dispatched=f.store.job(job.id);
    assert.equal(dispatched.state,'unknown');
    applyReceipt({store:f.store,messageId:dispatched.message_id,jid:personalJid,state:'accepted',level:'server_ack',
      personalJids:[personalJid],onPersonalMessageAccepted:(text,jid)=>sendPersonalNtfyNotification(text,
        {jid,fetchImpl:async()=>{throw new Error('offline');}})});
    await flush();
    assert.equal(f.store.job(job.id).state,'accepted');
    assert.equal(warnings.length,1);
    f.store.setSetting('last_dispatch_at','0');
    await sender();
    assert.equal(sends,1);
    assert.equal(f.store.job(job.id).state,'accepted');
  }finally{console.warn=oldWarn;f.close();}
});
