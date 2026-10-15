import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import test from 'node:test';
import {createGateway} from '../src/gateway.js';
import {createDropsTransport} from '../src/drops-transport.js';

const key='uol:recovery:v1',jobId='abcdef01-1234-1234-1234-abcdef123456';
const request=()=>new Request('http://gateway.test/v1/send-offer',{
  method:'POST',headers:{Authorization:'Bearer test','Content-Type':'application/json','Idempotency-Key':key},
  body:JSON.stringify({link:'https://clube.uol.com.br/offer',text:'An offer https://clube.uol.com.br/offer'}),
});

test('committed Drops job with lost response recovers after restart and late ACK without a second POST',async()=>{
  const directory=mkdtempSync(join(tmpdir(),'drops-recovery-')),databasePath=join(directory,'ledger.sqlite');
  let posts=0,lookups=0,committed,accepted=false;
  const transport=createDropsTransport({token:'a'.repeat(64),fetchImpl:async(url,init)=>{
    const path=new URL(url).pathname;
    if(init.method==='POST'){
      posts++;committed=JSON.parse(init.body);
      const db=new DatabaseSync(databasePath),row=db.prepare('SELECT * FROM deliveries WHERE idempotency_key=?').get(key);db.close();
      const identity=JSON.parse(row.response_json);
      assert.equal(row.status,'pending');assert.equal(identity.route,'uol');
      assert.equal(identity.keyHash,committed.keyHash);assert.equal(identity.requestHash,committed.requestHash);
      throw new Error('response_lost_after_commit');
    }
    lookups++;
    assert.equal(path,`/v1/gateway/jobs/by-key/uol/${committed.keyHash}/${committed.requestHash}`);
    return Response.json({jobId});
  }});
  const handler=()=>createGateway({token:'test',chatId:'uol-group',transport:'baileys',databasePath,
    probeTransport:async()=>({ok:true}),sendMessageImpl:transport.sendMessage,lookupDeliveryImpl:transport.lookupDelivery,
    confirmDeliveryImpl:async()=>accepted?{state:'accepted',messageId:'same-message',deliveryState:'accepted_by_whatsapp_server',confirmation:'server_ack'}:{state:'pending'},
    logger:{info(){},warn(){}}});
  try{
    assert.equal((await handler()(request())).status,503);
    const restarted=handler();assert.equal((await restarted(request())).status,409);
    accepted=true;assert.equal((await restarted(request())).status,202);
    assert.equal((await restarted(request())).status,200);
    assert.equal(posts,1);assert.equal(lookups,1);
  }finally{rmSync(directory,{recursive:true,force:true});}
});

test('missing or conflicting lookup and legacy ambiguity never permit another dispatch',async()=>{
  for(const status of [404,409,'legacy']){
    const directory=mkdtempSync(join(tmpdir(),'drops-lookup-')),databasePath=join(directory,'ledger.sqlite');
    let posts=0,lookups=0;
    const handler=createGateway({token:'test',chatId:'uol-group',transport:'baileys',databasePath,
      probeTransport:async()=>({ok:true}),sendMessageImpl:async()=>{posts++;throw new Error('lost');},
      lookupDeliveryImpl:async()=>{lookups++;throw Object.assign(new Error('lookup'),{status});},
      confirmDeliveryImpl:async()=>{throw new Error('no job');},logger:{info(){},warn(){}}});
    try{
      assert.equal((await handler(request())).status,503);
      if(status==='legacy'){
        const db=new DatabaseSync(databasePath);db.prepare("UPDATE deliveries SET response_json='{}'").run();db.close();
      }
      const response=await handler(request());
      assert.equal(response.status,status===404?503:409);assert.equal(posts,1);assert.equal(lookups,status==='legacy'?0:1);
    }finally{rmSync(directory,{recursive:true,force:true});}
  }
});

test('explicit pre-dispatch rejection permits a later safe attempt',async()=>{
  const directory=mkdtempSync(join(tmpdir(),'drops-rejected-')),databasePath=join(directory,'ledger.sqlite');
  let posts=0;
  const handler=createGateway({token:'test',chatId:'uol-group',transport:'baileys',databasePath,
    probeTransport:async()=>({ok:true}),sendMessageImpl:async()=>{
      posts++;
      if(posts===1)throw Object.assign(new Error('whatsapp_not_ready'),{status:503,ambiguous:false});
      return {pendingMessageID:jobId};
    },confirmDeliveryImpl:async()=>({state:'accepted',messageId:'same-message',
      deliveryState:'accepted_by_whatsapp_server',confirmation:'server_ack'}),logger:{info(){},warn(){}}});
  try{
    assert.equal((await handler(request())).status,502);
    assert.equal((await handler(request())).status,202);
    assert.equal((await handler(request())).status,200);
    assert.equal(posts,2);
  }finally{rmSync(directory,{recursive:true,force:true});}
});
