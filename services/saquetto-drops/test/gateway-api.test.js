import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Readable} from 'node:stream';
import test from 'node:test';
import sharp from 'sharp';
import {createGatewayApi} from '../src/gateway-api.js';
import {openStore} from '../src/store.js';
import {createSender} from '../src/sender.js';

const link='https://clube.uol.com.br/campanhasdeingresso/pPM-2-ingressos-bgs-distrito-anhembi-sp';
const text=`Clube UOL publicou um Story\n${link}`;
const input=(bytes,overrides={})=>({route:'uol',keyHash:'a'.repeat(64),requestHash:'b'.repeat(64),text,
  preview:{link,title:'Clube UOL',summary:'',imageBase64:bytes.toString('base64')},deliveryFormat:'story_photo',...overrides});
const send=(api,body)=>{
  const req=Readable.from([Buffer.from(JSON.stringify(body))]);req.method='POST';req.headers={'content-type':'application/json'};
  return api.route(req,'/v1/gateway/send');
};
const fixture=(overrides={})=>{
  const directory=mkdtempSync(join(tmpdir(),'drops-story-test-'));const store=openStore(join(directory,'state.sqlite'));
  const config={operation:{paused:false,dryRun:false,minDelayMs:0},destinations:{uol:{type:'group',jid:'12345@g.us',verified:true}}};
  const whatsapp={isReady:()=>true,ownDestination:()=>({jid:'self@s.whatsapp.net'}),verifyDestination:async()=>{},socket:()=>({}),...overrides};
  const api=createGatewayApi({store,dataDir:directory,getConfig:()=>config,whatsapp,canSend:()=>true,routes:{uol:'uol'}});
  return {store,config,whatsapp,api,close:()=>{store.close();rmSync(directory,{recursive:true,force:true});}};
};

test('queued Story photo prepares original portrait bytes and caption without preview composition',async()=>{
  const original=await sharp({create:{width:90,height:160,channels:3,background:'#b85544'}}).png().toBuffer();
  const f=fixture();
  try{
    const result=await send(f.api,input(original));assert.equal(result.status,202);
    assert.equal(Object.hasOwn(result.body,'deliveryFormat'),false);
    const payload=JSON.parse(f.store.job(result.body.jobId).payload);
    assert.equal(payload.deliveryFormat,'story_photo');assert.equal(payload.gatewayImageMimeType,'image/png');
    const prepared=await f.api.prepare(payload,{waUploadToServer:()=>{throw new Error('must_not_upload_preview');}});
    assert.deepEqual(prepared,{image:original,caption:text,mimetype:'image/png'});
    const meta=await sharp(prepared.image).metadata();assert.equal(meta.width,90);assert.equal(meta.height,160);
    const replay=await send(f.api,input(original));assert.equal(replay.body.jobId,result.body.jobId);
    assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM jobs').get().n,1);
    assert.equal((await send(f.api,input(original,{deliveryFormat:undefined}))).status,409);
  }finally{f.close();}
});

test('private adapter rejects invalid Story format and destinations before enqueue',async()=>{
  const original=await sharp({create:{width:9,height:16,channels:3,background:'#ffffff'}}).png().toBuffer();
  const f=fixture();
  try{
    const invalid=[{deliveryFormat:'photo'},{deliveryFormat:null},{route:'self'},
      {preview:{link,title:'Clube UOL',summary:''}},
      ...[link+'?query=1',link+'#hash',link.replace('clube.','user@clube.'),link.replace('/campanhasdeingresso/','/fotoregistro/'),
        link.replace('.br/','.br:443/'),link.replace(/pPM-.+$/,'utilize'),link.replace(/pPM-.+$/,'pQg-utilize/beneficio')]
        .map(target=>({text:`Story\n${target}`,preview:{link:target,title:'Clube UOL',summary:'',imageBase64:original.toString('base64')}}))];
    for(const overrides of invalid)assert.equal((await send(f.api,input(original,overrides))).status,400);
    assert.equal((await send(f.api,input(Buffer.from('not an image')))).status,415);
    assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM jobs').get().n,0);
    const target='https://clube.uol.com.br/campanhasdeingresso/pQg-2-ingressos-30-10-nubank-parque-sp';
    assert.equal((await send(f.api,input(original,{text:`Story\n${target}`,
      preview:{link:target,title:'Clube UOL',summary:'',imageBase64:original.toString('base64')}}))).status,202);
  }finally{f.close();}
});

test('normal gateway jobs keep the standard text and link preview payload',async()=>{
  const f=fixture();
  try{
    const result=await send(f.api,{route:'uol',keyHash:'a'.repeat(64),requestHash:'b'.repeat(64),text,
      preview:{link,title:'Clube UOL',summary:''}});assert.equal(result.status,202);
    const payload=JSON.parse(f.store.job(result.body.jobId).payload);
    assert.equal(Object.hasOwn(payload,'deliveryFormat'),false);assert.equal(Object.hasOwn(payload,'gatewayImageMimeType'),false);
    assert.deepEqual(await f.api.prepare(payload,{}),{text,linkPreview:{'canonical-url':link,'matched-text':link,title:'Clube UOL',description:''}});
    const dispatched=f.store.claim();f.store.receipt(dispatched.message_id,dispatched.destination,'accepted','server_ack');
    const receipt=await f.api.route({method:'GET'},`/v1/gateway/jobs/${result.body.jobId}`);
    assert.equal(Object.hasOwn(receipt.body,'deliveryFormat'),false);
  }finally{f.close();}
});

test('Story jobs reuse the queue through safe preparation retry and WhatsApp acknowledgement',async()=>{
  const original=await sharp({create:{width:90,height:160,channels:3,background:'#112233'}}).png().toBuffer();
  let clock=Date.now(),attempts=0,sends=0,sent;const f=fixture();
  const run=createSender({store:f.store,getConfig:()=>f.config,whatsapp:f.whatsapp,canSend:()=>true,now:()=>clock,
    prepare:async payload=>{attempts++;if(attempts===1)throw new Error('temporary pre-dispatch failure');return f.api.prepare(payload,{});}});
  f.whatsapp.socket=()=>({sendMessage:async(jid,content,options)=>{sends++;sent=content;f.store.receipt(options.messageId,jid,'accepted','server_ack');}});
  try{
    const result=await send(f.api,input(original));const id=result.body.jobId;
    await run();assert.equal(f.store.job(id).state,'queued');assert.equal(sends,0);
    const replay=await send(f.api,input(original));assert.equal(replay.body.jobId,id);
    clock+=31000;await run();assert.equal(f.store.job(id).state,'accepted');assert.equal(sends,1);
    assert.deepEqual(sent.image,original);assert.equal(sent.caption,text);
    const get=await f.api.route({method:'GET'},`/v1/gateway/jobs/${id}`);
    assert.equal(get.body.state,'accepted');assert.equal(get.body.confirmation,'server_ack');assert.ok(get.body.messageId);
    assert.equal(get.body.deliveryFormat,'story_photo');
    await run();assert.equal(sends,1);
  }finally{f.close();}
});
