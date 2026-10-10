import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdtempSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {DatabaseSync} from 'node:sqlite';
import test from 'node:test';
import sharp from 'sharp';
import {createGateway} from '../src/gateway.js';
import {createDropsTransport} from '../src/drops-transport.js';

const link='https://clube.uol.com.br/campanhasdeingresso/pPM-2-ingressos-bgs-distrito-anhembi-sp';
const imageUrl='https://cdn.discordapp.com/attachments/123/456/story.png';
const text=`Story do Clube UOL\n${link}`;
const sha=value=>createHash('sha256').update(value).digest('hex');
const key='uol:instagram:4004185955500703427:abcdef:v1';
const request=(overrides={},idempotencyKey=key,path='/v1/send-offer')=>new Request(`http://gateway.test${path}`,{
  method:'POST',headers:{Authorization:'Bearer test-token','Content-Type':'application/json','Idempotency-Key':idempotencyKey},
  body:JSON.stringify({link,text,preview:{title:'Clube UOL',summary:'',imageUrl},deliveryFormat:'story_photo',...overrides}),
});
const fixture=(options={})=>{
  const directory=mkdtempSync(join(tmpdir(),'story-gateway-test-'));
  const handler=createGateway({token:'test-token',chatId:'uol-group',transport:'baileys',
    databasePath:join(directory,'deliveries.sqlite'),probeTransport:async()=>({ok:true}),
    fetchImpl:async()=>{throw new Error('unexpected download');},
    sendMessageImpl:async()=>({pendingMessageID:'test-job'}),
    confirmDeliveryImpl:async()=>({state:'accepted',messageId:'accepted-id',
      deliveryState:'accepted_by_whatsapp_server',confirmation:'server_ack',deliveryFormat:'story_photo'}),
    logger:{info(){},warn(){}},...options});
  return {directory,handler,close:()=>rmSync(directory,{recursive:true,force:true})};
};

test('Story photo keeps original vertical bytes and opts into a distinct delivery hash',async()=>{
  const original=await sharp({create:{width:90,height:160,channels:3,background:'#ffaa00'}}).png().toBuffer();
  let sent;
  const f=fixture({fetchImpl:async url=>{assert.equal(url,imageUrl);return new Response(original,{headers:{'Content-Type':'image/png'}});},
    transformPersonalThumbnail:async()=>{throw new Error('must_not_transform_story');},
    sendMessageImpl:async message=>{sent=message;assert.deepEqual(readFileSync(new URL(message.preview.img)),original);
      return {pendingMessageID:'story-job'};}});
  try{
    const accepted=await f.handler(request());assert.equal(accepted.status,202);
    assert.equal((await accepted.json()).deliveryFormat,'story_photo');
    assert.equal(sent.deliveryFormat,'story_photo');assert.equal(sent.text,text);assert.equal(sent.preview.imgType,'image/png');
    assert.equal(sent.requestHash,sha(JSON.stringify({link,text,preview:{link,title:'Clube UOL',summary:'',type:'website',imageUrl},deliveryFormat:'story_photo'})));
    const replay=await f.handler(request());assert.equal(replay.status,200);
    const receipt=await replay.json();assert.equal(receipt.replayed,true);assert.equal(receipt.deliveryFormat,'story_photo');
  }finally{f.close();}
});

test('normal offers retain the preexisting hash and preview format',async()=>{
  let sent;
  const f=fixture({sendMessageImpl:async message=>{sent=message;return {pendingMessageID:'normal-job'};}});
  try{
    const accepted=await f.handler(request({deliveryFormat:undefined,preview:undefined},'uol:normal:v1'));
    assert.equal(accepted.status,202);assert.equal(Object.hasOwn(await accepted.json(),'deliveryFormat'),false);
    assert.equal(Object.hasOwn(sent,'deliveryFormat'),false);
    assert.equal(sent.requestHash,sha(JSON.stringify({link,text,preview:{link,title:'Clube UOL',summary:'',type:'website',imageUrl:''}})));
    assert.equal(sent.preview.link,link);
  }finally{f.close();}
});

test('canonical pQg campaign accepts the Story photo format',async()=>{
  const target='https://clube.uol.com.br/campanhasdeingresso/pQg-2-ingressos-30-10-nubank-parque-sp';
  const f=fixture({fetchImpl:async()=>new Response(Buffer.from('story'),{headers:{'Content-Type':'image/jpeg'}})});
  try{assert.equal((await f.handler(request({link:target,text:`Story\n${target}`}))).status,202);}finally{f.close();}
});

test('Story retry checks the same Drops receipt without downloading or sending again',async()=>{
  const original=await sharp({create:{width:9,height:16,channels:3,background:'#ffaa00'}}).png().toBuffer();
  let downloads=0,sends=0,checks=0;
  const f=fixture({fetchImpl:async()=>{downloads++;return new Response(original,{headers:{'Content-Type':'image/png'}});},
    sendMessageImpl:async()=>{sends++;return {pendingMessageID:'story-pending-job'};},
    confirmDeliveryImpl:async({pendingMessageID})=>{assert.equal(pendingMessageID,'story-pending-job');checks++;
      return checks===1?{state:'pending'}:{state:'delivered',messageId:'story-message',
        deliveryState:'confirmed_by_whatsapp_receipt',confirmation:'participant_receipt',deliveryFormat:'story_photo'};}});
  try{
    assert.equal((await f.handler(request())).status,409);
    const retry=await f.handler(request());assert.equal(retry.status,202);
    const receipt=await retry.json();assert.equal(receipt.confirmation,'participant_receipt');assert.equal(receipt.deliveryFormat,'story_photo');
    assert.equal(downloads,1);assert.equal(sends,1);assert.equal(checks,2);
  }finally{f.close();}
});

test('Story acknowledgement without persisted photo format stays ambiguous without resending',async()=>{
  let sends=0;
  const f=fixture({fetchImpl:async()=>new Response(Buffer.from('story'),{headers:{'Content-Type':'image/jpeg'}}),
    sendMessageImpl:async()=>{sends++;return {pendingMessageID:'story-job'};},
    confirmDeliveryImpl:async()=>({state:'accepted',messageId:'id',deliveryState:'accepted_by_whatsapp_server',confirmation:'server_ack'})});
  try{
    for(let i=0;i<2;i++){
      const response=await f.handler(request());assert.equal(response.status,503);
      const body=await response.json();assert.equal(body.code,'delivery_unknown');assert.equal(Object.hasOwn(body,'deliveryFormat'),false);
    }
    assert.equal(sends,1);
  }finally{f.close();}
});

test('cached Story acceptance missing format reconciles its same queue receipt',async()=>{
  let sends=0,checks=0;
  const f=fixture({fetchImpl:async()=>new Response(Buffer.from('story'),{headers:{'Content-Type':'image/jpeg'}}),
    sendMessageImpl:async()=>{sends++;return {pendingMessageID:'cached-story-job'};},
    confirmDeliveryImpl:async({pendingMessageID})=>{checks++;assert.equal(pendingMessageID,'cached-story-job');
      return {state:'accepted',messageId:'cached-id',deliveryState:'accepted_by_whatsapp_server',confirmation:'server_ack',deliveryFormat:'story_photo'};}});
  try{
    const first=await f.handler(request());const receipt=await first.json();delete receipt.deliveryFormat;
    const database=new DatabaseSync(join(f.directory,'deliveries.sqlite'));
    database.prepare('UPDATE deliveries SET response_json=? WHERE idempotency_key=?').run(JSON.stringify(receipt),key);database.close();
    const replay=await f.handler(request());assert.equal(replay.status,202);assert.equal((await replay.json()).deliveryFormat,'story_photo');
    assert.equal(sends,1);assert.equal(checks,2);
  }finally{f.close();}
});

test('Story format rejects invalid flags, missing media and noncanonical campaigns before dispatch',async()=>{
  let sends=0,downloads=0;
  const f=fixture({sendMessageImpl:async()=>{sends++;},fetchImpl:async()=>{downloads++;}});
  try{
    const invalid=[
      [{deliveryFormat:'photo'},key], [{deliveryFormat:null},key], [{preview:undefined},key],
      [{preview:{imageUrl:'https://example.invalid/story.png'}},key], [{},'uol:normal:v1'],
      ...['https://clube.uol.com.br/fotoregistro/other','https://clube.uol.com.br/campanhasdeingresso/',
        `${link}?tracking=1`,`${link}#fragment`,link.replace('clube.','user@clube.'),link.replace('.br/','.br:444/'),
        link.replace('.br/','.br:443/'),link.replace(/pPM-.+$/,'utilize'),link.replace(/pPM-.+$/,'pQg-utilize/beneficio')]
        .map(target=>[{link:target,text:`Story\n${target}`},key]),
    ];
    for(const [payload,id] of invalid)assert.equal((await f.handler(request(payload,id))).status,400);
    assert.equal((await f.handler(request({},key,'/v1/send-buyticket'))).status,400);
    assert.equal(sends,0);assert.equal(downloads,0);
  }finally{f.close();}
  const legacy=fixture({transport:'beeper',accountId:'account',beeperAccessToken:'token'});
  try{assert.equal((await legacy.handler(request())).status,400);}finally{legacy.close();}
});

test('Drops transport forwards opt-in format and reconciles the same job receipt',async()=>{
  const directory=mkdtempSync(join(tmpdir(),'story-transport-test-'));
  const original=Buffer.from('original-story-bytes');const path=join(directory,'story.png');writeFileSync(path,original);
  const jobId='abcdef01-1234-1234-1234-abcdef123456';let enqueued;let queries=0;
  const transport=createDropsTransport({token:'a'.repeat(64),fetchImpl:async(url,init)=>{
    if(init.method==='POST'){enqueued=JSON.parse(init.body);return Response.json({jobId});}
    queries++;assert.equal(new URL(url).pathname,`/v1/gateway/jobs/${jobId}`);
    return Response.json({state:'accepted',confirmation:'server_ack',messageId:'whatsapp-message',deliveryFormat:enqueued.deliveryFormat});
  }});
  try{
    const receipt=await transport.sendMessage({route:'uol',idempotencyKey:key,requestHash:'b'.repeat(64),text,
      deliveryFormat:'story_photo',preview:{link,title:'Clube UOL',summary:'',img:pathToFileURL(path).href}});
    assert.equal(enqueued.deliveryFormat,'story_photo');assert.equal(enqueued.keyHash,sha(key));
    assert.deepEqual(Buffer.from(enqueued.preview.imageBase64,'base64'),original);
    assert.deepEqual(receipt,{pendingMessageID:jobId});
    const accepted=await transport.confirmDelivery(receipt);assert.equal(accepted.state,'accepted');assert.equal(accepted.deliveryFormat,'story_photo');assert.equal(queries,1);
    await transport.sendMessage({route:'uol',idempotencyKey:'uol:normal:v1',requestHash:'b'.repeat(64),text,preview:{link,title:'Clube UOL',summary:''}});
    assert.equal(Object.hasOwn(enqueued,'deliveryFormat'),false);
    assert.equal(Object.hasOwn(await transport.confirmDelivery(receipt),'deliveryFormat'),false);
  }finally{rmSync(directory,{recursive:true,force:true});}
});
