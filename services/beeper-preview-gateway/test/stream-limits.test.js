import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createGateway } from '../src/gateway.js';
import { createDropsTransport } from '../src/drops-transport.js';
import { httpRequestBody } from '../src/http-body.js';

const token='bounded-test-token';
const link='https://clube.uol.com.br/campanhasdeingresso/teste';
const fixture=overrides=>{
  const dir=mkdtempSync(join(tmpdir(),'gateway-stream-'));
  const handler=createGateway({token,chatId:'uol-chat',buyticketChatId:'',accountId:'wa',beeperAccessToken:'local',
    databasePath:join(dir,'ledger.sqlite'),logger:{info(){},warn(){}},
    sendMessageImpl:async()=>{throw new Error('must_not_dispatch');},confirmDeliveryImpl:async()=>({state:'unknown'}),...overrides});
  return {handler,close(){rmSync(dir,{recursive:true,force:true});}};
};
const headers={'Authorization':`Bearer ${token}`,'Content-Type':'application/json','Idempotency-Key':'uol:bounded:test'};
const request=body=>new Request('http://gateway/v1/send-offer',{method:'POST',headers,body,duplex:'half'});
const endless=(chunkSize,onCancel)=>new ReadableStream({pull(controller){controller.enqueue(new Uint8Array(chunkSize));},cancel:onCancel});

test('UOL rejects chunked body at 64 KiB and cancels before JSON or dispatch',async()=>{
  let cancelled=0;
  const f=fixture({});
  try {
    const response=await f.handler(request(endless(32768,()=>cancelled++)));
    assert.equal(response.status,413);assert.equal((await response.json()).code,'body_too_large');assert.equal(cancelled,1);
  } finally {f.close();}
});

test('declared oversized body is cancelled without reading it',async()=>{
  let cancelled=0;
  const f=fixture({});
  try {
    const body=endless(1,()=>cancelled++);
    const response=await f.handler(new Request('http://gateway/v1/send-offer',{method:'POST',
      headers:{...headers,'Content-Length':'65537'},body,duplex:'half'}));
    assert.equal(response.status,413);assert.equal(cancelled,1);
  } finally {f.close();}
});

test('image without declared length is cancelled above 5 MiB and returns 413 without dispatch',async()=>{
  let cancelled=0,sends=0;
  const f=fixture({sendMessageImpl:async()=>{sends++;},fetchImpl:async()=>new Response(endless(1024*1024,()=>cancelled++),{headers:{'Content-Type':'image/jpeg'}})});
  try {
    const response=await f.handler(request(JSON.stringify({link,text:`Offer\n${link}`,
      preview:{title:'Offer',imageUrl:'https://ddrxgn8ucibei.cloudfront.net/image.jpg'}})));
    assert.equal(response.status,413);assert.equal((await response.json()).code,'preview_image_too_large');
    assert.equal(cancelled,1);assert.equal(sends,0);
  } finally {f.close();}
});

test('authenticated UOL health probes only UOL destination while global health remains degraded',async()=>{
  const seen=[];
  const f=fixture({fetchImpl:async url=>{
    seen.push(url);
    return url.endsWith('/v1/accounts')?Response.json([{accountID:'wa',network:'WhatsApp',status:'connected'}]):
      url.endsWith('/v1/chats/uol-chat')?Response.json({id:'uol-chat',accountID:'wa',network:'WhatsApp'}):new Response('',{status:404});
  }});
  try {
    assert.equal((await f.handler(new Request('http://gateway/v1/readyz/uol'))).status,401);
    const ready=await f.handler(new Request('http://gateway/v1/readyz/uol',{headers:{Authorization:`Bearer ${token}`}}));
    assert.equal(ready.status,200);assert.equal(seen.length,2);
    const publicReady=await f.handler(new Request('http://gateway/readyz/uol'));
    assert.equal(publicReady.status,200);assert.equal('ledger' in await publicReady.json(),false);
    assert.equal((await f.handler(new Request('http://gateway/v1/readyz',{headers:{Authorization:`Bearer ${token}`}}))).status,503);
  } finally {f.close();}
});

test('Baileys UOL health selects the route-specific authenticated Drops endpoint',async()=>{
  const paths=[];
  const drops=createDropsTransport({token:'a'.repeat(64),fetchImpl:async(url,init)=>{
    paths.push(url.pathname);assert.equal(init.headers.Authorization,`Bearer ${'a'.repeat(64)}`);
    return Response.json({ok:true,transport:'baileys',deliveryConfirmation:'baileys_ack_or_receipt',components:{transport:true}});
  }});
  const f=fixture({transport:'baileys',probeTransport:route=>drops.readiness(route)});
  try {
    assert.equal((await f.handler(new Request('http://gateway/v1/readyz/uol',{headers:{Authorization:`Bearer ${token}`}}))).status,200);
    assert.deepEqual(paths,['/v1/gateway/readyz/uol']);
  } finally {f.close();}
});

test('native HTTP adapter writes 413 after cancelling an oversized request',async()=>{
  const f=fixture({});let declared;
  const server=createServer(async(req,res)=>{
    declared=req.headers['content-length'];
    const response=await f.handler(new Request('http://gateway/v1/send-offer',{method:'POST',headers:req.headers,
      body:httpRequestBody(req),duplex:'half'}));
    if(response.status===413)res.setHeader('Connection','close');
    res.writeHead(response.status,Object.fromEntries(response.headers));res.end(await response.text());
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  try {
    const response=await fetch(`http://127.0.0.1:${server.address().port}/v1/send-offer`,{
      method:'POST',headers,duplex:'half',body:new ReadableStream({start(controller){
        for(let n=0;n<3;n++)controller.enqueue(new Uint8Array(32768));controller.close();
      }})});
    assert.equal(response.status,413);assert.equal((await response.json()).code,'body_too_large');assert.equal(declared,undefined);
  } finally {await new Promise(resolve=>server.close(resolve));f.close();}
});
