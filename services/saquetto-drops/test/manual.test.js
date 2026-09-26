import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,readdirSync,statSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Readable} from 'node:stream';
import {randomUUID} from 'node:crypto';
import sharp from 'sharp';
import {openStore} from '../src/store.js';
import {createManualApi,manualAuthorized} from '../src/manual.js';
import {createSender} from '../src/sender.js';

const request=(method,value,type='application/json')=>{
  const bytes=Buffer.isBuffer(value)?value:Buffer.from(JSON.stringify(value??{}));
  const req=Readable.from([bytes]);req.method=method;req.headers={'content-type':type,'content-length':String(bytes.length)};return req;
};
function fixture(){
  const dir=mkdtempSync(join(tmpdir(),'drops-manual-'));
  const store=openStore(join(dir,'state.sqlite'));
  let enabled=true,clock=Date.now();
  const config={destinations:{'eu-mesmo':{type:'contact',jid:'123456@s.whatsapp.net',verified:true},
    group:{type:'group',jid:'987654@g.us',verified:true},blocked:{type:'group',jid:'111@g.us',verified:false}},
    operation:{paused:false,dryRun:false,minDelayMs:5000}};
  const make=()=>createManualApi({store,dataDir:dir,getConfig:()=>config,canSend:()=>enabled,now:()=>clock});
  const api=make();
  return {dir,store,api,config,make,disable(){enabled=false;},advance(ms){clock+=ms;},now:()=>clock,
    close(){store.close();rmSync(dir,{recursive:true,force:true});}};
}
test('manual credential is separate, exact and never enabled by missing token',()=>{
  const token='a'.repeat(64);
  assert.equal(manualAuthorized('Bearer '+token,token),true);
  for(const value of ['',token+'x','Bearer '+'b'.repeat(64)])assert.equal(manualAuthorized(value,token),false);
  assert.equal(manualAuthorized('',undefined),false);
});
test('destinations expose only approved aliases; all-or-nothing validation and no raw JIDs',async()=>{
  const f=fixture();try{
    const list=await f.api.route(request('GET'),'/v1/whatsapp/destinations');
    assert.deepEqual(list.body.destinations.map(x=>x.id),['eu-mesmo','group']);
    assert.ok(!JSON.stringify(list).includes('@'));
    for(const destinations of [['eu-mesmo','blocked'],['123456@s.whatsapp.net'],['__proto__']]){
      const result=await f.api.route(request('POST',{requestId:randomUUID(),destinations,text:'text',mediaIds:[]}),'/v1/whatsapp/send');
      assert.equal(result.status,400);assert.equal(f.store.snapshot().jobs.length,0);
    }
  }finally{f.close();}
});
test('batch deduplicates aliases, survives recreation, and retries never recreate confirmed/unknown jobs',async()=>{
  const f=fixture();try{
    const data={requestId:randomUUID(),destinations:['group','eu-mesmo','group'],text:'Oi https://example.org',mediaIds:[]};
    const send=api=>api.route(request('POST',data),'/v1/whatsapp/send');
    const first=await send(f.api);assert.equal(first.status,202);assert.equal(first.body.jobs.length,2);
    f.store.updateJob(first.body.jobs[0].id,'confirmed');f.store.updateJob(first.body.jobs[1].id,'unknown');
    f.disable();const again=await send(f.make());assert.equal(again.status,202);
    assert.deepEqual(again.body.jobs.map(x=>x.id),first.body.jobs.map(x=>x.id));
    assert.deepEqual(again.body.jobs.map(x=>x.state),['confirmed','unknown']);
    assert.ok(!JSON.stringify(again).includes('@'));
    const conflict=await f.api.route(request('POST',{...data,text:'changed'}),'/v1/whatsapp/send');assert.equal(conflict.status,409);
    const stat=await f.api.route(request('GET'),'/v1/whatsapp/requests/'+data.requestId);assert.equal(stat.status,200);
    assert.equal((await f.api.route(request('GET'),'/v1/whatsapp/requests/../../auth')).status,404);
  }finally{f.close();}
});
test('uploads validate actual image, reject oversize and spoofed mime, and prepare photos without branding',async()=>{
  const f=fixture();try{
    assert.equal((await f.api.route(request('POST',Buffer.from('not image'),'image/jpeg'),'/v1/whatsapp/media')).status,415);
    const big=request('POST',Buffer.from('x'),'image/jpeg');big.headers['content-length']='9000000';
    assert.equal((await f.api.route(big,'/v1/whatsapp/media')).status,413);
    const png=await sharp({create:{width:30,height:20,channels:4,background:'#8844ee'}}).png().toBuffer();
    assert.equal((await f.api.route(request('POST',png,'image/jpeg'),'/v1/whatsapp/media')).status,415);
    const upload=await f.api.route(request('POST',png,'image/png'),'/v1/whatsapp/media');assert.equal(upload.status,201);
    const mediaId=upload.body.mediaId;
    assert.equal(statSync(join(f.dir,'manual-media',mediaId)).mode&0o777,0o600);
    const data={requestId:randomUUID(),destinations:['group','eu-mesmo'],text:'Legenda',mediaIds:[mediaId,mediaId]};
    const batch=await f.api.route(request('POST',data),'/v1/whatsapp/send');assert.equal(batch.body.jobs.length,4);
    const payloads=batch.body.jobs.map(j=>JSON.parse(f.store.job(j.id).payload));
    assert.deepEqual(payloads.map(p=>p.text),['Legenda','','Legenda','']);
    const prepared=await f.api.prepare(payloads[0]);assert.equal(prepared.caption,'Legenda');assert.equal(prepared.mimetype,'image/png');
    const meta=await sharp(prepared.image).metadata();assert.equal(meta.width,30);assert.equal(meta.height,20);
    assert.equal((await f.api.route(request('POST',{...data,requestId:randomUUID(),text:'a'.repeat(1025)}),'/v1/whatsapp/send')).status,400);
    f.advance(2*86400000);f.api.cleanup();
    assert.ok(batch.body.jobs.every(j=>f.store.job(j.id).code==='request_expired'));
    assert.equal(readdirSync(join(f.dir,'manual-media')).length,0);
  }finally{f.close();}
});
test('manual jobs expire before dispatch and uncertain transport calls are never retried',async()=>{
  const f=fixture();try{
    const r=await f.api.route(request('POST',{requestId:randomUUID(),destinations:['eu-mesmo'],text:'Hi',mediaIds:[]}),'/v1/whatsapp/send');
    let sends=0;const whatsapp={isReady:()=>true,verifyDestination:async()=>{},socket:()=>({sendMessage:async()=>{sends++;throw new Error('timeout');}})};
    const sender=createSender({store:f.store,getConfig:()=>f.config,whatsapp,canSend:()=>true,prepare:f.api.prepare,now:f.now});
    await sender();assert.equal(sends,1);assert.equal(f.store.job(r.body.jobs[0].id).state,'unknown');
    f.advance(6000);await sender();assert.equal(sends,1);
    const r2=await f.api.route(request('POST',{requestId:randomUUID(),destinations:['eu-mesmo'],text:'Later',mediaIds:[]}),'/v1/whatsapp/send');
    f.advance(31*60000);await sender();assert.equal(sends,1);assert.equal(f.store.job(r2.body.jobs[0].id).code,'request_expired');
  }finally{f.close();}
});
test('expiry during image preparation prevents a late send',async()=>{
  const f=fixture();try{
    const r=await f.api.route(request('POST',{requestId:randomUUID(),destinations:['eu-mesmo'],text:'Hi'}),'/v1/whatsapp/send');
    let sends=0;const whatsapp={isReady:()=>true,verifyDestination:async()=>{},socket:()=>({sendMessage:async()=>{sends++;}})};
    const sender=createSender({store:f.store,getConfig:()=>f.config,whatsapp,canSend:()=>true,now:f.now,
      prepare:async()=>{f.advance(31*60000);return {text:'Hi'};}});
    await sender();assert.equal(sends,0);assert.equal(f.store.job(r.body.jobs[0].id).code,'request_expired');
  }finally{f.close();}
});
