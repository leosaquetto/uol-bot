import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,readdirSync,statSync,readFileSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Readable} from 'node:stream';
import {randomUUID} from 'node:crypto';
import sharp from 'sharp';
import {openStore} from '../src/store.js';
import {createManualApi,manualAuthorized,registerMacsendDestination,applySavedMacsendDestination} from '../src/manual.js';
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
test('mac-send preserves text whitespace and its idempotency identity while other aliases trim',async()=>{
  const f=fixture();try{
    f.config.destinations['mac-send']={type:'group',jid:'123456@g.us',verified:true};
    const text='\t\n  copied text\n\n',requestId=randomUUID();
    const input={requestId,destinations:['mac-send'],text};
    const result=await f.api.route(request('POST',input),'/v1/whatsapp/send');assert.equal(result.status,202);
    assert.equal(JSON.parse(f.store.job(result.body.jobs[0].id).payload).text,text);
    assert.equal((await f.api.route(request('POST',input),'/v1/whatsapp/send')).status,202);
    assert.equal((await f.api.route(request('POST',{...input,text:text.trim()}),'/v1/whatsapp/send')).status,409);
    const legacy=await f.api.route(request('POST',{requestId:randomUUID(),destinations:['group'],text}),'/v1/whatsapp/send');
    assert.equal(JSON.parse(f.store.job(legacy.body.jobs[0].id).payload).text,text.trim());
    assert.equal((await f.api.route(request('POST',{...input,requestId:randomUUID(),text:' \t\n'}),'/v1/whatsapp/send')).status,400);
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
test('document uploads stream unchanged bytes, preserve UTF8 names and MIME, and clean both private files',async()=>{
  const f=fixture();try{
    const bytes=Buffer.from([0,255,13,10,80,75,3,4]);
    const req=Readable.from([bytes.subarray(0,3),bytes.subarray(3)]);
    req.method='POST';req.headers={'content-type':'application/octet-stream',
      'x-macsend-filename':encodeURIComponent('../pasta/relatório-📄.zip'),'x-macsend-mime':'application/zip'};
    const uploaded=await f.api.route(req,'/v1/whatsapp/media');assert.equal(uploaded.status,201);
    const id=uploaded.body.mediaId;
    const content=await f.api.prepare({mediaId:id,text:'Arquivo'});
    assert.deepEqual(readFileSync(content.document.url),bytes);
    assert.deepEqual(content,{document:{url:join(f.dir,'manual-media',id)},fileName:'relatório-📄.zip',caption:'Arquivo',mimetype:'application/zip'});
    assert.equal(f.store.db.prepare('SELECT bytes FROM manual_media WHERE id=?').get(id).bytes,bytes.length);
    for(const name of readdirSync(join(f.dir,'manual-media')))assert.equal(statSync(join(f.dir,'manual-media',name)).mode&0o777,0o600);
    // Small files use actual storage rather than each reserving the maximum upload.
    f.store.db.prepare('INSERT INTO manual_media VALUES(?,?,?,?)').run(randomUUID(),'application/octet-stream',500*1024*1024,f.now());
    const small=request('POST',Buffer.from('small'),'application/octet-stream');
    small.headers['x-macsend-filename']='small.txt';small.headers['x-macsend-mime']='text/plain';
    assert.equal((await f.api.route(small,'/v1/whatsapp/media')).status,201);
    f.advance(2*86400000);f.api.cleanup();
    assert.equal(readdirSync(join(f.dir,'manual-media')).length,0);
  }finally{f.close();}
});
test('document uploads reject oversized, invalid metadata and interrupted bodies without residual data',async()=>{
  const f=fixture();try{
    const raw=()=>{const req=request('POST',Buffer.from('x'),'application/octet-stream');
      req.headers['x-macsend-filename']='file.bin';req.headers['x-macsend-mime']='application/octet-stream';return req;};
    for(const [key,value,status] of [['content-length',String(100*1024*1024+1),413],
      ['x-macsend-filename','%ZZ',400],['x-macsend-filename','..',400],
      ['x-macsend-mime','text/plain\r\nInjected: yes',400],['content-length','-1',400]]){
      const req=raw();req.headers[key]=value;
      assert.equal((await f.api.route(req,'/v1/whatsapp/media')).status,status);
    }
    const mismatch=raw();mismatch.headers['content-length']='2';
    assert.equal((await f.api.route(mismatch,'/v1/whatsapp/media')).status,400);
    const interrupted=Readable.from((async function*(){yield Buffer.from('first');throw new Error('disconnected');})());
    interrupted.method='POST';interrupted.headers=raw().headers;delete interrupted.headers['content-length'];
    assert.equal((await f.api.route(interrupted,'/v1/whatsapp/media')).status,500);
    assert.equal(readdirSync(join(f.dir,'manual-media')).length,0);
    assert.equal(f.store.db.prepare('SELECT COUNT(*) AS n FROM manual_media').get().n,0);
  }finally{f.close();}
});
test('streaming document limit accepts 100 MiB and rejects an extra byte without content-length',async()=>{
  const f=fixture();try{
    const chunk=Buffer.alloc(1024*1024,37);
    const upload=extra=>{
      const req=Readable.from((async function*(){for(let i=0;i<100;i++)yield chunk;if(extra)yield Buffer.from('x');})());
      req.method='POST';req.headers={'content-type':'application/octet-stream','x-macsend-filename':'video.mp4','x-macsend-mime':'video/mp4'};
      return f.api.route(req,'/v1/whatsapp/media');
    };
    const accepted=await upload(false);assert.equal(accepted.status,201);
    assert.equal(statSync(join(f.dir,'manual-media',accepted.body.mediaId)).size,100*1024*1024);
    const rejected=await upload(true);assert.equal(rejected.status,413);
    assert.equal(readdirSync(join(f.dir,'manual-media')).length,2);
    assert.equal(f.store.db.prepare('SELECT COUNT(*) AS n FROM manual_media').get().n,1);
  }finally{f.close();}
});
test('startup removes orphaned uploads and metadata left by interrupted process',()=>{
  const f=fixture();try{
    const id=randomUUID(),directory=join(f.dir,'manual-media');
    writeFileSync(join(directory,id),'partial');writeFileSync(join(directory,id+'.json'),'{}');
    f.make();assert.equal(readdirSync(directory).length,0);
  }finally{f.close();}
});
test('mac-send registration refuses ambiguous groups and atomically preserves other routes',async()=>{
  const f=fixture();try{
    const configPath=join(f.dir,'config.json'),destinationPath=join(f.dir,'macsend-destination.private.json');
    let config=JSON.parse(readFileSync(new URL('../config.example.json',import.meta.url),'utf8'));
    const before=structuredClone(config);writeFileSync(configPath,JSON.stringify(config));
    assert.deepEqual(applySavedMacsendDestination(config,destinationPath),config);
    const original=readFileSync(configPath,'utf8');let matches=[],writable=true;
    const whatsapp={isReady:()=>true,findGroupsByName:async name=>{assert.equal(name,'mac-send 💻');return matches;},
      verifyDestination:async d=>{assert.equal(d.jid,'12345@g.us');return {writable,name:'mac-send 💻'};},
      ownDestination:()=>({jid:'551199998888@s.whatsapp.net'})};
    const register=()=>registerMacsendDestination({input:{name:'mac-send 💻',alias:'mac-send'},whatsapp,destinationPath,
      getConfig:()=>config,setConfig:next=>{config=next;}});
    for(const groups of [[],[{jid:'12345@g.us'},{jid:'67890@g.us'}]]){
      matches=groups;const result=await register();assert.equal(result.status,409);assert.equal(result.body.matches,groups.length);
      assert.equal(readFileSync(configPath,'utf8'),original);
    }
    matches=[{jid:'12345@g.us'}];writable=false;
    await assert.rejects(register,/destination_not_writable/);assert.equal(readFileSync(configPath,'utf8'),original);
    writable=true;const result=await register();assert.equal(result.status,200);
    assert.deepEqual(result.body,{name:'mac-send 💻',matches:1,alias:'mac-send',sender:'****8888'});
    assert.deepEqual(config.destinations['mac-send'],{type:'group',jid:'12345@g.us',verified:true});
    const {['mac-send']:registered,...rest}=config.destinations;assert.deepEqual(rest,before.destinations);
    assert.deepEqual(config.rules,before.rules);assert.equal(readFileSync(configPath,'utf8'),original);
    assert.deepEqual(applySavedMacsendDestination(before,destinationPath),config);
    const current=config;
    await assert.rejects(()=>registerMacsendDestination({input:{name:'mac-send 💻',alias:'mac-send'},whatsapp,
      destinationPath:join(f.dir,'missing','destination.json'),getConfig:()=>config,setConfig:next=>{config=next;}}));
    assert.equal(config,current);
    assert.equal(statSync(destinationPath).mode&0o777,0o600);
    assert.deepEqual(Object.keys(JSON.parse(readFileSync(destinationPath))),['name','alias','destination']);
    writeFileSync(destinationPath,'{broken');
    assert.equal(applySavedMacsendDestination(config,destinationPath).destinations['mac-send'],undefined);
    const routed=structuredClone(config);routed.rules[0].destinations.push('mac-send');
    assert.equal(applySavedMacsendDestination(routed,destinationPath).destinations['mac-send'].verified,false);
    assert.equal(readdirSync(f.dir).some(name=>name.endsWith('.tmp')),false);
    assert.equal(JSON.stringify(result).includes('@'),false);
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
