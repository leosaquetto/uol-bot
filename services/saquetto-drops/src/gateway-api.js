import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, readdirSync, statSync, writeFileSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import sharp from 'sharp';
import { prepareWAMessageMedia } from '@whiskeysockets/baileys';

const HASH = /^[a-f0-9]{64}$/;
const UUID = /^[a-f0-9-]{36}$/;
const MAX_IMAGE = 5 * 1024 * 1024, MAX_BODY = 8 * 1024 * 1024;
const fail = (code, status = 400) => { throw Object.assign(new Error(code), { status }); };
const hash = value => createHash('sha256').update(value).digest('hex');

// Private, separately authenticated adapter for the existing preview gateway.
// All sends share the normal Drops queue, receipts and destination checks.
export function createGatewayApi({store,dataDir,getConfig,whatsapp,canSend,routes,now=Date.now}) {
  const directory=join(dataDir,'gateway-media');
  mkdirSync(directory,{recursive:true,mode:0o700});
  const imageFile=id=>{if(!HASH.test(id))fail('invalid_image');return join(directory,id);};
  const destination=route=>{
    const alias=routes[route], d=getConfig().destinations[alias];
    if(!['uol','buyticket','self'].includes(route)||!d?.verified||!['group','contact'].includes(d.type))fail('destination_not_allowed',409);
    if(route==='self'&&d.jid!==whatsapp.ownDestination().jid)fail('self_destination_mismatch',409);
    return {alias,...d};
  };
  const status=row=>({jobId:row.id,messageId:row.message_id,state:row.state,confirmation:row.confirmation,code:row.code});
  const getJob=id=>{
    if(!UUID.test(id))fail('job_not_found',404);
    const row=store.job(id);
    if(!row||JSON.parse(row.payload).gateway!==true)fail('job_not_found',404);
    return status(row);
  };
  const readiness=()=>{
    const ready=canSend()&&whatsapp.isReady();
    let mapped=true;
    try { for(const route of ['uol','buyticket','self'])destination(route); } catch {mapped=false;}
    return {ok:ready&&mapped,transport:'baileys',deliveryConfirmation:'baileys_ack_or_receipt',
      components:{transport:whatsapp.isReady(),destinations:mapped,sender:canSend()}};
  };
  const enqueue=async input=>{
    if(!input||typeof input!=='object'||!HASH.test(input.keyHash)||!HASH.test(input.requestHash)||
      typeof input.text!=='string'||!input.text.trim()||Buffer.byteLength(input.text)>1024*1024)fail('invalid_request');
    const d=destination(input.route), key=`gateway:${input.route}:${input.keyHash}`;
    const old=store.jobByKey(key);
    if(old){
      if(JSON.parse(old.payload).gatewayRequestHash!==input.requestHash||old.destination!==d.jid)fail('idempotency_conflict',409);
      return status(old);
    }
    if(!canSend()||!whatsapp.isReady())fail('whatsapp_not_ready',503);
    if(store.db.prepare("SELECT count(*) AS n FROM jobs WHERE state IN ('queued','dispatching')").get().n>=200)fail('queue_full',429);
    const p=input.preview;
    if(p&&(!/^https:\/\//.test(p.link)||typeof p.title!=='string'||p.title.length>500||
      typeof p.summary!=='string'||p.summary.length>2000))fail('invalid_preview');
    let gatewayImage;
    if(p?.imageBase64){
      if(typeof p.imageBase64!=='string'||p.imageBase64.length>Math.ceil(MAX_IMAGE/3)*4||
        !/^[A-Za-z0-9+/]+={0,2}$/.test(p.imageBase64))fail('invalid_image');
      const bytes=Buffer.from(p.imageBase64,'base64');
      if(!bytes.length||bytes.length>MAX_IMAGE)fail('invalid_image',413);
      try { await sharp(bytes,{limitInputPixels:25000000}).metadata(); } catch {fail('invalid_image',415);}
      gatewayImage=hash(bytes);
      const used=readdirSync(directory).filter(name=>HASH.test(name)).reduce((n,name)=>n+statSync(imageFile(name)).size,0);
      if(used+bytes.length>128*1024*1024)fail('media_storage_full',507);
      try {writeFileSync(imageFile(gatewayImage),bytes,{flag:'wx',mode:0o600});}catch(e){if(e.code!=='EEXIST')throw e;}
    }
    // The image read above is asynchronous: recheck gates and destination before enqueue.
    if(!canSend()||!whatsapp.isReady())fail('whatsapp_not_ready',503);
    if(destination(input.route).jid!==d.jid)fail('destination_changed',409);
    const concurrent=store.jobByKey(key);
    if(concurrent){
      if(JSON.parse(concurrent.payload).gatewayRequestHash!==input.requestHash||concurrent.destination!==d.jid)fail('idempotency_conflict',409);
      return status(concurrent);
    }
    if(store.db.prepare("SELECT count(*) AS n FROM jobs WHERE state IN ('queued','dispatching')").get().n>=200)fail('queue_full',429);
    const payload={gateway:true,gatewayRequestHash:input.requestHash,destinationAlias:d.alias,text:input.text,
      expiresAt:now()+30*60000,...(p?{preview:{link:p.link,title:p.title,summary:p.summary}}:{}),
      ...(gatewayImage?{gatewayImage}:{})};
    return status(store.enqueue({key,destination:d.jid,payload,priority:5}));
  };
  return {
    async prepare(payload,socket) {
      const p=payload.preview;
      if(!p)return {text:payload.text,linkPreview:null};
      const linkPreview={'canonical-url':p.link,'matched-text':p.link,title:p.title,description:p.summary};
      if(payload.gatewayImage){
        const bytes=readFileSync(imageFile(payload.gatewayImage));
        const meta=await sharp(bytes,{limitInputPixels:25000000}).metadata();
        const jpegThumbnail=await sharp(bytes).resize(1024,1024,{fit:'inside',withoutEnlargement:true})
          .jpeg({quality:92,chromaSubsampling:'4:4:4'}).toBuffer();
        const uploaded=await prepareWAMessageMedia({image:bytes,jpegThumbnail,width:meta.width,height:meta.height},{upload:socket.waUploadToServer});
        Object.assign(linkPreview,{jpegThumbnail,highQualityThumbnail:uploaded.imageMessage});
      }
      return {text:payload.text,linkPreview};
    },
    cleanup(){
      store.db.prepare("UPDATE jobs SET state='failed',code='request_expired',updated_at=? WHERE state='queued' AND json_extract(payload,'$.gateway')=1 AND json_extract(payload,'$.expiresAt')<?").run(now(),now());
      for(const name of readdirSync(directory).filter(name=>HASH.test(name))){
        if(statSync(imageFile(name)).mtimeMs>now()-7*86400000)continue;
        const used=store.db.prepare("SELECT 1 FROM jobs WHERE state IN ('queued','dispatching') AND json_extract(payload,'$.gatewayImage')=? LIMIT 1").get(name);
        if(!used)unlinkSync(imageFile(name));
      }
    },
    async route(req,path){
      try {
        if(req.method==='GET'&&path==='/v1/gateway/readyz'){
          const body=readiness();return {status:body.ok?200:503,body};
        }
        if(req.method==='GET'&&path.startsWith('/v1/gateway/jobs/'))return {status:200,body:getJob(path.slice('/v1/gateway/jobs/'.length))};
        if(req.method==='POST'&&path==='/v1/gateway/send'){
          if(!String(req.headers['content-type']||'').startsWith('application/json'))fail('json_required',415);
          let size=0;const chunks=[];
          for await(const chunk of req){size+=chunk.length;if(size>MAX_BODY)fail('body_too_large',413);chunks.push(chunk);}
          let input;try{input=JSON.parse(Buffer.concat(chunks).toString());}catch{fail('invalid_json');}
          return {status:202,body:await enqueue(input)};
        }
        return {status:404,body:{code:'not_found'}};
      } catch(e){return {status:e.status||500,body:{code:e.status?e.message:'internal_error'}};}
    },
  };
}
