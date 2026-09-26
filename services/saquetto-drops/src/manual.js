import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';
import { mkdirSync, writeFileSync, readFileSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import sharp from 'sharp';

const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const MAX_UPLOAD = 8 * 1024 * 1024, MAX_STORAGE = 128 * 1024 * 1024;
const DAY = 86400000, SEND_TTL = 30 * 60000;
const names = {
  'eu-mesmo':'Eu mesmo', 'gabfilho':'gabfilho', 'amanda-costa':'Amanda Costa',
  'os-quatro-faustinhos':'Os quatro faustinhos', 'lover-tour':'Lover Tour',
  'osten-a-origem':'Osten - A Origem', 'bass-persuades':'BASS PERSUADES',
  'festivais-shows-e-afins':'Festivais, Shows e Afins!', 'motopapis':'motopapis',
  'lollapalooza-2027':'Lollapalooza 2027', 'ev-sanctuary':'EV SANCTUARY',
};
const fail = (code, status = 400) => { throw Object.assign(new Error(code), { status }); };
const hash = value => createHash('sha256').update(value).digest('hex');
export function manualAuthorized(value, token) {
  return typeof token === 'string' && /^[a-f0-9]{64}$/.test(token) &&
    timingSafeEqual(Buffer.from(hash(String(value || '').replace(/^Bearer\s+/i,''))), Buffer.from(hash(token)));
}
async function body(req, limit) {
  if (Number(req.headers['content-length'] || 0) > limit) fail('body_too_large',413);
  const chunks=[];let size=0;
  for await(const chunk of req) {
    size+=chunk.length;if(size>limit)fail('body_too_large',413);chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}
export function createManualApi({store,dataDir,getConfig,canSend,now=Date.now}) {
  const {db}=store, directory=join(dataDir,'manual-media');
  mkdirSync(directory,{recursive:true,mode:0o700});
  db.exec(`CREATE TABLE IF NOT EXISTS manual_media(id TEXT PRIMARY KEY,mime TEXT NOT NULL,bytes INTEGER NOT NULL,created_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS manual_requests(id TEXT PRIMARY KEY,hash TEXT NOT NULL,created_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS manual_jobs(request_id TEXT NOT NULL,job_id TEXT UNIQUE NOT NULL,alias TEXT NOT NULL,media_id TEXT,position INTEGER NOT NULL);`);
  const file = id => {if(!UUID.test(id))fail('invalid_media');return join(directory,id);};
  const destinations=()=>Object.entries(getConfig().destinations)
    .filter(([,d])=>d.verified && ['group','contact'].includes(d.type))
    .map(([id,d])=>({id,name:names[id]||id,type:d.type}));
  const status = requestId => {
    if(!UUID.test(requestId)||!db.prepare('SELECT id FROM manual_requests WHERE id=?').get(requestId))fail('request_not_found',404);
    return {requestId,jobs:db.prepare(`SELECT j.id,m.alias AS destination,j.state,j.code,j.confirmation
      FROM manual_jobs m JOIN jobs j ON j.id=m.job_id WHERE m.request_id=? ORDER BY m.position`).all(requestId)};
  };
  let uploading=false, lastCleanup=0;
  const cleanup=()=>{
    // Expire unsent work even while WhatsApp is offline or the sender is paused.
    db.prepare(`UPDATE jobs SET state='failed',code='request_expired',updated_at=?
      WHERE state='queued' AND id IN (SELECT job_id FROM manual_jobs)
      AND json_extract(payload,'$.expiresAt')<?`).run(now(),now());
    if(now()-lastCleanup<3600000)return;
    lastCleanup=now();
    for(const row of db.prepare(`SELECT id FROM manual_media WHERE created_at<? AND NOT EXISTS
      (SELECT 1 FROM manual_jobs m JOIN jobs j ON j.id=m.job_id
       WHERE m.media_id=manual_media.id AND j.state IN ('queued','dispatching'))`).all(now()-DAY)) {
      try{unlinkSync(file(row.id));}catch(e){if(e.code!=='ENOENT')continue;}
      db.prepare('DELETE FROM manual_media WHERE id=?').run(row.id);
    }
  };
  const upload=async req=>{
    if(!canSend())fail('sending_paused',409);
    if(uploading)fail('upload_busy',429);
    const mime=String(req.headers['content-type']||'').split(';')[0].toLowerCase();
    if(!['image/jpeg','image/png'].includes(mime))fail('unsupported_image',415);
    uploading=true;
    try {
      cleanup();
      if(db.prepare('SELECT COALESCE(SUM(bytes),0) AS n FROM manual_media').get().n+MAX_UPLOAD>MAX_STORAGE)fail('media_storage_full',507);
      const bytes=await body(req,MAX_UPLOAD);
      let output;
      try {
        const input=sharp(bytes,{limitInputPixels:25000000,failOn:'warning'});
        const meta=await input.metadata();
        if(meta.format!==(mime==='image/jpeg'?'jpeg':'png')||(meta.pages||1)>1)fail('unsupported_image',415);
        const resized=input.rotate().resize(2560,2560,{fit:'inside',withoutEnlargement:true});
        output=await (mime==='image/jpeg'?resized.jpeg({quality:92,chromaSubsampling:'4:4:4'}):resized.png()).toBuffer();
      } catch(error){if(error.status)throw error;fail('invalid_image',415);}
      if(output.length>MAX_UPLOAD)fail('image_too_large',413);
      const mediaId=randomUUID();writeFileSync(file(mediaId),output,{flag:'wx',mode:0o600});
      try {db.prepare('INSERT INTO manual_media VALUES(?,?,?,?)').run(mediaId,mime,output.length,now());}
      catch(error){unlinkSync(file(mediaId));throw error;}
      return {mediaId};
    } finally {uploading=false;}
  };
  const enqueue=input=>{
    if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).some(k=>!['requestId','destinations','text','mediaIds'].includes(k)))fail('invalid_request');
    const {requestId}=input;
    if(typeof requestId!=='string'||!UUID.test(requestId))fail('invalid_request_id');
    if(!Array.isArray(input.destinations)||!input.destinations.length||input.destinations.length>20||input.destinations.some(a=>typeof a!=='string'))fail('invalid_destinations');
    if(typeof input.text!=='string'||input.text.length>8000)fail('text_too_long');
    const mediaIds=input.mediaIds??[];
    if(!Array.isArray(mediaIds)||mediaIds.length>10||mediaIds.some(id=>typeof id!=='string'||!UUID.test(id)))fail('invalid_media');
    const aliases=[...new Set(input.destinations)].sort(),text=input.text.trim();
    if(!text&&!mediaIds.length)fail('empty_message');
    if(mediaIds.length&&text.length>1024)fail('caption_too_long');
    const fingerprint=hash(JSON.stringify({aliases,text,mediaIds}));
    const existing=db.prepare('SELECT hash FROM manual_requests WHERE id=?').get(requestId);
    if(existing){if(existing.hash!==fingerprint)fail('idempotency_conflict',409);return status(requestId);}
    if(!canSend())fail('sending_paused',409);
    const allowed=new Set(destinations().map(d=>d.id));
    if(aliases.some(a=>!allowed.has(a)))fail('destination_not_allowed',400);
    for(const id of mediaIds){const m=db.prepare('SELECT created_at FROM manual_media WHERE id=?').get(id);if(!m||m.created_at<now()-DAY)fail('media_expired',410);}
    const count=aliases.length*Math.max(1,mediaIds.length);
    if(db.prepare("SELECT COUNT(*) AS n FROM jobs WHERE state IN ('queued','dispatching')").get().n+count>200)fail('queue_full',429);
    store.transaction(()=>{
      db.prepare('INSERT INTO manual_requests VALUES(?,?,?)').run(requestId,fingerprint,now());
      let position=0;
      for(const alias of aliases)for(const [index,mediaId] of (mediaIds.length?mediaIds:[null]).entries()){
        const destination=getConfig().destinations[alias].jid;
        const payload={manual:true,requestId,destinationAlias:alias,text:index===0?text:'',mediaId,expiresAt:now()+SEND_TTL};
        const serialized=JSON.stringify(payload),id=randomUUID();
        db.prepare(`INSERT INTO jobs(id,dedup_key,request_hash,destination,payload,priority,created_at,updated_at)
          VALUES(?,?,?,?,?,?,?,?)`).run(id,`manual:${requestId}:${alias}:${index}`,hash(JSON.stringify({destination,payload})),destination,serialized,10,now()+position,now());
        db.prepare('INSERT INTO manual_jobs VALUES(?,?,?,?,?)').run(requestId,id,alias,mediaId,position++);
      }
    });
    return status(requestId);
  };
  cleanup();
  return {
    cleanup,
    async prepare(payload) {
      if(!payload.mediaId)return {text:payload.text,linkPreview:null};
      const media=db.prepare('SELECT mime FROM manual_media WHERE id=?').get(payload.mediaId);
      if(!media)fail('media_expired',410);
      return {image:readFileSync(file(payload.mediaId)),caption:payload.text,mimetype:media.mime};
    },
    async route(req,path) {
      try {
        if(req.method==='GET'&&path==='/v1/whatsapp/destinations')return {status:200,body:{destinations:destinations()}};
        if(req.method==='GET'&&path.startsWith('/v1/whatsapp/requests/'))return {status:200,body:status(path.slice('/v1/whatsapp/requests/'.length))};
        if(req.method==='POST'&&path==='/v1/whatsapp/media')return {status:201,body:await upload(req)};
        if(req.method==='POST'&&path==='/v1/whatsapp/send'){
          if(!String(req.headers['content-type']||'').startsWith('application/json'))fail('json_required',415);
          let input;try{input=JSON.parse((await body(req,65536)).toString());}catch(e){if(e.status)throw e;fail('invalid_json');}
          return {status:202,body:enqueue(input)};
        }
        return {status:404,body:{code:'not_found'}};
      } catch(e) {return {status:e.status||500,body:{code:e.status?e.message:'internal_error'}};}
    },
  };
}
