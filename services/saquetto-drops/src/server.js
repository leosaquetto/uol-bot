import { createServer } from 'node:http';
import { createHash, timingSafeEqual } from 'node:crypto';
import { readFileSync, writeFileSync, unlinkSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { loadConfig, matchRules, routedDestinationsVerified } from './config.js';
import { openStore } from './store.js';
import { observeWebPush, decodePushEvent, validateRegistration } from './webpush.js';
import { createProcessor } from './processor.js';
import { startWhatsApp } from './whatsapp.js';
import { createSender, prepareContent } from './sender.js';
import { createManualApi, manualAuthorized } from './manual.js';
import { acquireLock } from './process-lock.js';
import { createPilot } from './pilot.js';

process.umask(0o077);
const data = process.env.DROPS_DATA || '/var/lib/saquetto-drops';
const configPath = process.env.DROPS_CONFIG || '/etc/saquetto-drops/config.json';
const token = process.env.DROPS_TOKEN;
const manualToken = process.env.DROPS_MANUAL_TOKEN;
if (!token || !/^[a-f0-9]{64}$/.test(token)) throw new Error('DROPS_TOKEN_required');
mkdirSync(data,{recursive:true,mode:0o700});
const releaseLock=acquireLock(join(data,'service.lock'));
const store = openStore(join(data,'drops.sqlite'));
const pushTransport=process.env.DROPS_PUSH_TRANSPORT || 'webpush';
if (!['cdp','webpush'].includes(pushTransport)) throw new Error('invalid_push_transport');
const registration=pushTransport==='webpush' ? validateRegistration(JSON.parse(readFileSync(
  process.env.DROPS_PUSH_REGISTRATION || join(data,'webpush.private.json'),'utf8'))) : null;
let {config} = loadConfig(configPath), configError = null, observer = null, observerStarting = false;
let qrAvailable = false, closing = false;
const qrPath = join(data,'pairing-qr.private.txt');
const whatsapp = process.env.DROPS_WHATSAPP === '1' ? startWhatsApp({store,onQr(qr) {
  qrAvailable = Boolean(qr);
  if (qr) writeFileSync(qrPath,qr,{mode:0o600});
  else { try {unlinkSync(qrPath);} catch {} }
}}) : {state:()=> 'disabled',isReady:()=>false,stop:async()=>{}};
const acceptance = () => {
  try { return JSON.parse(readFileSync(join(data,'acceptance.private.json'),'utf8')); } catch { return {}; }
};
const gates = () => {
  const a = acceptance();
  return { realPush:a.realPush===true, pushRestart:a.pushRestart===true,
    pushContinuity:pushTransport==='webpush' ? a.pushContinuity===true : a.recordingRenewal===true, capacity:a.capacity===true,
    whatsappPilot:a.whatsappPilot===true, previewPilot:a.previewPilot===true };
};
const canSend = () => Object.values(gates()).every(Boolean);
const canPilot = alias => !closing && process.env.DROPS_PILOT==='1' && gates().realPush && gates().pushRestart &&
  (!alias || alias==='self');
const decodeEvent=event=>registration?decodePushEvent(event,registration):event;
const pilot=createPilot({store,whatsapp,enabled:canPilot,decodeEvent});
const processEvents = createProcessor({store,getConfig:()=>config,getContext:()=>observer?.context,
  decodeEvent});
const manual=createManualApi({store,dataDir:data,getConfig:()=>config,
  canSend:()=>!closing && canSend() && !config.operation.paused && !config.operation.dryRun});
const sendNext = createSender({store,getConfig:()=>config,whatsapp,canSend,canPilot,
  prepare:(payload,socket)=>payload.manual===true?manual.prepare(payload):prepareContent(payload,socket)});
const connectObserver = async () => {
  if (closing || observerStarting || observer?.handlesReconnect || observer?.isReady()) return;
  observerStarting = true;
  try {
    if (observer) await observer.stop().catch(()=>{});
    const onEvent=()=>processEvents();
    observer = registration ? observeWebPush({registration,store,onEvent}) :
      await (await import('./push.js')).observePush({endpoint:process.env.DROPS_CDP || 'http://127.0.0.1:9225',store,
        onEvent:()=>onEvent().catch(()=>{})});
    await processEvents();
  } catch { observer = null; }
  finally { observerStarting = false; }
};
const timers = [setInterval(()=>connectObserver(),10000),setInterval(()=>sendNext().catch(()=>{}),1000),
  setInterval(()=>{try{manual.cleanup();}catch{}},60000)];
await connectObserver();
const equal = value => timingSafeEqual(createHash('sha256').update(value).digest(),createHash('sha256').update(token).digest());
const safeJob = row => ({id:row.id,state:row.state,attempts:row.attempts,code:row.code,confirmation:row.confirmation});
const status = () => ({ok:true,mode:config.operation.dryRun?'dry_run':'live',paused:config.operation.paused,
  pushTransport,observerState:observer?.state?.() || null,
  pilotEnabled:canPilot(),
  pilotGroupEnabled:canPilot('lover-tour'),
  observerConnected:observer?.isReady()===true,whatsapp:whatsapp.state(),pairingRequired:qrAvailable,
  configError,gates:gates(),...store.snapshot()});

const server = createServer(async (req,res) => {
  const reply = (code,value) => {res.writeHead(code,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(value));};
  const path = new URL(req.url,'http://localhost').pathname;
  if(path.startsWith('/v1/whatsapp/')) {
    if(!manualAuthorized(req.headers.authorization,manualToken))return reply(401,{code:'unauthorized'});
    const result=await manual.route(req,path);return reply(result.status,result.body);
  }
  if (!equal(String(req.headers.authorization || '').replace(/^Bearer\s+/i,''))) return reply(401,{code:'unauthorized'});
  try {
    if (req.method==='GET' && path==='/v1/status') return reply(200,status());
    if (req.method==='GET' && /^\/v1\/messages\/[a-f0-9-]+$/.test(path)) {
      const row=store.job(path.split('/').at(-1));return reply(row?200:404,row?safeJob(row):{code:'not_found'});
    }
    if (req.method==='GET' && path==='/v1/pending') return reply(200,{
      jobs:store.db.prepare("SELECT * FROM jobs WHERE state IN ('queued','unknown','failed') LIMIT 100").all().map(safeJob),
      events:store.db.prepare("SELECT id,state,code FROM push_events WHERE state='pending_review' LIMIT 100").all(),
    });
    if (req.method==='POST' && path==='/v1/config/reload') {
      const loaded = loadConfig(configPath,config);config=loaded.config;configError=loaded.error;
      if (store.getSetting('paused')==='true') config.operation.paused=true;
      return reply(configError?400:200,{ok:!configError,code:configError||'reloaded'});
    }
    if (req.method==='POST' && path==='/v1/pause') {store.setSetting('paused','true');config.operation.paused=true;return reply(200,{paused:true});}
    if (req.method==='POST' && path==='/v1/activate') {
      if (!canSend() || !whatsapp.isReady() || !observer?.isReady() || config.operation.dryRun ||
          !routedDestinationsVerified(config)) return reply(409,{code:'acceptance_incomplete',gates:gates()});
      if (!store.getSetting('activated_at')) store.setSetting('activated_at',new Date().toISOString());
      store.setSetting('paused','false');config.operation.paused=false;
      return reply(200,{activatedAt:store.getSetting('activated_at')});
    }
    if (req.method==='POST' && path==='/v1/simulate') {
      const chunks=[];let size=0;
      for await (const chunk of req) {size+=chunk.length;if(size>65536)return reply(413,{code:'body_too_large'});chunks.push(chunk);}
      const post=JSON.parse(Buffer.concat(chunks).toString());
      if (typeof post.text!=='string' || typeof post.author!=='string' || typeof post.type!=='string') return reply(400,{code:'invalid_post'});
      return reply(200,{destinations:matchRules(config,post),sent:false});
    }
    if(req.method==='POST' && path==='/v1/destinations/verify') {
      if(!whatsapp.isReady())return reply(409,{code:'whatsapp_not_ready'});
      const chunks=[];let size=0;
      for await(const chunk of req){size+=chunk.length;if(size>4096)return reply(413,{code:'body_too_large'});chunks.push(chunk);}
      const {alias}=JSON.parse(Buffer.concat(chunks).toString());
      if(typeof alias!=='string' || !Object.hasOwn(config.destinations,alias))return reply(404,{code:'destination_not_found'});
      const result=await whatsapp.verifyDestination(config.destinations[alias]);
      return reply(200,{alias,...result,sent:false});
    }
    if(req.method==='POST' && ['/v1/pilot/verify','/v1/pilot/send'].includes(path)) {
      if(!canPilot())return reply(409,{code:'pilot_disabled'});
      const chunks=[];let size=0;
      for await(const chunk of req){size+=chunk.length;if(size>4096)return reply(413,{code:'body_too_large'});chunks.push(chunk);}
      const request=JSON.parse(Buffer.concat(chunks).toString());
      if(path==='/v1/pilot/verify')return reply(200,await pilot.verify(request.alias));
      const row=await pilot.enqueue(request);return reply(202,safeJob(row));
    }
    return reply(404,{code:'not_found'});
  } catch {return reply(400,{code:'request_failed'});}
});
server.requestTimeout=45000;server.headersTimeout=10000;
if (store.getSetting('paused')==='true') config.operation.paused=true;
server.listen(Number(process.env.DROPS_PORT || 8788),'127.0.0.1');
const shutdown = async () => {
  if(closing)return;closing=true;timers.forEach(clearInterval);server.close();
  await observer?.stop().catch(()=>{});await whatsapp.stop();store.close();releaseLock();process.exit(0);
};
process.on('SIGTERM',shutdown);process.on('SIGINT',shutdown);
