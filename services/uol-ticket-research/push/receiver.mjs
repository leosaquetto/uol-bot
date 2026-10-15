import { resolve, dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
// This shared module is transport cryptography only; it does not import the X adapter.
import { decryptPush } from '../../saquetto-drops/src/webpush-crypto.js';
import { privateRead, validateRegistration } from './private.mjs';
import { openPushStore, PUSH_STORE_LIMITS } from './store.mjs';
import { classifyInstagramPush } from './classify.mjs';

export function processPending(store, registration, decrypt=decryptPush) {
  for (const event of store.pending()) {
    let signal=null, reason='unrecognized_instagram_push';
    try { signal=classifyInstagramPush(decrypt(JSON.parse(event.packet),registration)); }
    catch { reason='push_decryption_rejected'; }
    store.finish(event,signal,signal ? null : reason);
  }
}

export function observeInstagramPush({registration,store,WebSocketImpl=WebSocket,logger=console,
  handshakeMs=15000,heartbeatMs=240000,pongMs=45000,retryMs=2000,
  maintenanceMs=1000,retentionMs=60000,decrypt=decryptPush,onTerminal=()=>{}}) {
  const r=validateRegistration(registration);
  if (r.instagramRegistration?.status !== 'accepted') throw new Error('instagram_registration_not_confirmed');
  let socket,stopped=false,ready=false,waitingForCapacity=false,retry,handshake,heartbeat,pong,maintenance,attempts=0;
  const diagnostic=(...keys)=>store.diagnostics?.(keys);
  const state = status => { store.health(status); logger.log(JSON.stringify({event:status})); };
  const timers=()=>{clearTimeout(handshake);clearInterval(heartbeat);clearTimeout(pong);};
  const halt=(status,terminal=true)=>{
    if(stopped)return;
    stopped=true;ready=false;timers();clearTimeout(retry);clearTimeout(maintenance);
    try { if(status)state(status); }
    catch { /* Storage failure must not prevent socket and process cleanup. */ }
    finally {
      try { socket?.close(); } catch { /* Already closed or unavailable. */ }
      finally { if(terminal)onTerminal(status); }
    }
  };
  diagnostic('receiver_start');processPending(store,r,decrypt);
  const sweep=()=>{
    if(stopped)return;
    try {
      processPending(store,r,decrypt);
      const result=store.maintain?.();
      if(waitingForCapacity && !store.capacity?.().pressure) {
        waitingForCapacity=false;connect();
      }
      maintenance=setTimeout(sweep,waitingForCapacity||result?.remaining?maintenanceMs:retentionMs);
    } catch { halt('push_storage_unavailable'); }
  };
  const initial=store.maintain?.();
  const capacity=store.capacity?.();
  waitingForCapacity=Boolean(capacity?.pressure);
  maintenance=setTimeout(sweep,waitingForCapacity||initial?.remaining?maintenanceMs:retentionMs);
  const pauseForCapacity=()=>{
    waitingForCapacity=true;ready=false;timers();clearTimeout(retry);clearTimeout(maintenance);
    state('push_payload_limit');socket?.close();
    maintenance=setTimeout(sweep,maintenanceMs);
  };
  const connect=()=>{
    if(stopped||waitingForCapacity)return;
    diagnostic('connect_attempt');
    state('connecting');const ws=new WebSocketImpl('wss://push.services.mozilla.com/');socket=ws;
    handshake=setTimeout(()=>ws.close(),handshakeMs);
    ws.addEventListener('open',()=>{
      if(stopped||socket!==ws)return;
      try {
        diagnostic('connection_open');
        ws.send(JSON.stringify({messageType:'hello',uaid:r.uaid,use_webpush:true,channelIDs:[r.channelID]}));
      } catch { halt('push_storage_unavailable'); }
    });
    ws.addEventListener('message',({data})=>{
      if(stopped||waitingForCapacity||socket!==ws)return;
      try {
        diagnostic('frames_received');
        const reject=key=>{diagnostic(key);ready=false;state('frame_rejected');ws.close();};
        if(typeof data!=='string')return reject('frames_rejected_type');
        if(Buffer.byteLength(data,'utf8')>PUSH_STORE_LIMITS.packetBytes)return reject('frames_rejected_size');
        let m;
        try { m=JSON.parse(data); } catch { return reject('frames_rejected_json'); }
        if(!m||typeof m!=='object'||Array.isArray(m))return reject('frames_rejected_shape');
        if(m.messageType==='hello') {
          if(ready||m.status!==200||m.uaid!==r.uaid||m.use_webpush!==true) {
            diagnostic('frames_rejected_hello');return halt('registration_requires_review');
          }
          diagnostic('connection_ready');clearTimeout(handshake);attempts=0;ready=true;state('connected');
          processPending(store,r,decrypt);
          heartbeat=setInterval(()=>{ws.send('{}');pong=setTimeout(()=>ws.close(),pongMs);},heartbeatMs);
        } else if(m.messageType==='notification') {
          if(!ready)return reject('frames_rejected_ready');
          if(m.channelID!==r.channelID)return reject('frames_rejected_channel');
          if(typeof m.version!=='string'||!m.version.length||m.version.length>512)return reject('frames_rejected_version');
          if(typeof m.data!=='string'||m.data.length>90000)return reject('frames_rejected_data');
          try { store.persist(m); } catch(error) {
            const reason=['push_pending_limit','push_payload_limit','push_identity_limit','push_packet_limit',
              'push_version_conflict'].includes(error.message)?error.message:'push_storage_unavailable';
            diagnostic('storage_rejected_'+(reason==='push_storage_unavailable'?'unavailable':reason.slice(5)));
            if(reason==='push_payload_limit')return pauseForCapacity();
            return halt(reason);
          }
          // FULL SQLite commit happens before ACK. A crash after ACK recovers pending packets.
          diagnostic('ack_attempted');
          try { ws.send(JSON.stringify({messageType:'ack',updates:[{channelID:m.channelID,version:m.version,code:100}]})); }
          catch { diagnostic('ack_failed');ready=false;state('connection_error');ws.close();return; }
          // send() returning proves a local write only, not a remote ACK receipt.
          diagnostic('ack_sent');
          processPending(store,r,decrypt);
        } else if(!m.messageType||m.messageType==='ping') { clearTimeout(pong);state('connected'); }
        else diagnostic('frames_unknown_message_type');
      } catch { halt('push_storage_unavailable'); }
    });
    ws.addEventListener('error',()=>{
      if(socket===ws&&!stopped&&!waitingForCapacity) {
        try { diagnostic('connection_error');ready=false;state('connection_error'); }
        catch { halt('push_storage_unavailable'); }
      }
    });
    ws.addEventListener('close',()=>{
      if(socket!==ws||stopped)return;
      ready=false;timers();
      try {
        diagnostic('connection_closed');
        if(!waitingForCapacity){state('disconnected');retry=setTimeout(()=>{
          try { connect(); } catch { halt('push_storage_unavailable'); }
        },Math.min(60000,retryMs*2**Math.min(attempts++,5)));}
      } catch { halt('push_storage_unavailable'); }
    });
  };
  try {
    if(waitingForCapacity)state(capacity.payloadBytes>=capacity.limits.payloadBytes?'push_payload_limit':'push_payload_pressure');
    else connect();
  } catch(error) { halt('push_storage_unavailable');throw error; }
  return {isReady:()=>ready&&!stopped,stop(){if(!stopped)halt(waitingForCapacity?null:'stopped',false);}};
}

export function main({file=resolve(process.argv[2] || ''),openStore=openPushStore,observeOptions={}}={}) {
  process.umask(0o077);
  const registration=privateRead(file),store=openStore(join(dirname(file),'push.sqlite'));
  let closed=false;
  const close=()=>{
    if(closed)return;
    closed=true;
    try { store.close(); } catch { process.exitCode=1; }
  };
  try {
    const receiver=observeInstagramPush({...observeOptions,registration,store,
      onTerminal(){process.exitCode=1;close();}});
    const stop=()=>{try { receiver.stop(); } finally { close(); }};
    process.once('SIGTERM',stop);process.once('SIGINT',stop);
  } catch(error) { close();throw error; }
}
if(process.argv[1] && import.meta.url===pathToFileURL(resolve(process.argv[1])).href) {
  try { main(); } catch { console.error(JSON.stringify({status:'failed',reason:'private_push_start_failed'}));process.exitCode=1; }
}
