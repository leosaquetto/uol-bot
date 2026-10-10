import { resolve, dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
// This shared module is transport cryptography only; it does not import the X adapter.
import { decryptPush } from '../../saquetto-drops/src/webpush-crypto.js';
import { privateRead, validateRegistration } from './private.mjs';
import { openPushStore } from './store.mjs';
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
  handshakeMs=15000,heartbeatMs=240000,pongMs=45000,retryMs=2000,decrypt=decryptPush}) {
  const r=validateRegistration(registration);
  if (r.instagramRegistration?.status !== 'accepted') throw new Error('instagram_registration_not_confirmed');
  let socket,stopped=false,ready=false,retry,handshake,heartbeat,pong,attempts=0;
  const state = status => { store.health(status); logger.log(JSON.stringify({event:status})); };
  const timers=()=>{clearTimeout(handshake);clearInterval(heartbeat);clearTimeout(pong);};
  const halt=status=>{stopped=true;ready=false;timers();clearTimeout(retry);state(status);socket?.close();};
  processPending(store,r,decrypt);
  const connect=()=>{
    if(stopped)return;
    state('connecting');const ws=new WebSocketImpl('wss://push.services.mozilla.com/');socket=ws;
    handshake=setTimeout(()=>ws.close(),handshakeMs);
    ws.addEventListener('open',()=>{if(!stopped&&socket===ws)ws.send(JSON.stringify({
      messageType:'hello',uaid:r.uaid,use_webpush:true,channelIDs:[r.channelID]}));});
    ws.addEventListener('message',({data})=>{
      if(stopped||socket!==ws)return;
      try {
        if(typeof data!=='string'||data.length>262144)throw new Error('invalid_frame');
        const m=JSON.parse(data);
        if(m.messageType==='hello') {
          if(ready||m.status!==200||m.uaid!==r.uaid||m.use_webpush!==true)return halt('registration_requires_review');
          clearTimeout(handshake);attempts=0;ready=true;state('connected');
          processPending(store,r,decrypt);
          heartbeat=setInterval(()=>{ws.send('{}');pong=setTimeout(()=>ws.close(),pongMs);},heartbeatMs);
        } else if(m.messageType==='notification') {
          if(!ready||m.channelID!==r.channelID||typeof m.version!=='string'||!m.version.length||m.version.length>512||
             typeof m.data!=='string'||m.data.length>90000)throw new Error('invalid_notification');
          let saved;
          try { saved=store.persist(m); } catch { return halt('push_storage_unavailable'); }
          // FULL SQLite commit happens before ACK. A crash after ACK recovers pending packets.
          ws.send(JSON.stringify({messageType:'ack',updates:[{channelID:m.channelID,version:m.version,code:100}]}));
          processPending(store,r,decrypt);
        } else if(!m.messageType||m.messageType==='ping') { clearTimeout(pong);state('connected'); }
      } catch { ready=false;state('frame_rejected');ws.close(); }
    });
    ws.addEventListener('error',()=>{if(socket===ws){ready=false;state('connection_error');}});
    ws.addEventListener('close',()=>{
      if(socket!==ws)return;
      ready=false;timers();
      if(!stopped){state('disconnected');retry=setTimeout(connect,Math.min(60000,retryMs*2**Math.min(attempts++,5)));}
    });
  };
  connect();
  return {isReady:()=>ready&&!stopped,stop(){halt('stopped');}};
}

export function main() {
  process.umask(0o077);
  const file=resolve(process.argv[2] || '');
  const registration=privateRead(file),store=openPushStore(join(dirname(file),'push.sqlite'));
  const receiver=observeInstagramPush({registration,store});
  const stop=()=>{receiver.stop();store.close();};
  process.once('SIGTERM',stop);process.once('SIGINT',stop);
}
if(process.argv[1] && import.meta.url===pathToFileURL(resolve(process.argv[1])).href) {
  try { main(); } catch { console.error(JSON.stringify({status:'failed',reason:'private_push_start_failed'}));process.exitCode=1; }
}
