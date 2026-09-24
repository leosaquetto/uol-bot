import { decryptPush } from './webpush-crypto.js';
import { canonicalPost } from './x-post.js';

const uuid = /^[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}$/i;
export function validateRegistration(value) {
  const r = structuredClone(value), url = new URL(r.endpoint);
  if (url.protocol !== 'https:' || url.hostname !== 'updates.push.services.mozilla.com' ||
      url.port || url.username || url.password || !url.pathname.startsWith('/wpush/v2/') ||
      !uuid.test(r.uaid) || !uuid.test(r.channelID)) throw new Error('invalid_registration');
  for (const [name,size] of [['privateKey',32],['publicKey',65],['auth',16],['applicationServerKey',65]]) {
    if (typeof r[name] !== 'string' || !/^[A-Za-z0-9_-]+$/.test(r[name]) ||
        Buffer.from(r[name],'base64url').length !== size) throw new Error('invalid_registration_key');
  }
  return r;
}

export function pushEnvelope(message, registration) {
  if (message.messageType !== 'notification' || message.channelID !== registration.channelID ||
      typeof message.version !== 'string' || !message.version.length || message.version.length > 512 ||
      typeof message.data !== 'string' || message.data.length > 90000) throw new Error('invalid_notification');
  // Timestamp zero is intentional: redelivery must retain the exact same event ID.
  return {origin:'https://x.com',service:'pushMessaging',timestamp:0,
    instanceId:registration.channelID,eventName:'encrypted_webpush',
    eventMetadata:[{key:'encrypted',value:JSON.stringify(message)}]};
}

export function decodePushEvent(event, registration) {
  if (event.eventName !== 'encrypted_webpush') return event;
  if (event.instanceId !== registration.channelID || event.eventMetadata?.length !== 1 ||
      event.eventMetadata[0].key !== 'encrypted') throw new Error('unexpected_push_registration');
  const packet = JSON.parse(event.eventMetadata[0].value);
  pushEnvelope(packet,registration);
  const payload = decryptPush(packet,registration);
  // Only the notification's own URI identifies a post. Never use URLs in its text.
  if (payload?.data?.type !== 'tweet' || typeof payload.data.uri !== 'string') throw new Error('unsupported_notification_type');
  const post = canonicalPost(new URL(payload.data.uri,'https://x.com').href);
  return {...event,eventName:'decrypted_webpush',eventMetadata:[{key:'url',value:post.url}]};
}

export function observeWebPush({registration,store,onEvent=()=>{},logger=console,
  WebSocketImpl=WebSocket,handshakeMs=15000,heartbeatMs=240000,pongMs=45000,retryMs=2000}) {
  const r = validateRegistration(registration);
  let socket, stopped=false, ready=false, state='connecting', retry, handshake, heartbeat, pong, attempts=0;
  const log = event => logger.log(JSON.stringify({event}));
  const clearTimers = () => {clearTimeout(handshake);clearInterval(heartbeat);clearTimeout(pong);};
  const halt = code => {state=code;stopped=true;ready=false;clearTimers();clearTimeout(retry);log(code);socket?.close();};
  const connect = () => {
    if (stopped) return;
    state='connecting';const ws=new WebSocketImpl('wss://push.services.mozilla.com/');socket=ws;
    handshake=setTimeout(()=>ws.close(),handshakeMs);
    ws.addEventListener('open',()=>{
      if (!stopped && socket===ws) ws.send(JSON.stringify({messageType:'hello',uaid:r.uaid,use_webpush:true,channelIDs:[r.channelID]}));
    });
    ws.addEventListener('message',({data})=>{
      if (stopped || socket!==ws) return;
      let m;
      try {
        if (typeof data !== 'string' || data.length > 262144) throw new Error('invalid_frame');
        m=JSON.parse(data);
        if (m.messageType==='hello') {
          if (ready || m.status!==200 || m.uaid!==r.uaid || m.use_webpush!==true) return halt('registration_requires_review');
          clearTimeout(handshake);attempts=0;ready=true;state='connected';log('webpush_connected');
          heartbeat=setInterval(()=>{
            ws.send('{}');pong=setTimeout(()=>ws.close(),pongMs);
          },heartbeatMs);
        } else if (m.messageType==='notification') {
          if (!ready) throw new Error('notification_before_handshake');
          const envelope=pushEnvelope(m,r);
          let saved;
          try {saved=store.recordPush(envelope);} catch {return halt('push_storage_unavailable');}
          // recordPush is synchronous and SQLite commits before this ACK.
          ws.send(JSON.stringify({messageType:'ack',updates:[{channelID:m.channelID,version:m.version,code:100}]}));
          if (saved.added) {log('webpush_persisted');Promise.resolve().then(onEvent).catch(()=>log('push_processing_pending'));}
        } else if (!m.messageType || m.messageType==='ping') clearTimeout(pong);
      } catch {ready=false;state='invalid_frame';log('webpush_frame_rejected');ws.close();}
    });
    ws.addEventListener('error',()=>{if(socket===ws){ready=false;state='connection_error';}});
    ws.addEventListener('close',()=>{
      if(socket!==ws)return;
      ready=false;clearTimers();
      if (!stopped) {
        state='disconnected';
        retry=setTimeout(connect,Math.min(60000,retryMs*2**Math.min(attempts++,5)));
      }
    });
  };
  connect();
  return {handlesReconnect:true,isReady:()=>ready&&!stopped,state:()=>state,
    async stop(){halt('webpush_stopped');}};
}
