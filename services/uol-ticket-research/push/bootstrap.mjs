// One explicit subscription creation. Never creates or replaces a browser/X subscription.
import { createECDH, randomBytes, randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { privateRead, privateWrite, privateDirectory } from './private.mjs';
import { dirname } from 'node:path';

export async function bootstrap({applicationServerKey,output,WebSocketImpl=WebSocket,timeoutMs=15000}) {
  if(typeof applicationServerKey!=='string'||!/^[A-Za-z0-9_-]+$/.test(applicationServerKey)||
     Buffer.from(applicationServerKey,'base64url').length!==65||Buffer.from(applicationServerKey,'base64url')[0]!==4)
    throw new Error('invalid_application_server_key');
  privateDirectory(dirname(output));
  if(existsSync(output))throw new Error('registration_already_exists');
  const key=createECDH('prime256v1');key.generateKeys();
  const registration={origin:'https://www.instagram.com',channelID:randomUUID(),applicationServerKey,
    privateKey:key.getPrivateKey().toString('base64url'),publicKey:key.getPublicKey().toString('base64url'),
    auth:randomBytes(16).toString('base64url'),instagramRegistration:{status:'not_attempted'}};
  return new Promise((accept,reject)=>{
    const socket=new WebSocketImpl('wss://push.services.mozilla.com/');let finished=false;
    const finish=(error)=>{if(finished)return;finished=true;clearTimeout(timer);socket.close();error?reject(error):accept(registration);};
    const timer=setTimeout(()=>finish(new Error('bootstrap_timeout')),timeoutMs);
    socket.addEventListener('open',()=>socket.send(JSON.stringify({messageType:'hello',use_webpush:true,channelIDs:[]})));
    socket.addEventListener('message',({data})=>{
      if(finished)return;
      try {
        if(typeof data!=='string'||data.length>32768)throw new Error('invalid_frame');
        const m=JSON.parse(data);
        if(m.messageType==='hello') {
          if(registration.uaid||m.status!==200||typeof m.uaid!=='string'||m.use_webpush!==true)throw new Error('bootstrap_handshake_failed');
          registration.uaid=m.uaid;
          // Preserve identity/keys before channel creation. Failure requires review of this file, not a new UAID.
          privateWrite(output,{...registration,bootstrapStatus:'pending'});
          socket.send(JSON.stringify({messageType:'register',channelID:registration.channelID,key:applicationServerKey}));
        } else if(m.messageType==='register') {
          if(m.status!==200||m.channelID!==registration.channelID||typeof m.pushEndpoint!=='string')throw new Error('bootstrap_register_failed');
          const endpoint=new URL(m.pushEndpoint);
          if(endpoint.protocol!=='https:'||endpoint.hostname!=='updates.push.services.mozilla.com'||endpoint.port||
             endpoint.username||endpoint.password||!endpoint.pathname.startsWith('/wpush/v2/'))throw new Error('invalid_endpoint');
          registration.endpoint=endpoint.href;registration.bootstrapStatus='created';
          privateWrite(output,registration);finish();
        }
      }catch {finish(new Error('bootstrap_failed'));}
    });
    socket.addEventListener('error',()=>finish(new Error('bootstrap_connection_failed')));
    socket.addEventListener('close',()=>{if(!finished)finish(new Error('bootstrap_disconnected'));});
  });
}

if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href) {
  process.umask(0o077);
  try {
    const options=privateRead(resolve(process.argv[2]||''));
    await bootstrap({applicationServerKey:options.applicationServerKey,output:resolve(process.argv[3]||'')});
    console.log(JSON.stringify({status:'mozilla_subscription_created',instagramRegistered:false}));
  }catch {console.error(JSON.stringify({status:'failed',reason:'bootstrap_failed_review_private_state'}));process.exitCode=1;}
}
