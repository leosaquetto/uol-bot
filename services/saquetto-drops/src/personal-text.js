import {generateWAMessage,proto} from '@whiskeysockets/baileys';

const UUID=/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const SAFE=/^[a-zA-Z0-9_:-]{1,96}$/;
const CONTROL=/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/;
const SEND_TTL=30*60*1000;
const messageLogger={level:'silent',trace(){},debug(){},info(){},warn(){},error(){},child(){return this;}};
const fail=(code,status=400)=>{throw Object.assign(new Error(code),{status});};
const exact=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>keys.includes(key));
const safeValue=value=>typeof value==='string'&&SAFE.test(value)?value:null;

function body(req,limit){
  return (async()=>{
    if(Number(req.headers?.['content-length']||0)>limit)fail('body_too_large',413);
    const chunks=[];let size=0;
    for await(const chunk of req){size+=chunk.length;if(size>limit)fail('body_too_large',413);chunks.push(chunk);}
    return Buffer.concat(chunks);
  })();
}

function jobOutput(store,requestId){
  if(!UUID.test(requestId))fail('invalid_request_id');
  const job=store.jobByKey(`personal-mcp:${requestId}`);
  if(!job)fail('request_not_found',404);
  let payload;try{payload=JSON.parse(job.payload);}catch{fail('request_state_invalid',500);}
  if(payload?.personalMcp!==true||payload.requestId!==requestId)fail('request_state_invalid',500);
  const destination=payload.destinationAlias==='self'?'self':UUID.test(payload.chatRef||'')?payload.chatRef:null;
  if(!destination)fail('request_state_invalid',500);
  if(!['queued','dispatching','accepted','confirmed','unknown','failed'].includes(job.state))fail('request_state_invalid',500);
  return {requestId,jobs:[{destination,state:job.state,code:safeValue(job.code),confirmation:safeValue(job.confirmation)}]};
}

export function createPersonalTextApi({store,getConfig,canSend,inbox=null,now=Date.now}){
  const destinations=()=>{
    const self=getConfig()?.destinations?.self;
    return self?.verified===true&&self.type==='contact'&&typeof self.jid==='string'&&self.jid
      ? [{id:'self',name:'self',type:'contact'}] : [];
  };
  const status=requestId=>jobOutput(store,requestId);
  const enqueueJob=({requestId,destination,payload})=>{
    const key=`personal-mcp:${requestId}`;
    if(store.jobByKey(key)){store.enqueue({key,destination,payload,priority:10},now());return status(requestId);}
    if(!canSend())fail('sending_paused',409);
    store.enqueue({key,destination,payload,priority:10},now());
    return status(requestId);
  };
  const enqueue=input=>{
    if(!exact(input,['requestId','destinations','text','mediaIds']))fail('invalid_request');
    if(typeof input.requestId!=='string'||!UUID.test(input.requestId))fail('invalid_request_id');
    if(!Array.isArray(input.destinations)||input.destinations.length!==1||input.destinations[0]!=='self')fail('destination_not_allowed');
    if(!Array.isArray(input.mediaIds)||input.mediaIds.length!==0)fail('media_not_allowed');
    if(typeof input.text!=='string'||input.text.length>8000||!input.text.trim()||CONTROL.test(input.text))fail('invalid_text');
    const self=getConfig()?.destinations?.self;
    if(!self||self.verified!==true||self.type!=='contact'||typeof self.jid!=='string'||!self.jid)fail('destination_not_allowed');
    const payload={personalMcp:true,requestId:input.requestId,destinationAlias:'self',text:input.text,expiresAt:now()+SEND_TTL};
    return enqueueJob({requestId:input.requestId,destination:self.jid,payload});
  };
  const enqueueChat=input=>{
    if(!exact(input,['requestId','chatRef','text']))fail('invalid_request');
    if(typeof input.requestId!=='string'||!UUID.test(input.requestId))fail('invalid_request_id');
    if(typeof input.chatRef!=='string'||!UUID.test(input.chatRef))fail('invalid_chat_ref');
    if(typeof input.text!=='string'||input.text.length>8000||!input.text.trim()||CONTROL.test(input.text))fail('invalid_text');
    const chat=inbox?.resolveChat?.(input.chatRef);
    if(!chat||typeof chat.jid!=='string'||!['contact','group'].includes(chat.kind))fail('chat_not_found',404);
    const payload={personalMcp:true,requestId:input.requestId,chatRef:input.chatRef,text:input.text,expiresAt:now()+SEND_TTL};
    return enqueueJob({requestId:input.requestId,destination:chat.jid,payload});
  };
  return {
    destinations,status,enqueue,enqueueChat,
    async route(req,path){
      try{
        if(req.method==='GET'&&path==='/v1/whatsapp/destinations')return {status:200,body:{destinations:destinations()}};
        if(req.method==='GET'&&path.startsWith('/v1/whatsapp/requests/'))return {status:200,body:status(path.slice('/v1/whatsapp/requests/'.length))};
        if(req.method==='POST'&&(path==='/v1/whatsapp/send'||path==='/v1/whatsapp/send-chat')){
          if(!String(req.headers?.['content-type']||'').startsWith('application/json'))fail('json_required',415);
          let input;try{input=JSON.parse((await body(req,65536)).toString());}catch(error){if(error?.status)throw error;fail('invalid_json');}
          return {status:202,body:path.endsWith('send-chat')?enqueueChat(input):enqueue(input)};
        }
        return {status:404,body:{code:'not_found'}};
      }catch(error){return {status:error.status||500,body:{code:error.status?error.message:'internal_error'}};}
    },
  };
}

export function createPersonalTextSender({store,getConfig,inbox=null,whatsapp,canSend,generateMessage=generateWAMessage,now=Date.now}){
  let busy=false;
  return async()=>{
    if(busy||!whatsapp.isReady()||!canSend())return;
    const config=getConfig();
    if(config.operation?.paused===true||config.operation?.dryRun===true)return;
    const last=Number(store.getSetting('last_dispatch_at')||0);
    if(now()-last<Number(config.operation?.minDelayMs||0))return;
    busy=true;let job,dispatched=false;
    try{
      job=store.claim(now(),{pilotOnly:false,allowPilot:false});
      if(!job)return;
      let payload;try{payload=JSON.parse(job.payload);}catch{throw new Error('invalid_payload');}
      if(payload?.personalMcp!==true||payload.requestId===undefined||typeof payload.text!=='string')throw new Error('unexpected_job');
      if(!Number.isFinite(payload.expiresAt)||now()>payload.expiresAt){store.updateJob(job.id,'failed','request_expired');return;}
      const resolveDestination=()=>{
        if(payload.destinationAlias==='self')return getConfig()?.destinations?.self;
        if(UUID.test(payload.chatRef||'')){const chat=inbox?.resolveChat?.(payload.chatRef);return chat?{type:chat.kind,jid:chat.jid,verified:true}:null;}
        return null;
      };
      const destination=resolveDestination();
      if(!destination?.verified||!['contact','group'].includes(destination.type)||destination.jid!==job.destination)throw new Error('destination_changed');
      await whatsapp.verifyDestination(destination);
      const socket=whatsapp.socket();
      const wire=await generateMessage(destination.jid,{text:payload.text},{messageId:job.message_id,userJid:socket.user?.id||whatsapp.ownDestination().jid,upload:socket.waUploadToServer,logger:messageLogger});
      if(wire?.key?.id!==job.message_id||wire.key.remoteJid!==job.destination||!wire.message)throw new Error('invalid_generated_message');
      const current=resolveDestination();
      if(!canSend()||getConfig().operation?.paused===true||getConfig().operation?.dryRun===true){store.updateJob(job.id,'queued','paused');return;}
      if(now()>payload.expiresAt){store.updateJob(job.id,'failed','request_expired');return;}
      if(!current?.verified||!['contact','group'].includes(current.type)||current.jid!==job.destination)throw new Error('destination_changed');
      store.saveWireMessage(job,proto.Message.encode(wire.message).finish());
      store.setSetting('last_dispatch_at',now());
      dispatched=true;
      await socket.relayMessage(destination.jid,wire.message,{messageId:job.message_id});
      if(store.job(job.id).state==='dispatching')store.updateJob(job.id,'unknown','awaiting_whatsapp_ack');
    }catch{
      if(job){
        const current=store.job(job.id);
        if(current?.state==='dispatching'&&current.message_id===job.message_id&&current.destination===job.destination&&current.attempts===job.attempts){
          if(dispatched)store.updateJob(job.id,'unknown','dispatch_unconfirmed');
          else if(job.attempts<3)store.updateJob(job.id,'queued','pre_dispatch_failure',null,now()+30000*2**(job.attempts-1));
          else store.updateJob(job.id,'failed','pre_dispatch_failure');
        }
      }
    }finally{busy=false;}
  };
}
