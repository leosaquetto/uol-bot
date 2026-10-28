import {randomUUID} from 'node:crypto';

const UUID=/^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i;
const SNAPSHOT_TTL_MS=15*60*1000;
const fail=(code,status=400)=>{throw Object.assign(new Error(code),{status});};
const exactUuid=(value,code)=>typeof value==='string'&&UUID.test(value)?value:fail(code);
const int=(value,min,max,code)=>{const n=Number(value);if(!Number.isInteger(n)||n<min||n>max)fail(code);return n;};
const exactObject=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>keys.includes(key));

async function jsonBody(req,limit=16384){
  if(!String(req.headers?.['content-type']||'').startsWith('application/json'))fail('json_required',415);
  if(Number(req.headers?.['content-length']||0)>limit)fail('body_too_large',413);
  const chunks=[];let size=0;
  for await(const chunk of req){size+=chunk.length;if(size>limit)fail('body_too_large',413);chunks.push(chunk);}
  try{return JSON.parse(Buffer.concat(chunks).toString('utf8'));}catch{fail('invalid_json');}
}

const safeCode=error=>[
  'last_message_unavailable','last_message_changed','chat_not_found','message_not_found','whatsapp_not_ready'
].includes(error?.message)?error.message:'mutation_failed';

export function createPersonalChatActions({store,inbox,whatsapp,now=Date.now}){
  const {db}=store;
  db.exec(`CREATE TABLE IF NOT EXISTS personal_cleanup_snapshots(
    snapshot_id TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    consumed_at INTEGER,
    items_json TEXT NOT NULL
  );`);

  const socket=()=>{
    if(whatsapp?.isReady?.()!==true)fail('whatsapp_not_ready',409);
    const value=whatsapp.socket?.();
    if(!value)fail('whatsapp_not_ready',409);
    return value;
  };

  const currentLast=(chatRef,expectedLastMessageRef=null)=>{
    const ref=exactUuid(chatRef,'invalid_chat_ref');
    const chat=inbox.resolveChat(ref);
    if(!chat)fail('chat_not_found',404);
    const last=inbox.lastMessage(ref);
    if(!last||last.deleted||!last.key?.id||!last.key?.remoteJid||!Number.isFinite(last.messageTimestamp)||last.messageTimestamp<=0){
      fail('last_message_unavailable',409);
    }
    if(expectedLastMessageRef!==null){
      const expected=exactUuid(expectedLastMessageRef,'invalid_last_message_ref');
      if(last.messageRef!==expected)fail('last_message_changed',409);
    }
    return {chat,last};
  };

  const lastPayload=last=>({key:last.key,messageTimestamp:last.messageTimestamp});

  const archiveChat=async({chatRef})=>{
    const {chat,last}=currentLast(chatRef);
    await socket().chatModify({archive:true,lastMessages:[lastPayload(last)]},chat.jid);
    return {state:'archived',chatRef:chat.chatRef};
  };

  const deleteChat=async({chatRef,lastMessageRef})=>{
    if(lastMessageRef===undefined||lastMessageRef===null)fail('last_message_ref_required');
    const {chat,last}=currentLast(chatRef,lastMessageRef);
    await socket().chatModify({delete:true,lastMessages:[lastPayload(last)]},chat.jid);
    inbox.removeChat(chat.chatRef);
    return {state:'deleted',chatRef:chat.chatRef};
  };

  const deleteMessage=async({messageRef,mode='for_me'})=>{
    const ref=exactUuid(messageRef,'invalid_message_ref');
    if(!['for_me','everyone'].includes(mode))fail('invalid_delete_mode');
    const message=inbox.resolveMessage(ref);
    if(!message||message.deleted)fail('message_not_found',404);
    const chat=inbox.resolveChat(message.chatRef);
    if(!chat||!message.key?.id||!message.key?.remoteJid||!Number.isFinite(message.messageTimestamp)||message.messageTimestamp<=0){
      fail('message_not_found',404);
    }
    const wa=socket();
    if(mode==='for_me'){
      await wa.chatModify({deleteForMe:{deleteMedia:false,key:message.key,timestamp:message.messageTimestamp}},chat.jid);
    }else{
      await wa.sendMessage(chat.jid,{delete:message.key});
    }
    inbox.ingestDeletes({keys:[message.key]});
    return {state:'deleted',messageRef:ref,mode};
  };

  const previewCleanup=({inactiveBefore,maxIndexedMessages=3,limit=50}={})=>{
    const before=Number(inactiveBefore);
    if(!Number.isFinite(before)||before<=0)fail('invalid_inactive_before');
    const max=int(maxIndexedMessages,0,100,'invalid_max_messages');
    const bounded=int(limit,1,100,'invalid_limit');
    const chats=inbox.listChats({before,limit:100})
      .filter(chat=>chat.indexedMessageCount<=max)
      .slice(0,bounded);
    const snapshotId=randomUUID();
    const createdAt=now();
    const expiresAt=createdAt+SNAPSHOT_TTL_MS;
    const items=chats.map(chat=>({chatRef:chat.chatRef,lastMessageRef:chat.lastMessageRef||null}));
    db.prepare('INSERT INTO personal_cleanup_snapshots(snapshot_id,created_at,expires_at,consumed_at,items_json) VALUES(?,?,?,?,?)')
      .run(snapshotId,createdAt,expiresAt,null,JSON.stringify(items));
    return {
      snapshotId,expiresAt,
      chats:chats.map(chat=>({...chat,deletable:Boolean(chat.lastMessageRef)})),
    };
  };

  const applyCleanup=async({snapshotId,chatRefs}={})=>{
    const id=exactUuid(snapshotId,'invalid_snapshot_id');
    if(!Array.isArray(chatRefs)||chatRefs.length<1||chatRefs.length>100)fail('invalid_chat_refs');
    const refs=chatRefs.map(ref=>exactUuid(ref,'invalid_chat_ref'));
    if(new Set(refs).size!==refs.length)fail('invalid_chat_refs');
    const row=db.prepare('SELECT * FROM personal_cleanup_snapshots WHERE snapshot_id=?').get(id);
    if(!row||row.expires_at<now())fail('snapshot_expired',409);
    if(row.consumed_at!==null)fail('snapshot_consumed',409);
    let items;
    try{items=JSON.parse(row.items_json);}catch{fail('snapshot_invalid',500);}
    if(!Array.isArray(items))fail('snapshot_invalid',500);
    const approved=new Map(items.map(item=>[item.chatRef,item.lastMessageRef]));
    if(refs.some(ref=>!approved.has(ref)))fail('snapshot_mismatch',409);
    const consumed=db.prepare('UPDATE personal_cleanup_snapshots SET consumed_at=? WHERE snapshot_id=? AND consumed_at IS NULL').run(now(),id);
    if(consumed.changes!==1)fail('snapshot_consumed',409);

    const receipts=[];
    for(const ref of refs){
      const lastMessageRef=approved.get(ref);
      if(!lastMessageRef){
        receipts.push({chatRef:ref,state:'skipped',code:'last_message_unavailable'});
        continue;
      }
      try{
        await deleteChat({chatRef:ref,lastMessageRef});
        receipts.push({chatRef:ref,state:'deleted'});
      }catch(error){
        receipts.push({chatRef:ref,state:'failed',code:safeCode(error)});
      }
    }
    return receipts;
  };

  const matches=(method,path)=>method==='POST'&&(
    /^\/v1\/whatsapp\/chats\/[a-f0-9-]{36}\/(archive|delete)$/i.test(path)||
    /^\/v1\/whatsapp\/messages\/[a-f0-9-]{36}\/delete$/i.test(path)||
    path==='/v1/whatsapp/cleanup/preview'||path==='/v1/whatsapp/cleanup/apply');

  const route=async(req,url)=>{
    try{
      const path=url.pathname;
      const input=await jsonBody(req);
      let match=path.match(/^\/v1\/whatsapp\/chats\/([a-f0-9-]{36})\/(archive|delete)$/i);
      if(match){
        if(!exactObject(input,['lastMessageRef']))fail('invalid_request');
        const result=match[2]==='archive'
          ? await archiveChat({chatRef:match[1]})
          : await deleteChat({chatRef:match[1],lastMessageRef:input.lastMessageRef});
        return {status:200,body:result};
      }
      match=path.match(/^\/v1\/whatsapp\/messages\/([a-f0-9-]{36})\/delete$/i);
      if(match){
        if(!exactObject(input,['mode']))fail('invalid_request');
        return {status:200,body:await deleteMessage({messageRef:match[1],mode:input.mode??'for_me'})};
      }
      if(path==='/v1/whatsapp/cleanup/preview'){
        if(!exactObject(input,['inactiveBefore','maxIndexedMessages','limit']))fail('invalid_request');
        return {status:200,body:previewCleanup(input)};
      }
      if(path==='/v1/whatsapp/cleanup/apply'){
        if(!exactObject(input,['snapshotId','chatRefs']))fail('invalid_request');
        return {status:200,body:{receipts:await applyCleanup(input)}};
      }
      return {status:404,body:{code:'not_found'}};
    }catch(error){
      return {status:error?.status||500,body:{code:error?.status?error.message:'mutation_failed'}};
    }
  };

  return {archiveChat,deleteChat,deleteMessage,previewCleanup,applyCleanup,matches,route};
}
