const UUID=/^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i;
const fail=(code,status=400)=>{throw Object.assign(new Error(code),{status});};
const bounded=(value,max,code='invalid_limit')=>{
  const n=Number(value);
  if(!Number.isInteger(n)||n<1||n>max)fail(code);
  return n;
};
const chatRef=value=>typeof value==='string'&&UUID.test(value)?value:fail('invalid_chat_ref');
const beforeValue=value=>{
  if(value===undefined||value===null||value==='')return undefined;
  const n=Number(value);if(!Number.isFinite(n)||n<0)fail('invalid_before');return Math.trunc(n);
};
async function jsonBody(req,limit=4096){
  if(!String(req.headers?.['content-type']||'').startsWith('application/json'))fail('json_required',415);
  if(Number(req.headers?.['content-length']||0)>limit)fail('body_too_large',413);
  const chunks=[];let size=0;
  for await(const chunk of req){size+=chunk.length;if(size>limit)fail('body_too_large',413);chunks.push(chunk);}
  try{return JSON.parse(Buffer.concat(chunks).toString('utf8'));}catch{fail('invalid_json');}
}

export function createPersonalInboxApi({inbox,whatsapp}){
  const ensureChat=ref=>{const exact=chatRef(ref);if(!inbox.resolveChat(exact))fail('chat_not_found',404);return exact;};
  const chats=({query='',limit=50,before}={})=>{
    const boundedLimit=bounded(limit,100);
    if(typeof query!=='string'||query.length>200)fail('invalid_query');
    return inbox.listChats({query,limit:boundedLimit,before:beforeValue(before)});
  };
  const messages=({chatRef:ref,limit=50,before}={})=>{
    const exact=ensureChat(ref);
    return inbox.listMessages(exact,{limit:bounded(limit,100),before:beforeValue(before)});
  };
  const search=({query,chatRef:ref,limit=50}={})=>{
    if(typeof query!=='string'||!query.trim()||query.length>500)fail('invalid_query');
    const exact=ref===undefined?undefined:ensureChat(ref);
    return inbox.searchMessages(query,{chatRef:exact,limit:bounded(limit,100)});
  };
  const historyMore=async({chatRef:ref,count=50}={})=>{
    const exact=ensureChat(ref);
    const amount=bounded(count,50,'invalid_count');
    if(whatsapp?.isReady?.()!==true)fail('whatsapp_not_ready',409);
    const oldest=inbox.oldestMessage(exact);
    if(!oldest||oldest.deleted||!oldest.key?.id||!oldest.key?.remoteJid||!Number.isFinite(oldest.messageTimestamp)||oldest.messageTimestamp<=0)fail('history_anchor_unavailable',409);
    const socket=whatsapp.socket?.();
    if(!socket||typeof socket.fetchMessageHistory!=='function')fail('history_unavailable',503);
    await socket.fetchMessageHistory(amount,oldest.key,oldest.messageTimestamp);
    return {accepted:true,chatRef:exact,count:amount};
  };
  const matches=(method,path)=>{
    if(method==='GET'&&path==='/v1/whatsapp/chats')return true;
    if(method==='GET'&&path==='/v1/whatsapp/search')return true;
    if(method==='GET'&&/^\/v1\/whatsapp\/chats\/[a-f0-9-]{36}\/messages$/i.test(path))return true;
    if(method==='POST'&&/^\/v1\/whatsapp\/chats\/[a-f0-9-]{36}\/history$/i.test(path))return true;
    return false;
  };
  const route=async(req,url)=>{
    try{
      const {pathname,searchParams}=url;
      if(req.method==='GET'&&pathname==='/v1/whatsapp/chats'){
        return {status:200,body:{chats:chats({query:searchParams.get('query')||'',limit:searchParams.get('limit')||50,before:searchParams.get('before')||undefined})}};
      }
      if(req.method==='GET'&&pathname==='/v1/whatsapp/search'){
        return {status:200,body:{messages:search({query:searchParams.get('query')||'',chatRef:searchParams.get('chatRef')||undefined,limit:searchParams.get('limit')||50})}};
      }
      let match=pathname.match(/^\/v1\/whatsapp\/chats\/([a-f0-9-]{36})\/messages$/i);
      if(req.method==='GET'&&match){
        return {status:200,body:{messages:messages({chatRef:match[1],limit:searchParams.get('limit')||50,before:searchParams.get('before')||undefined})}};
      }
      match=pathname.match(/^\/v1\/whatsapp\/chats\/([a-f0-9-]{36})\/history$/i);
      if(req.method==='POST'&&match){
        const input=await jsonBody(req);
        if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).some(k=>k!=='count'))fail('invalid_request');
        return {status:202,body:await historyMore({chatRef:match[1],count:input.count??50})};
      }
      return {status:404,body:{code:'not_found'}};
    }catch(error){return {status:error?.status||500,body:{code:error?.status?error.message:'internal_error'}};}
  };
  return {chats,messages,search,historyMore,matches,route};
}
