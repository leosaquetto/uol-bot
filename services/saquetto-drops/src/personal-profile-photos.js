// Private, authenticated loopback-only profile photo retrieval.
// No number or photo URL is returned to shared MCP / WhatsApp transports.
const E164=/^[1-9][0-9]{9,14}$/;
const MAX_BODY=1024, MAX_IMAGE=1024*1024;
const err=(code,status=400)=>Object.assign(new Error(code),{status});
const safeHost=(raw)=>{
  let url;
  try {url=new URL(raw);}catch{throw err('invalid_photo_url',502);}
  const h=url.hostname.toLowerCase();
  if(url.protocol!=='https:'||url.port||url.username||url.password||
     !(/(^|\.)(whatsapp\.net|whatsapp\.com|fbcdn\.net)$/.test(h)))
    throw err('image_host_denied',502);
  return url;
};
function classify(bytes){
  if(bytes.length>=3&&bytes[0]===0xff&&bytes[1]===0xd8&&bytes[2]===0xff)return 'image/jpeg';
  if(bytes.length>=8&&bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10])))return 'image/png';
  throw err('invalid_image',502);
}
export async function safeFetch(url){
  let target=safeHost(url);
  const signal=AbortSignal.timeout(12000);
  for(let hop=0;hop<3;hop++){
    const response=await fetch(target,{redirect:'manual',signal});
    if([301,302,303,307,308].includes(response.status)){
      const location=response.headers.get('location');
      response.body?.cancel().catch(()=>{});
      if(!location)throw err('image_unavailable',502);
      target=safeHost(new URL(location,target).href);
      continue;
    }
    if(!response.ok||!response.body)throw err('image_unavailable',502);
    const size=Number(response.headers.get('content-length')||0);
    if(size>MAX_IMAGE)throw err('image_too_large',502);
    const reader=response.body.getReader();
    const chunks=[];let bytes=0;
    try{
      while(true){
        const {done,value}=await reader.read();
        if(done)break;
        bytes+=value.byteLength;
        if(bytes>MAX_IMAGE)throw err('image_too_large',502);
        chunks.push(Buffer.from(value));
      }
    }catch(e){await reader.cancel().catch(()=>{});throw e;}
    return Buffer.concat(chunks);
  }
  throw err('too_many_redirects',502);
}
async function parseInput(req){
  if(!String(req.headers?.['content-type']||'').toLowerCase().startsWith('application/json'))throw err('json_required',415);
  if(Number(req.headers?.['content-length']||0)>MAX_BODY)throw err('body_too_large',413);
  let bytes=0;const parts=[];
  for await(const part of req){
    bytes+=part.length;
    if(bytes>MAX_BODY)throw err('body_too_large',413);
    parts.push(part);
  }
  let value;
  try{value=JSON.parse(Buffer.concat(parts).toString('utf8'));}catch{throw err('invalid_json');}
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>k!=='numbers')||
    !Array.isArray(value.numbers)||value.numbers.length<1||value.numbers.length>3||
    value.numbers.some(n=>typeof n!=='string'||!E164.test(n))||
    new Set(value.numbers).size!==value.numbers.length)throw err('invalid_numbers');
  return value.numbers;
}
const unavailable=e=>[403,404].includes(Number(e?.output?.statusCode||e?.status||e?.statusCode))||
  /not found|forbidden|not authorized|privacy/i.test(String(e?.message||''));

export function createPersonalProfilePhotos({whatsapp,fetchImage=safeFetch}){
  const matches=(method,path)=>method==='POST'&&path==='/v1/whatsapp/profile-photos';
  const route=async(req,url)=>{
    try{
      if(!matches(req.method,url.pathname))throw err('not_found',404);
      const numbers=await parseInput(req);
      if(whatsapp?.isReady?.()!==true)throw err('whatsapp_not_ready',409);
      const sock=whatsapp.socket?.();
      if(typeof sock?.profilePictureUrl!=='function')throw err('whatsapp_unavailable',503);
      const photos=[];
      for(const [index,number] of numbers.entries()){
        let status='failed',mime,base64;
        try{
          const photoUrl=await sock.profilePictureUrl(number+'@s.whatsapp.net','image');
          if(!photoUrl){status='unavailable';}
          else{
            safeHost(photoUrl); // Reject internal HTTP/IP URLs even if an injected fetcher is used.
            const bytes=Buffer.from(await fetchImage(photoUrl));
            if(bytes.length>MAX_IMAGE)throw err('image_too_large',502);
            mime=classify(bytes);
            base64=bytes.toString('base64');
            status='available';
          }
        }catch(e){status=unavailable(e)?'unavailable':'failed';}
        photos.push(status==='available'?{index,status,mime,base64}:{index,status});
      }
      return {status:200,body:{photos}};
    }catch(error){return {status:error?.status||500,body:{code:error?.status?error.message:'internal_error'}};}
  };
  return {matches,route};
}
