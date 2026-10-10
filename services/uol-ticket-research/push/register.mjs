// Explicit single Instagram registration; all inputs and responses remain private.
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { privateRead, privateWrite, validateRegistration } from './private.mjs';

export async function registerInstagram({registrationPath,sessionPath,fieldsPath,fetchImpl=fetch}) {
  const registration=validateRegistration(privateRead(registrationPath)),session=privateRead(sessionPath),fields=privateRead(fieldsPath);
  if(registration.instagramRegistration?.status!=='not_attempted')throw new Error('registration_attempt_requires_review');
  if(typeof session.user_agent!=='string'||/[\r\n]/.test(session.user_agent)||session.user_agent.length>2048||
     typeof fields.fb_dtsg!=='string'||!fields.fb_dtsg||typeof fields.jazoest!=='string'||!/^\d{1,30}$/.test(fields.jazoest))
    throw new Error('invalid_registration_input');
  const cookies=new Map();
  for(const c of session.cookies||[]) {
    if(['.instagram.com','www.instagram.com','instagram.com'].includes(c.domain)&&c.path==='/'&&
       typeof c.name==='string'&&/^[a-zA-Z0-9_]+$/.test(c.name)&&typeof c.value==='string'&&!/[\r\n;]/.test(c.value))cookies.set(c.name,c.value);
  }
  // The monitor converts legacy cookie_header into the private cookie jar on its first successful request.
  if(!cookies.get('sessionid')||!cookies.get('csrftoken')||!cookies.get('mid'))throw new Error('session_cookie_jar_required');
  const body=new URLSearchParams({device_token:registration.endpoint,device_type:'web_vapid',mid:cookies.get('mid'),
    subscription_keys:JSON.stringify({p256dh:registration.publicKey,auth:registration.auth}),
    jazoest:fields.jazoest,fb_dtsg:fields.fb_dtsg});
  const headers={'Content-Type':'application/x-www-form-urlencoded','Accept':'application/json',
    'User-Agent':session.user_agent,'Origin':'https://www.instagram.com','Referer':'https://www.instagram.com/',
    'X-CSRFToken':cookies.get('csrftoken'),'Cookie':[...cookies].map(([k,v])=>`${k}=${v}`).join('; ')};
  if(fields.appId&&/^\d{1,30}$/.test(fields.appId))headers['X-IG-App-ID']=fields.appId;
  registration.instagramRegistration={status:'attempted',attemptedAt:new Date().toISOString()};
  privateWrite(registrationPath,registration);
  try {
    const response=await fetchImpl('https://www.instagram.com/api/v1/web/push/register/',{
      method:'POST',redirect:'error',signal:AbortSignal.timeout(20000),headers,body:body.toString()});
    let raw='';
    for await(const chunk of response.body) {raw+=Buffer.from(chunk).toString('utf8');if(raw.length>32768)throw new Error('response_limit');}
    let value;try{value=JSON.parse(raw);}catch{value=null;}
    const accepted=response.status===200&&(value?.status==='ok'||value?.success===true);
    registration.instagramRegistration={...registration.instagramRegistration,status:accepted?'accepted':'rejected',httpStatus:response.status,
      recordedAt:new Date().toISOString()};
    privateWrite(registrationPath,registration);
    return {status:accepted?'instagram_registration_accepted':'instagram_registration_not_confirmed',httpStatus:response.status,
      storyPushProven:false};
  }catch {
    registration.instagramRegistration={...registration.instagramRegistration,status:'uncertain'};
    privateWrite(registrationPath,registration);
    return {status:'instagram_registration_uncertain',storyPushProven:false};
  }
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href) {
  process.umask(0o077);
  try {console.log(JSON.stringify(await registerInstagram({registrationPath:resolve(process.argv[2]||''),
    sessionPath:resolve(process.argv[3]||''),fieldsPath:resolve(process.argv[4]||'')})));}
  catch {console.error(JSON.stringify({status:'failed',reason:'private_registration_input_failed'}));process.exitCode=1;}
}
