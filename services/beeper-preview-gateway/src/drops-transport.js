import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export function createDropsTransport({token,baseUrl='http://127.0.0.1:8788',fetchImpl=fetch}) {
  const base=new URL(baseUrl);
  if(base.origin!=='http://127.0.0.1:8788'||base.pathname!=='/'||base.search||base.hash||base.username||base.password)throw new Error('invalid_drops_url');
  if(!/^[a-f0-9]{64}$/.test(token||''))throw new Error('DROPS_GATEWAY_TOKEN_required');
  const call=async(path,body)=>{
    let response;
    try {
      response=await fetchImpl(new URL(path,base),{method:body?'POST':'GET',
        headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},
        ...(body?{body:JSON.stringify(body)}:{}),redirect:'error',signal:AbortSignal.timeout(10000)});
      const result=await response.json();
      if(!response.ok)throw Object.assign(new Error('drops_rejected'),{status:response.status,ambiguous:Boolean(body)&&response.status>=500&&result?.code==='internal_error'});
      return result;
    }catch(error){
      if(error.status)throw error;
      throw Object.assign(new Error('drops_unavailable'),{ambiguous:Boolean(body)});
    }
  };
  return {
    readiness:()=>call('/v1/gateway/readyz'),
    async sendMessage({route,idempotencyKey,requestHash,text,preview}){
      const image=preview?.img?readFileSync(fileURLToPath(preview.img)):null;
      if(image&&image.length>5*1024*1024)throw new Error('preview_image_too_large');
      const job=await call('/v1/gateway/send',{route,
        keyHash:createHash('sha256').update(idempotencyKey).digest('hex'),requestHash,text,
        ...(preview?{preview:{link:preview.link,title:preview.title,summary:preview.summary||'',
          ...(image?{imageBase64:image.toString('base64')}:{})}}:{})});
      if(!/^[a-f0-9-]{36}$/.test(job?.jobId||''))throw Object.assign(new Error('drops_invalid_receipt'),{ambiguous:true});
      return {pendingMessageID:job.jobId};
    },
    async confirmDelivery({pendingMessageID}){
      if(!/^[a-f0-9-]{36}$/.test(pendingMessageID||''))return {state:'unknown'};
      const deadline=Date.now()+20000;
      do {
        const job=await call(`/v1/gateway/jobs/${pendingMessageID}`);
        if(job.state==='confirmed'&&['recipient_receipt','participant_receipt'].includes(job.confirmation)&&job.messageId)
          return {state:'delivered',deliveryState:'confirmed_by_whatsapp_receipt',confirmation:job.confirmation,messageId:job.messageId};
        if(job.state==='accepted'&&job.confirmation==='server_ack'&&job.messageId)
          return {state:'accepted',deliveryState:'accepted_by_whatsapp_server',confirmation:job.confirmation,messageId:job.messageId};
        if(job.state==='failed')return {state:'rejected'};
        // An ack may arrive just after sendMessage returns. Keep observing the same job.
        if(Date.now()>=deadline)return {state:job.state==='unknown'?'unknown':'pending'};
        await new Promise(resolve=>setTimeout(resolve,500));
      }while(true);
    },
  };
}
