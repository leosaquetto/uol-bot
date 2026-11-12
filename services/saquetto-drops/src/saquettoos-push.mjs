/** Optional second transport. Persistence uses each producer's existing state store. */
export function createSaquettoOSPush({get,set,env={},channel,fetchImpl=fetch}) {
 let running=false;
 const configured=()=>env.SAQUETTOOS_PUSH_ENABLED==='1'&&!!env.SAQUETTOOS_PUSH_TOKEN&&!!env.SAQUETTOOS_PUSH_ORIGIN;
 function read(){const raw=get();const value=raw===''?null:typeof raw==='string'?JSON.parse(raw):raw;return Array.isArray(value)?value:[];}
 function write(rows){set(JSON.stringify(rows));}
 function enqueue(event){
  if(!configured())return {status:'disabled'};
  if(typeof event.id!=='string'||!event.id||typeof event.title!=='string'||typeof event.body!=='string')throw Error('invalid_push_event');
  let rows=read().filter(r=>r.created>Date.now()-7*86400000);
  if(rows.some(r=>r.key===event.id))return {status:'duplicate'};
  if(rows.length>=256){const index=rows.findIndex(r=>['accepted','failed'].includes(r.state));if(index<0)throw Error('push_outbox_full');rows.splice(index,1);}
  rows.push({key:event.id,event:{title:event.title.slice(0,120),body:event.body.slice(0,700)},created:Date.now(),next:0,attempts:0,state:'pending'});write(rows);return {status:'queued'};
 }
 async function drain(){
  if(!configured()||running)return;running=true;
  try{
   const origin=new URL(env.SAQUETTOOS_PUSH_ORIGIN);
   if(origin.protocol!=='https:'||origin.username||origin.password||origin.pathname!=='/'||origin.search||origin.hash)throw Error('invalid_push_origin');
   const headers={Authorization:`Bearer ${env.SAQUETTOOS_PUSH_TOKEN}`,'Content-Type':'application/json'};
   const due=read().filter(r=>r.state==='pending'&&r.next<=Date.now()).slice(0,3);
   for(const row of due){
    const bytes=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(row.key));
    const id=Array.from(new Uint8Array(bytes),b=>b.toString(16).padStart(2,'0')).join('');
    const payload={id,channel,...row.event,source:channel==='uol'?'clube-uol':'saquettoos',route:channel==='uol'?'saquettoOS://open/clube-uol':'saquettoOS://home'};
    let state='pending',next=Date.now()+60000;
    try{
     const response=await fetchImpl(new URL('/api/push/events',origin),{method:'POST',headers,body:JSON.stringify(payload),redirect:'manual',signal:AbortSignal.timeout(15000)});
     if(response.status===202){const result=await response.json();if(result.status==='accepted'&&result.eventId===id)state='accepted';}
     else if(response.status===429||response.status>=500){const retry=response.headers.get('Retry-After');const delay=retry&&/^\d+$/.test(retry)?Number(retry)*1000:retry?Date.parse(retry)-Date.now():0;next=Date.now()+Math.max(60000,Number.isFinite(delay)?delay:0);await response.body?.cancel();}
     else{state='failed';await response.body?.cancel();}
    }catch{/* Retrying this backend ingestion is safe: same event ID and payload, server deduplicates. */}
    const rows=read();const live=rows.find(r=>r.key===row.key);if(live){live.attempts++;live.state=live.attempts>=5&&state==='pending'?'failed':state;live.next=next;write(rows);}
   }
   // Existing producer ticks also advance server-side provider retries.
   const response=await fetchImpl(new URL('/api/push/drain',origin),{method:'POST',headers,body:'{}',redirect:'manual',signal:AbortSignal.timeout(15000)}).catch(()=>null);
   await response?.body?.cancel();
  }finally{running=false;}
 }
 function nextAt(){if(!configured())return null;const active=read().filter(r=>(r.state==='pending'||r.state==='accepted')&&r.created>Date.now()-86400000);return active.length?Math.max(Date.now()+60000,Math.min(...active.map(r=>r.next||0))):null;}
 return {enqueue,drain,configured,nextAt};
}
