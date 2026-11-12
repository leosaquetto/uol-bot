import test from 'node:test';import assert from 'node:assert/strict';
import {createSaquettoOSPush} from '../src/saquettoos-push.mjs';
const env={SAQUETTOOS_PUSH_ENABLED:'1',SAQUETTOOS_PUSH_ORIGIN:'https://example.test',SAQUETTOOS_PUSH_TOKEN:'fixture'};
test('optional transport disabled by default',()=>{let writes=0;const p=createSaquettoOSPush({get:()=>null,set:()=>writes++,env:{},channel:'drops'});assert.equal(p.enqueue({id:'1',title:'a',body:'b'}).status,'disabled');assert.equal(writes,0)});
test('durable stable IDs, independent retry and receipt validation',async()=>{let saved=null,calls=[];const opts={get:()=>saved,set:v=>saved=v,env,channel:'drops',fetchImpl:async(url,init)=>{calls.push({url:String(url),init});if(String(url).endsWith('/drain'))return Response.json({});const event=JSON.parse(init.body);return Response.json({status:'accepted',eventId:event.id},{status:202})}};const p=createSaquettoOSPush(opts);p.enqueue({id:'job-1',title:'a',body:'b'});assert.equal(p.enqueue({id:'job-1',title:'a',body:'b'}).status,'duplicate');await p.drain();assert.equal(JSON.parse(saved)[0].state,'accepted');assert.equal(JSON.parse(calls[0].init.body).route,'saquettoOS://home');await createSaquettoOSPush(opts).drain();assert.equal(calls.filter(c=>c.url.endsWith('/events')).length,1)});
test('failed web transport retains event without touching ntfy',async()=>{let saved=null;const p=createSaquettoOSPush({get:()=>saved,set:v=>saved=v,env,channel:'uol',fetchImpl:async()=>{throw Error('offline')}});p.enqueue({id:'event',title:'a',body:'b'});await p.drain();assert.equal(JSON.parse(saved)[0].state,'pending');assert.equal(JSON.parse(saved)[0].attempts,1)});
test('absent producer state initializes a durable outbox',async()=>{
 for(const initial of [undefined,null,'']){
  let saved=initial;const events=[];
  const p=createSaquettoOSPush({get:()=>saved,set:v=>saved=v,env,channel:'operations',fetchImpl:async(url,init)=>{
   if(String(url).endsWith('/drain'))return Response.json({});
   const event=JSON.parse(init.body);events.push(event);return Response.json({status:'accepted',eventId:event.id},{status:202});
  }});
  assert.equal(p.nextAt(),null);
  assert.equal(p.enqueue({id:'episode-1',title:'a',body:'b'}).status,'queued');
  assert.equal(JSON.parse(saved)[0].state,'pending');
  await p.drain();
  assert.equal(JSON.parse(saved)[0].state,'accepted');
  assert.equal(events.length,1);assert.match(events[0].id,/^[a-f0-9]{64}$/);
 }
});
test('corrupt nonempty outbox is rejected without overwriting state',async()=>{
 for(const initial of ['{invalid',' ']){
  let writes=0,calls=0;
  const p=createSaquettoOSPush({get:()=>initial,set:()=>writes++,env,channel:'operations',fetchImpl:async()=>{calls++;return Response.json({});}});
  assert.throws(()=>p.enqueue({id:'episode-1',title:'a',body:'b'}),SyntaxError);
  assert.throws(()=>p.nextAt(),SyntaxError);
  await assert.rejects(p.drain(),SyntaxError);
  assert.equal(writes,0);assert.equal(calls,0);
 }
});
