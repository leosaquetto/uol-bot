import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,readFileSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {validateConfig,loadConfig,matchRules,routedDestinationsVerified} from '../src/config.js';
import {openStore} from '../src/store.js';
import {sqliteAuth} from '../src/auth.js';
import {acceptedPush,postFromPush} from '../src/push.js';
import {canonicalPost,parsePost,formatPost,allowedImage,readLimited,fetchPost} from '../src/x-post.js';
import {createProcessor} from '../src/processor.js';
import {createSender} from '../src/sender.js';
import {acquireLock} from '../src/process-lock.js';
import {createPilot} from '../src/pilot.js';

const example=JSON.parse(readFileSync(new URL('../config.example.json',import.meta.url),'utf8'));
const config=()=>{
  const c=structuredClone(example);c.destinations['lover-tour']={type:'group',jid:'12345@g.us',verified:true};return c;
};
const fixture=()=>{
  const dir=mkdtempSync(join(tmpdir(),'drops-test-'));const path=join(dir,'state.sqlite');const store=openStore(path);
  return {dir,path,store,close(){store.close();rmSync(dir,{recursive:true,force:true});}};
};
const post={author:'taylorswift13',id:'2102952373022851247',type:'post',url:'https://x.com/taylorswift13/status/2102952373022851247',
  name:'Taylor Swift',text:'PLANTÃO: novo álbum',publishedAt:new Date().toISOString(),imageUrl:'',avatarUrl:''};
const event={origin:'https://x.com',service:'pushMessaging',timestamp:12,instanceId:'a',eventName:'Push event',eventMetadata:[{key:'url',value:post.url}]};

test('configuration rejects unverified shapes and preserves last valid version',()=>{
  const c=validateConfig(config());const f=fixture();
  try {
    for(const mutate of [c=>c.sources=['bad/name'],c=>c.operation.minDelayMs=0,c=>c.rules[0].destinations=['missing'],c=>c.destinations['lover-tour'].jid='private.invalid']){
      const bad=structuredClone(c);mutate(bad);assert.throws(()=>validateConfig(bad));
    }
    writeFileSync(join(f.dir,'config.json'),'{broken');
    assert.deepEqual(loadConfig(join(f.dir,'config.json'),c),{config:c,error:'invalid_json'});
  }finally{f.close();}
});
test('rules normalize case and accents, combine terms and destinations, exclude reply and repost',()=>{
  const c=config();c.rules[0].any=['plantão'];c.rules[0].all=['ALBUM'];c.rules[0].none=['cancelado'];
  c.rules.push({...c.rules[0],id:'duplicate'});
  assert.deepEqual(matchRules(c,post),['lover-tour']);
  for(const p of [{...post,type:'reply'},{...post,type:'repost'},{...post,author:'unrelated'},{...post,text:'plantao album cancelado'}])assert.deepEqual(matchRules(c,p),[]);
  assert.deepEqual(matchRules(c,{...post,type:'quote'}),['lover-tour']);
});
test('saved destinations do not route or block activation until referenced by a rule',()=>{
  const c=config();c.destinations['future-group']={type:'group',jid:'777@g.us',verified:false};
  validateConfig(c);
  assert.equal(routedDestinationsVerified(c),true);
  assert.deepEqual(matchRules(c,post),['lover-tour']);
  c.rules[0].destinations.push('future-group');
  assert.equal(routedDestinationsVerified(c),false);
  c.destinations['future-group'].verified=true;
  assert.equal(routedDestinationsVerified(c),true);
});
test('push identity is exact, foreign origins and conflicting links fail closed',()=>{
  assert.equal(acceptedPush(event),true);assert.equal(acceptedPush({...event,origin:'https://evil.test'}),false);
  for (const eventMetadata of [[null],[{value:'x'}],[{key:1,value:'x'}],[{key:'x',value:42}]]) {
    assert.equal(acceptedPush({...event,eventMetadata}),false);
  }
  assert.deepEqual(postFromPush(event),{author:post.author,id:post.id,url:post.url});
  assert.equal(postFromPush({...event,eventMetadata:[{key:'text',value:'new tweet 123'}]}),null);
  assert.equal(postFromPush({...event,eventMetadata:[...event.eventMetadata,{key:'other',value:'https://x.com/other/status/2102952373022851248'}]}),null);
});
test('ledger deduplicates events, preserves ambiguous sends after restart, isolates destinations and prioritizes alerts',()=>{
  const f=fixture();let reopened;
  try {
    assert.equal(f.store.recordPush(event).added,true);assert.equal(f.store.recordPush(event).added,false);
    const a=f.store.enqueue({key:'x:1:a',destination:'12345@g.us',payload:{text:'A'},priority:10});
    assert.equal(f.store.enqueue({key:'x:1:a',destination:'12345@g.us',payload:{text:'A'}}).id,a.id);
    assert.throws(()=>f.store.enqueue({key:'x:1:a',destination:'12345@g.us',payload:{text:'B'}}),/conflict/);
    const b=f.store.enqueue({key:'offer:2:b',destination:'456@g.us',payload:{text:'B'},priority:0});
    assert.equal(f.store.claim().id,b.id);f.store.close();reopened=openStore(f.path);
    assert.equal(reopened.job(b.id).state,'unknown');assert.equal(reopened.claim().id,a.id);
    assert.equal(reopened.claim(),null);
  }finally{reopened?.close();rmSync(f.dir,{recursive:true,force:true});}
});
test('auth persists typed buffers and rolls back invalid batches atomically',async()=>{
  const f=fixture();
  try{
    const a=sqliteAuth(f.store);a.saveCreds();await a.state.keys.set({session:{one:Buffer.from([1,2,3])}});
    const b=sqliteAuth(f.store);assert.deepEqual(b.state.creds.noiseKey,a.state.creds.noiseKey);
    assert.deepEqual((await b.state.keys.get('session',['one'])).one,Buffer.from([1,2,3]));
    await assert.rejects(b.state.keys.set({session:{two:Buffer.from([2]),bad:1n}}));
    assert.equal((await b.state.keys.get('session',['two'])).two,null);
  }finally{f.close();}
});
test('process lock prevents concurrent writers',()=>{
  const f=fixture();try{const path=join(f.dir,'lock');const unlock=acquireLock(path);assert.throws(()=>acquireLock(path),/already_running/);unlock();acquireLock(path)();}finally{f.close();}
});

const html=()=>`<meta property="og:title" content="Taylor Swift (@taylorswift13) on X"><article><a href="/taylorswift13/status/${post.id}">time</a><div data-testid="tweetText">Full text<br>second <img alt="♥"> <a href="https://example.com/full">short</a></div><img src="https://pbs.twimg.com/profile_images/1/avatar_normal.jpg"></article>`;
test('extractor preserves body, publication time and identity; excludes quoted text and unsafe images',()=>{
  const p=parsePost(html(),post.url);assert.match(p.text,/Full text\nsecond ♥ https:\/\/example.com\/full/);
  assert.equal(p.imageUrl,'https://pbs.twimg.com/profile_images/1/avatar_400x400.jpg');assert.equal(p.type,'post');
  const quote=html().replace('</article>','<a href="/other/status/2102952373022851248">Quote</a><div data-testid="tweetText">other author</div></article>');
  const q=parsePost(quote,post.url);assert.equal(q.type,'quote');assert.ok(!q.text.includes('other author'));
  assert.match(formatPost(p).text,/\?s=46```$/);assert.equal(formatPost(p).preview.summary,'');
  assert.throws(()=>parsePost(html().replace('Full text','<a data-testid="tweet-text-show-more-link">more</a>'),post.url),/incomplete/);
  assert.throws(()=>parsePost(html(),'https://x.com/other/status/'+post.id),/missing/);
  const overlay=html().replace('<a href="/taylorswift13/status/',
    '<div class="pointer-events-none absolute"><a href="/other/status/2102952373022851248">parent</a></div><a href="/taylorswift13/status/');
  assert.equal(parsePost(overlay,post.url).id,post.id);
  assert.throws(()=>allowedImage('https://pbs.twimg.com.evil.test/media/a'),/not_allowed/);
  assert.throws(()=>canonicalPost('https://x.com@evil.test/user/status/'+post.id),/invalid/);
});
test('bounded downloads stop at byte limit and preserve rate-limit errors',async()=>{
  await assert.rejects(readLimited(new Response('large'),2),/too_large/);
  await assert.rejects(readLimited(new Response('',{status:429})),/rate_limited/);
});
test('post fetch follows only a redirect to the same identity',async()=>{
  const redirects=[];
  const fetchImpl=async (url)=>{
    redirects.push(url);
    return redirects.length===1 ? new Response(null,{status:307,headers:{location:post.url+'?s=20'}}) : new Response(html());
  };
  assert.equal((await fetchPost(post.url,{fetchImpl})).id,post.id);
  assert.equal(redirects.length,2);
  let calls=0;
  await assert.rejects(fetchPost(post.url,{fetchImpl:async()=>{
    calls++;
    return new Response(null,{status:307,headers:{location:'https://x.com/other/status/'+post.id}});
  }}),/unexpected_post_redirect/);
  assert.equal(calls,1);
});
test('processor simulates without enqueueing, ignores old posts and deduplicates repeated notifications',async()=>{
  const f=fixture();const c=config();const run=createProcessor({store:f.store,getConfig:()=>c,readPost:async()=>post});
  try{
    f.store.recordPush(event);await run();assert.equal(f.store.snapshot().jobs.length,0);
    c.operation.dryRun=false;c.operation.paused=false;f.store.setSetting('activated_at','2020-01-01T00:00:00.000Z');
    f.store.recordPush({...event,timestamp:13});await run();
    f.store.recordPush({...event,timestamp:14});await run();
    assert.equal(f.store.snapshot().jobs[0].count,1);
    f.store.setSetting('activated_at','2099-01-01T00:00:00.000Z');f.store.recordPush({...event,timestamp:15});await run();
    assert.equal(f.store.snapshot().jobs[0].count,1);
  }finally{f.close();}
});
test('unsupported push types are ignored and post failures keep a safe reason',async()=>{
  const f=fixture();const c=config();c.operation.dryRun=false;c.operation.paused=false;
  try{
    f.store.recordPush(event);
    await createProcessor({store:f.store,getConfig:()=>c,decodeEvent:()=>{throw new Error('unsupported_notification_type');}})();
    assert.equal(f.store.db.prepare('SELECT state FROM push_events LIMIT 1').get().state,'ignored');
    f.store.recordPush({...event,timestamp:13});
    await createProcessor({store:f.store,getConfig:()=>c,readPost:async()=>{throw new Error('post_unavailable');}})();
    assert.equal(f.store.db.prepare("SELECT code FROM push_events WHERE state='pending_review'").get().code,'post_unavailable');
  }finally{f.close();}
});
test('sender requires gates, records ambiguity, never retries an uncertain dispatch and honors receipts',async()=>{
  const f=fixture();const c=config();c.operation={paused:false,dryRun:false,minDelayMs:5000};let enabled=false,sends=0;
  const whatsapp={isReady:()=>true,verifyDestination:async()=>{},socket:()=>({sendMessage:async()=>{sends++;throw new Error('network timeout');}})};
  const run=createSender({store:f.store,getConfig:()=>c,whatsapp,canSend:()=>enabled,prepare:async()=>({text:'hello'})});
  try{
    const job=f.store.enqueue({key:'a',destination:'12345@g.us',payload:{destinationAlias:'lover-tour',text:'hello'}});
    await run();assert.equal(sends,0);enabled=true;await run();assert.equal(sends,1);assert.equal(f.store.job(job.id).state,'unknown');
    f.store.setSetting('last_dispatch_at','0');await run();assert.equal(sends,1);
    const sent=f.store.job(job.id);f.store.receipt(sent.message_id,'wrong@g.us','confirmed','recipient_receipt');assert.equal(f.store.job(job.id).state,'unknown');
    f.store.receipt(sent.message_id,sent.destination,'accepted','server_ack');assert.equal(f.store.job(job.id).state,'accepted');
    f.store.receipt(sent.message_id,sent.destination,'confirmed','recipient_receipt');assert.equal(f.store.job(job.id).state,'confirmed');
    f.store.receipt(sent.message_id,sent.destination,'accepted','server_ack');assert.equal(f.store.job(job.id).state,'confirmed');
  }finally{f.close();}
});
test('pre-dispatch failure is safely retryable and a late pause prevents sending',async()=>{
  const f=fixture();const c=config();c.operation={paused:false,dryRun:false,minDelayMs:5000};let sends=0;
  const whatsapp={isReady:()=>true,verifyDestination:async()=>{},socket:()=>({sendMessage:async()=>sends++})};
  try{
    const job=f.store.enqueue({key:'b',destination:'12345@g.us',payload:{destinationAlias:'lover-tour',text:'hello'}});
    const failing=createSender({store:f.store,getConfig:()=>c,whatsapp,canSend:()=>true,prepare:async()=>{throw new Error('image failed');}});
    await failing();assert.equal(sends,0);assert.equal(f.store.job(job.id).state,'queued');assert.ok(f.store.job(job.id).available_at>Date.now());
    f.store.updateJob(job.id,'queued');
    const pausing=createSender({store:f.store,getConfig:()=>c,whatsapp,canSend:()=>true,prepare:async()=>{c.operation.paused=true;return {text:'x'};}});
    await pausing();assert.equal(sends,0);assert.equal(f.store.job(job.id).state,'queued');
  }finally{f.close();}
});

test('destination removed, replaced or unverified during preparation prevents dispatch',async()=>{
  for (const change of [c=>delete c.destinations['lover-tour'],c=>c.destinations['lover-tour'].jid='67890@g.us',c=>c.destinations['lover-tour'].verified=false]) {
    const f=fixture();const c=config();c.operation={paused:false,dryRun:false,minDelayMs:5000};let sends=0;
    const whatsapp={isReady:()=>true,verifyDestination:async()=>{},socket:()=>({sendMessage:async()=>sends++})};
    try {
      const job=f.store.enqueue({key:'changed',destination:'12345@g.us',payload:{destinationAlias:'lover-tour',text:'hello'}});
      const run=createSender({store:f.store,getConfig:()=>c,whatsapp,canSend:()=>true,prepare:async()=>{change(c);return {text:'x'};}});
      await run();assert.equal(sends,0);assert.equal(f.store.job(job.id).state,'queued');
    } finally {f.close();}
  }
});

test('explicit pilot sends only a stored notified post to fixed destinations while automation stays paused',async()=>{
  const f=fixture();const c=config();let sends=0,enabled=true;
  const whatsapp={isReady:()=>true,ownDestination:()=>({type:'contact',jid:'123@s.whatsapp.net',verified:true}),verifyDestination:async()=>{},socket:()=>({sendMessage:async()=>sends++})};
  const pilot=createPilot({store:f.store,getConfig:()=>c,whatsapp,enabled:()=>enabled,decodeEvent:e=>e,readPost:async()=>post});
  try{
    const {id:eventId}=f.store.recordPush(event);
    const automatic=f.store.enqueue({key:'auto',destination:'12345@g.us',payload:{destinationAlias:'lover-tour',text:'automatic'},priority:-1});
    await assert.rejects(pilot.enqueue({alias:'arbitrary',eventId}),/not_allowed/);
    await assert.rejects(pilot.enqueue({alias:'self',eventId:'0'.repeat(64)}),/not_found/);
    const job=await pilot.enqueue({alias:'self',eventId});
    assert.equal((await pilot.enqueue({alias:'self',eventId})).id,job.id);
    const run=createSender({store:f.store,getConfig:()=>c,whatsapp,canSend:()=>false,canPilot:()=>enabled,prepare:async()=>({text:'pilot'})});
    await run();assert.equal(sends,1);assert.equal(f.store.job(job.id).state,'unknown');assert.equal(f.store.job(automatic.id).state,'queued');
    f.store.setSetting('last_dispatch_at','0');await run();assert.equal(sends,1);
    await assert.rejects(pilot.enqueue({alias:'self',eventId,revision:'bad/revision'}),/invalid_revision/);
    const revision=await pilot.enqueue({alias:'self',eventId,revision:'visual-v2'});
    assert.notEqual(revision.id,job.id);
    assert.equal((await pilot.enqueue({alias:'self',eventId,revision:'visual-v2'})).id,revision.id);
    assert.equal(f.store.job(job.id).state,'unknown');
    enabled=false;await assert.rejects(pilot.enqueue({alias:'lover-tour',eventId}),/disabled/);
  }finally{f.close();}
});

test('self-only pilot blocks group enqueue and dispatch of an existing group pilot job',async()=>{
  const f=fixture();const c=config();let sends=0;
  const enabled=alias=>!alias||alias==='self';
  const whatsapp={isReady:()=>true,verifyDestination:async()=>{},socket:()=>({sendMessage:async()=>sends++})};
  const pilot=createPilot({store:f.store,getConfig:()=>c,whatsapp,enabled,decodeEvent:e=>e,readPost:async()=>post});
  try {
    const {id:eventId}=f.store.recordPush(event);
    await assert.rejects(pilot.enqueue({alias:'lover-tour',eventId}),/pilot_disabled/);
    const job=f.store.enqueue({key:'group-pilot',destination:'12345@g.us',payload:{destinationAlias:'lover-tour',pilot:true,text:'blocked'}});
    const run=createSender({store:f.store,getConfig:()=>c,whatsapp,canSend:()=>false,canPilot:enabled});
    await run();assert.equal(sends,0);assert.equal(f.store.job(job.id).state,'failed');
    assert.equal(f.store.job(job.id).code,'pilot_destination_not_allowed');
  } finally {f.close();}
});

test('pilot cannot target groups or other contacts even with permissive gates and matching rules',async()=>{
  const f=fixture();const c=config();let sends=0;
  const whatsapp={isReady:()=>true,ownDestination:()=>({type:'contact',jid:'123@s.whatsapp.net',verified:true}),
    verifyDestination:async()=>{},socket:()=>({sendMessage:async()=>sends++})};
  const makePilot=readPost=>createPilot({store:f.store,getConfig:()=>c,whatsapp,enabled:()=>true,decodeEvent:e=>e,readPost});
  try {
    const eventId=f.store.recordPush(event).id;
    let reads=0;const pilot=makePilot(async()=>{reads++;return post;});
    for(const alias of ['lover-tour','another-contact']){
      await assert.rejects(pilot.enqueue({alias,eventId}),/pilot_destination_not_allowed/);
    }
    assert.equal(reads,0);
    const job=f.store.enqueue({key:'legacy-group-pilot',destination:'12345@g.us',payload:{destinationAlias:'lover-tour',pilot:true,text:'blocked'}});
    const run=createSender({store:f.store,getConfig:()=>c,whatsapp,canSend:()=>false,canPilot:()=>true,
      prepare:async()=>{throw new Error('must_not_prepare_group_test');}});
    await run();assert.equal(sends,0);assert.equal(f.store.job(job.id).state,'failed');
    assert.equal(f.store.job(job.id).code,'pilot_destination_not_allowed');
  } finally {f.close();}
});
