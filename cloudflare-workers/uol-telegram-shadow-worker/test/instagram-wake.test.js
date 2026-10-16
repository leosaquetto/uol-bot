import test from 'node:test';
import assert from 'node:assert/strict';
import { InstagramWakeQueue, authenticateWake, normalizeWake } from '../src/instagram-wake.js';
const now = 1792179000000;
const event = (id = 1) => ({ eventId: `00000000-0000-4000-8000-${String(id).padStart(12,'0')}`,
  profile: 'clubeuol', source: 'ios_shortcuts', observedAt: new Date(now).toISOString() });
function setup(fetchImpl) {
  let saved = '', clock = now;
  const env = { INSTAGRAM_WAKE_RELAY_URL: 'https://oracle.test/instagram-wake', INSTAGRAM_WAKE_RELAY_TOKEN: 'a'.repeat(64) };
  const queue = () => new InstagramWakeQueue(() => saved, (_, raw) => { saved = raw; }, env, fetchImpl, () => clock);
  return { queue, state: () => JSON.parse(saved), advance: ms => { clock += ms; } };
}
const ack = id => Response.json({ ok: true, persisted: true, eventId: id }, { status: 202 });
test('scoped per-phone credentials accept only their configured hash', async () => {
  const token = `w1.friend-1.${'d'.repeat(64)}`;
  const hash = Buffer.from(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(token))).toString('hex');
  const env = { INSTAGRAM_WAKE_CLIENTS: JSON.stringify({ 'friend-1': hash }) };
  assert.equal(await authenticateWake(new Request('https://w.test', {headers:{Authorization:`Bearer ${token}`}}), env), 'friend-1');
  assert.equal(await authenticateWake(new Request('https://w.test', {headers:{Authorization:`Bearer ${token.replace(/d$/, 'e')}`}}), env), '');
  assert.equal(await authenticateWake(new Request('https://w.test', {headers:{Authorization:'Bearer admin'}}), env), '');
  assert.equal(await authenticateWake(new Request('https://w.test', {headers:{Authorization:`Bearer ${token}`}}), {}), '');
});
test('unattempted hints from several phones coalesce, replay conflicts fail closed', async () => {
  const s = setup(), q = s.queue();
  assert.equal(q.enqueue(event(), 'owner').duplicate, false);
  assert.equal(q.enqueue(event(), 'owner').duplicate, true);
  q.enqueue(event(2), 'friend');
  assert.equal(s.state().pending.length, 1);
  assert.equal(s.state().pending[0].count, 2);
  assert.throws(() => q.enqueue({...event(), source:'scriptable'}, 'owner'), /identity_conflict/);
  assert.equal(s.queue().enqueue(event(), 'owner').duplicate, true);
});
test('persisted reservation precedes network; hints during relay get a later durable item', async () => {
  let release, sent = [];
  const s = setup(async (_, init) => {
    const body = JSON.parse(init.body); sent.push(body.eventId);
    assert.ok(s.state().pending[0].attempts > 0);
    if(sent.length === 1) await new Promise(resolve => {release = resolve;});
    return ack(body.eventId);
  });
  const q = s.queue();q.enqueue(event(1), 'owner');
  const drain = q.drain();
  q.enqueue(event(2), 'friend');
  assert.equal(s.state().pending.length, 2);
  assert.equal((await q.drain()).status, 'busy');
  release();await drain;
  assert.equal(sent.length, 2);
  assert.equal(s.state().pending.length, 0);
});
test('lost response and restart retry only the same event; invalid acknowledgements stay pending', async () => {
  const sent = []; let lose = true;
  const s = setup(async (_, init) => { const id=JSON.parse(init.body).eventId;sent.push(id);
    if(lose) throw new Error('response_lost');return ack(id); });
  const q=s.queue();q.enqueue(event(), 'owner');await q.drain();
  assert.equal(s.state().pending.length,1);
  lose=false;s.advance(15001);await s.queue().drain();
  assert.deepEqual(sent,[event().eventId,event().eventId]);
  assert.equal(s.state().pending.length,0);
  const bad=setup(async()=>Response.json({ok:true,persisted:true,eventId:event(9).eventId}));
  bad.queue().enqueue(event(),'owner');await bad.queue().drain();assert.equal(bad.state().pending.length,1);
});
test('hourly cap is durable and does not reset on restart', () => {
  const s=setup();for(let i=0;i<30;i++)s.queue().enqueue(event(i+1),'friend');
  assert.throws(()=>s.queue().enqueue(event(99),'friend'),/rate_limit/);
  assert.throws(()=>normalizeWake({...event(),profile:'anyone'},now),/invalid/);
  assert.equal(normalizeWake({...event(),observedAt:new Date(now-86400001).toISOString()},now).profile,'clubeuol');
  assert.throws(()=>normalizeWake({...event(),observedAt:new Date(now+300001).toISOString()},now),/time_invalid/);
});
test('relay never follows a redirect or returns its credential in diagnostics', async () => {
  const s=setup(async (url,init)=>{assert.equal(url,'https://oracle.test/instagram-wake');
    assert.equal(init.redirect,'error');assert.equal(init.headers.Authorization,`Bearer ${'a'.repeat(64)}`);
    return new Response('redirect',{status:302});});
  const q=s.queue();q.enqueue(event(),'owner');await q.drain();
  assert.equal(q.status().last.status,'relay_unconfirmed');assert.ok(!JSON.stringify(q.status()).includes('a'.repeat(64)));
});

test('relay outage over a day does not strand newer events behind an old durable hint', async()=>{
 let online=false; const sent=[];
 const s=setup(async(_,init)=>{const e=JSON.parse(init.body);if(!online)throw Error('offline');sent.push(e.eventId);return ack(e.eventId);});
 const q=s.queue();q.enqueue(event(1),'owner');await q.drain();s.advance(2*86400000);
 online=true;const restarted=s.queue();restarted.enqueue({...event(2),observedAt:new Date(now+2*86400000).toISOString()},'friend');
 await restarted.drain();assert.deepEqual(sent,[event(1).eventId,event(2).eventId]);assert.equal(restarted.status().pending,0);
});
