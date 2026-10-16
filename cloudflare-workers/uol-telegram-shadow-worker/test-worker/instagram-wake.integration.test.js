import { env, exports } from 'cloudflare:workers';
import { runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
const token = `w1.test.${'a'.repeat(64)}`;
const payload = () => ({eventId:crypto.randomUUID(),profile:'clubeuol',source:'ios_shortcuts',observedAt:new Date().toISOString()});
describe('Instagram phone notification ingress', () => {
  it('phones cannot use the ingest/admin credential or read monitor status', async () => {
    for(const path of ['/instagram-wake','/instagram-story-status','/instagram-wake-status']) {
      const r=await exports.default.fetch(`https://worker.test${path}`,{method:path==='/instagram-wake'?'POST':'GET',
        headers:{Authorization:`Bearer ${path==='/instagram-wake'?'vitest-admin-token-not-a-secret':token}`},body:path==='/instagram-wake'?'{}':undefined});
      expect(r.status).toBe(401);
    }
  });
  it('persists a wake receipt without creating an offer or destination delivery', async () => {
    const p=payload(), headers={Authorization:`Bearer ${token}`,'Content-Type':'application/json'};
    const first=await exports.default.fetch('https://worker.test/instagram-wake',{method:'POST',headers,body:JSON.stringify(p)});
    expect(first.status).toBe(202);expect(await first.json()).toMatchObject({ok:true,accepted:true,eventId:p.eventId,duplicate:false});
    const second=await exports.default.fetch('https://worker.test/instagram-wake',{method:'POST',headers,body:JSON.stringify(p)});
    expect(second.status).toBe(202);expect(await second.json()).toMatchObject({duplicate:true});
  });
  it('cancels a large body before processing and rejects profile/UUID substitution', async () => {
    const headers={Authorization:`Bearer ${token}`};
    const large=await exports.default.fetch('https://worker.test/instagram-wake',{method:'POST',headers,body:'x'.repeat(4097)});
    expect(large.status).toBe(413);
    const bad=await exports.default.fetch('https://worker.test/instagram-wake',{method:'POST',headers,body:JSON.stringify({...payload(),profile:'leotheblueblur'})});
    expect(bad.status).toBe(400);
  });
  it('durable queue uses the existing metadata table and never changes offer or Story schemas', async () => {
    const stub=env.UOL_TELEGRAM_SHADOW.getByName('notification-schema-isolation');
    const p=payload();await stub.recordInstagramWake(p,'test');
    await runInDurableObject(stub,(instance,state)=>{
      const saved=JSON.parse(instance.metadataValue('instagram_notification_wake_v1'));
      expect(saved.pending).toHaveLength(1);expect(saved.pending[0].event.eventId).toBe(p.eventId);
      expect(state.storage.sql.exec('SELECT count(*) AS n FROM offers').one().n).toBe(0);
      expect(state.storage.sql.exec('SELECT count(*) AS n FROM instagram_story_outbox').one().n).toBe(0);
    });
  });
});

it('preserves the native Workers fetch receiver without network access', async()=>{
  const stub=env.UOL_TELEGRAM_SHADOW.getByName('notification-native-fetch-context');
  await runInDurableObject(stub,async(instance)=>{
    const response=await instance.instagramWake.fetch('data:application/json,%7B%22ok%22%3Atrue%7D');
    expect(await response.json()).toEqual({ok:true});
  });
});
