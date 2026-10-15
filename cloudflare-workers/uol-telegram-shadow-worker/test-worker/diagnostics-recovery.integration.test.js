import { env, exports } from 'cloudflare:workers';
import { runInDurableObject } from 'cloudflare:test';
import { describe, expect, it, vi } from 'vitest';
import worker from '../src/worker.js';
const admin={Authorization:'Bearer vitest-admin-token-not-a-secret'};
function insert(state,id,fields={}) {
  const row={id,link:`https://clube.uol.com.br/beneficios/${id}`,preview_title:'Private title',first_seen_at:'2026-01-01',last_seen_at:'',status:'delivered',...fields};
  state.storage.sql.exec(`INSERT INTO offers(${Object.keys(row).join(',')}) VALUES (${Object.keys(row).map(()=>'?').join(',')})`,...Object.values(row));
}
function configure(instance) {
  instance.env={...instance.env,DELIVERY_MODE:'live',TELEGRAM_TOKEN:'test',TELEGRAM_CHAT_ID:'-100111',CANAL2_DELIVERY_ENABLED:'false',DISCORD_DELIVERY_ENABLED:'false',BEEPER_DELIVERY_ENABLED:'false'};
  instance.setMetadata('delivery_mode_override','live');
}
describe('bounded input and evidence-based recovery',()=>{
  it('looks up old IDs independently of the recent inventory, without content disclosure or sends',async()=>{
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName('diagnostic-old'),async(instance,state)=>{
      insert(state,'old',{description:'Private description',main_sold_out_error:'telegram_editMessageText_400: bad target'});
      for(let i=0;i<301;i++)insert(state,`recent-${i}`,{first_seen_at:'2026-10-15'});
      expect((await instance.getInventory(300)).some(row=>row.id==='old')).toBe(false);
      const found=instance.getInventoryById('old');
      expect(found.id).toBe('old');expect(found.maintenance.main_sold_out.error).toContain('bad target');
      expect(JSON.stringify(found)).not.toContain('Private');
      expect(instance.getInventoryById('missing')).toBeNull();
    });
    expect((await exports.default.fetch('https://worker.test/inventory?id=missing',{headers:admin})).status).toBe(404);
    expect((await exports.default.fetch('https://worker.test/inventory?id=missing')).status).toBe(401);
  });
  it('grants exactly one sold-out edit of the same message and preserves the other destination',async()=>{
    const calls=[];
    vi.stubGlobal('fetch',async(url,init)=>{calls.push({url,body:JSON.parse(init.body)});return Response.json({ok:false,error_code:400,description:'Bad Request: target invalid'},{status:400});});
    try {await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName('sold-out-one-edit'),async(instance,state)=>{
      configure(instance);insert(state,'sold',{status:'sold_out',sold_out_at:'2026-10-01',main_message_id:91,main_message_kind:'text',main_sold_out_attempts:10,canal2_sold_out_attempts:7,canal2_message_id:92});
      instance.requeueDelivery('sold','main_sold_out');
      await instance.processSoldOutSync(new Date());await instance.processSoldOutSync(new Date());
      const row=state.storage.sql.exec("SELECT * FROM offers WHERE id='sold'").one();
      expect(row.main_sold_out_attempts).toBe(10);expect(row.canal2_sold_out_attempts).toBe(7);
      expect(row.main_message_id).toBe(91);
      expect(instance.deliveryTimelineForOffer('sold').some(x=>x.state==='requeued')).toBe(true);
    });expect(calls).toHaveLength(1);expect(calls[0].url).toContain('/editMessageText');expect(calls[0].body.message_id).toBe(91);
    } finally {vi.unstubAllGlobals();}
  });
  it('keeps the unconfirmed comment remainder uncertain and records only proven IDs',async()=>{
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName('partial-comment-proof'),async(instance,state)=>{
      configure(instance);insert(state,'comment',{description:'details '.repeat(1500),discussion_message_id:20,comment_delivery_unknown_at:'2026-10-13',comment_delivery_attempts:1});
      expect(instance.getInventoryById('comment').comment.expectedChunks).toBeGreaterThan(1);
      instance.resolveDeliveryUnknown('comment','comment','sent',{messageIds:[21]});
      const row=state.storage.sql.exec("SELECT * FROM offers WHERE id='comment'").one();
      expect(row.comment_sent_at).toBe('');expect(row.comment_delivery_unknown_at).toBe('2026-10-13');expect(row.comment_message_ids).toBe('[21]');
      expect(await instance.processDiscussionComments(1)).toEqual({sent:0,failed:0});
      const timeline=instance.deliveryTimelineForOffer('comment');expect(timeline.some(x=>x.state==='resolved_partial')).toBe(true);
    });
  });
  it('clears only the resolved legacy unknown target and keeps its receipt in the ledger',async()=>{
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName('legacy-unknown-resolution'),(instance,state)=>{
      configure(instance);insert(state,'legacy',{status:'delivery_unknown',delivery_unknown_at:'2026-10-01',delivery_unknown_target:'main,discord',main_delivery_unknown_at:'2026-10-01',discord_delivery_unknown_at:'2026-10-01'});
      instance.resolveDeliveryUnknown('legacy','main','sent',{messageId:41});
      const row=state.storage.sql.exec("SELECT * FROM offers WHERE id='legacy'").one();
      expect(row.delivery_unknown_target).toBe('discord');expect(row.main_message_id).toBe(41);
      expect(instance.deliveryTimelineForOffer('legacy').some(x=>x.state==='resolved_sent'&&x.externalId==='41')).toBe(true);
    });
  });
  it('historical API cards cannot refresh success while the current API fails',async()=>{
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName('historical-api-evidence'),instance=>{
      const now=new Date(),previous=new Date(now.getTime()-3600000).toISOString();
      instance.setRuntimeSnapshot('source_health',{fullSourceSuccessAt:previous});
      const cards=Array.from({length:5},(_,i)=>({id:`c${i}`,link:`https://clube.uol.com.br/beneficios/c${i}`,previewTitle:'Card'}));
      const args={listingResult:{status:'fulfilled'},ticketListingResult:{status:'fulfilled'},mainListingCards:cards,ticketListingCards:[],apiCards:cards,now};
      instance.updateSourceHealth({...args,apiResult:{status:'rejected'}});
      expect(instance.runtimeSnapshot('source_health').fullSourceSuccessAt).toBe(previous);
      const observed=new Date(now.getTime()-1000).toISOString();
      instance.updateSourceHealth({...args,apiResult:{status:'fulfilled',successAt:observed}});
      expect(instance.runtimeSnapshot('source_health').fullSourceSuccessAt).toBe(observed);
      instance.updateSourceHealth({...args,now:new Date(now.getTime()+60000),apiResult:{status:'fulfilled',successAt:observed}});
      expect(instance.runtimeSnapshot('source_health').fullSourceSuccessAt).toBe(observed);
    });
  });
  it.each([['/mode',16*1024,admin],['/run',16*1024,admin],['/telegram-webhook',1024*1024,{'X-Telegram-Bot-Api-Secret-Token':'vitest-webhook-token-not-a-secret'}]])('cancels an undeclared oversized %s body before processing',async(path,limit,headers)=>{
    let cancelled=false;
    const body=new ReadableStream({pull(c){c.enqueue(new Uint8Array(limit+1));},cancel(){cancelled=true;}});
    const response=await worker.fetch(new Request(`https://worker.test${path}`,{method:'POST',headers,body,duplex:'half'}),env,{});
    expect(response.status).toBe(413);expect(cancelled).toBe(true);
  });
  it('does not recover source-health from an earlier success while the current API fails',async()=>{
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName('source-current-failure'),instance=>{
      const now=new Date(),at=new Date(now.getTime()-60000).toISOString();
      const row={key:'source-health',last_detected_at:new Date(now.getTime()-120000).toISOString()};
      const sourceHealth={listingFailureStreak:0,listingDropStreak:0,sourceDivergenceStreak:0,fullSourceSuccessAt:at};
      for(const failure of [{lastError:'uol_api_http_503'},{failureStreak:1},{lastRunError:'failed'}]) {
        expect(instance.operationalRecoveryObservedAt(row,{api:{lastError:'',failureStreak:0,...failure},sourceHealth},now)).toBeNull();
      }
      expect(instance.operationalRecoveryObservedAt(row,{api:{lastError:'',failureStreak:0},sourceHealth},now)).toBe(Date.parse(at));
    });
  });
});
