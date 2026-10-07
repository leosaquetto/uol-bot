import { DurableObject } from 'cloudflare:workers';
import campaigns from '../campaigns.json';
import { Ledger } from './ledger.js';
import { UolClient } from './uol-client.js';
import { parseCatalog, parseOffer, parseHistory, matchOffer, normalizeText } from './offers.js';
import { buildSuccessMessage, buildBlockedMessage, sendNtfy } from './notify.js';
import { digest, seal, unseal, normalizeLogin, readAccounts, matchesToken } from './security.js';
import { recognizeArtwork } from './artwork.js';

const CLOSED = new Set(['paused','blocked','expired','confirmed']);
const MONTH = now => {
  const parts=new Intl.DateTimeFormat('en-CA',{timeZone:'America/Sao_Paulo',year:'numeric',month:'2-digit'}).formatToParts(new Date(now));
  return `${parts.find(p=>p.type==='year').value}-${parts.find(p=>p.type==='month').value}`;
};
const campaignById = id => campaigns.find(c => c.id === id);
const response = (body,status=200) => Response.json(body,{status,headers:{'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'}});
const ticketEntries = history => history.entries.filter(entry => entry.category === 'campanhasdeingresso');
const baselineOf = history => ticketEntries(history).map(({id,title,imageUrl}) => ({id,title,imageUrl})).sort((a,b)=>a.id.localeCompare(b.id));
const sameBaseline = (left,right) => JSON.stringify(left) === JSON.stringify(right);
const safeCode = e => ({AUTH_REQUIRED:'auth_required',IDENTITY_MISMATCH:'identity_mismatch',CHALLENGE_REQUIRED:'captcha_required',HISTORY_INCOMPLETE:'history_incomplete',URL_BLOCKED:'site_changed',RESPONSE_TOO_LARGE:'site_changed',REQUEST_TIMEOUT:'upstream_unavailable',REQUEST_FAILED:'upstream_unavailable'}[e?.code] || 'verification_failed');
const isExpectedSlot=(offer,campaign)=>{
  const title=normalizeText(offer.title);
  const [,month,day]=campaign.date.split('-');
  return campaign.artistAliases.some(v=>title.includes(normalizeText(v)))
    || (campaign.venueAliases.some(v=>title.includes(normalizeText(v)))
      && new RegExp(`\\b0?${Number(day)}\\s*[/.-]\\s*0?${Number(month)}\\b`).test(title));
};

export function candidateCard(card,campaign) {
  const title=normalizeText(card.title);
  return campaign.venueAliases.some(v=>title.includes(normalizeText(v)))
      || campaign.artistAliases.some(v=>title.includes(normalizeText(v)));
}
export function confirmNewVoucher(history,attempt) {
  const old=new Set(attempt.baseline.entries.map(e=>e.id));
  const offer=attempt.baseline.offer;
  const matches=ticketEntries(history).filter(e=>!old.has(e.id)
    && normalizeText(e.title)===normalizeText(offer.title)
    && e.imageUrl && e.imageUrl===offer.imageUrl);
  return matches.length===1 ? matches[0] : null;
}

export class RedemptionAccount extends DurableObject {
  constructor(ctx,env) {
    super(ctx,env);
    this.ledger=new Ledger(ctx.storage);
    this.busy=false;
  }
  async exclusive(fn) {
    if(this.busy)throw Error('busy');
    this.busy=true;
    try{return await fn();}finally{this.busy=false;}
  }
  credential(accountId) {
    const entry=readAccounts(this.env)[accountId];
    if(!entry || entry.enabled!==true)throw Error('account_disabled');
    return entry;
  }
  async newClient(cfg) {
    const cred=this.credential(cfg.accountId);
    if(await digest(normalizeLogin(cred.quotaOwnerKey||cred.login))!==cfg.accountHash)throw Error('account_changed');
    const session=await unseal(this.ledger.getState('session'),this.env.SESSION_KEY,cfg.accountHash);
    return new UolClient({cookies:session.cookies});
  }
  async saveClient(client,cfg) {
    this.ledger.setState('session',await seal({cookies:client.snapshotCookies()},this.env.SESSION_KEY,cfg.accountHash));
  }
  persistConfig(cfg) {
    const current=this.ledger.getState('config');
    if(current?.mode==='paused' && cfg.mode!=='confirmed')cfg.mode='paused';
    this.ledger.setState('config',cfg);
  }
  async artworkEvidence(offer,campaign,cached=null) {
    const key=`artwork:${campaign.id}:${await digest(offer.imageUrl||'')}`;
    const saved=cached||this.ledger.getState(key);
    const budgetKey=`ocr-count:${campaign.id}`;
    const count=this.ledger.getState(budgetKey)||0;
    if(!saved && count>=3)throw Object.assign(Error('ocr_limit'),{code:'OCR_UNVERIFIED'});
    if(!saved)this.ledger.setState(budgetKey,count+1);
    const result=await recognizeArtwork(offer.imageUrl,{browserBinding:this.env.BROWSER,cached:saved});
    if(!result.ok && result.causeCode==='browser_rate_limited'){
      if(!saved)this.ledger.setState(budgetKey,count);
      throw Object.assign(Error('browser_rate_limited'),{code:'REQUEST_FAILED'});
    }
    if(!result.ok)throw Object.assign(Error('ocr_unverified'),{code:'OCR_UNVERIFIED'});
    this.ledger.setState(key,result);
    return result;
  }
  async probeArtwork(sampleIndex=0) {
    return this.exclusive(async()=>{
      const cfg=this.ledger.getState('config');if(!cfg)throw Error('not_configured');
      if(cfg.mode==='active'||cfg.mode==='reconciling')throw Error('pause_before_bootstrap');
      const client=await this.newClient(cfg);
      const catalog=await client.getCatalog();
      if(catalog.status!==200)throw Error('catalog_unavailable');
      if(!Number.isInteger(sampleIndex)||sampleIndex<0||sampleIndex>8)throw Error('invalid_artwork_sample');
      const sample=parseCatalog(catalog.html).filter(e=>e.imageUrl)[sampleIndex];
      if(!sample)throw Error('no_artwork');
      const result=await recognizeArtwork(sample.imageUrl,{browserBinding:this.env.BROWSER});
      cfg.ocrReady=result.ok;this.persistConfig(cfg);
      return {ok:result.ok,reason:result.reason||null,confidentWords:result.words.length,
        ...(result.stage?{stage:result.stage,causeCode:result.causeCode}:{}),
        ...(result.assetCode?{assetCode:result.assetCode}:{}),
        ...(result.totalWords!==undefined?{totalWords:result.totalWords,maxConfidence:result.maxConfidence}:{}),
        ...(result.httpStatus?{httpStatus:result.httpStatus}:{})};
    });
  }
  checkIdentity(history,cfg) {
    if(!history.authenticated || !history.historyScopeFound
      || normalizeText(history.signedInName)!==normalizeText(cfg.expectedName))throw Object.assign(Error('identity_mismatch'),{code:'IDENTITY_MISMATCH'});
    if(history.hasPagination)throw Object.assign(Error('history_incomplete'),{code:'HISTORY_INCOMPLETE'});
  }
  async bootstrap(accountId,body) {
    return this.exclusive(async()=>{
      const cred=this.credential(accountId);
      if(body.identity?.verified!==true || body.identity?.source!=='https://sac.uol.com.br/'
        || normalizeLogin(body.identity.login)!==normalizeLogin(cred.login))throw Error('identity_not_verified');
      const now=Date.now();
      if(body.quotaAttestedMonth!==MONTH(now))throw Error('monthly_attestation_required');
      const existing=this.ledger.getState('config');
      if(existing?.mode==='active' || existing?.mode==='reconciling')throw Error('pause_before_bootstrap');
      const accountHash=await digest(normalizeLogin(cred.quotaOwnerKey||cred.login));
      const client=new UolClient({cookies:body.cookies});
      const history=parseHistory((await client.getHistory()).html);
      const cfg={accountId,accountHash,expectedName:cred.expectedName,label:cred.label||accountId,
        mode:'prepared',quotaAttestedMonth:body.quotaAttestedMonth,baseline:baselineOf(history),
        initializedAt:now,identityVerifiedAt:now,campaignId:null,lastResult:'prepared'};
      this.checkIdentity(history,cfg);
      if(existing?.quotaAttestedMonth===cfg.quotaAttestedMonth && !sameBaseline(existing.baseline,cfg.baseline))throw Error('history_changed');
      await this.saveClient(client,cfg);
      this.ledger.setState('config',cfg);
      return this.status();
    });
  }
  async activate(accountId,campaignId) {
    return this.exclusive(async()=>{
      const cfg=this.ledger.getState('config');
      const campaign=campaignById(campaignId);
      const now=Date.now();
      this.credential(accountId);
      if(!cfg || cfg.accountId!==accountId || !campaign?.accountIds.includes(accountId))throw Error('campaign_not_allowed');
      if(this.env.REDEMPTION_ENABLED!=='true')throw Error('redemption_disabled');
      if(now>=Date.parse(campaign.expiresAt) || cfg.quotaAttestedMonth!==MONTH(now))throw Error('campaign_expired_or_quota_unknown');
      const attempt=this.ledger.getAttempt(MONTH(now));
      if(attempt)throw Error('monthly_attempt_exists');
      if(!cfg.lastProbeAt || now-cfg.lastProbeAt>15*60_000 || cfg.lastProbeResult!=='ready')throw Error('fresh_probe_required');
      cfg.campaignId=campaignId;cfg.mode='active';cfg.lastResult='watching';cfg.activatedAt=now;
      this.ledger.setState('config',cfg);
      await this.schedule();
      return this.status();
    });
  }
  async pause() {
    const cfg=this.ledger.getState('config');
    if(cfg){cfg.mode='paused';cfg.lastResult='paused';this.ledger.setState('config',cfg);}
    await this.schedule();
    return this.status();
  }
  async status() {
    const cfg=this.ledger.getState('config');
    if(!cfg)return {ready:false,mode:'unconfigured'};
    const attempt=this.ledger.getAttempt(cfg.attemptMonth||cfg.quotaAttestedMonth);
    return {ready:true,workerVersion:this.env.WORKER_VERSION?.id||'local',accountId:cfg.accountId,mode:cfg.mode,campaignId:cfg.campaignId,
      cutoff:campaignById(cfg.campaignId)?.expiresAt||null,lastCheckAt:cfg.lastCheckAt||null,
      lastResult:cfg.lastResult,identityVerified:!!cfg.identityVerifiedAt,
      lastProbeAt:cfg.lastProbeAt||null,lastProbeResult:cfg.lastProbeResult||null,
      catalogCount:cfg.catalogCount??null,matchingCandidates:cfg.matchingCandidates??null,
      ocrReady:cfg.ocrReady===true,
      quotaMonth:cfg.quotaAttestedMonth,monthlyAttempt:attempt?{status:attempt.status,createdAt:attempt.createdAt}:null,
      nextAlarmAt:await this.ctx.storage.getAlarm(),notificationPending:this.ledger.nextNotificationAt()!==null};
  }
  block(cfg,campaign,code) {
    if(this.ledger.getState('config')?.mode==='paused')return;
    cfg.mode='blocked';cfg.lastResult=code;
    this.persistConfig(cfg);
    this.ledger.enqueueNotification(`blocked:${campaign.id}:${code}`,buildBlockedMessage({accountLabel:cfg.label,campaign,code}),Date.now());
  }
  async reconcile(client,cfg,campaign,attempt) {
    const history=parseHistory((await client.getHistory()).html);
    this.checkIdentity(history,cfg);
    const voucher=confirmNewVoucher(history,attempt);
    if(voucher){
      const now=Date.now();
      this.ledger.updateAttempt(attempt.month,{status:'confirmed',confirmedAt:now,voucherUrl:voucher.url});
      cfg.mode='confirmed';cfg.lastResult='confirmed';
      this.persistConfig(cfg);
      this.ledger.enqueueNotification(`success:${campaign.id}:${attempt.month}`,buildSuccessMessage({accountLabel:cfg.label,campaign,confirmedAt:now}),now);
    }else if(Date.now()-attempt.createdAt>=10*60_000){
      this.ledger.updateAttempt(attempt.month,{status:'unconfirmed',updatedAt:Date.now(),reason:'ambiguous_result'});
      this.block(cfg,campaign,'ambiguous_result');
    }else{
      cfg.mode='reconciling';cfg.lastResult='awaiting_history';this.persistConfig(cfg);
    }
  }
  async probe() {
    return this.exclusive(async()=>{
      const cfg=this.ledger.getState('config');
      if(!cfg)throw Error('not_configured');
      const campaign=campaignById(cfg.campaignId)||campaigns.find(c=>c.accountIds.includes(cfg.accountId)&&Date.parse(c.expiresAt)>Date.now());
      if(!campaign)throw Error('no_campaign');
      cfg.lastProbeAt=null;cfg.lastProbeResult='checking';this.persistConfig(cfg);
      await this.scan(cfg,campaign,true);
      return this.status();
    });
  }
  async scan(cfg,campaign,dryRun) {
    const now=Date.now();
    const client=await this.newClient(cfg);
    try{
      if(dryRun || !cfg.lastAuthCheckAt || now-cfg.lastAuthCheckAt>15*60_000){
        const history=parseHistory((await client.getHistory()).html);
        this.checkIdentity(history,cfg);
        if(!sameBaseline(cfg.baseline,baselineOf(history)))throw Object.assign(Error('quota_used'),{code:'QUOTA_USED'});
        cfg.lastAuthCheckAt=now;
      }
      const catalog=await client.getCatalog();
      if(catalog.status!==200)throw Error('catalog_unavailable');
      const cards=parseCatalog(catalog.html);
      if(!cards.length)throw Error('catalog_empty');
      const candidates=cards.filter(c=>candidateCard(c,campaign));
      cfg.lastCheckAt=now;cfg.catalogCount=cards.length;cfg.matchingCandidates=candidates.length;
      const matches=[];
      if(candidates.length>5)throw Error('too_many_candidates');
      for(const card of candidates.slice(0,5)){
        const detail=await client.getOffer(card.url);
        if(detail.status!==200)continue;
        let offer=parseOffer(detail.html,card.url);
        if(offer.requiresLogin){
          await client.restoreSession();
          const authenticatedDetail=await client.getOffer(card.url);
          if(authenticatedDetail.status!==200)throw Error('offer_not_verified');
          offer=parseOffer(authenticatedDetail.html,card.url);
        }
        let decision=matchOffer(offer,campaign);
        let artwork=null;
        if(decision.reasons.length===1 && decision.reasons[0]==='ARTIST_MISSING'){
          artwork=await this.artworkEvidence(offer,campaign);
          decision=matchOffer(offer,campaign,artwork.text);
        }
        if(decision.ok)matches.push({...offer,artwork});
        else if(isExpectedSlot(offer,campaign) && decision.reasons.some(r=>!['ARTIST_MISSING','NOT_REDEEMABLE'].includes(r))){
          if(dryRun){cfg.lastProbeResult='ambiguous_offer';cfg.lastResult='ambiguous_offer';}
          else this.block(cfg,campaign,'ambiguous_offer');
          return;
        }
        // Other shows/dates at the same venue are normal; only complete evidence authorizes a match.
      }
      if(dryRun){cfg.consecutiveErrors=0;cfg.lastProbeAt=now;cfg.lastProbeResult='ready';cfg.lastResult=matches.length?'qualified_offer_dry_run':'watching';return;}
      if(matches.length>1){this.block(cfg,campaign,'ambiguous_offer');return;}
      if(matches.length===0){cfg.consecutiveErrors=0;cfg.lastResult='watching';return;}
      const candidate=matches[0];
      const history=parseHistory((await client.getHistory()).html);
      this.checkIdentity(history,cfg);
      if(!sameBaseline(cfg.baseline,baselineOf(history))){this.block(cfg,campaign,'quota_used');return;}
      // Re-read the exact offer immediately before reserving the irreversible attempt.
      const refreshedDetail=await client.getOffer(candidate.url);
      if(refreshedDetail.status!==200){this.block(cfg,campaign,'ambiguous_offer');return;}
      const fresh=parseOffer(refreshedDetail.html,candidate.url);
      const refreshedArtwork=candidate.artwork?await this.artworkEvidence(fresh,campaign,candidate.artwork):null;
      if(!matchOffer(fresh,campaign,refreshedArtwork?.text||'').ok || fresh.title!==candidate.title || fresh.imageUrl!==candidate.imageUrl
        || fresh.description!==candidate.description){this.block(cfg,campaign,'ambiguous_offer');return;}
      cfg.consecutiveErrors=0;
      const latest=this.ledger.getState('config');
      const beforeRequest=Date.now();
      if(latest.mode!=='active' || this.env.REDEMPTION_ENABLED!=='true'
        || beforeRequest>=Date.parse(campaign.expiresAt) || beforeRequest<Date.parse(campaign.startsAt)
        || cfg.quotaAttestedMonth!==MONTH(beforeRequest))return;
      const attempt=this.ledger.reserveAttempt({month:MONTH(beforeRequest),campaignId:campaign.id,offerUrl:fresh.url,
        baseline:{entries:baselineOf(history),offer:{title:fresh.title,imageUrl:fresh.imageUrl}},createdAt:beforeRequest});
      if(!attempt)return;
      cfg.attemptMonth=attempt.month;
      cfg.mode='reconciling';cfg.lastResult='attempt_reserved';this.persistConfig(cfg);
      // Flush the durable reservation before the external request. A crash burns our attempt,
      // never the user's quota on a second request.
      await this.ctx.storage.sync();
      if(this.ledger.getState('config').mode!=='reconciling' || Date.now()>=Date.parse(campaign.expiresAt))return;
      try{await client.redeemOnce(fresh.url,{permit:true});}
      catch{/* Outcome is uncertain. Only history reconciliation is allowed from here. */}
      this.ledger.updateAttempt(attempt.month,{status:'reconciling',updatedAt:Date.now()});
      await this.reconcile(client,cfg,campaign,attempt);
    }finally{
      await this.saveClient(client,cfg);
      // A concurrent pause must not be overwritten after awaited upstream work.
      this.persistConfig(cfg);
    }
  }
  async drainNotifications() {
    for(const entry of this.ledger.pendingNotifications(Date.now(),5)){
      const delivered=await sendNtfy(entry.payload,{topicUrl:this.env.NTFY_TOPIC_URL});
      if(delivered.ok)this.ledger.markNotificationSent(entry.key,Date.now());
      else this.ledger.failNotification(entry.key,Date.now());
    }
  }
  async alarm() {
    if(this.busy){await this.ctx.storage.setAlarm(Date.now()+30_000);return;}
    await this.exclusive(async()=>{
      let cfg=this.ledger.getState('config');
      const campaign=campaignById(cfg?.campaignId);
      try{
        if(cfg && campaign && !CLOSED.has(cfg.mode)){
          const attempt=this.ledger.getAttempt(cfg.attemptMonth||cfg.quotaAttestedMonth);
          if(attempt){
            if(attempt.campaignId!==campaign.id){this.block(cfg,campaign,'monthly_attempt_exists');return;}
            const client=await this.newClient(cfg);
            try{await this.reconcile(client,cfg,campaign,attempt);}finally{await this.saveClient(client,cfg);}
          }else if(Date.now()>=Date.parse(campaign.expiresAt)){
            cfg.mode='expired';cfg.lastResult='expired';this.persistConfig(cfg);
          }else{await this.scan(cfg,campaign,false);}
        }
      }catch(e){
        cfg=this.ledger.getState('config');
        if(cfg&&campaign){
          const code=e?.code==='IDENTITY_MISMATCH'?'identity_mismatch':e?.code==='QUOTA_USED'?'quota_used':safeCode(e);
          if(code==='upstream_unavailable'){
            cfg.consecutiveErrors=(cfg.consecutiveErrors||0)+1;
            cfg.lastResult=code;this.persistConfig(cfg);
            if(cfg.consecutiveErrors>=3)this.block(cfg,campaign,code);
          }else this.block(cfg,campaign,code);
        }
      }finally{
        await this.drainNotifications();
        await this.schedule();
      }
    });
  }
  async schedule() {
    const cfg=this.ledger.getState('config');
    const campaign=campaignById(cfg?.campaignId);
    const now=Date.now();
    let next=null;
    if(campaign&&cfg&&!CLOSED.has(cfg.mode)){
      const poll=Math.max(30_000,Math.min(300_000,Number(this.env.POLL_INTERVAL_MS)||30_000));
      next=now+poll;
      if(cfg.mode==='active')next=Math.min(next,Date.parse(campaign.expiresAt));
    }
    const notifyAt=this.ledger.nextNotificationAt();
    if(notifyAt!==null && campaign && now<Date.parse(campaign.expiresAt)+24*3600_000)next=next===null?notifyAt:Math.min(next,notifyAt);
    if(next===null)await this.ctx.storage.deleteAlarm();
    else await this.ctx.storage.setAlarm(Math.max(now+1000,next));
  }
}

export default {
  async fetch(req,env) {
    const u=new URL(req.url);
    if(u.pathname==='/health'&&req.method==='GET')return response({status:'Ready',service:'uol-redemption-sentinel',version:env.WORKER_VERSION?.id||'local'});
    if(!await matchesToken(req.headers.get('Authorization')?.replace(/^Bearer /,''),env.ADMIN_TOKEN))return response({error:'unauthorized'},401);
    const match=/^\/admin\/accounts\/([a-z][a-z0-9_-]{0,39})\/(status|bootstrap|probe|probe-artwork|activate|pause)$/.exec(u.pathname);
    if(!match||u.search)return response({error:'not_found'},404);
    const [,accountId,action]=match;
    if(req.method!==(action==='status'?'GET':'POST'))return response({error:'method_not_allowed'},405);
    try{
      const cred=readAccounts(env)[accountId];
      if(!cred||cred.enabled!==true)return response({error:'account_disabled'},403);
      const stub=env.ACCOUNTS.getByName(await digest(normalizeLogin(cred.quotaOwnerKey||cred.login)));
      if(action==='status')return response(await stub.status());
      if(action==='probe')return response(await stub.probe());
      if(action==='probe-artwork'){
        if(Number(req.headers.get('content-length')||0)>128)return response({error:'body_too_large'},413);
        const text=await req.text();if(text.length>128)return response({error:'body_too_large'},413);
        return response(await stub.probeArtwork(JSON.parse(text||'{}').sampleIndex??0));
      }
      if(action==='pause')return response(await stub.pause());
      if(Number(req.headers.get('content-length')||0)>64_000)return response({error:'body_too_large'},413);
      const text=await req.text();if(text.length>64_000)return response({error:'body_too_large'},413);
      const body=JSON.parse(text);
      if(action==='bootstrap')return response(await stub.bootstrap(accountId,body));
      return response(await stub.activate(accountId,body.campaignId));
    }catch(e){
      const known=new Set(['busy','identity_not_verified','monthly_attestation_required','pause_before_bootstrap','history_changed','campaign_not_allowed','redemption_disabled','campaign_expired_or_quota_unknown','monthly_attempt_exists','fresh_probe_required','not_configured','no_campaign']);
      return response({error:known.has(e?.message)?e.message:safeCode(e),
        ...(e?.code==='REQUEST_FAILED'?{stage:e.stage,causeCode:e.causeCode}:{})},409);
    }
  }
};
