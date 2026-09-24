import sharp from 'sharp';
import { prepareWAMessageMedia } from '@whiskeysockets/baileys';
import { createPersonalThumbnail } from '../../beeper-preview-gateway/src/personal-thumbnail.js';
import { allowedImage, readLimited } from './x-post.js';

export const previewAppearance = Object.freeze({badgeRatio:0.50,avatarRatio:0.20,insetRatio:0.055,shadowRatio:1.10,shadowOpacity:0.78});

export async function prepareContent(payload, socket, fetchImpl = fetch) {
  const preview = payload.preview || {};
  if (!preview.imageUrl) return { text:payload.text,linkPreview:null };
  const download = async (url, avatar) => {
    allowedImage(url,avatar);
    const response = await fetchImpl(url,{redirect:'error',signal:AbortSignal.timeout(10000)});
    if (!/^image\//.test(response.headers.get('content-type') || '')) throw new Error('invalid_image_type');
    return readLimited(response,5*1024*1024);
  };
  const bytes = await download(preview.imageUrl,false);
  let avatarBytes;
  if (preview.avatarUrl) avatarBytes = await download(preview.avatarUrl,true).catch(() => undefined);
  const image = await createPersonalThumbnail(bytes,{avatarBytes,...previewAppearance});
  // WhatsApp can display the embedded thumbnail before (or instead of) the
  // uploaded version. Keep vector branding crisp on high-density displays too.
  const jpegThumbnail = await sharp(image.bytes).resize(1024,1024,{fit:'inside',withoutEnlargement:true})
    .jpeg({quality:92,chromaSubsampling:'4:4:4'}).toBuffer();
  const uploaded = await prepareWAMessageMedia({image:image.bytes,jpegThumbnail,
    width:image.imgSize.width,height:image.imgSize.height},{upload:socket.waUploadToServer});
  return { text:payload.text,linkPreview:{
    'canonical-url':payload.link,'matched-text':payload.link,title:preview.title,description:'',
    jpegThumbnail,highQualityThumbnail:uploaded.imageMessage,
  } };
}

export function createSender({store,getConfig,whatsapp,canSend,canPilot=()=>false,prepare=prepareContent,now=Date.now}) {
  let busy = false;
  return async () => {
    if (busy || !whatsapp.isReady()) return;
    const config = getConfig();
    const automatic=canSend() && !config.operation.paused && !config.operation.dryRun;
    if (!automatic && !canPilot()) return;
    const last = Number(store.getSetting('last_dispatch_at') || 0);
    if (now() - last < config.operation.minDelayMs) return;
    busy = true;
    let job, dispatched = false;
    try {
      job = store.claim(now(),{pilotOnly:!automatic,allowPilot:canPilot()});
      if (!job) return;
      const payload = JSON.parse(job.payload);
      const pilot=payload.pilot===true;
      if(pilot && payload.destinationAlias!=='self'){store.updateJob(job.id,'failed','pilot_destination_not_allowed');return;}
      if(pilot && !canPilot(payload.destinationAlias)){store.updateJob(job.id,'queued','pilot_disabled');return;}
      const resolveDestination=()=>pilot && payload.destinationAlias==='self' ? whatsapp.ownDestination() : getConfig().destinations[payload.destinationAlias];
      const destination = resolveDestination();
      if ((!pilot && !destination?.verified) || !destination || destination.jid !== job.destination) throw new Error('destination_changed');
      await whatsapp.verifyDestination(destination);
      const content = await prepare(payload,whatsapp.socket());
      // Recheck pause/gates after network calls and before any message can leave.
      if (pilot ? !canPilot(payload.destinationAlias) : !canSend() || getConfig().operation.paused || getConfig().operation.dryRun) {
        store.updateJob(job.id,'queued','paused'); return;
      }
      const currentDestination = resolveDestination();
      if ((!pilot && !currentDestination?.verified) || !currentDestination || currentDestination.jid !== job.destination ||
          currentDestination.type !== destination.type) throw new Error('destination_changed');
      store.setSetting('last_dispatch_at',now());
      dispatched = true;
      await whatsapp.socket().sendMessage(destination.jid,content,{messageId:job.message_id});
      if (store.job(job.id).state === 'dispatching') {
        store.updateJob(job.id,'unknown','awaiting_whatsapp_ack');
      }
    } catch {
      if (job) {
        const current = store.job(job.id);
        if (!['accepted','confirmed'].includes(current.state)) {
          if (dispatched) store.updateJob(job.id,'unknown','dispatch_unconfirmed');
          else if (job.attempts < 3) store.updateJob(job.id,'queued','pre_dispatch_failure',null,now()+30000*2**(job.attempts-1));
          else store.updateJob(job.id,'failed','pre_dispatch_failure');
        }
      }
    } finally { busy = false; }
  };
}
