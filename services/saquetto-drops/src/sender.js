import sharp from 'sharp';
import { prepareWAMessageMedia } from '@whiskeysockets/baileys';
import { createPersonalThumbnail } from '../../beeper-preview-gateway/src/personal-thumbnail.js';
import { allowedImage, readLimited } from './x-post.js';

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
  const image = await createPersonalThumbnail(bytes,{avatarBytes});
  const jpegThumbnail = await sharp(image.bytes).resize(240,240,{fit:'inside'}).jpeg({quality:85}).toBuffer();
  const uploaded = await prepareWAMessageMedia({image:image.bytes},{upload:socket.waUploadToServer});
  return { text:payload.text,linkPreview:{
    'canonical-url':payload.link,'matched-text':payload.link,title:preview.title,description:'',
    jpegThumbnail,highQualityThumbnail:uploaded.imageMessage,
  } };
}

export function createSender({store,getConfig,whatsapp,canSend,prepare=prepareContent,now=Date.now}) {
  let busy = false;
  return async () => {
    if (busy || !canSend() || !whatsapp.isReady()) return;
    const config = getConfig();
    if (config.operation.paused || config.operation.dryRun) return;
    const last = Number(store.getSetting('last_dispatch_at') || 0);
    if (now() - last < config.operation.minDelayMs) return;
    busy = true;
    let job, dispatched = false;
    try {
      job = store.claim(now());
      if (!job) return;
      const payload = JSON.parse(job.payload);
      const destination = config.destinations[payload.destinationAlias];
      if (!destination?.verified || destination.jid !== job.destination) throw new Error('destination_changed');
      await whatsapp.verifyDestination(destination);
      const content = await prepare(payload,whatsapp.socket());
      // Recheck pause/gates after network calls and before any message can leave.
      if (!canSend() || getConfig().operation.paused || getConfig().operation.dryRun) {
        store.updateJob(job.id,'queued','paused'); return;
      }
      const currentDestination = getConfig().destinations[payload.destinationAlias];
      if (!currentDestination?.verified || currentDestination.jid !== job.destination ||
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
