import { matchRules } from './config.js';
import { postFromPush } from './push.js';
import { formatPost, fetchPost } from './x-post.js';

export function createProcessor({ store, getConfig, getContext, readPost = fetchPost, decodeEvent = value => value }) {
  let busy = false;
  return async () => {
    if (busy) return;
    if (getConfig().operation.paused && !getConfig().operation.dryRun) return;
    busy = true;
    try {
      let event;
      while ((event = store.nextEvent())) {
        store.updateEvent(event.id,'processing');
        const config = getConfig();
        let push;
        try {push = decodeEvent(JSON.parse(event.payload));}
        catch (error) {
          const unsupported = error.message === 'unsupported_notification_type';
          store.updateEvent(event.id,unsupported ? 'ignored' : 'pending_review',
            unsupported ? 'unsupported_notification_type' : 'push_decode_failed');
          continue;
        }
        const post = postFromPush(push);
        if (!post) { store.updateEvent(event.id,'pending_review','post_identity_missing'); continue; }
        if (!config.sources.includes(post.author)) { store.updateEvent(event.id,'ignored','unconfigured_author'); continue; }
        try {
          const full = await readPost(post.url,{context:getContext?.()});
          const activated = store.getSetting('activated_at');
          if (!config.operation.dryRun && (!activated || Date.parse(full.publishedAt) < Date.parse(activated))) {
            store.updateEvent(event.id,'ignored','before_activation'); continue;
          }
          const destinations = matchRules(config,full);
          if (config.operation.dryRun) {
            store.updateEvent(event.id,'simulated',destinations.length ? 'rule_matched' : 'no_matching_rule'); continue;
          }
          for (const alias of destinations) {
            const d = config.destinations[alias];
            if (!d.verified) throw new Error('destination_unverified');
            const key = `x:${post.id}:${d.jid}`;
            // A second push must not re-enqueue a post whose text or avatar changed.
            if (!store.jobByKey(key)) store.enqueue({ key,destination:d.jid,payload:{...formatPost(full),destinationAlias:alias},priority:10 });
          }
          store.updateEvent(event.id,'processed');
        } catch (error) {
          const known = new Set(['post_unavailable','post_article_missing','post_record_missing',
            'post_text_incomplete','x_rate_limited','download_failed','unexpected_post_redirect']);
          store.updateEvent(event.id,'pending_review',known.has(error.message) ? error.message : 'post_processing_failed');
        }
      }
    } finally { busy = false; }
  };
}
