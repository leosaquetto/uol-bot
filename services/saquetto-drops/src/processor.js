import { matchRules } from './config.js';
import { postFromPush } from './push.js';
import { formatPost, fetchPost } from './x-post.js';

export function createProcessor({ store, getConfig, getContext, readPost = fetchPost }) {
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
        const push = JSON.parse(event.payload);
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
        } catch {
          store.updateEvent(event.id,'pending_review','post_processing_failed');
        }
      }
    } finally { busy = false; }
  };
}
