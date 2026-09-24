import { postFromPush } from './push.js';
import { fetchPost, formatPost } from './x-post.js';

export function createPilot({store,whatsapp,enabled,decodeEvent,readPost=fetchPost}) {
  const destination = alias => {
    if(!enabled(alias))throw new Error('pilot_disabled');
    if(!whatsapp.isReady())throw new Error('whatsapp_not_ready');
    if(alias!=='self')throw new Error('pilot_destination_not_allowed');
    return whatsapp.ownDestination();
  };
  return {
    async verify(alias){await whatsapp.verifyDestination(destination(alias));return {alias,writable:true};},
    async enqueue({alias,eventId,revision}) {
      const d=destination(alias);
      if(revision!==undefined && (typeof revision!=='string'||!/^[a-z0-9][a-z0-9-]{0,31}$/.test(revision)))throw new Error('invalid_revision');
      if(typeof eventId!=='string'||!/^[a-f0-9]{64}$/.test(eventId))throw new Error('invalid_event');
      const event=store.db.prepare('SELECT payload FROM push_events WHERE id=?').get(eventId);
      if(!event)throw new Error('event_not_found');
      const identity=postFromPush(decodeEvent(JSON.parse(event.payload)));
      if(!identity)throw new Error('post_identity_missing');
      await whatsapp.verifyDestination(d);
      // A new visual revision is an explicit pilot request, never an automatic
      // retry of an accepted/ambiguous message. Repeating the revision is deduped.
      const key=`pilot:${identity.id}:${d.jid}${revision?':'+revision:''}`;
      const previous=store.jobByKey(key);if(previous)return previous;
      const post=await readPost(identity.url);
      if(!enabled(alias)||destination(alias).jid!==d.jid)throw new Error('pilot_changed');
      const payload=formatPost(post);
      payload.text='[Teste de migração do Saquetto Drops]\n'+payload.text;
      return store.enqueue({key,destination:d.jid,payload:{...payload,destinationAlias:alias,pilot:true},priority:0});
    },
  };
}
