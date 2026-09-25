import makeWASocket, { DisconnectReason, jidNormalizedUser } from '@whiskeysockets/baileys';
import { sqliteAuth } from './auth.js';

const silentLogger = { level: 'silent', trace() {}, debug() {}, info() {}, warn() {}, error() {}, fatal() {}, child() { return this; } };

export function startWhatsApp({ store, onQr = () => {}, logger = console, socketFactory = makeWASocket }) {
  const auth = sqliteAuth(store);
  let socket, state = 'starting', stopped = false, timer, attempts = 0;
  let opened = false, pendingReceived = false;
  const ready = () => opened && pendingReceived && (auth.state.creds.accountSyncCounter || 0) > 0;
  const connect = () => {
    opened = false;
    pendingReceived = false;
    socket = socketFactory({
      auth: auth.state, logger: silentLogger, markOnlineOnConnect: false,
      // Keep the limited initial sync enabled: Baileys needs LID mappings and
      // session metadata for a newly paired device. `shouldSyncHistoryMessage`
      // returning false for every event can leave a fresh session unusable.
      syncFullHistory: false,
      getMessage: async key => {
        const row = store.db.prepare('SELECT payload FROM jobs WHERE message_id=? AND destination=?').get(key.id,key.remoteJid);
        const message = row && JSON.parse(row.payload).wireMessage;
        return message || undefined;
      },
    });
    socket.ev.on('creds.update', () => {
      auth.saveCreds();
      if (ready()) state = 'connected';
    });
    socket.ev.on('connection.update', ({ connection, lastDisconnect, qr, receivedPendingNotifications }) => {
      if (qr) { state = 'pairing_required'; onQr(qr); }
      if (connection === 'open') {
        opened = true; attempts = 0; onQr(null);
        state = ready() ? 'connected' : 'synchronizing';
      }
      if (receivedPendingNotifications === true) {
        pendingReceived = true;
        if (ready()) state = 'connected';
      }
      if (connection === 'close') {
        opened = false; pendingReceived = false; onQr(null);
        const code = lastDisconnect?.error?.output?.statusCode;
        const permanent = [DisconnectReason.loggedOut, DisconnectReason.badSession,
          DisconnectReason.connectionReplaced, DisconnectReason.multideviceMismatch].includes(code);
        state = permanent ? 'reconnect_required' : 'disconnected';
        logger.log(JSON.stringify({ event: 'whatsapp_disconnect', code: Number.isInteger(code) ? code : null,
          permanent }));
        if (!stopped && !permanent) {
          const delay = Math.min(60000, 2000 * 2 ** Math.min(attempts++, 5));
          timer = setTimeout(connect,delay); timer.unref();
        }
      }
      if (connection) logger.log(JSON.stringify({ event: 'whatsapp_connection', state }));
    });
    // A transport write or local echo is not an acknowledgement.
    socket.ws.on('CB:ack,class:message', node => {
      const { id, from, error } = node.attrs || {};
      if (!id || !from || error) return;
      store.receipt(id,jidNormalizedUser(from),'accepted','server_ack');
    });
    socket.ev.on('messages.update', updates => {
      for (const { key, update } of updates) {
        if (key.fromMe !== true || !key.id || !key.remoteJid) continue;
        if (update.status >= 3) store.receipt(key.id,key.remoteJid,'confirmed','recipient_receipt');
        else if (update.status === 2) store.receipt(key.id,key.remoteJid,'accepted','server_ack');
      }
    });
    socket.ev.on('message-receipt.update', updates => {
      for (const { key, receipt } of updates) {
        if (key.fromMe === true && (receipt.receiptTimestamp || receipt.readTimestamp)) {
          store.receipt(key.id,key.remoteJid,'confirmed','participant_receipt');
        }
      }
    });
  };
  connect();
  return {
    state: () => state,
    isReady: () => state === 'connected',
    socket: () => socket,
    ownDestination: () => ({type:'contact',jid:jidNormalizedUser(socket?.user?.id || ''),verified:state==='connected'}),
    async verifyDestination(destination) {
      if (state !== 'connected') throw new Error('whatsapp_not_ready');
      const me = jidNormalizedUser(socket.user?.id || '');
      if (destination.type === 'group') {
        const group = await socket.groupMetadata(destination.jid);
        const ownIds=[me,jidNormalizedUser(socket.user?.lid || '')].filter(Boolean);
        const mine = group.participants.find(p => [jidNormalizedUser(p.id),jidNormalizedUser(p.phoneNumber || '')]
          .some(id => id && ownIds.includes(id)));
        if (!mine || (group.announce && !mine.admin)) throw new Error('destination_not_writable');
        return {writable:true,type:'group',name:group.subject,
          community:group.isCommunity===true,communityAnnouncements:group.isCommunityAnnounce===true,
          announcementOnly:group.announce===true};
      }
      if (destination.type === 'contact') {
        if (destination.jid === me) return {writable:true,type:'contact',self:true};
        const results = await socket.onWhatsApp(destination.jid);
        if (!results?.some(r => r.exists && r.jid === destination.jid)) throw new Error('contact_unverified');
        return {writable:true,type:'contact',self:false};
      }
      // Channel publication stays closed until its real permission + preview pilot.
      throw new Error('channel_pilot_required');
    },
    async stop() { stopped = true; clearTimeout(timer); state = 'stopped'; socket?.end(new Error('shutdown')); },
  };
}
