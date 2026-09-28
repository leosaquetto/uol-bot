import makeWASocket, { DisconnectReason, jidNormalizedUser } from '@whiskeysockets/baileys';
import { sqliteAuth } from './auth.js';

const silentLogger = { level: 'silent', trace() {}, debug() {}, info() {}, warn() {}, error() {}, fatal() {}, child() { return this; } };
const MAX_RECONNECT_ATTEMPTS = 8;
const SYNC_TIMEOUT_MS = 120000;
const STABLE_CONNECTION_MS = 300000;

function reportPersonalNotificationFailure(logger) {
  const message = JSON.stringify({event:'personal_ntfy_failed'});
  if (typeof logger.warn === 'function') logger.warn(message);
  else logger.log?.(message);
}

export function applyReceipt({store,messageId,jid,state,level,personalJids=[],onPersonalMessageAccepted=()=>{},logger=console}) {
  const accepted = store.receipt(messageId,jid,state,level);
  const normalizedPersonal = new Set(personalJids.filter(Boolean).map(jid => jidNormalizedUser(jid)));
  if (!normalizedPersonal.has(jidNormalizedUser(jid))) return accepted;
  for (const job of accepted) {
    try {
      const payload = JSON.parse(job.payload);
      const result = onPersonalMessageAccepted(payload.text || '', job.destination);
      Promise.resolve(result).catch(() => reportPersonalNotificationFailure(logger));
    } catch {
      reportPersonalNotificationFailure(logger);
    }
  }
  return accepted;
}

export function startWhatsApp({ store, onQr = () => {}, getPersonalJid = () => '',
  onPersonalMessageAccepted = () => {}, logger = console, socketFactory = makeWASocket }) {
  const auth = sqliteAuth(store);
  let socket, state = 'starting', stopped = false, timer, attempts = 0;
  let syncTimer, stableTimer, detachSocket = () => {};
  let opened = false, pendingReceived = false;
  const ready = () => opened && pendingReceived && (auth.state.creds.accountSyncCounter || 0) > 0;
  const clearSocketTimers = () => {
    clearTimeout(syncTimer); clearTimeout(stableTimer);
    syncTimer = stableTimer = undefined;
  };
  const connect = () => {
    if (stopped) return;
    clearTimeout(timer); timer = undefined;
    clearSocketTimers(); detachSocket();
    opened = false;
    pendingReceived = false;
    const current = socket = socketFactory({
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
    let closed = false, closing = false;
    const active = () => !stopped && !closed && !closing && socket === current;
    const recordReceipt = (messageId,jid,receiptState,level) => {
      const personalJids = [current.user?.id || ''];
      try {
        const configuredPersonalJid = getPersonalJid();
        if (configuredPersonalJid) personalJids.push(configuredPersonalJid);
      } catch {}
      return applyReceipt({
        store,messageId,jid:jidNormalizedUser(jid),state:receiptState,level,
        personalJids,onPersonalMessageAccepted,logger,
      });
    };
    const updateReady = () => {
      if (!active() || !ready() || state === 'connected') return;
      state = 'connected';
      clearTimeout(syncTimer); syncTimer = undefined;
      // A brief open/sync cycle must not replenish the reconnect budget.
      stableTimer = setTimeout(() => {
        if (active() && ready()) attempts = 0;
      }, STABLE_CONNECTION_MS);
      stableTimer.unref();
      logger.log(JSON.stringify({ event: 'whatsapp_ready' }));
    };
    const onCreds = () => {
      if (socket !== current) return;
      auth.saveCreds();
      updateReady();
    };
    const onConnection = ({ connection, lastDisconnect, qr, receivedPendingNotifications }) => {
      if (stopped || closed || socket !== current || (closing && connection !== 'close')) return;
      if (qr) { state = 'pairing_required'; onQr(qr); }
      if (connection === 'open') {
        opened = true; onQr(null);
        state = 'synchronizing';
        clearSocketTimers();
        syncTimer = setTimeout(() => {
          if (!active() || ready()) return;
          closing = true; opened = false; state = 'disconnected';
          logger.log(JSON.stringify({ event: 'whatsapp_sync_timeout', timeoutMs: SYNC_TIMEOUT_MS,
            pendingReceived, hasSyncCheckpoint: (auth.state.creds.accountSyncCounter || 0) > 0 }));
          // end() closes the transport before emitting close; only that event schedules a new socket.
          const error = Object.assign(new Error('whatsapp_sync_timeout'), {
            output: { statusCode: DisconnectReason.timedOut },
          });
          Promise.resolve().then(() => current.end(error)).catch(() => {
            if (stopped || closed || socket !== current) return;
            state = 'reconnect_required';
            logger.log(JSON.stringify({ event: 'whatsapp_socket_close_failed' }));
          });
        }, SYNC_TIMEOUT_MS);
        syncTimer.unref();
        updateReady();
      }
      if (receivedPendingNotifications === true) {
        pendingReceived = true;
        updateReady();
      }
      if (connection === 'close') {
        closed = true;
        clearSocketTimers();
        opened = false; pendingReceived = false; onQr(null);
        const code = lastDisconnect?.error?.output?.statusCode;
        // Baileys also maps generic stream errors to badSession (500).
        // Retry with the saved auth state; never erase credentials automatically.
        const permanent = [DisconnectReason.loggedOut,
          DisconnectReason.connectionReplaced, DisconnectReason.multideviceMismatch].includes(code);
        const retryExhausted = !permanent && attempts >= MAX_RECONNECT_ATTEMPTS;
        state = permanent || retryExhausted ? 'reconnect_required' : 'disconnected';
        logger.log(JSON.stringify({ event: 'whatsapp_disconnect', code: Number.isInteger(code) ? code : null,
          permanent, retryExhausted }));
        if (!permanent && !retryExhausted) {
          const baseDelay = Math.min(60000, 2000 * 2 ** Math.min(attempts++, 5));
          const delay = Math.min(60000, Math.round(baseDelay * (0.8 + Math.random() * 0.4)));
          logger.log(JSON.stringify({ event: 'whatsapp_reconnect_scheduled', attempt: attempts, delayMs: delay }));
          timer = setTimeout(connect,delay); timer.unref();
        }
      }
      if (connection) logger.log(JSON.stringify({ event: 'whatsapp_connection', state }));
    };
    // A transport write or local echo is not an acknowledgement.
    const onAck = node => {
      if (socket !== current) return;
      const { id, from, error } = node.attrs || {};
      if (!id || !from || error) return;
      recordReceipt(id,from,'accepted','server_ack');
    };
    const onMessages = updates => {
      if (socket !== current) return;
      for (const { key, update } of updates) {
        if (key.fromMe !== true || !key.id || !key.remoteJid) continue;
        if (update.status >= 3) recordReceipt(key.id,key.remoteJid,'confirmed','recipient_receipt');
        else if (update.status === 2) recordReceipt(key.id,key.remoteJid,'accepted','server_ack');
      }
    };
    const onReceipts = updates => {
      if (socket !== current) return;
      for (const { key, receipt } of updates) {
        if (key.fromMe === true && (receipt.receiptTimestamp || receipt.readTimestamp)) {
          recordReceipt(key.id,key.remoteJid,'confirmed','participant_receipt');
        }
      }
    };
    current.ev.on('creds.update', onCreds);
    current.ev.on('connection.update', onConnection);
    current.ws.on('CB:ack,class:message', onAck);
    current.ev.on('messages.update', onMessages);
    current.ev.on('message-receipt.update', onReceipts);
    detachSocket = () => {
      current.ev.off('creds.update', onCreds);
      current.ev.off('connection.update', onConnection);
      current.ws.off('CB:ack,class:message', onAck);
      current.ev.off('messages.update', onMessages);
      current.ev.off('message-receipt.update', onReceipts);
    };
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
    async stop() {
      stopped = true; clearTimeout(timer); clearSocketTimers(); state = 'stopped';
      opened = false; pendingReceived = false; onQr(null);
      try { await socket?.end(new Error('shutdown')); }
      finally { detachSocket(); }
    },
  };
}
