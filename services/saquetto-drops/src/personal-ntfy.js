const DEFAULT_TOPIC = 'leo-saquetto-wpp-3054';
const TITLE = 'WhatsApp · Conversa Pessoal';

export function personalWhatsAppLink(jid) {
  const [address, server] = String(jid || '').split('@');
  if (server !== 's.whatsapp.net') throw new Error('invalid_personal_jid');
  const phone = address.split(':', 1)[0];
  if (!/^\d{7,15}$/.test(phone)) throw new Error('invalid_personal_jid');
  return `https://wa.me/${phone}`;
}

export async function sendPersonalNtfyNotification(text, {
  jid,
  fetchImpl = globalThis.fetch,
} = {}) {
  const encodedTitle = `=?utf-8?B?${Buffer.from(TITLE,'utf8').toString('base64')}?=`;
  const cleanText = typeof text === 'string' ? text.replace(/\*\*/g,'').replace(/\*/g,'').replace(/`/g,'') : '';
  const message = cleanText || 'Mensagem com mídia enviada ao WhatsApp.';
  try {
    const chatLink = personalWhatsAppLink(jid);
    const response = await fetchImpl(`https://ntfy.sh/${DEFAULT_TOPIC}`, {
      method:'POST',
      headers:{
        Title:encodedTitle,
        Priority:'default',
        Tags:'speech_balloon,calling',
        Click:chatLink,
        Actions:`view, Abrir Conversa, ${chatLink}, clear=true`,
        'User-Agent':'saquetto-drops/1.0',
      },
      body:message,
      signal:AbortSignal.timeout(10000),
    });
    if (!response.ok) throw new Error(`http_${response.status}`);
    return true;
  } catch {
    console.warn(JSON.stringify({event:'personal_ntfy_failed'}));
    return false;
  }
}
