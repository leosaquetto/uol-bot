const HISTORY_URL = 'https://clube.uol.com.br/perfil/beneficios';
const BLOCKED_REASONS = Object.freeze({
  auth_required: 'A sessão UOL deixou de funcionar.',
  session_expired: 'A sessão UOL expirou.',
  identity_mismatch: 'A identidade da conta não corresponde à conta autorizada.',
  history_incomplete: 'O histórico foi dividido em páginas e não permite verificar a cota com segurança.',
  quota_used: 'A cota mensal já está utilizada.',
  monthly_attempt_exists: 'Já existe uma tentativa registrada neste mês.',
  ambiguous_offer: 'Os dados da oferta não permitem confirmar o show com segurança.',
  ambiguous_result: 'A tentativa foi enviada, mas o resgate ainda não foi confirmado no histórico.',
  unconfirmed: 'A tentativa foi enviada, mas o resgate ainda não foi confirmado no histórico.',
  captcha_required: 'O UOL pediu uma verificação humana.',
  mfa_required: 'O UOL pediu uma confirmação de acesso.',
  expired: 'A campanha encerrou sem resgate confirmado.',
});

function label(value, fallback, max = 100) {
  if (typeof value !== 'string') return fallback;
  const clean = value.replace(/[\u0000-\u001f\u007f]/g, ' ').trim();
  return clean ? clean.slice(0, max) : fallback;
}

function displayDate(value) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value ?? '');
  return match ? `${match[3]}/${match[2]}/${match[1]}` : label(value, 'data configurada', 30);
}

function displayTime(value) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return '';
  return new Intl.DateTimeFormat('pt-BR', {
    timeZone: 'America/Sao_Paulo', dateStyle: 'short', timeStyle: 'short',
  }).format(date);
}

/** Voucher URLs and codes are deliberately excluded from the public notification topic. */
export function buildSuccessMessage({ accountLabel, campaign, voucherUrl: _voucherUrl, confirmedAt }) {
  const artist = label(campaign?.artist, 'show');
  const quantity = Number.isSafeInteger(campaign?.quantity) && campaign.quantity > 0 ? campaign.quantity : 2;
  const when = displayTime(confirmedAt);
  return {
    title: `Resgate confirmado — ${artist}`,
    message: [
      `${label(accountLabel, 'Conta autorizada')}: ${quantity} ingressos.`,
      `${displayDate(campaign?.eventDate)} · ${label(campaign?.venue, 'local configurado')}.`,
      ...(when ? [`Confirmado em ${when} (São Paulo).`] : []),
      `Meus Resgates: ${HISTORY_URL}`,
    ].join('\n'),
    priority: 5,
    tags: ['tada', 'ticket'],
    click: HISTORY_URL,
  };
}

/** Pass a stable reason code, never an upstream response or exception message. */
export function buildBlockedMessage({ accountLabel, campaign, code, reason }) {
  const explanation = BLOCKED_REASONS[code ?? reason] ?? 'A sentinela parou por segurança; verifique o status privado.';
  return {
    title: `Sentinela bloqueada — ${label(campaign?.artist, 'Clube UOL')}`,
    message: `${label(accountLabel, 'Conta autorizada')}: ${explanation}\nNenhum novo resgate será tentado automaticamente.\nMeus Resgates: ${HISTORY_URL}`,
    priority: 4,
    tags: ['warning'],
    click: HISTORY_URL,
  };
}

export async function sendNtfy(payload, { topicUrl, fetchImpl = globalThis.fetch, token } = {}) {
  let stage = 'config';
  let httpStatus;
  try {
    const url = new URL(topicUrl);
    if (url.origin !== 'https://ntfy.sh' || url.username || url.password || url.search || url.hash || !/^\/[A-Za-z0-9_-]{1,64}$/.test(url.pathname)) {
      return { ok: false, reason: 'invalid_ntfy_config' };
    }
    if (typeof payload?.title !== 'string' || typeof payload?.message !== 'string') return { ok: false, reason: 'invalid_ntfy_payload' };
    const topic = url.pathname.slice(1);
    const body = {
      topic,
      title: payload.title,
      message: payload.message,
      priority: payload.priority === 5 ? 5 : 4,
      tags: Array.isArray(payload.tags) ? payload.tags.filter((tag) => typeof tag === 'string') : [],
      click: HISTORY_URL,
    };
    const headers = { 'Content-Type': 'application/json', Accept: 'application/json', 'User-Agent': 'uol-redemption-sentinel/1.0' };
    if (token) headers.Authorization = `Bearer ${token}`;
    stage = 'fetch';
    const response = await fetchImpl('https://ntfy.sh', {
      method: 'POST', headers, body: JSON.stringify(body), redirect: 'manual', signal: AbortSignal.timeout(10_000),
    });
    httpStatus = response.status;
    if (!response.ok) return { ok: false, reason: 'ntfy_http_error', httpStatus: response.status };
    stage = 'receipt';
    const receipt = await response.json();
    if (typeof receipt?.id !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(receipt.id)
      || !Number.isSafeInteger(receipt.time) || receipt.time <= 0
      || receipt.topic !== topic || receipt.event !== 'message') {
      return { ok: false, reason: 'ntfy_invalid_receipt', httpStatus: response.status };
    }
    return { ok: true, id: receipt.id, time: receipt.time };
  } catch (error) {
    return { ok: false, reason: 'ntfy_delivery_failed', stage, ...(httpStatus?{httpStatus}:{}),
      causeCode: error?.name === 'AbortError' || error?.name === 'TimeoutError' ? 'timeout'
        : error?.name === 'SyntaxError' ? 'invalid_json'
        : /illegal invocation/i.test(error?.message || '') ? 'illegal_invocation' : 'network_or_response_error' };
  }
}
