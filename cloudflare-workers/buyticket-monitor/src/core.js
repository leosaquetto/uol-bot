export const EVENTS = [
  { day: '16/09/2026', label: '16/09 QUA - DEMI LOVATO', date: '1789527599000', local: '1779910250255x792501787503624200' },
  { day: '17/09/2026', label: '17/09 QUI - DEMI LOVATO', date: '1789613999000', local: '1779910250255x792501787503624200' },
];
export const ALERT_PRICE_LIMIT = 29_900;
export const keys = ['Gramado', 'Comfort Zone'].flatMap(s => ['Inteira', 'Meia Estudante', 'Meia PCD'].map(c => `${s}||${c}`));
export const eventUrl = e => `https://buyticketbrasil.com/evento/demilovato%E2%80%93itsnotthatdeeptour-2026?data=${e.date}&evento_local=${e.local}&cidade=S%C3%A3o+Paulo`;
export function parse(text) {
  // Flight text records can span lines. Extract only balanced JSON matrix objects.
  const found = [];
  const marker = '"matriz_preco":';
  let offset = 0;
  while ((offset = text.indexOf(marker, offset)) !== -1) {
    const start = offset + marker.length;
    offset = start;
    let depth = 0, quoted = false, escaped = false;
    for (let i = start; i < text.length; i++) {
      const ch = text[i];
      if (quoted) {
        if (escaped) escaped = false;
        else if (ch === '\\') escaped = true;
        else if (ch === '"') quoted = false;
      } else if (ch === '"') quoted = true;
      else if (ch === '{') depth++;
      else if (ch === '}' && --depth === 0) {
        found.push(JSON.parse(text.slice(start, i + 1)));
        offset = i + 1;
        break;
      }
    }
  }
  if (found.length !== 1 || !Object.keys(found[0]).length) throw new Error('matrix_invalid');
  const matrix = found[0];
  for (const v of Object.values(matrix)) {
    if (!Number.isSafeInteger(v.preco_min) || v.preco_min < 0 || !Number.isSafeInteger(v.disponivel) || v.disponivel < 0 || (v.disponivel > 0 && (v.preco_min <= 0 || typeof v.id_ref !== 'string'))) throw new Error('price_invalid');
  }
  return matrix;
}
export function qualifyingOffers(current) {
  return current.flatMap((matrix, i) => Object.entries(matrix || {}).flatMap(([key, value]) =>
    value?.disponivel > 0 && value.preco_min > 0 && value.preco_min < ALERT_PRICE_LIMIT && value.id_ref
      ? [{ i, key, price: value.preco_min, available: value.disponivel, idRef: value.id_ref }]
      : []));
}
const money = n => `R$ ${(n / 100).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
export function format(current, offers, at, dayIndex = null) {
  const lines = [offers.length ? '🎟️ *OFERTA • DEMI LOVATO*' : '🎟️ *DEMI LOVATO • PREÇOS ATUAIS*'];
  offers = offers.filter(offer => dayIndex === null || offer.i === dayIndex);
  current.forEach((m, i) => {
    if (dayIndex !== null && i !== dayIndex) return;
    lines.push('', `🗓️ *${EVENTS[i].label}*`);
    const displayKeys = Object.keys(m);
    for (const sector of [...new Set(displayKeys.map(k => k.split('||')[0]))]) {
      lines.push('', `${sector === 'Gramado' ? '🌿' : '✨'} *${sector}*`);
      for (const key of displayKeys.filter(k => k.startsWith(sector + '||'))) {
        const v = m[key], offer = offers.find(item => item.i === i && item.key === key && item.idRef === v?.id_ref);
        const row = `${key.split('||')[1]}: ${v?.disponivel ? `${money(v.preco_min)} (🎟️ ${v.disponivel})` : 'sem oferta (🎟️ 0)'}`;
        lines.push(offer ? `🔥 *${row}*` : `• ${row}`);
      }
    }
    lines.push('', `🔗 Ver ingressos: ${eventUrl(EVENTS[i])}`);
  });
  lines.push('', new Date(at).toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo', day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }));
  return lines.join('\n').replaceAll('PCD', '♿️').replaceAll('Estudante', '👨🏻‍🎓');
}
