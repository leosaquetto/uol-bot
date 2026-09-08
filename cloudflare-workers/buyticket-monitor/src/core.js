export const EVENTS = [
  { day: '12/09/2026', label: '12/09 SÁB - DEMI LOVATO', date: '1789261200000', local: '1765323797528x513509114247905300' },
  { day: '13/09/2026', label: '13/09 DOM - HALSEY', date: '1789347600000', local: '1765323829346x381107157350744060' },
];
export const keys = ['Gramado', 'Comfort Zone'].flatMap(s => ['Inteira', 'Meia Estudante', 'Meia PCD'].map(c => `${s}||${c}`));
export const eventUrl = e => `https://buyticketbrasil.com/evento/rockinrio2026?data=${e.date}&evento_local=${e.local}&cidade=Rio+de+Janeiro`;
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
  return { ...Object.fromEntries(keys.map(k => [k, { preco_min: 0, disponivel: 0 }])), ...matrix };
}
export function drops(previous, current) {
  if (!previous) return [];
  const minimum = m => Math.min(...Object.values(m || {}).filter(v => v.disponivel > 0 && v.preco_min > 0).map(v => v.preco_min));
  return current.flatMap((m, i) => {
    const before = minimum(previous[i]), after = minimum(m);
    if (!Number.isFinite(before) || !Number.isFinite(after) || after >= before) return [];
    return Object.entries(m).filter(([, v]) => v.disponivel > 0 && v.preco_min === after)
      .map(([key]) => ({ i, key, before, after }));
  });
}
const money = n => `R$ ${(n / 100).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
export function format(current, changes, at, dayIndex = null) {
  const lines = [changes.length ? '📉 *BAIXOU! • ROCK IN RIO 2026*' : '🎟️ *ROCK IN RIO 2026 • PREÇOS ATUAIS*'];
  changes = changes.filter(d => dayIndex === null || d.i === dayIndex);
  for (const d of changes) lines.push('', `💚 *${EVENTS[d.i].label} • ${d.key.replace('||', ' • ')}*`, `Menor do dia: de ${money(d.before)} para *${money(d.after)}*`, `Queda no menor do dia: *${money(d.before - d.after)}*`);
  current.forEach((m, i) => {
    if (dayIndex !== null && i !== dayIndex) return;
    lines.push('', `🗓️ *${EVENTS[i].label}*`);
    const displayKeys = [...new Set([...keys, ...changes.filter(d => d.i === i).map(d => d.key)])];
    for (const sector of [...new Set(displayKeys.map(k => k.split('||')[0]))]) {
      lines.push('', `${sector === 'Gramado' ? '🌿' : '✨'} *${sector}*`);
      for (const key of displayKeys.filter(k => k.startsWith(sector + '||'))) {
        const v = m[key], drop = changes.find(d => d.i === i && d.key === key);
        const row = `${key.split('||')[1]}: ${v.disponivel ? `${money(v.preco_min)}${drop ? ` 🔻 ${money(drop.before - drop.after)}` : ''} (🎟️ ${v.disponivel})` : 'sem oferta (🎟️ 0)'}`;
        lines.push(drop ? `🔥 *${row}*` : `• ${row}`);
      }
    }
    lines.push('', `🔗 Ver ingressos: ${eventUrl(EVENTS[i])}`);
  });
  lines.push('', new Date(at).toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo', day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }));
  return lines.join('\n').replaceAll('PCD', '♿️').replaceAll('Estudante', '👨🏻‍🎓');
}
