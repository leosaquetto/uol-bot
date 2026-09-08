export const EVENTS = [
  { day: '12/09/2026', date: '1789261200000', local: '1765323797528x513509114247905300' },
  { day: '13/09/2026', date: '1789347600000', local: '1765323829346x381107157350744060' },
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
  return Object.fromEntries(keys.map(k => [k, matrix[k] || { preco_min: 0, disponivel: 0 }]));
}
export function drops(previous, current) {
  if (!previous) return [];
  return current.flatMap((m, i) => keys.flatMap(key => {
    const a = previous[i]?.[key], b = m[key];
    return a?.disponivel > 0 && b.disponivel > 0 && b.preco_min < a.preco_min ? [{ i, key, before: a.preco_min, after: b.preco_min }] : [];
  }));
}
const money = n => `R$ ${(n / 100).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
export function format(current, changes, at) {
  const lines = [changes.length ? '📉 *BAIXOU! • ROCK IN RIO 2026*' : '🎟️ *ROCK IN RIO 2026 • PREÇOS ATUAIS*'];
  for (const d of changes) lines.push('', `💚 *${EVENTS[d.i].day} • ${d.key.replace('||', ' • ')}*`, `De ${money(d.before)} por *${money(d.after)}*`, `Economia: *${money(d.before - d.after)}*`);
  current.forEach((m, i) => {
    lines.push('', `🗓️ *${EVENTS[i].day}*`);
    for (const sector of ['Gramado', 'Comfort Zone']) {
      lines.push('', `${sector === 'Gramado' ? '🌿' : '✨'} *${sector}*`);
      for (const key of keys.filter(k => k.startsWith(sector + '||'))) {
        const v = m[key], drop = changes.find(d => d.i === i && d.key === key);
        lines.push(`• ${key.split('||')[1]}: ${v.disponivel ? `*${money(v.preco_min)}* · 🎟️ ${v.disponivel}${drop ? ` 🔻 ${money(drop.before - drop.after)}` : ''}` : 'sem oferta · 🎟️ 0'}`);
      }
    }
    lines.push('', `🔗 Ver ingressos: ${eventUrl(EVENTS[i])}`);
  });
  lines.push('', `🕒 ${new Date(at).toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' })} • Brasília`, 'Menor valor anunciado por categoria. Quantidades não indicam estoque todo nesse preço. Valores sujeitos a alteração.');
  return lines.join('\n');
}
