export function parseBrl(value) {
  const match = String(value || '').match(/R\$\s*([\d.]+,\d{2})/i);
  if (!match) return null;
  const cents = Number(match[1].replaceAll('.', '').replace(',', ''));
  return Number.isSafeInteger(cents) ? cents : null;
}

export function parseFinalReview(text) {
  const section = String(text || '').split('Resumo da compra')[1];
  if (!section || !/M[eé]todo de pagamento\s*(?:Editar\s*)?Pix\b/i.test(section)) return null;
  const amount = pattern => parseBrl(section.match(pattern)?.[1]);
  const ticket = amount(/Ingresso\s*(R\$\s*[\d.]+,\d{2})/i);
  const fee = amount(/Taxa de servi[cç]o\s*\([^)]*\)\s*(R\$\s*[\d.]+,\d{2})/i);
  const discount = amount(/Cupom de desconto\s*-\s*(R\$\s*[\d.]+,\d{2})/i);
  const total = amount(/Valor total\s*(R\$\s*[\d.]+,\d{2})/i);
  if (![ticket, fee, discount, total].every(Number.isSafeInteger) || discount <= 0 ||
      total <= 0 || ticket + fee - discount !== total) return null;
  return { ticket, fee, discount, total, payment: 'PIX' };
}

export function extractPixTotal(text) {
  const source = String(text || '');
  const start = source.search(/\bPIX\b/i);
  if (start < 0) return null;
  const afterPix = source.slice(start, start + 300);
  const card = afterPix.search(/Cart[aã]o de cr[eé]dito/i);
  const section = card > 0 ? afterPix.slice(0, card) : afterPix;
  const values = [...section.matchAll(/R\$\s*[\d.]+,\d{2}/gi)]
    .map(match => parseBrl(match[0])).filter(Number.isSafeInteger);
  return values.length ? Math.min(...values) : null;
}

export function extractPixCode(value) {
  const values = Array.isArray(value) ? value : [value];
  for (const candidate of values) {
    const match = String(candidate || '').match(/000201[\s\S]{55,700}/);
    if (!match) continue;
    const code = match[0].split(/(?:\n{2,}|<|>|"|\\n)/, 1)[0].trim();
    if (code.length >= 60 && code.length <= 700) return code;
  }
  return null;
}
