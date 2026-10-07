// Read-only HTML interpretation. This module never fetches or follows links.
const ORIGIN = 'https://clube.uol.com.br';
const VOID = new Set(['area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr']);
const IGNORE = new Set(['script', 'style', 'template', 'noscript']);
const ENTITIES = { amp: '&', quot: '"', apos: "'", lt: '<', gt: '>', nbsp: ' ', aacute: 'á', agrave: 'à', acirc: 'â', atilde: 'ã', eacute: 'é', ecirc: 'ê', iacute: 'í', oacute: 'ó', ocirc: 'ô', otilde: 'õ', uacute: 'ú', ccedil: 'ç' };

function decode(value = '') {
  return String(value).replace(/&(#x[\da-f]+|#\d+|[a-z]+);/gi, (full, entity) => {
    if (entity[0] === '#') {
      const code = entity[1].toLowerCase() === 'x' ? parseInt(entity.slice(2), 16) : Number(entity.slice(1));
      return code > 0 && code <= 0x10ffff ? String.fromCodePoint(code) : full;
    }
    const decoded = ENTITIES[entity.toLowerCase()];
    return decoded ? (entity[0] === entity[0].toUpperCase() ? decoded.toUpperCase() : decoded) : full;
  });
}

export function normalizeText(value = '') {
  return decode(value).normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/\s+/g, ' ').trim();
}

function parseHtml(html) {
  if (typeof html !== 'string' || html.length > 2_000_000) throw new Error('HTML_INPUT_INVALID');
  const root = { tag: 'root', attrs: {}, children: [] };
  const stack = [root];
  const tokens = html.match(/<!--[\s\S]*?-->|<![^>]*>|<\/?[a-z](?:[^>"']|"[^"]*"|'[^']*')*>|[^<]+|</gi) || [];
  for (const token of tokens) {
    if (token.startsWith('<!--') || token.startsWith('<!')) continue;
    const close = token.match(/^<\/([\w-]+)/);
    if (IGNORE.has(stack.at(-1).tag) && close?.[1]?.toLowerCase() !== stack.at(-1).tag) continue;
    if (close) {
      const index = stack.findLastIndex((node) => node.tag === close[1].toLowerCase());
      if (index > 0) stack.length = index;
      continue;
    }
    const open = token.match(/^<([\w-]+)/);
    if (open) {
      const tag = open[1].toLowerCase();
      // Do not interpret markup-like strings embedded inside script/style blocks.
      if (IGNORE.has(stack.at(-1).tag)) continue;
      const attrs = {};
      const rest = token.slice(open[0].length, token.length - 1);
      for (const attr of rest.matchAll(/([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/g)) {
        attrs[attr[1].toLowerCase()] = decode(attr[2] ?? attr[3] ?? attr[4] ?? '');
      }
      const node = { tag, attrs, children: [], parent: stack.at(-1) };
      node.parent.children.push(node);
      if (!VOID.has(tag) && !/\/\s*>$/.test(token)) stack.push(node);
    } else if (!IGNORE.has(stack.at(-1).tag)) stack.at(-1).children.push(decode(token));
  }
  return root;
}

const hasClass = (node, name) => (node.attrs.class || '').split(/\s+/).includes(name);
const hidden = (node) => IGNORE.has(node.tag) || 'hidden' in node.attrs || node.attrs['aria-hidden'] === 'true' || /(?:display\s*:\s*none|visibility\s*:\s*hidden)/i.test(node.attrs.style || '') || hasClass(node, 'd-none');
function all(node, predicate) {
  const results = [];
  function walk(current) {
    if (typeof current === 'string' || hidden(current)) return;
    if (predicate(current)) results.push(current);
    for (const child of current.children) walk(child);
  }
  if (node) walk(node);
  return results;
}
const first = (node, predicate) => all(node, predicate)[0] || null;
function nodeText(node) {
  if (!node) return '';
  if (typeof node === 'string') return node;
  if (hidden(node)) return '';
  return node.children.map(nodeText).join(['strong', 'b', 'em', 'span', 'i'].includes(node.tag) ? '' : ' ').replace(/\s+/g, ' ').trim();
}
function resolve(value, base = ORIGIN) {
  try {
    const url = new URL(value, base);
    return url.protocol === 'https:' && !url.username && !url.password ? url.href : null;
  } catch { return null; }
}
function offerUrl(value, base = ORIGIN) {
  const absolute = resolve(value, base);
  if (!absolute) return null;
  const url = new URL(absolute);
  return url.origin === ORIGIN && /^\/campanhasdeingresso\/[^/]+$/.test(url.pathname) && !url.search && !url.hash ? url.href : null;
}
function imageFrom(node, base) {
  const image = first(node, (element) => element.attrs['data-src'] || (element.tag === 'img' && element.attrs.src));
  return image ? resolve(image.attrs['data-src'] || image.attrs.src, base) : null;
}
function meta(root, key) {
  return first(root, (node) => node.tag === 'meta' && (node.attrs.property === key || node.attrs.name === key))?.attrs.content || null;
}

export function parseCatalog(html, baseUrl = ORIGIN) {
  const root = parseHtml(html);
  const section = first(root, (node) => node.attrs.id === 'beneficios');
  if (!section) return [];
  const entries = new Map();
  for (const card of all(section, (node) => hasClass(node, 'beneficio'))) {
    if (normalizeText(card.attrs['data-categoria']) !== 'ingressos exclusivos') continue;
    const urls = [...new Set(all(card, (node) => node.tag === 'a').map((node) => offerUrl(node.attrs.href, baseUrl)).filter(Boolean))];
    if (urls.length !== 1) continue;
    const title = nodeText(first(card, (node) => hasClass(node, 'titulo')));
    const image = first(card, (node) => hasClass(node, 'imagem-beneficio'));
    if (!title || entries.has(urls[0])) continue;
    entries.set(urls[0], { url: urls[0], title, imageUrl: imageFrom(image, baseUrl) });
  }
  return [...entries.values()];
}

export function parseOffer(html, url) {
  const root = parseHtml(html);
  const scopes = all(root, (node) => node.attrs.id === 'beneficio');
  const scope = scopes.length === 1 ? scopes[0] : null;
  const titles = all(scope, (node) => node.tag === 'h2');
  const descriptions = all(scope, (node) => hasClass(node, 'info-beneficio'));
  const title = titles.length === 1 ? nodeText(titles[0]) : '';
  const description = descriptions.length === 1 ? nodeText(descriptions[0]) : '';
  const canonicalRaw = first(root, (node) => node.tag === 'link' && node.attrs.rel === 'canonical')?.attrs.href;
  const canonicalUrl = canonicalRaw ? resolve(canonicalRaw, url) : null;
  const ogUrl = meta(root, 'og:url');
  const canonicalMatches = canonicalUrl === url && (!ogUrl || resolve(ogUrl, url) === url);
  const actionNodes = all(scope, (node) => node.attrs.id === 'rescue_button');
  const action = actionNodes.length === 1 ? actionNodes[0] : null;
  const actionUrl = action?.attrs.href ? resolve(action.attrs.href, url) : null;
  const actionMatches = Boolean(offerUrl(url) && actionUrl === `${url}/resgatar`);
  const requiresLogin = Boolean(actionUrl && new URL(actionUrl).origin === ORIGIN && new URL(actionUrl).pathname === '/auth/uol/login');
  const actionText = nodeText(action);
  const disabled = Boolean(action && ('disabled' in action.attrs || action.attrs['aria-disabled'] === 'true' || hasClass(action, 'disabled')));
  const availabilityText = nodeText(first(scope, (node) => hasClass(node, 'detalhes')));
  const soldOut = /\b(?:esgotad[oa]s?|indisponive[li]s?|encerrad[oa]s?)\b/.test(normalizeText(`${title} ${description} ${availabilityText}`));
  const imageScope = first(scope, (node) => node.attrs.id === 'ilustracoes');
  const imageUrl = imageFrom(first(imageScope, (node) => hasClass(node, 'thumb-image')) || imageScope, url);
  return {
    url, title, description, text: `${title}\n${description}`.trim(), imageUrl,
    redeemUrl: actionMatches ? actionUrl : null,
    available: actionMatches && normalizeText(actionText) === 'utilizar este beneficio' && !disabled && !soldOut,
    requiresLogin, soldOut,
    identityEvidence: { scoped: scopes.length === 1, titleCount: titles.length, descriptionCount: descriptions.length, canonicalUrl, canonicalMatches, actionCount: actionNodes.length, actionMatches },
  };
}

export function parseHistory(html) {
  const root = parseHtml(html);
  const text = nodeText(root);
  const name = text.match(/Ol[áa],\s*([^!]+)!/i)?.[1]?.trim() || null;
  const signedInName = name && name.length <= 100 ? name : null;
  const section = first(root, (node) => node.attrs.id === 'beneficios');
  const entries = new Map();
  for (const card of all(section, (node) => hasClass(node, 'beneficio'))) {
    const links = [...new Set(all(card, (node) => node.tag === 'a').map((node) => resolve(node.attrs.href)).filter((value) => value && new URL(value).origin === ORIGIN && /^\/perfil\/beneficios\/[^/?#]+$/.test(new URL(value).pathname) && !new URL(value).search && !new URL(value).hash))];
    if (links.length !== 1) continue;
    const url = links[0];
    const titleNode = first(card, (node) => hasClass(node, 'titulo') || /^h[234]$/.test(node.tag));
    const title = nodeText(titleNode);
    const partnerText = nodeText(first(card, (node) => hasClass(node, 'parceiro')));
    const category = normalizeText(partnerText) === 'campanhas de ingressos' ? 'campanhasdeingresso' : null;
    const imageScope = first(card, (node) => hasClass(node, 'imagem-beneficio')) || card;
    const associated = all(card, (node) => node.tag === 'a').map((node) => offerUrl(node.attrs.href)).filter(Boolean);
    const id = new URL(url).pathname.split('/').at(-1);
    entries.set(id, { id, url, title, category, partnerText, imageUrl: imageFrom(imageScope, ORIGIN), offerUrl: [...new Set(associated)].length === 1 ? associated[0] : null, text: nodeText(card), redemptionDate: null });
  }
  const paginationNodes = all(root, (node) => hasClass(node, 'pagination') || node.attrs.rel === 'next' || node.attrs.rel === 'prev' || /pagina[cç][aã]o/i.test(node.attrs['aria-label'] || ''));
  const pageLinks = all(root, (node) => node.tag === 'a').map((node) => resolve(node.attrs.href)).filter((value) => value && new URL(value).origin === ORIGIN && new URL(value).pathname === '/perfil/beneficios' && /(?:^|&)(?:page|pagina|offset)=/.test(new URL(value).search.slice(1)));
  return { signedInName, entries: [...entries.values()], authenticated: Boolean(signedInName && section && /meus resgates/i.test(text)), historyScopeFound: Boolean(section), hasPagination: paginationNodes.length > 0 || pageLinks.length > 0, paginationUrls: [...new Set(pageLinks)], redemptionDatesObservable: false };
}

function containsAlias(text, aliases = []) {
  return aliases.some((alias) => {
    const normalized = normalizeText(alias);
    if (!normalized) return false;
    let index = text.indexOf(normalized);
    while (index >= 0) {
      if (!/[a-z0-9]/.test(text[index - 1] || '') && !/[a-z0-9]/.test(text[index + normalized.length] || '')) return true;
      index = text.indexOf(normalized, index + 1);
    }
    return false;
  });
}
const MONTHS = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'];
function datesIn(text) {
  const normalized = normalizeText(text);
  const dates = [];
  const add = (day, month, year, source) => dates.push({ day: Number(day), month: Number(month), year: year ? Number(year) : null, source });
  for (const match of normalized.matchAll(/(?<!\d)(\d{1,2})[/-](\d{1,2})(?:[/-](\d{4}|\d{2}))?(?!\d)/g)) add(match[1], match[2], match[3] ? (match[3].length === 2 ? `20${match[3]}` : match[3]) : null, match[0]);
  const months = MONTHS.join('|');
  for (const match of normalized.matchAll(new RegExp(`(?<!\\d)(\\d{1,2})\\s+(?:de\\s+)?(${months})(?:\\s+(?:de\\s+)?(\\d{4}))?`, 'g'))) add(match[1], MONTHS.indexOf(match[2]) + 1, match[3], match[0]);
  for (const match of normalized.matchAll(/\b(\d{4})-(\d{2})-(\d{2})\b/g)) add(match[3], match[2], match[1], match[0]);
  return dates;
}

export function matchOffer(offer, campaign) {
  const reasons = [];
  const title = normalizeText(offer?.title);
  const description = normalizeText(offer?.description);
  const text = `${title} ${description}`;
  const identity = offer?.identityEvidence;
  if (!offerUrl(offer?.url) || (campaign.category && campaign.category !== 'campanhasdeingresso')) reasons.push('WRONG_CATEGORY_OR_URL');
  if (!identity?.scoped || identity.titleCount !== 1 || identity.descriptionCount !== 1 || !identity.canonicalMatches || identity.actionCount !== 1 || !identity.actionMatches) reasons.push('IDENTITY_UNVERIFIED');
  if (!title || !description) reasons.push('CONTENT_INCOMPLETE');
  if (!offer?.available || offer?.requiresLogin || offer?.soldOut) reasons.push('NOT_REDEEMABLE');
  const artistSource = containsAlias(text, campaign.artistAliases) ? 'offer_text' : null;
  if (!artistSource) reasons.push('ARTIST_MISSING');
  const pair = /\b(?:0?2|dois)\s+ingressos?\b/.test(text) || /\b(?:0?1|um)\s+par\s+de\s+ingressos\b/.test(text);
  const ticketCounts = [...text.matchAll(/\b(\d+)\s+ingressos?\b/g)].map((match) => Number(match[1]));
  const pairCounts = [...text.matchAll(/\b(\d+)\s+pares?\s+de\s+ingressos\b/g)].map((match) => Number(match[1]));
  if (campaign.quantity !== 2 || !pair || ticketCounts.some((count) => count !== 2) || pairCounts.some((count) => count !== 1) || /\bum\s+ingresso\b/.test(text)) reasons.push('QUANTITY_MISMATCH');
  if (/\bdescontos?\b|\d\s*%/.test(text)) reasons.push('DISCOUNT_OFFER');
  if (!containsAlias(text, campaign.venueAliases)) reasons.push('VENUE_MISSING');
  const cityAliases = campaign.cityAliases || (normalizeText(campaign.city) === 'sao paulo' ? ['sao paulo', 'sp'] : [campaign.city]);
  if (!containsAlias(text, cityAliases)) reasons.push('CITY_MISSING');
  const target = /^(\d{4})-(\d{2})-(\d{2})$/.exec(campaign.date || '');
  const dates = [...datesIn(title).map((date) => ({ ...date, field: 'title' })), ...datesIn(description).map((date) => ({ ...date, field: 'description' }))];
  if (/\b\d{1,2}\s*(?:e|a|ate|–)\s*\d{1,2}(?:\s*\/|\s+(?:de\s+)?(?:janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro))\b/.test(text)) reasons.push('EVENT_DATE_AMBIGUOUS');
  if (!target) reasons.push('CAMPAIGN_DATE_INVALID');
  else {
    const [, year, month, day] = target.map(Number);
    if (!dates.length) reasons.push('EVENT_DATE_MISSING');
    else if (dates.some((date) => date.day !== day || date.month !== month || (date.year !== null && date.year !== year))) reasons.push('EVENT_DATE_CONFLICT');
  }
  return { ok: reasons.length === 0, reasons, evidence: { artistSource, dates, quantity: pair ? 2 : null, venue: containsAlias(text, campaign.venueAliases), city: containsAlias(text, cityAliases) } };
}
