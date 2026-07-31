const BASE_URL = "https://clube.uol.com.br";

export function cleanText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function decodeRepeatedly(value) {
  let decoded = String(value || "");
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const next = decodeURIComponent(decoded);
      if (next === decoded) break;
      decoded = next;
    } catch {
      break;
    }
  }
  return decoded;
}

function repairMojibake(value) {
  const replacements = new Map([
    ["Ã¡", "á"],
    ["Ã ", "à"],
    ["Ã¢", "â"],
    ["Ã£", "ã"],
    ["Ã©", "é"],
    ["Ãª", "ê"],
    ["Ã­", "í"],
    ["Ã³", "ó"],
    ["Ã´", "ô"],
    ["Ãµ", "õ"],
    ["Ãº", "ú"],
    ["Ã§", "ç"],
    ["Ã‰", "É"],
    ["Ã‡", "Ç"],
  ]);
  let repaired = String(value || "");
  for (const [bad, good] of replacements) repaired = repaired.replaceAll(bad, good);
  return repaired;
}

function applyKnownCanonicalFixes(value) {
  let canonical = String(value || "");
  const knownFixes = new Map([
    ["ltima", "ultima"],
    ["ltimo", "ultimo"],
    ["seleo", "selecao"],
    ["graduao", "graduacao"],
    ["grtis", "gratis"],
    ["ms", "imas"],
    ["preo", "preco"],
  ]);
  for (const [bad, good] of knownFixes) {
    canonical = canonical.replace(
      new RegExp(`(^|-)${bad}(?=-|$)`, "g"),
      (_, prefix) => `${prefix}${good}`,
    );
  }
  canonical = canonical.replace(/(^|-)ps(?=-|$)/g, (_, prefix) => `${prefix}pos`);
  canonical = canonical.replaceAll("-at-", "-ate-");
  return canonical.replace(/-+/g, "-").replace(/^-+|-+$/g, "");
}

export function canonicalKey(value) {
  let raw = cleanText(value);
  if (!raw) return "";
  raw = repairMojibake(decodeRepeatedly(raw))
    .replaceAll("\u00a0", " ")
    .toLowerCase()
    .split("?")[0]
    .split("#")[0]
    .replaceAll("&", " e ")
    .replaceAll("º", "o")
    .replaceAll("ª", "a")
    .replace(/[\s_]+/g, "-")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
  return applyKnownCanonicalFixes(raw);
}

export function normalizeText(value) {
  return canonicalKey(value).replaceAll("-", " ");
}

function rawOfferTail(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  let tail = raw;
  try {
    const url = new URL(raw, BASE_URL);
    tail = url.pathname.replace(/\/+$/, "").split("/").pop() || "";
  } catch {
    tail = raw.split("?")[0].split("#")[0].replace(/\/+$/, "").split("/").pop() || "";
  }

  return decodeRepeatedly(tail);
}

export function slugTailVariants(value) {
  const base = canonicalKey(rawOfferTail(value));
  if (!base) return [];
  const variants = new Set([base, base.replaceAll("-de-", "-")]);
  if (base.includes("joo")) variants.add(base.replaceAll("joo", "joao"));
  if (base.includes("joao")) variants.add(base.replaceAll("joao", "joo"));
  return [...variants].filter(Boolean).sort();
}

export function normalizeOfferId(value) {
  return slugTailVariants(value)[0] || "";
}

export function offerSourceKey(value) {
  try {
    const url = new URL(String(value || ""), BASE_URL);
    const segments = url.pathname
      .split("/")
      .filter(Boolean)
      .map(decodeRepeatedly);
    if (segments.length < 2) return "";
    const partner = canonicalKey(segments.at(-2));
    const firstToken = String(segments.at(-1) || "").split("-")[0];
    const offerCode = canonicalKey(firstToken);
    if (!partner || !/^p[a-z0-9]{2,5}$/.test(offerCode)) return "";
    return `${partner}|${offerCode}`;
  } catch {
    return "";
  }
}

export function offerIdentityKeys(value) {
  const keys = slugTailVariants(value).map((variant) => `slug:${variant}`);
  const sourceKey = offerSourceKey(value);
  if (sourceKey) keys.push(`source:${sourceKey}`);
  return [...new Set(keys)];
}

export function normalizePublicLink(value) {
  try {
    const url = new URL(String(value || ""), BASE_URL);
    if (url.protocol !== "https:" || url.hostname !== "clube.uol.com.br") return "";
    url.hash = "";
    url.search = "";
    return url.href;
  } catch {
    return "";
  }
}

export function normalizeCard(raw) {
  const link = normalizePublicLink(raw?.link);
  const id = normalizeOfferId(link);
  const title = cleanText(raw?.title);
  if (!link || !id || !title) return null;

  return {
    id,
    link,
    previewTitle: title,
    category: cleanText(raw?.category),
    cardImageUrl: String(raw?.cardImageUrl || "").trim(),
    partnerImageUrl: String(raw?.partnerImageUrl || "").trim(),
    partnerName: cleanText(raw?.partnerName),
  };
}

export function dedupeCards(rawCards) {
  const cards = [];
  const seenKeys = new Set();
  for (const raw of rawCards || []) {
    const card = normalizeCard(raw);
    if (!card) continue;
    const identityKeys = offerIdentityKeys(card.link);
    if (identityKeys.some((key) => seenKeys.has(key))) continue;
    cards.push(card);
    for (const key of identityKeys) seenKeys.add(key);
  }
  return cards;
}

export function isTicketCampaign(offer) {
  const link = String(offer?.link || "").toLowerCase();
  const category = normalizeText(offer?.category);
  return link.includes("/campanhasdeingresso/") || category.includes("campanhasdeingresso");
}

function containsWord(text, term) {
  const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`).test(text);
}

export function shouldSendToCanal2(offer) {
  const blob = normalizeText([
    offer?.title,
    offer?.previewTitle,
    offer?.description,
    offer?.category,
    offer?.link,
  ].filter(Boolean).join(" "));

  const blockedTerms = [
    "partida",
    "partidas",
    "campeonato",
    "campeonatos",
    "futebol",
    "jogo",
    "jogos",
    "teatro",
    "stand up",
    "standup",
  ];
  if (blockedTerms.some((term) => containsWord(blob, term))) return false;
  return isTicketCampaign(offer);
}

export function extractValidity(text) {
  const value = cleanText(text);
  if (!value) return "";
  const patterns = [
    /benefício válido de[^.!?\n]*[.!?]?/i,
    /válido até[^.!?\n]*[.!?]?/i,
    /\d{2}\/\d{2}\/\d{4}[\s\S]{0,80}\d{2}\/\d{2}\/\d{4}/i,
  ];
  for (const pattern of patterns) {
    const match = value.match(pattern);
    if (match?.[0]) return cleanText(match[0]);
  }
  return "";
}

function brDateTimeToUtc(value) {
  const match = String(value || "").match(
    /(\d{2})\/(\d{2})\/(\d{4})(?:\s+(?:às\s+)?(\d{2}):(\d{2}))?/i,
  );
  if (!match) return null;
  const [, day, month, year, hour = "00", minute = "00"] = match;
  const timestamp = Date.UTC(
    Number(year),
    Number(month) - 1,
    Number(day),
    Number(hour) + 3,
    Number(minute),
  );
  const parsed = new Date(timestamp);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function parseValidityWindow(validity) {
  const matches = [...String(validity || "").matchAll(
    /(\d{2}\/\d{2}\/\d{4}(?:\s+(?:às\s+)?\d{2}:\d{2})?)/gi,
  )];
  return {
    start: matches[0]?.[1] ? brDateTimeToUtc(matches[0][1]) : null,
    end: matches[1]?.[1] ? brDateTimeToUtc(matches[1][1]) : null,
  };
}

export function evaluateDetailQuality(detail) {
  const hasTitle = cleanText(detail?.title).length >= 4;
  const hasValidity = cleanText(detail?.validity).length >= 8;
  const hasDescription = cleanText(detail?.description).length >= 60;
  const hasImage = Boolean(String(detail?.imageUrl || "").trim());

  if (hasTitle && hasValidity && hasDescription && hasImage) return "complete";
  if ((hasValidity && hasDescription) || (hasDescription && hasImage) || (hasValidity && hasImage)) {
    return "partial";
  }
  if (hasTitle || hasDescription || hasImage || hasValidity) return "weak";
  return "failed";
}

function descriptionAnchor(value) {
  return normalizeText(value).slice(0, 900);
}

export async function sha256Hex(value) {
  const bytes = new TextEncoder().encode(String(value || ""));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

export async function buildDedupeKeys(offer) {
  const title = normalizeText(offer?.title || offer?.previewTitle);
  const validity = normalizeText(offer?.validity);
  const description = descriptionAnchor(offer?.description);
  return {
    dedupeKey: await sha256Hex(`${title}|${validity}|${description}`),
    looseDedupeKey: await sha256Hex(`${title}|${description.slice(0, 280)}`),
    titleValidityKey: title && validity ? await sha256Hex(`${title}|${validity}`) : "",
  };
}

export function decideShadowDelivery(offer, options = {}) {
  const now = options.now instanceof Date ? options.now : new Date();
  const maxValidFromAgeHours = Number(options.maxValidFromAgeHours || 36);
  const validity = parseValidityWindow(offer?.validity);

  if (validity.end && now > validity.end) {
    return {
      eligible: false,
      discardReason: "validade_expirada",
      wouldSendMain: false,
      wouldSendCanal2: false,
    };
  }

  if (validity.start && !validity.end) {
    const ageHours = (now.getTime() - validity.start.getTime()) / 3_600_000;
    if (ageHours > maxValidFromAgeHours) {
      return {
        eligible: false,
        discardReason: "inicio_validade_antigo_sem_fim",
        wouldSendMain: false,
        wouldSendCanal2: false,
      };
    }
  }

  const hasMinimumPayload = Boolean(
    cleanText(offer?.title || offer?.previewTitle) && normalizePublicLink(offer?.link),
  );
  if (!hasMinimumPayload) {
    return {
      eligible: false,
      discardReason: "incompleta",
      wouldSendMain: false,
      wouldSendCanal2: false,
    };
  }

  return {
    eligible: true,
    discardReason: "",
    wouldSendMain: true,
    wouldSendCanal2: shouldSendToCanal2(offer),
  };
}

export function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

const HASHTAG_RULES = [
  ["#ingresso", ["ingresso", "ingressos"]],
  ["#show", ["show", "festival", "musical", "turne", "apresentacao"]],
  ["#teatro", ["teatro", "musical", "espetaculo", "peca"]],
  ["#standup", ["stand up", "standup", "comediante", "humor"]],
  ["#entretenimentoviagens", ["cinema", "ingressos", "espetaculo", "evento"]],
  ["#servicos", ["terapia"]],
  ["#beleza", ["depilacao", "axilas", "beleza", "barba"]],
  ["#comerbeber", ["vinho", "cerveja", "jantar", "almoco", "sobremesa", "restaurante"]],
  ["#cursos", ["curso", "cursos", "ingles", "english"]],
  ["#compraspresentes", ["ovo de pascoa", "vivara", "presente"]],
  ["#educacao", ["graduacao", "pos", "ead", "aprender", "enem"]],
  ["#viagem", ["viagem", "viagens"]],
  ["#eletrodomesticoseletronicos", ["dell", "lg", "eletro", "geladeira", "lavadora"]],
];

const SILENT_HASHTAGS = new Set([
  "#servicos",
  "#beleza",
  "#cursos",
  "#educacao",
  "#eletrodomesticoseletronicos",
]);

export function buildSmartHashtags(offer) {
  const title = normalizeText(offer?.title || offer?.previewTitle);
  const description = normalizeText(offer?.description);
  const full = `${title} ${description}`;
  const tags = [];

  if (isTicketCampaign(offer)) tags.push("#campanhasdeingresso");
  for (const [tag, keywords] of HASHTAG_RULES) {
    const haystack = [
      "#servicos",
      "#beleza",
      "#cursos",
      "#compraspresentes",
      "#educacao",
      "#viagem",
      "#eletrodomesticoseletronicos",
    ].includes(tag) ? title : full;
    if (keywords.some((keyword) => containsWord(haystack, keyword))) tags.push(tag);
  }
  return [...new Set(tags)];
}

export function shouldSendSilent(offer) {
  if (isTicketCampaign(offer)) return false;
  return buildSmartHashtags(offer).some((tag) => SILENT_HASHTAGS.has(tag));
}

export function extractLocationSummary(description) {
  const text = cleanText(description);
  if (!text) return "";
  const explicit = text.match(
    /\blocal\s*:\s*(.{2,220}?)(?=\s+(?:importante|regras?(?:\s+de\s+resgate)?|aten[cç][aã]o|observa[cç][oõ]es?)\s*:|$)/i,
  );
  const candidate = cleanText(explicit?.[1] || "");
  if (!candidate) return "";
  if (candidate.length <= 160) return candidate.replace(/[.,;]+$/, "");
  const cityState = candidate.match(
    /([A-Za-zÀ-ÖØ-öø-ÿ'`\- ]+?)\s*[-/]\s*([A-Za-z]{2})(?=$|[\s,;)])/i,
  );
  if (cityState) {
    return `${cleanText(cityState[1]).replace(/[.,;]+$/, "")} - ${cityState[2].toUpperCase()}`;
  }
  return candidate.slice(0, 157).replace(/\s+\S*$/, "") + "…";
}

function formatSoldOutTime(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return cleanText(value);
  return new Intl.DateTimeFormat("pt-BR", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

export function buildTelegramCaption(offer, options = {}) {
  const title = cleanText(offer?.title || offer?.previewTitle || "Oferta").slice(0, 280);
  const link = normalizePublicLink(offer?.link);
  const tags = buildSmartHashtags(offer);
  const validity = cleanText(offer?.validity).replace(/\s+\./g, ".").slice(0, 360);
  const location = extractLocationSummary(offer?.description);
  const soldOutAt = options.soldOutAt || offer?.soldOutAt || offer?.sold_out_at || "";
  const ticketTitle = isTicketCampaign(offer) ? `‼️ ${title} ‼️` : title;
  const decoratedTitle = soldOutAt ? `[ESGOTADO] ${ticketTitle}` : ticketTitle;
  const titleHtml = soldOutAt
    ? `<s>${escapeHtml(decoratedTitle)}</s>`
    : escapeHtml(decoratedTitle);
  const lines = [`<b>${titleHtml}</b>`];

  if (tags.length) lines.push(escapeHtml(tags.join(" ")));
  if (location) lines.push(`📍 ${escapeHtml(location)}`);
  if (validity) lines.push(`📅 ${escapeHtml(validity.endsWith(".") ? validity : `${validity}.`)}`);
  if (soldOutAt) lines.push(`❌ Oferta esgotada às ${escapeHtml(formatSoldOutTime(soldOutAt))}.`);
  if (link) lines.push(`🔗 ${escapeHtml(link)}`);
  if (options.commentsEnabled && !soldOutAt) {
    lines.push("💬 Veja os detalhes completos nos comentários.");
  }

  return lines.join("\n\n");
}

function splitBoundedText(value, maxLength) {
  const chunks = [];
  let remaining = cleanText(value);
  while (remaining.length > maxLength) {
    let cut = remaining.lastIndexOf(" ", maxLength);
    if (cut < Math.floor(maxLength * 0.6)) cut = maxLength;
    chunks.push(remaining.slice(0, cut).trim());
    remaining = remaining.slice(cut).trim();
  }
  if (remaining) chunks.push(remaining);
  return chunks;
}

export function buildDiscussionCommentChunks(offer) {
  const title = cleanText(offer?.title || offer?.previewTitle || "Oferta");
  let description = cleanText(offer?.description);
  const validity = cleanText(offer?.validity);
  const link = normalizePublicLink(offer?.link);
  const labels = [
    "Sobre o Parceiro", "Benefício", "Regras do benefício", "Regras",
    "Como utilizar", "Como resgatar", "Passo a passo para resgate",
    "Data do Show", "Data", "Quando", "Local", "Importante",
    "REGRAS DE RESGATE", "Atenção, Assinante UOL!",
  ];
  const labelPattern = labels
    .sort((a, b) => b.length - a.length)
    .map((label) => label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
    .join("|");
  description = description.replace(
    new RegExp(`\\s*(${labelPattern})\\s*:?\\s*`, "gi"),
    "\n\n$1: ",
  );
  description = description
    .replace(/\s*•\s*/g, "\n• ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  const blocks = [`📋 <b>${escapeHtml(title)}</b>`];
  for (const rawBlock of description.split(/\n\n+/).map((item) => item.trim()).filter(Boolean)) {
    const labelMatch = rawBlock.match(/^([^:!]{2,60})([:!])\s*(.*)$/s);
    if (labelMatch) {
      const [, label, punctuation, rest] = labelMatch;
      const icon = /^(data|quando)/i.test(label) ? "🗓️ "
        : /^local/i.test(label) ? "📍 "
        : /^(importante|atenção)/i.test(label) ? "❗ "
        : /^regras/i.test(label) ? "📌 " : "";
      blocks.push(`${icon}<b>${escapeHtml(label)}${escapeHtml(punctuation)}</b>${rest ? ` ${escapeHtml(rest)}` : ""}`);
    } else {
      blocks.push(escapeHtml(rawBlock));
    }
  }
  if (validity) blocks.push(`📅 ${escapeHtml(validity.endsWith(".") ? validity : `${validity}.`)}`);
  if (link) blocks.push(`🔗 ${escapeHtml(link)}`);

  const chunks = [];
  let current = "";
  for (const block of blocks.flatMap((item) => splitBoundedText(item, 3_600))) {
    const candidate = current ? `${current}\n\n${block}` : block;
    if (candidate.length > 3_800 && current) {
      chunks.push(current);
      current = block;
    } else {
      current = candidate;
    }
  }
  if (current) chunks.push(current);
  return chunks;
}
