const BASE_URL = "https://clube.uol.com.br";

export function cleanText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

export function normalizeText(value) {
  return cleanText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeOfferId(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  let tail = raw;
  try {
    const url = new URL(raw, BASE_URL);
    tail = url.pathname.replace(/\/+$/, "").split("/").pop() || "";
  } catch {
    tail = raw.split("?")[0].split("#")[0].replace(/\/+$/, "").split("/").pop() || "";
  }

  try {
    tail = decodeURIComponent(tail);
  } catch {
    // O slug original ainda é uma identidade útil.
  }

  return tail
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
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
  const byId = new Map();
  for (const raw of rawCards || []) {
    const card = normalizeCard(raw);
    if (!card || byId.has(card.id)) continue;
    byId.set(card.id, card);
  }
  return [...byId.values()];
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
  const explicit = text.match(/\blocal\s*:\s*([^|.;]{2,120})/i);
  const candidate = cleanText(explicit?.[1] || "");
  if (!candidate) return "";
  const cityState = candidate.match(
    /([A-Za-zÀ-ÖØ-öø-ÿ'`\- ]+?)\s*[-/]\s*([A-Za-z]{2})(?=$|[\s,;)])/i,
  );
  if (cityState) {
    return `${cleanText(cityState[1]).replace(/[.,;]+$/, "")} - ${cityState[2].toUpperCase()}`;
  }
  return candidate.slice(0, 100);
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

  return lines.join("\n\n");
}
