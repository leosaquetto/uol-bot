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
