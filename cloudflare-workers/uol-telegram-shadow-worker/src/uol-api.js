import {
  cleanText,
  evaluateDetailQuality,
  normalizeCard,
  offerIdentityKeys,
} from "./core.js";

const API_URL = "https://gateway.produtos.uol.com.br/clubeuol/v2/coupons";
const BASE_URL = "https://clube.uol.com.br";
const TICKET_CATEGORY_ID = "162";
const MAX_API_BYTES = 1_000_000;

function authorizationValue(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return /^bearer\s/i.test(raw) ? raw : `Bearer ${raw}`;
}

export function ticketApiConfiguration(env) {
  return {
    configured: Boolean(
      authorizationValue(env.UOL_API_AUTHORIZATION) &&
      authorizationValue(env.UOL_OAUTH_AUTHORIZATION),
    ),
  };
}

function publicOfferLink(item) {
  for (const candidate of [item?.url, item?.link]) {
    try {
      const url = new URL(String(candidate || ""), BASE_URL);
      if (url.protocol === "https:" && url.hostname === "clube.uol.com.br") return url.href;
    } catch {
      // Ignore malformed API fields and try the next candidate.
    }
  }
  return "";
}

function parsePartner(value) {
  if (value && typeof value === "object") return value;
  try {
    return JSON.parse(String(value || "{}"));
  } catch {
    return {};
  }
}

function apiDate(value) {
  const match = String(value || "").match(
    /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::\d{2})?/,
  );
  if (!match) return "";
  return `${match[3]}/${match[2]}/${match[1]} ${match[4]}:${match[5]}`;
}

function validityText(item) {
  const start = apiDate(item?.inicio);
  const end = apiDate(item?.fim);
  if (start && end) return `Benefício válido de ${start} até ${end}.`;
  if (end) return `Benefício válido até ${end}.`;
  return "";
}

export function mapTicketApiItem(item) {
  const partner = parsePartner(item?.parceiro);
  const card = normalizeCard({
    link: publicOfferLink(item),
    title: item?.titulo,
    category: "campanhasdeingresso",
    cardImageUrl: item?.imagem,
    partnerImageUrl: partner?.imagem,
    partnerName: partner?.titulo,
  });
  if (!card) return null;

  const detail = {
    title: cleanText(item?.titulo) || card.previewTitle,
    validity: validityText(item),
    description: cleanText(item?.descricao).slice(0, 4_000),
    imageUrl: String(item?.imagem || "").trim(),
  };
  return {
    ...card,
    apiDetail: {
      ...detail,
      quality: evaluateDetailQuality(detail),
    },
  };
}

export function mapTicketApiPayload(payload) {
  const cards = [];
  const seen = new Set();
  for (const item of payload?.beneficios || []) {
    const card = mapTicketApiItem(item);
    if (!card) continue;
    const keys = offerIdentityKeys(card.link);
    if (keys.some((key) => seen.has(key))) continue;
    cards.push(card);
    for (const key of keys) seen.add(key);
  }
  return cards;
}

export function mergeOfferCards(primary, secondary) {
  const merged = [];
  const seen = new Set();
  for (const card of [...(primary || []), ...(secondary || [])]) {
    const keys = offerIdentityKeys(card.link);
    if (!keys.length || keys.some((key) => seen.has(key))) continue;
    merged.push(card);
    for (const key of keys) seen.add(key);
  }
  return merged;
}

export async function fetchTicketOffersFromApi(env, fetchImpl = fetch) {
  if (!ticketApiConfiguration(env).configured) throw new Error("uol_api_not_configured");
  const url = new URL(API_URL);
  url.searchParams.set("offset", "0");
  url.searchParams.set("category_id", TICKET_CATEGORY_ID);
  url.searchParams.set("order", "new");
  url.searchParams.set("_uol_worker_ts", String(Date.now()));

  const response = await fetchImpl(url.href, {
    headers: {
      Authorization: authorizationValue(env.UOL_API_AUTHORIZATION),
      "X-Authorization": authorizationValue(env.UOL_OAUTH_AUTHORIZATION),
      Accept: "application/json",
      "Cache-Control": "no-cache, no-store, max-age=0",
      "User-Agent": "UOLTelegramCloudflare/1.0",
    },
    cf: { cacheTtl: 0, cacheEverything: false },
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error(`uol_api_http_${response.status}`);
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new Error("uol_api_content_type_invalido");
  }
  const contentLength = Number(response.headers.get("content-length") || 0);
  if (contentLength > MAX_API_BYTES) throw new Error("uol_api_json_excede_limite");
  const text = await response.text();
  if (text.length > MAX_API_BYTES) throw new Error("uol_api_json_excede_limite");
  return mapTicketApiPayload(JSON.parse(text));
}
