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

function couponUrl(categoryId = "") {
  const url = new URL(API_URL);
  url.searchParams.set("offset", "0");
  if (categoryId) url.searchParams.set("category_id", categoryId);
  url.searchParams.set("order", "new");
  url.searchParams.set("_uol_worker_ts", String(Date.now()));
  return url;
}

function authorizationValue(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return /^bearer\s/i.test(raw) ? raw : `Bearer ${raw}`;
}

export function ticketApiConfiguration(env) {
  return {
    configured: Boolean(authorizationValue(env.UOL_API_AUTHORIZATION)),
    personalAuthorizationRequired: false,
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

function categoryFromLink(link, fallback = "") {
  try {
    return new URL(link).pathname.split("/").filter(Boolean)[0] || fallback;
  } catch {
    return fallback;
  }
}

export function mapTicketApiItem(item, fallbackCategory = "campanhasdeingresso") {
  const partner = parsePartner(item?.parceiro);
  const link = publicOfferLink(item);
  const card = normalizeCard({
    link,
    title: item?.titulo,
    category: categoryFromLink(link, fallbackCategory),
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

export function mapTicketApiPayload(payload, fallbackCategory = "campanhasdeingresso") {
  const cards = [];
  const seen = new Set();
  for (const item of payload?.beneficios || []) {
    const card = mapTicketApiItem(item, fallbackCategory);
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
  const indexesByIdentity = new Map();
  for (const card of primary || []) {
    const keys = offerIdentityKeys(card.link);
    if (!keys.length || keys.some((key) => indexesByIdentity.has(key))) continue;
    merged.push(card);
    const index = merged.length - 1;
    for (const key of keys) indexesByIdentity.set(key, index);
  }
  for (const card of secondary || []) {
    const keys = offerIdentityKeys(card.link);
    if (!keys.length) continue;
    const existingIndex = keys.map((key) => indexesByIdentity.get(key))
      .find((index) => Number.isInteger(index));
    if (Number.isInteger(existingIndex)) {
      const existing = merged[existingIndex];
      merged[existingIndex] = {
        ...existing,
        // A imagem exposta na listagem pública é acessível ao Telegram. A
        // variante devolvida pela API da UOL pode exigir contexto e retornar 403.
        cardImageUrl: card.cardImageUrl || existing.cardImageUrl,
        partnerImageUrl: card.partnerImageUrl || existing.partnerImageUrl,
        partnerName: card.partnerName || existing.partnerName,
      };
      continue;
    }
    merged.push(card);
    const index = merged.length - 1;
    for (const key of keys) indexesByIdentity.set(key, index);
  }
  return merged;
}

export async function fetchOffersFromApi(
  env,
  fetchImpl = fetch,
  { categoryId = "" } = {},
) {
  if (!ticketApiConfiguration(env).configured) throw new Error("uol_api_not_configured");
  const url = couponUrl(categoryId);

  const headers = {
    Authorization: authorizationValue(env.UOL_API_AUTHORIZATION),
    Accept: "application/json",
    "Cache-Control": "no-cache, no-store, max-age=0",
    "User-Agent": "UOLTelegramCloudflare/1.0",
  };
  const response = await fetchImpl(url.href, {
    headers: {
      ...headers,
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
  return mapTicketApiPayload(JSON.parse(text), categoryId === TICKET_CATEGORY_ID
    ? "campanhasdeingresso"
    : "");
}

export async function probeCouponAuthentication(
  env,
  fetchImpl = fetch,
  personalAuthorization = "",
) {
  const application = authorizationValue(env.UOL_API_AUTHORIZATION);
  const personal = authorizationValue(personalAuthorization || env.UOL_OAUTH_AUTHORIZATION);
  const variants = [
    { name: "both", application: true, personal: true },
    { name: "application_only", application: true, personal: false },
    { name: "personal_only", application: false, personal: true },
    { name: "none", application: false, personal: false },
  ];
  const results = [];
  for (const variant of variants) {
    const headers = {
      Accept: "application/json",
      "Cache-Control": "no-cache, no-store, max-age=0",
      "User-Agent": "UOLTelegramCloudflare/1.0",
    };
    if (variant.application && application) headers.Authorization = application;
    if (variant.personal && personal) headers["X-Authorization"] = personal;
    const response = await fetchImpl(couponUrl(TICKET_CATEGORY_ID).href, {
      headers,
      cf: { cacheTtl: 0, cacheEverything: false },
      signal: AbortSignal.timeout(10_000),
    });
    const text = await response.text();
    let payload = {};
    try {
      payload = JSON.parse(text);
    } catch {
      payload = {};
    }
    results.push({
      name: variant.name,
      status: response.status,
      accepted: response.ok,
      offers: Array.isArray(payload?.beneficios) ? payload.beneficios.length : 0,
      challenge: Boolean(response.headers.get("www-authenticate")),
      errorCode: cleanText(payload?.error || "").slice(0, 80),
    });
  }
  return results;
}

export async function fetchTicketOffersFromApi(
  env,
  fetchImpl = fetch,
  options = {},
) {
  return fetchOffersFromApi(env, fetchImpl, {
    categoryId: TICKET_CATEGORY_ID,
    ...options,
  });
}
