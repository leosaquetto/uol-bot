import {
  cleanText,
  evaluateDetailQuality,
  normalizeCard,
  offerIdentityKeys,
} from "./core.js";
import { validateTicketApiPayload } from "./uol-contract.js";

const API_URL = "https://gateway.produtos.uol.com.br/clubeuol/v2/coupons";
const BASE_URL = "https://clube.uol.com.br";
const MAX_API_BYTES = 1_000_000;

function couponUrl() {
  const url = new URL(API_URL);
  url.searchParams.set("offset", "0");
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

export function prepareImmediateApiOffer(card) {
  if (!card?.apiDetail) return null;
  const detail = {
    ...card.apiDetail,
    quality: evaluateDetailQuality(card.apiDetail),
  };
  return {
    ...card,
    ...detail,
    detailOk: true,
    detailError: "",
    detailElapsedMs: 0,
  };
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
) {
  if (!ticketApiConfiguration(env).configured) throw new Error("uol_api_not_configured");
  const url = couponUrl();

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
  let payload;
  try {
    payload = JSON.parse(text);
  } catch {
    const error = new Error("uol_api_json_invalido");
    error.contract = {
      ok: false,
      reason: "json_invalid",
      total: 0,
      valid: 0,
      invalid: 0,
      fields: [],
    };
    throw error;
  }
  const contract = validateTicketApiPayload(payload);
  if (!contract.ok) {
    const error = new Error(
      `uol_api_contract_invalid:${contract.reason}:total=${contract.total}:valid=${contract.valid}`,
    );
    error.contract = contract;
    throw error;
  }
  const cards = mapTicketApiPayload(payload);
  Object.defineProperty(cards, "contract", {
    value: contract,
    enumerable: false,
    configurable: false,
  });
  return cards;
}
