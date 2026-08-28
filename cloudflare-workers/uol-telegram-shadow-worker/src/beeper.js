import { cleanText } from "./core.js";
import {
  DeliveryTransportError,
  createAmbiguousResponseTransportError,
  createHttpTransportError,
  createNetworkTransportError,
} from "./transport-error.js";

const BEEPER_GATEWAY_TIMEOUT_MS = 45_000;
const BEEPER_GATEWAY_HEALTH_TIMEOUT_MS = 5_000;
const BEEPER_GATEWAY_MAX_RESPONSE_BYTES = 32 * 1024;
const BEEPER_DELIVERY_FILTER_MAX_IDS = 32;

function enabled(value) {
  return ["1", "true", "yes", "on", "enabled"].includes(
    String(value ?? "").trim().toLowerCase(),
  );
}

export function beeperDestinationKey(env) {
  const value = String(env?.BEEPER_DESTINATION_KEY || "").trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9._-]{2,63}$/.test(value)) {
    throw new Error("beeper_destination_key_invalid");
  }
  return value;
}

export function beeperDeliveryIdempotencyKey(offerId, destinationKey) {
  return `uol:${String(offerId || "").trim()}:${String(destinationKey || "").trim()}:v1`;
}

export function beeperDeliveryOfferIds(env) {
  const raw = String(env?.BEEPER_DELIVERY_OFFER_IDS || "").trim();
  if (!raw) return [];
  const ids = [...new Set(raw.split(",").map((value) => value.trim()).filter(Boolean))]
    .sort();
  if (
    ids.length > BEEPER_DELIVERY_FILTER_MAX_IDS ||
    ids.some((value) => value.length > 200 || !/^[a-zA-Z0-9._:-]+$/.test(value))
  ) {
    throw new Error("beeper_delivery_offer_ids_invalid");
  }
  return ids;
}

export function beeperGatewayReadinessUrl(env) {
  const configuredUrl = String(env?.BEEPER_GATEWAY_URL || "").trim();
  let url;
  try {
    url = new URL(configuredUrl);
  } catch {
    throw new Error("beeper_gateway_url_invalid");
  }
  const localHttp = url.protocol === "http:" &&
    ["127.0.0.1", "::1", "localhost"].includes(url.hostname);
  if (
    url.protocol !== "https:" && !localHttp ||
    url.username || url.password || url.search || url.hash ||
    url.pathname !== "/v1/send-offer"
  ) {
    throw new Error("beeper_gateway_url_invalid");
  }
  return new URL("/v1/readyz", url.origin).href;
}

export function beeperGatewayConfiguration(env) {
  const active = enabled(env.BEEPER_DELIVERY_ENABLED);
  const url = String(env.BEEPER_GATEWAY_URL || "").trim();
  const token = String(env.BEEPER_GATEWAY_TOKEN || "").trim();
  let destinationKey = "";
  let offerIds = [];
  let urlValid = false;
  try {
    destinationKey = beeperDestinationKey(env);
  } catch {}
  try {
    offerIds = beeperDeliveryOfferIds(env);
  } catch {}
  try {
    beeperGatewayReadinessUrl(env);
    urlValid = true;
  } catch {}
  const configured = Boolean(url && urlValid && token && destinationKey) &&
    (!String(env?.BEEPER_DELIVERY_OFFER_IDS || "").trim() || offerIds.length > 0);
  return {
    enabled: active,
    configured,
    ready: !active || configured,
    url,
    destinationKey,
    offerIds,
    filterActive: offerIds.length > 0,
  };
}

export function beeperDeliveryErrorCode(error) {
  const description = String(error?.description || "").trim().toLowerCase();
  if (/^[a-z0-9][a-z0-9._:-]{0,159}$/.test(description)) return description;
  const structured = String(error?.code || "").trim().toLowerCase();
  if (/^[a-z0-9][a-z0-9._:-]{0,159}$/.test(structured)) return structured;
  const message = String(error?.message || error || "").trim().toLowerCase();
  if (/^beeper_[a-z0-9_]{1,150}$/.test(message)) return message;
  const status = Number(error?.status || error?.httpStatus || 0);
  if (Number.isInteger(status) && status > 0) return `beeper_http_${status}`;
  return "beeper_delivery_failed";
}

async function readBoundedJson(response) {
  const declaredSize = Number(response.headers.get("content-length") || 0);
  if (declaredSize > BEEPER_GATEWAY_MAX_RESPONSE_BYTES) {
    throw new Error("beeper_gateway_response_too_large");
  }
  if (!response.body) return {};
  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > BEEPER_GATEWAY_MAX_RESPONSE_BYTES) {
        await reader.cancel("response_too_large");
        throw new Error("beeper_gateway_response_too_large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  if (!total) return {};
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    return {};
  }
}

export async function probeBeeperGateway(env, fetchImpl = fetch) {
  const configuration = beeperGatewayConfiguration(env);
  const checkedAt = new Date().toISOString();
  if (!configuration.enabled) {
    return { checkedAt, ok: true, status: 0, code: "disabled" };
  }
  if (!configuration.configured) {
    return { checkedAt, ok: false, status: 0, code: "unconfigured" };
  }
  try {
    const response = await fetchImpl(beeperGatewayReadinessUrl(env), {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${String(env.BEEPER_GATEWAY_TOKEN || "").trim()}`,
        "Accept": "application/json",
      },
      signal: AbortSignal.timeout(BEEPER_GATEWAY_HEALTH_TIMEOUT_MS),
    });
    const payload = await readBoundedJson(response);
    const contractReady = payload?.deliveryConfirmation === "confirmed_by_whatsapp_bridge";
    const ok = response.status === 200 && payload?.ok === true && contractReady;
    return {
      checkedAt,
      ok,
      status: response.status,
      code: ok
        ? "ready"
        : response.status === 200 && payload?.ok === true
          ? "delivery_contract_mismatch"
          : `http_${response.status}`,
    };
  } catch (error) {
    const transportError = createNetworkTransportError({
      transport: "beeper",
      operation: "ready",
      error,
      signal: null,
      ambiguous: false,
    });
    return {
      checkedAt,
      ok: false,
      status: 0,
      code: beeperDeliveryErrorCode(transportError),
    };
  }
}

export function buildBeeperOfferText(offer) {
  const title = cleanText(
    offer?.title || offer?.previewTitle || "Novo benefício do Clube UOL",
  );
  const link = String(offer?.link || "").trim();
  return [`🚨 ${title}`, link].filter(Boolean).join("\n");
}

export async function sendBeeperOffer(
  env,
  offer,
  { idempotencyKey },
  fetchImpl = fetch,
) {
  const configuration = beeperGatewayConfiguration(env);
  if (!configuration.configured) throw new Error("beeper_gateway_missing");
  const link = String(offer?.link || "").trim();
  const title = cleanText(offer?.title || offer?.previewTitle || "");
  const summary = cleanText(offer?.description || "").slice(0, 2_000);
  const imageUrl = String(
    offer?.imageUrl || offer?.cardImageUrl || offer?.partnerImageUrl ||
      offer?.discordImageProxyUrl || "",
  ).trim();
  const key = String(idempotencyKey || "").trim();
  if (!key) throw new Error("beeper_idempotency_key_missing");

  let response;
  try {
    response = await fetchImpl(configuration.url, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${String(env.BEEPER_GATEWAY_TOKEN || "").trim()}`,
        "Content-Type": "application/json",
        "Idempotency-Key": key,
      },
      body: JSON.stringify({
        link,
        text: buildBeeperOfferText(offer),
        title,
        preview: { title, summary, imageUrl },
      }),
      signal: AbortSignal.timeout(BEEPER_GATEWAY_TIMEOUT_MS),
    });
  } catch (error) {
    throw createNetworkTransportError({
      transport: "beeper",
      operation: "send",
      error,
      signal: null,
      ambiguous: false,
    });
  }

  let payload;
  try {
    payload = await readBoundedJson(response);
  } catch (error) {
    if (response.ok) {
      throw createAmbiguousResponseTransportError({
        transport: "beeper",
        operation: "send",
        httpStatus: response.status,
      });
    }
    payload = {};
  }
  if (!response.ok) {
    if ([409, 503].includes(response.status) && payload?.code === "delivery_unknown") {
      throw new DeliveryTransportError({
        transport: "beeper",
        operation: "send",
        status: 409,
        httpStatus: 409,
        ambiguous: true,
        description: "delivery_unknown",
      });
    }
    if (response.status === 409 && payload?.code === "delivery_pending") {
      throw new DeliveryTransportError({
        transport: "beeper",
        operation: "send",
        status: 409,
        httpStatus: 409,
        retryable: true,
        description: "delivery_pending",
      });
    }
    throw createHttpTransportError({
      transport: "beeper",
      operation: "send",
      status: response.status,
      httpStatus: response.status,
      retryAfterSeconds: Number(response.headers.get("Retry-After") || 0),
      description: cleanText(payload?.code || payload?.error || "").slice(0, 160),
    });
  }
  const pendingMessageId = String(
    payload?.pendingMessageID || payload?.pendingMessageId || "",
  ).trim();
  const deliveryState = String(payload?.deliveryState || "").trim();
  if (!pendingMessageId || deliveryState !== "confirmed_by_whatsapp_bridge") {
    throw createAmbiguousResponseTransportError({
      transport: "beeper",
      operation: "send",
      httpStatus: response.status,
    });
  }
  return {
    pendingMessageId,
    replayed: Boolean(payload?.replayed),
  };
}
