import { cleanText } from "./core.js";
import {
  DeliveryTransportError,
  createHttpTransportError,
  createNetworkTransportError,
} from "./transport-error.js";

const BEEPER_GATEWAY_TIMEOUT_MS = 15_000;

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

export function beeperGatewayConfiguration(env) {
  const active = enabled(env.BEEPER_DELIVERY_ENABLED);
  const url = String(env.BEEPER_GATEWAY_URL || "").trim();
  const token = String(env.BEEPER_GATEWAY_TOKEN || "").trim();
  let destinationKey = "";
  try {
    destinationKey = beeperDestinationKey(env);
  } catch {}
  return {
    enabled: active,
    configured: Boolean(url && token && destinationKey),
    ready: !active || Boolean(url && token && destinationKey),
    url,
  };
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
    offer?.discordImageProxyUrl || offer?.imageUrl || offer?.cardImageUrl ||
      offer?.partnerImageUrl || "",
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

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (response.status === 409 && payload?.code === "delivery_unknown") {
      throw new DeliveryTransportError({
        transport: "beeper",
        operation: "send",
        status: 409,
        httpStatus: 409,
        ambiguous: true,
        description: "delivery_unknown",
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
  return {
    pendingMessageId: String(payload?.pendingMessageID || payload?.pendingMessageId || ""),
    replayed: Boolean(payload?.replayed),
  };
}
