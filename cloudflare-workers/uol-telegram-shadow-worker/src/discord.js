import { cleanText, isTicketCampaign } from "./core.js";
import {
  createAmbiguousResponseTransportError,
  createHttpTransportError,
  createNetworkTransportError,
} from "./transport-error.js";

const DISCORD_TIMEOUT_MS = 10_000;

export function discordConfiguration(env) {
  return {
    configured: Boolean(String(env.DISCORD_WEBHOOK_URL || "").trim()),
    imageCacheConfigured: Boolean(
      String(env.DISCORD_IMAGE_CACHE_WEBHOOK_URL || "").trim(),
    ),
  };
}

export function buildDiscordPayload(offer, { soldOutAt = "" } = {}) {
  const title = cleanText(offer?.title || offer?.previewTitle || "Novo benefício do Clube UOL");
  const link = String(offer?.link || "").trim();
  const imageUrl = String(offer?.imageUrl || offer?.cardImageUrl || "").trim();
  const soldOut = Boolean(String(soldOutAt || "").trim());
  const ticket = isTicketCampaign(offer);
  const decoratedTitle = soldOut ? `[ESGOTADO] ${title}` : title;
  const embed = {
    title: decoratedTitle,
    url: link,
    color: soldOut ? 0xd83c3e : 0xf5a623,
    description: soldOut
      ? "❌ Esta oferta não está mais disponível no Clube UOL."
      : ticket
        ? "🎟️ Novo benefício na categoria de ingressos do Clube UOL."
        : "✨ Novo benefício disponível no Clube UOL.",
    fields: [{
      name: "Abrir oferta",
      value: `[Acessar agora no Clube UOL](${link})`,
      inline: false,
    }],
    footer: { text: "Clube UOL • monitor independente" },
    timestamp: soldOut ? String(soldOutAt) : new Date().toISOString(),
  };
  if (imageUrl) embed.image = { url: imageUrl };
  return {
    username: "Clube UOL",
    content: `${soldOut ? "❌" : "🚨"} **${decoratedTitle}**`,
    embeds: [embed],
    allowed_mentions: { parse: [] },
  };
}

function discordRetryAfterSeconds(response, payload) {
  return Number(
    payload?.retry_after ||
    response.headers.get("Retry-After") ||
    response.headers.get("X-RateLimit-Reset-After") ||
    0,
  );
}

function discordError(operation, response, payload) {
  return createHttpTransportError({
    transport: "discord",
    operation,
    status: response.status,
    httpStatus: response.status,
    retryAfterSeconds: discordRetryAfterSeconds(response, payload),
    description: cleanText(payload?.message || "").slice(0, 160),
  });
}

async function discordRequest(url, init, operation, ambiguous, fetchImpl) {
  try {
    return await fetchImpl(url, init);
  } catch (error) {
    throw createNetworkTransportError({
      transport: "discord",
      operation,
      error,
      signal: init?.signal,
      ambiguous,
    });
  }
}

export async function sendDiscordOffer(env, offer, fetchImpl = fetch) {
  const webhookUrl = String(env.DISCORD_WEBHOOK_URL || "").trim();
  if (!webhookUrl) throw new Error("discord_webhook_missing");
  const url = new URL(webhookUrl);
  url.searchParams.set("wait", "true");
  const response = await discordRequest(url.href, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildDiscordPayload(offer)),
    signal: AbortSignal.timeout(DISCORD_TIMEOUT_MS),
  }, "sendWebhook", true, fetchImpl);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw discordError("sendWebhook", response, payload);
  if (!String(payload?.id || "")) {
    throw createAmbiguousResponseTransportError({
      transport: "discord",
      operation: "sendWebhook",
      httpStatus: response.status,
    });
  }
  return {
    messageId: String(payload?.id || ""),
    imageProxyUrl: String(payload?.embeds?.[0]?.image?.proxy_url || ""),
  };
}

export async function cacheDiscordOfferImage(env, offer, fetchImpl = fetch) {
  const webhookUrl = String(env.DISCORD_IMAGE_CACHE_WEBHOOK_URL || "").trim();
  if (!webhookUrl) throw new Error("discord_image_cache_webhook_missing");
  const feedOffer = {
    ...offer,
    imageUrl: String(
      offer?.imageUrl || offer?.cardImageUrl || offer?.partnerImageUrl || "",
    ).trim(),
  };
  const url = new URL(webhookUrl);
  url.searchParams.set("wait", "true");
  const response = await discordRequest(url.href, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildDiscordPayload(feedOffer)),
    signal: AbortSignal.timeout(DISCORD_TIMEOUT_MS),
  }, "cacheImage", true, fetchImpl);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw discordError("cacheImage", response, payload);
  if (!String(payload?.id || "")) {
    throw createAmbiguousResponseTransportError({
      transport: "discord",
      operation: "cacheImage",
      httpStatus: response.status,
    });
  }
  return {
    messageId: String(payload.id),
    imageProxyUrl: String(payload?.embeds?.[0]?.image?.proxy_url || ""),
  };
}

export async function editDiscordOffer(
  env,
  { messageId, offer, soldOutAt = "", webhookUrl = "" },
  fetchImpl = fetch,
) {
  const resolvedWebhookUrl = String(webhookUrl || env.DISCORD_WEBHOOK_URL || "").trim();
  if (!resolvedWebhookUrl) throw new Error("discord_webhook_missing");
  if (!String(messageId || "").trim()) throw new Error("discord_message_id_missing");
  const url = new URL(resolvedWebhookUrl);
  url.pathname = `${url.pathname.replace(/\/+$/, "")}/messages/${encodeURIComponent(messageId)}`;
  url.search = "";
  const { username: _username, ...payload } = buildDiscordPayload(offer, { soldOutAt });
  const response = await discordRequest(url.href, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(DISCORD_TIMEOUT_MS),
  }, "editWebhookMessage", true, fetchImpl);
  const responsePayload = await response.json().catch(() => ({}));
  if (!response.ok) throw discordError("editWebhookMessage", response, responsePayload);
  return { messageId: String(responsePayload?.id || messageId) };
}

export async function sendDiscordOperationsAlert(env, text, fetchImpl = fetch) {
  const webhookUrl = String(env.DISCORD_OPS_WEBHOOK_URL || "").trim();
  if (!webhookUrl) throw new Error("discord_operations_webhook_missing");
  const response = await discordRequest(webhookUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      username: "Clube UOL • Operações",
      content: cleanText(text).slice(0, 1_900),
      allowed_mentions: { parse: [] },
    }),
    signal: AbortSignal.timeout(DISCORD_TIMEOUT_MS),
  }, "operations_alert", true, fetchImpl);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw discordError("operations_alert", response, payload);
  }
  return { ok: true };
}

export async function getDiscordMessageImageProxy(
  env,
  messageId,
  fetchImpl = fetch,
  webhookOverride = "",
) {
  const webhookUrl = String(webhookOverride || env.DISCORD_WEBHOOK_URL || "").trim();
  if (!webhookUrl) throw new Error("discord_webhook_missing");
  if (!String(messageId || "").trim()) throw new Error("discord_message_id_missing");
  const url = new URL(webhookUrl);
  url.pathname = `${url.pathname.replace(/\/+$/, "")}/messages/${encodeURIComponent(messageId)}`;
  url.search = "";
  const response = await discordRequest(url.href, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(DISCORD_TIMEOUT_MS),
  }, "getMessage", false, fetchImpl);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw discordError("getMessage", response, payload);
  return String(payload?.embeds?.[0]?.image?.proxy_url || "");
}
