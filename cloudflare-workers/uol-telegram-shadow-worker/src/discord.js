import { cleanText, isTicketCampaign } from "./core.js";
import {
  createAmbiguousResponseTransportError,
  createHttpTransportError,
  createNetworkTransportError,
} from "./transport-error.js";

const DISCORD_TIMEOUT_MS = 10_000;
const DISCORD_TRUNCATION_SUFFIX = "...";
const DISCORD_EMBED_LIMITS = Object.freeze({
  title: 240,
  description: 1_200,
  validity: 500,
  partner: 250,
  category: 250,
  url: 1_000,
});

const DISCORD_SECTION_LABELS = [
  "Atenção, Assinante UOL",
  "REGRAS DE RESGATE",
  "Data do Show",
  "Importante",
  "Local",
  "Data",
];

const DISCORD_SECTION_PATTERN = DISCORD_SECTION_LABELS
  .sort((a, b) => b.length - a.length)
  .map((label) => label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
  .join("|");

function boundedDiscordText(value, maxLength) {
  const text = cleanText(value);
  if (text.length <= maxLength) return text;
  const prefixLength = Math.max(0, maxLength - DISCORD_TRUNCATION_SUFFIX.length);
  return `${text.slice(0, prefixLength).trimEnd()}${DISCORD_TRUNCATION_SUFFIX}`;
}

function formatDiscordDescription(value) {
  const text = cleanText(value);
  if (!text) return "";
  return text
    .replace(
      new RegExp(`\\s+(${DISCORD_SECTION_PATTERN})(?=\\s*[!:])`, "gi"),
      "\n\n$1",
    )
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function boundedDiscordDescription(value, maxLength) {
  const text = formatDiscordDescription(value);
  if (text.length <= maxLength) return text;
  const prefixLength = Math.max(0, maxLength - DISCORD_TRUNCATION_SUFFIX.length);
  return `${text.slice(0, prefixLength).trimEnd()}${DISCORD_TRUNCATION_SUFFIX}`;
}

function discordOfferFields(offer, link) {
  const fields = [];
  const addField = (name, value, maxLength, inline = false) => {
    const text = boundedDiscordText(value, maxLength);
    if (text) fields.push({ name, value: text, inline });
  };
  addField("Validade", offer?.validity, DISCORD_EMBED_LIMITS.validity);
  addField("Parceiro", offer?.partnerName, DISCORD_EMBED_LIMITS.partner, true);
  addField("Categoria", offer?.category, DISCORD_EMBED_LIMITS.category, true);
  addField("URL da oferta", link, DISCORD_EMBED_LIMITS.url);
  return fields;
}

export function discordConfiguration(env) {
  return {
    configured: Boolean(String(env.DISCORD_WEBHOOK_URL || "").trim()),
    imageCacheConfigured: Boolean(
      String(env.DISCORD_IMAGE_CACHE_WEBHOOK_URL || "").trim(),
    ),
  };
}

export function buildDiscordPayload(offer, { soldOutAt = "" } = {}) {
  const title = boundedDiscordText(
    offer?.title || offer?.previewTitle || "Novo benefício do Clube UOL",
    DISCORD_EMBED_LIMITS.title,
  );
  const link = String(offer?.link || "").trim();
  const imageUrl = String(offer?.imageUrl || offer?.cardImageUrl || "").trim();
  const soldOut = Boolean(String(soldOutAt || "").trim());
  const ticket = isTicketCampaign(offer);
  const decoratedTitle = soldOut ? `[ESGOTADO] ${title}` : title;
  const statusDescription = soldOut
    ? "❌ Esta oferta não está mais disponível no Clube UOL."
    : ticket
      ? "🎟️ Novo benefício na categoria de ingressos do Clube UOL."
      : "✨ Novo benefício disponível no Clube UOL.";
  const summary = ticket
    ? boundedDiscordDescription(offer?.description, DISCORD_EMBED_LIMITS.description)
    : boundedDiscordText(offer?.description, DISCORD_EMBED_LIMITS.description);
  const embed = {
    title: decoratedTitle,
    url: link,
    color: soldOut ? 0xd83c3e : 0xf5a623,
    description: summary ? `${statusDescription}\n\n${summary}` : statusDescription,
    fields: discordOfferFields(offer, link),
    footer: { text: "Clube UOL • monitor independente" },
    timestamp: soldOut ? String(soldOutAt) : new Date().toISOString(),
  };
  if (imageUrl) embed.image = { url: imageUrl };
  return {
    username: "Clube UOL",
    content: `${soldOut ? "❌" : "🚨"} **${decoratedTitle}**${link ? `\n${link}` : ""}`,
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
