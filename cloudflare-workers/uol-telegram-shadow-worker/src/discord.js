import { cleanText } from "./core.js";

const DISCORD_TIMEOUT_MS = 10_000;

export function discordConfiguration(env) {
  return {
    configured: Boolean(String(env.DISCORD_WEBHOOK_URL || "").trim()),
  };
}

export function buildDiscordPayload(offer) {
  const title = cleanText(offer?.title || offer?.previewTitle || "Novo benefício de ingressos");
  const link = String(offer?.link || "").trim();
  const imageUrl = String(offer?.imageUrl || offer?.cardImageUrl || "").trim();
  const embed = {
    title,
    url: link,
    color: 0xf5a623,
    description: "🎟️ Novo benefício na categoria de ingressos do Clube UOL.",
    fields: [{
      name: "Abrir oferta",
      value: `[Acessar agora no Clube UOL](${link})`,
      inline: false,
    }],
    footer: { text: "Clube UOL • monitor independente" },
    timestamp: new Date().toISOString(),
  };
  if (imageUrl) embed.image = { url: imageUrl };
  return {
    username: "Clube UOL • Ingressos",
    content: `🚨 **${title}**`,
    embeds: [embed],
    allowed_mentions: { parse: [] },
  };
}

export async function sendDiscordOffer(env, offer, fetchImpl = fetch) {
  const webhookUrl = String(env.DISCORD_WEBHOOK_URL || "").trim();
  if (!webhookUrl) throw new Error("discord_webhook_missing");
  const url = new URL(webhookUrl);
  url.searchParams.set("wait", "true");
  const response = await fetchImpl(url.href, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildDiscordPayload(offer)),
    signal: AbortSignal.timeout(DISCORD_TIMEOUT_MS),
  });
  if (!response.ok) {
    throw new Error(`discord_http_${response.status}`);
  }
  const payload = await response.json().catch(() => ({}));
  return {
    messageId: String(payload?.id || ""),
    imageProxyUrl: String(payload?.embeds?.[0]?.image?.proxy_url || ""),
  };
}

export async function getDiscordMessageImageProxy(env, messageId, fetchImpl = fetch) {
  const webhookUrl = String(env.DISCORD_WEBHOOK_URL || "").trim();
  if (!webhookUrl) throw new Error("discord_webhook_missing");
  if (!String(messageId || "").trim()) throw new Error("discord_message_id_missing");
  const url = new URL(webhookUrl);
  url.pathname = `${url.pathname.replace(/\/+$/, "")}/messages/${encodeURIComponent(messageId)}`;
  url.search = "";
  const response = await fetchImpl(url.href, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(DISCORD_TIMEOUT_MS),
  });
  if (!response.ok) throw new Error(`discord_message_http_${response.status}`);
  const payload = await response.json();
  return String(payload?.embeds?.[0]?.image?.proxy_url || "");
}
