import {
  buildTelegramCaption,
  cleanText,
  shouldSendSilent,
} from "./core.js";

const TELEGRAM_TIMEOUT_MS = 15_000;
const IMAGE_FETCH_TIMEOUT_MS = 6_000;
const MAX_UPLOAD_IMAGE_BYTES = 8 * 1024 * 1024;

function telegramConfig(env) {
  return {
    token: String(env.TELEGRAM_TOKEN || "").trim(),
    mainChatId: String(env.TELEGRAM_CHAT_ID || "").trim(),
    canal2Id: String(env.CANAL2_ID || "").trim(),
  };
}

function telegramError(method, status, payload) {
  const description = cleanText(payload?.description || "").slice(0, 160);
  return new Error(`telegram_${method}_${status}${description ? `:${description}` : ""}`);
}

export function telegramConfiguration(env) {
  const config = telegramConfig(env);
  return {
    tokenConfigured: Boolean(config.token),
    mainConfigured: Boolean(config.mainChatId),
    canal2Configured: Boolean(config.canal2Id),
    liveReady: Boolean(config.token && config.mainChatId && config.canal2Id),
  };
}

export async function telegramCall(env, method, payload, fetchImpl = fetch) {
  const { token } = telegramConfig(env);
  if (!token) throw new Error("telegram_token_missing");

  const response = await fetchImpl(`https://api.telegram.org/bot${token}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(TELEGRAM_TIMEOUT_MS),
  });
  let data = {};
  try {
    data = await response.json();
  } catch {
    // O status HTTP ainda identifica a falha sem registrar o corpo bruto.
  }
  if (!response.ok || data?.ok !== true) {
    throw telegramError(method, response.status, data);
  }
  return data.result;
}

async function uploadTelegramPhoto(env, { chatId, imageUrl, caption, disableNotification }, fetchImpl) {
  const { token } = telegramConfig(env);
  const imageResponse = await fetchImpl(imageUrl, {
    headers: { Accept: "image/*" },
    signal: AbortSignal.timeout(IMAGE_FETCH_TIMEOUT_MS),
  });
  if (!imageResponse.ok) throw new Error(`offer_image_http_${imageResponse.status}`);
  const contentType = String(imageResponse.headers.get("Content-Type") || "").toLowerCase();
  if (!contentType.startsWith("image/")) throw new Error("offer_image_invalid_content_type");
  const declaredSize = Number(imageResponse.headers.get("Content-Length") || 0);
  if (declaredSize > MAX_UPLOAD_IMAGE_BYTES) throw new Error("offer_image_too_large");
  const bytes = await imageResponse.arrayBuffer();
  if (!bytes.byteLength || bytes.byteLength > MAX_UPLOAD_IMAGE_BYTES) {
    throw new Error("offer_image_invalid_size");
  }

  const form = new FormData();
  form.set("chat_id", chatId);
  form.set("caption", caption);
  form.set("parse_mode", "HTML");
  form.set("disable_notification", String(disableNotification));
  form.set("photo", new Blob([bytes], { type: contentType }), "oferta.jpg");
  const response = await fetchImpl(`https://api.telegram.org/bot${token}/sendPhoto`, {
    method: "POST",
    body: form,
    signal: AbortSignal.timeout(TELEGRAM_TIMEOUT_MS),
  });
  let data = {};
  try {
    data = await response.json();
  } catch {
    // O status HTTP ainda identifica a falha sem registrar o corpo bruto.
  }
  if (!response.ok || data?.ok !== true) {
    throw telegramError("sendPhoto", response.status, data);
  }
  return data.result;
}

export async function sendMainOffer(env, offer, fetchImpl = fetch) {
  const { mainChatId } = telegramConfig(env);
  if (!mainChatId) throw new Error("telegram_main_chat_missing");
  const caption = buildTelegramCaption(offer);
  const disableNotification = shouldSendSilent(offer);
  const imageUrl = String(offer?.imageUrl || offer?.cardImageUrl || "").trim();

  if (imageUrl) {
    try {
      const result = await telegramCall(env, "sendPhoto", {
        chat_id: mainChatId,
        photo: imageUrl,
        caption,
        parse_mode: "HTML",
        disable_notification: disableNotification,
      }, fetchImpl);
      return {
        messageId: Number(result?.message_id || 0),
        messageKind: "photo",
      };
    } catch {
      try {
        const result = await uploadTelegramPhoto(env, {
          chatId: mainChatId,
          imageUrl,
          caption,
          disableNotification,
        }, fetchImpl);
        return {
          messageId: Number(result?.message_id || 0),
          messageKind: "photo",
        };
      } catch {
        // Uma imagem recusada pelo Telegram não pode impedir a oferta em texto.
      }
    }
  }

  const result = await telegramCall(env, "sendMessage", {
    chat_id: mainChatId,
    text: caption,
    parse_mode: "HTML",
    disable_notification: disableNotification,
    link_preview_options: { is_disabled: true },
  }, fetchImpl);
  return {
    messageId: Number(result?.message_id || 0),
    messageKind: "text",
  };
}

export async function forwardToCanal2(env, mainMessageId, fetchImpl = fetch) {
  const { mainChatId, canal2Id } = telegramConfig(env);
  if (!mainChatId) throw new Error("telegram_main_chat_missing");
  if (!canal2Id) throw new Error("telegram_canal2_missing");
  const result = await telegramCall(env, "forwardMessage", {
    chat_id: canal2Id,
    from_chat_id: mainChatId,
    message_id: Number(mainMessageId),
    disable_notification: true,
  }, fetchImpl);
  return {
    messageId: Number(result?.message_id || 0),
  };
}

export async function editSoldOutMessage(
  env,
  {
    chatId,
    messageId,
    messageKind,
    offer,
    soldOutAt,
  },
  fetchImpl = fetch,
) {
  const caption = buildTelegramCaption(offer, { soldOutAt });
  const common = {
    chat_id: chatId,
    message_id: Number(messageId),
    parse_mode: "HTML",
  };
  if (messageKind === "photo") {
    await telegramCall(env, "editMessageCaption", {
      ...common,
      caption,
    }, fetchImpl);
  } else {
    await telegramCall(env, "editMessageText", {
      ...common,
      text: caption,
      link_preview_options: { is_disabled: true },
    }, fetchImpl);
  }
}

export async function sendTransportTest(env, fetchImpl = fetch) {
  const { mainChatId } = telegramConfig(env);
  if (!mainChatId) throw new Error("telegram_main_chat_missing");
  const result = await telegramCall(env, "sendMessage", {
    chat_id: mainChatId,
    text: "✅ Teste técnico do monitor Cloudflare concluído. Nenhuma oferta foi registrada por esta mensagem.",
    disable_notification: true,
    link_preview_options: { is_disabled: true },
  }, fetchImpl);
  const mainMessageId = Number(result?.message_id || 0);
  if (!mainMessageId) throw new Error("telegram_test_main_message_id_missing");
  const canal2 = await forwardToCanal2(env, mainMessageId, fetchImpl);
  return {
    mainMessageId,
    canal2MessageId: canal2.messageId,
  };
}
