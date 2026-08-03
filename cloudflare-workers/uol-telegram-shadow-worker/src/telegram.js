import {
  buildTelegramCaption,
  buildDiscussionCommentChunks,
  cleanText,
} from "./core.js";
import {
  createAmbiguousResponseTransportError,
  createHttpTransportError,
  createNetworkTransportError,
  shouldDeferTransportFallback,
} from "./transport-error.js";

const TELEGRAM_TIMEOUT_MS = 8_000;
const IMAGE_FETCH_TIMEOUT_MS = 6_000;
const MAX_UPLOAD_IMAGE_BYTES = 8 * 1024 * 1024;

function deadlineTimeoutMs(deadlineAt, maximumMs) {
  const deadlineMs = Date.parse(String(deadlineAt || ""));
  if (!Number.isFinite(deadlineMs)) return maximumMs;
  return Math.max(1, Math.min(maximumMs, Math.floor(deadlineMs - Date.now())));
}

function deadlineOpen(deadlineAt) {
  const deadlineMs = Date.parse(String(deadlineAt || ""));
  return !Number.isFinite(deadlineMs) || Date.now() < deadlineMs;
}

function detectedImageFormat(bytes, declaredContentType = "") {
  const view = new Uint8Array(bytes);
  if (view.length >= 3 && view[0] === 0xff && view[1] === 0xd8 && view[2] === 0xff) {
    return { contentType: "image/jpeg", extension: "jpg" };
  }
  if (view.length >= 8 &&
      view[0] === 0x89 && view[1] === 0x50 && view[2] === 0x4e && view[3] === 0x47 &&
      view[4] === 0x0d && view[5] === 0x0a && view[6] === 0x1a && view[7] === 0x0a) {
    return { contentType: "image/png", extension: "png" };
  }
  if (view.length >= 12 &&
      String.fromCharCode(...view.slice(0, 4)) === "RIFF" &&
      String.fromCharCode(...view.slice(8, 12)) === "WEBP") {
    return { contentType: "image/webp", extension: "webp" };
  }
  const contentType = String(declaredContentType || "").split(";", 1)[0].trim();
  return {
    contentType,
    extension: contentType.includes("png")
      ? "png"
      : contentType.includes("webp")
        ? "webp"
        : contentType.includes("gif")
          ? "gif"
          : "jpg",
  };
}

function telegramConfig(env) {
  return {
    token: String(env.TELEGRAM_TOKEN || "").trim(),
    mainChatId: String(env.TELEGRAM_CHAT_ID || "").trim(),
    canal2Id: String(env.CANAL2_ID || "").trim(),
  };
}

function telegramError(method, httpStatus, payload) {
  const description = cleanText(payload?.description || "").slice(0, 160);
  return createHttpTransportError({
    transport: "telegram",
    operation: method,
    status: Number(payload?.error_code || httpStatus || 0),
    httpStatus,
    retryAfterSeconds: Number(payload?.parameters?.retry_after || 0),
    description,
  });
}

function telegramMethodCanMutate(method) {
  return !/^get/i.test(String(method || ""));
}

async function telegramRequest(url, init, method, fetchImpl) {
  try {
    return await fetchImpl(url, init);
  } catch (error) {
    throw createNetworkTransportError({
      transport: "telegram",
      operation: method,
      error,
      signal: init?.signal,
      ambiguous: telegramMethodCanMutate(method),
    });
  }
}

function offerLinkPreview(offer) {
  const url = String(offer?.link || "").trim();
  if (!url) return { is_disabled: true };
  return {
    is_disabled: false,
    url,
    prefer_small_media: true,
    show_above_text: true,
  };
}

function telegramPhotoIdentity(result) {
  const photos = Array.isArray(result?.photo) ? result.photo : [];
  const largest = photos.at(-1) || {};
  return {
    photoFileId: String(largest.file_id || ""),
    photoFileUniqueId: String(largest.file_unique_id || ""),
  };
}

export function telegramConfiguration(env) {
  const config = telegramConfig(env);
  const opsChatId = String(env.OPS_TELEGRAM_CHAT_ID || config.mainChatId).trim();
  const tokenConfigured = Boolean(config.token);
  const mainConfigured = Boolean(config.mainChatId);
  const canal2Configured = Boolean(config.canal2Id);
  const mainReady = tokenConfigured && mainConfigured;
  const canal2Ready = tokenConfigured && canal2Configured;
  return {
    tokenConfigured,
    mainConfigured,
    canal2Configured,
    mainReady,
    canal2Ready,
    operationsConfigured: Boolean(opsChatId),
    operationsUsesMainFallback: Boolean(opsChatId && !String(env.OPS_TELEGRAM_CHAT_ID || "").trim()),
    // Compatibility field: callers that require both destinations keep the old contract.
    liveReady: mainReady && canal2Ready,
  };
}

export async function sendOperationsAlert(env, text, fetchImpl = fetch) {
  const { mainChatId } = telegramConfig(env);
  const chatId = String(env.OPS_TELEGRAM_CHAT_ID || mainChatId).trim();
  if (!chatId) throw new Error("telegram_operations_chat_missing");
  const result = await telegramCall(env, "sendMessage", {
    chat_id: chatId,
    text: cleanText(text).slice(0, 3_900),
    disable_notification: false,
    link_preview_options: { is_disabled: true },
  }, fetchImpl);
  return { messageId: Number(result?.message_id || 0) };
}

export async function telegramCall(
  env,
  method,
  payload,
  fetchImpl = fetch,
  timeoutMs = TELEGRAM_TIMEOUT_MS,
) {
  const { token } = telegramConfig(env);
  if (!token) throw new Error("telegram_token_missing");

  const response = await telegramRequest(`https://api.telegram.org/bot${token}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(Math.max(1, Math.min(TELEGRAM_TIMEOUT_MS, timeoutMs))),
  }, method, fetchImpl);
  let data = {};
  try {
    data = await response.json();
  } catch {
    // O status HTTP ainda identifica a falha sem registrar o corpo bruto.
  }
  if (response.ok && data?.ok !== true && data?.ok !== false) {
    throw createAmbiguousResponseTransportError({
      transport: "telegram",
      operation: method,
      httpStatus: response.status,
    });
  }
  if (!response.ok || data?.ok !== true) {
    throw telegramError(method, response.status, data);
  }
  return data.result;
}

async function uploadTelegramPhoto(
  env,
  { chatId, imageUrl, caption, disableNotification, imageDeadlineAt = "" },
  fetchImpl,
) {
  const { token } = telegramConfig(env);
  const imageResponse = await fetchImpl(imageUrl, {
    headers: { Accept: "image/*" },
    signal: AbortSignal.timeout(deadlineTimeoutMs(imageDeadlineAt, IMAGE_FETCH_TIMEOUT_MS)),
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
  const imageFormat = detectedImageFormat(bytes, contentType);

  const form = new FormData();
  form.set("chat_id", chatId);
  form.set("caption", caption);
  form.set("parse_mode", "HTML");
  form.set("disable_notification", String(disableNotification));
  form.set(
    "photo",
    new Blob([bytes], { type: imageFormat.contentType }),
    `oferta.${imageFormat.extension}`,
  );
  const response = await telegramRequest(`https://api.telegram.org/bot${token}/sendPhoto`, {
    method: "POST",
    body: form,
    signal: AbortSignal.timeout(deadlineTimeoutMs(imageDeadlineAt, TELEGRAM_TIMEOUT_MS)),
  }, "sendPhoto", fetchImpl);
  let data = {};
  try {
    data = await response.json();
  } catch {
    // O status HTTP ainda identifica a falha sem registrar o corpo bruto.
  }
  if (response.ok && data?.ok !== true && data?.ok !== false) {
    throw createAmbiguousResponseTransportError({
      transport: "telegram",
      operation: "sendPhoto",
      httpStatus: response.status,
    });
  }
  if (!response.ok || data?.ok !== true) {
    throw telegramError("sendPhoto", response.status, data);
  }
  return data.result;
}

async function uploadTelegramPhotoEdit(
  env,
  { chatId, messageId, imageUrl, caption },
  fetchImpl,
) {
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
  const imageFormat = detectedImageFormat(bytes, contentType);
  const form = new FormData();
  form.set("chat_id", chatId);
  form.set("message_id", String(messageId));
  form.set("media", JSON.stringify({
    type: "photo",
    media: "attach://photo",
    caption,
    parse_mode: "HTML",
  }));
  form.set(
    "photo",
    new Blob([bytes], { type: imageFormat.contentType }),
    `oferta.${imageFormat.extension}`,
  );
  const response = await telegramRequest(`https://api.telegram.org/bot${token}/editMessageMedia`, {
    method: "POST",
    body: form,
    signal: AbortSignal.timeout(TELEGRAM_TIMEOUT_MS),
  }, "editMessageMedia", fetchImpl);
  let data = {};
  try {
    data = await response.json();
  } catch {
    // O status HTTP ainda identifica a falha sem registrar o corpo bruto.
  }
  if (response.ok && data?.ok !== true && data?.ok !== false) {
    throw createAmbiguousResponseTransportError({
      transport: "telegram",
      operation: "editMessageMedia",
      httpStatus: response.status,
    });
  }
  if (!response.ok || data?.ok !== true) {
    throw telegramError("editMessageMedia", response.status, data);
  }
  return data.result;
}

export async function sendMainOffer(env, offer, fetchImpl = fetch) {
  const { mainChatId } = telegramConfig(env);
  if (!mainChatId) throw new Error("telegram_main_chat_missing");
  const caption = buildTelegramCaption(offer, {
    commentsEnabled: Boolean(String(env.GRUPO_COMENTARIO_ID || "").trim()),
  });
  const disableNotification = false;
  const imageDeadlineAt = String(offer?.imageDeadlineAt || "").trim();
  // Some ordinary benefits expose only the partner artwork in the listing and
  // the detail page may transiently reject Worker-origin requests. A verified
  // partner image is preferable to silently degrading the alert to text.
  const imageUrl = String(
    offer?.imageUrl || offer?.cardImageUrl || offer?.partnerImageUrl || "",
  ).trim();
  const cachedPhotoFileId = String(offer?.telegramPhotoFileId || "").trim();
  const strategyEnabled = {
    file_id: offer?.imageStrategies?.file_id !== false,
    remote_url: offer?.imageStrategies?.remote_url !== false,
    discord_proxy: offer?.imageStrategies?.discord_proxy !== false,
    upload: offer?.imageStrategies?.upload !== false,
  };
  const remoteStrategy = offer?.telegramImageRemoteStrategy === "discord_proxy"
    ? "discord_proxy"
    : "remote_url";
  const imageAttempts = [];

  if (cachedPhotoFileId && strategyEnabled.file_id && deadlineOpen(imageDeadlineAt)) {
    try {
      const result = await telegramCall(env, "sendPhoto", {
        chat_id: mainChatId,
        photo: cachedPhotoFileId,
        caption,
        parse_mode: "HTML",
        disable_notification: disableNotification,
      }, fetchImpl, deadlineTimeoutMs(imageDeadlineAt, TELEGRAM_TIMEOUT_MS));
      imageAttempts.push({ strategy: "file_id", ok: true, error: "" });
      return {
        messageId: Number(result?.message_id || 0),
        messageKind: "photo",
        imageStrategy: "file_id",
        imageAttempts,
        ...telegramPhotoIdentity(result),
      };
    } catch (error) {
      if (shouldDeferTransportFallback(error)) throw error;
      imageAttempts.push({
        strategy: "file_id",
        ok: false,
        error: cleanText(error?.message || error).slice(0, 160),
      });
    }
  }

  let imageError = "";
  if (imageUrl && deadlineOpen(imageDeadlineAt)) {
    if (strategyEnabled[remoteStrategy] && deadlineOpen(imageDeadlineAt)) {
      try {
        const result = await telegramCall(env, "sendPhoto", {
          chat_id: mainChatId,
          photo: imageUrl,
          caption,
          parse_mode: "HTML",
          disable_notification: disableNotification,
        }, fetchImpl, deadlineTimeoutMs(imageDeadlineAt, TELEGRAM_TIMEOUT_MS));
        imageAttempts.push({ strategy: remoteStrategy, ok: true, error: "" });
        return {
          messageId: Number(result?.message_id || 0),
          messageKind: "photo",
          imageStrategy: remoteStrategy,
          imageAttempts,
          ...telegramPhotoIdentity(result),
        };
      } catch (error) {
        if (shouldDeferTransportFallback(error)) throw error;
        imageError = cleanText(error?.message || error).slice(0, 160);
        imageAttempts.push({ strategy: remoteStrategy, ok: false, error: imageError });
      }
    }
    if (strategyEnabled.upload && deadlineOpen(imageDeadlineAt)) {
      try {
        const result = await uploadTelegramPhoto(env, {
          chatId: mainChatId,
          imageUrl,
          caption,
          disableNotification,
          imageDeadlineAt,
        }, fetchImpl);
        imageAttempts.push({ strategy: "upload", ok: true, error: "" });
        return {
          messageId: Number(result?.message_id || 0),
          messageKind: "photo",
          imageStrategy: "upload",
          imageAttempts,
          ...telegramPhotoIdentity(result),
        };
      } catch (error) {
        if (shouldDeferTransportFallback(error)) throw error;
        const uploadError = cleanText(error?.message || error).slice(0, 160);
        imageAttempts.push({ strategy: "upload", ok: false, error: uploadError });
        imageError = [imageError, uploadError].filter(Boolean).join("|").slice(0, 240);
      }
    }
  }

  if (offer?.deferTextFallback === true && deadlineOpen(imageDeadlineAt)) {
    return {
      deferred: true,
      messageId: 0,
      messageKind: "",
      imageError,
      imageStrategy: "pending_image",
      imageAttempts,
    };
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
    imageError,
    imageStrategy: "text_timeout",
    imageAttempts,
  };
}

export async function editMainOfferMedia(
  env,
  { messageId, offer, telegramPhotoFileId = "" },
  fetchImpl = fetch,
) {
  const { mainChatId } = telegramConfig(env);
  if (!mainChatId) throw new Error("telegram_main_chat_missing");
  const imageUrl = String(
    offer?.imageUrl || offer?.cardImageUrl || offer?.partnerImageUrl || "",
  ).trim();
  const cachedPhotoFileId = String(telegramPhotoFileId || "").trim();
  if (!cachedPhotoFileId && !imageUrl) throw new Error("telegram_main_image_missing");
  const caption = buildTelegramCaption(offer, {
    commentsEnabled: Boolean(String(env.GRUPO_COMENTARIO_ID || "").trim()),
  });
  const imageAttempts = [];
  for (const candidate of [
    cachedPhotoFileId && { media: cachedPhotoFileId, strategy: "file_id" },
    imageUrl && { media: imageUrl, strategy: "remote_url" },
  ].filter(Boolean)) {
    try {
      const result = await telegramCall(env, "editMessageMedia", {
        chat_id: mainChatId,
        message_id: Number(messageId || 0),
        media: {
          type: "photo",
          media: candidate.media,
          caption,
          parse_mode: "HTML",
        },
      }, fetchImpl);
      imageAttempts.push({ strategy: candidate.strategy, ok: true, error: "" });
      return {
        messageId: Number(result?.message_id || messageId || 0),
        messageKind: "photo",
        imageStrategy: `${candidate.strategy}_edit`,
        imageAttempts,
        ...telegramPhotoIdentity(result),
      };
    } catch (error) {
      if (shouldDeferTransportFallback(error)) throw error;
      imageAttempts.push({
        strategy: candidate.strategy,
        ok: false,
        error: cleanText(error?.message || error).slice(0, 160),
      });
    }
  }
  if (!imageUrl) throw new Error("telegram_main_image_edit_failed");
  const result = await uploadTelegramPhotoEdit(env, {
    chatId: mainChatId,
    messageId: Number(messageId || 0),
    imageUrl,
    caption,
  }, fetchImpl);
  imageAttempts.push({ strategy: "upload", ok: true, error: "" });
  return {
    messageId: Number(result?.message_id || messageId || 0),
    messageKind: "photo",
    imageStrategy: "upload_edit",
    imageAttempts,
    ...telegramPhotoIdentity(result),
  };
}

export async function sendDiscussionComments(env, offer, discussionMessageId, fetchImpl = fetch) {
  const groupChatId = String(env.GRUPO_COMENTARIO_ID || "").trim();
  if (!groupChatId) throw new Error("telegram_discussion_group_missing");
  const chunks = buildDiscussionCommentChunks(offer);
  const messageIds = [];
  for (const text of chunks) {
    const result = await telegramCall(env, "sendMessage", {
      chat_id: groupChatId,
      text,
      parse_mode: "HTML",
      disable_notification: false,
      reply_parameters: {
        message_id: Number(discussionMessageId),
        allow_sending_without_reply: false,
      },
      link_preview_options: { is_disabled: true },
    }, fetchImpl);
    messageIds.push(Number(result?.message_id || 0));
  }
  return { messageIds };
}

export async function sendDiscussionComment(env, text, discussionMessageId, fetchImpl = fetch) {
  const groupChatId = String(env.GRUPO_COMENTARIO_ID || "").trim();
  if (!groupChatId) throw new Error("telegram_discussion_group_missing");
  const result = await telegramCall(env, "sendMessage", {
    chat_id: groupChatId,
    text,
    parse_mode: "HTML",
    disable_notification: false,
    reply_parameters: {
      message_id: Number(discussionMessageId),
      allow_sending_without_reply: false,
    },
    link_preview_options: { is_disabled: true },
  }, fetchImpl);
  return { messageId: Number(result?.message_id || 0) };
}

export async function registerTelegramWebhook(env, fetchImpl = fetch) {
  const token = String(env.TELEGRAM_TOKEN || "").trim();
  const baseUrl = String(env.PUBLIC_BASE_URL || "").replace(/\/+$/, "");
  const secret = String(env.TELEGRAM_WEBHOOK_SECRET || "").trim();
  if (!token || !baseUrl || !secret) throw new Error("telegram_webhook_configuration_incomplete");
  const response = await telegramRequest(`https://api.telegram.org/bot${token}/setWebhook`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url: `${baseUrl}/telegram-webhook`,
      secret_token: secret,
      allowed_updates: ["message"],
      drop_pending_updates: false,
    }),
    signal: AbortSignal.timeout(TELEGRAM_TIMEOUT_MS),
  }, "setWebhook", fetchImpl);
  const data = await response.json().catch(() => ({}));
  if (response.ok && data?.ok !== true && data?.ok !== false) {
    throw createAmbiguousResponseTransportError({
      transport: "telegram",
      operation: "setWebhook",
      httpStatus: response.status,
    });
  }
  if (!response.ok || data?.ok !== true) {
    throw telegramError("setWebhook", response.status, data);
  }
  return { ok: true };
}

export async function getTelegramWebhookInfo(env, fetchImpl = fetch) {
  const info = await telegramCall(env, "getWebhookInfo", {}, fetchImpl);
  return {
    url: String(info?.url || ""),
    pendingUpdateCount: Number(info?.pending_update_count || 0),
    lastErrorDate: Number(info?.last_error_date || 0),
    lastErrorMessage: cleanText(info?.last_error_message || "").slice(0, 200),
    allowedUpdates: Array.isArray(info?.allowed_updates) ? info.allowed_updates : [],
  };
}

export async function forwardToCanal2(env, mainMessageId, fetchImpl = fetch) {
  const { mainChatId, canal2Id } = telegramConfig(env);
  if (!mainChatId) throw new Error("telegram_main_chat_missing");
  if (!canal2Id) throw new Error("telegram_canal2_missing");
  const result = await telegramCall(env, "copyMessage", {
    chat_id: canal2Id,
    from_chat_id: mainChatId,
    message_id: Number(mainMessageId),
    disable_notification: false,
  }, fetchImpl);
  return {
    messageId: Number(result?.message_id || 0),
  };
}

export async function editMainOfferMessage(
  env,
  { chatId = "", messageId, messageKind, offer },
  fetchImpl = fetch,
) {
  const { mainChatId } = telegramConfig(env);
  const caption = buildTelegramCaption(offer, {
    commentsEnabled: !chatId && Boolean(String(env.GRUPO_COMENTARIO_ID || "").trim()),
  });
  const common = {
    chat_id: String(chatId || mainChatId),
    message_id: Number(messageId),
    parse_mode: "HTML",
  };
  if (messageKind === "photo") {
    await telegramCall(env, "editMessageCaption", { ...common, caption }, fetchImpl);
  } else {
    await telegramCall(env, "editMessageText", {
      ...common,
      text: caption,
      link_preview_options: offerLinkPreview(offer),
    }, fetchImpl);
  }
  return { ok: true };
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
      link_preview_options: offerLinkPreview(offer),
    }, fetchImpl);
  }
}
