import test from "node:test";
import assert from "node:assert/strict";

import {
  editSoldOutMessage,
  sendSoldOutNotice,
  editMainOfferMessage,
  forwardToCanal2,
  editDiscussionComment,
  sendMainOffer,
  sendOperationsAlert,
  sendDiscussionComment,
  sendTransportTest,
  telegramConfiguration,
  registerTelegramWebhook,
  getTelegramWebhookInfo,
} from "../src/telegram.js";

const env = {
  TELEGRAM_TOKEN: "123456:test-token",
  TELEGRAM_CHAT_ID: "-100111",
  CANAL2_ID: "-100222",
};

const offer = {
  link: "https://clube.uol.com.br/campanhasdeingresso/show-teste",
  title: "2 ingressos: Show Teste",
  previewTitle: "2 ingressos: Show Teste",
  category: "Ingressos Exclusivos",
  description: "Local: São Paulo - SP. Regras completas da oferta.",
  imageUrl: "https://example.com/show.jpg",
};

function jsonResponse(result, status = 200) {
  return new Response(JSON.stringify({
    ok: status >= 200 && status < 300,
    result,
  }), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

test("envia alerta operacional com notificação normal e fallback para o canal principal", async () => {
  let payload;
  const result = await sendOperationsAlert(env, "⚠️ teste operacional", async (url, init) => {
    assert.match(url, /sendMessage$/);
    payload = JSON.parse(init.body);
    return jsonResponse({ message_id: 901 });
  });
  assert.equal(result.messageId, 901);
  assert.equal(payload.chat_id, env.TELEGRAM_CHAT_ID);
  assert.equal(payload.disable_notification, false);
  assert.equal(payload.link_preview_options.is_disabled, true);
  assert.equal(telegramConfiguration(env).operationsUsesMainFallback, true);
});

test("configuração live exige token e os dois canais", () => {
  assert.deepEqual(telegramConfiguration(env), {
    tokenConfigured: true,
    mainConfigured: true,
    canal2Configured: true,
    operationsConfigured: true,
    operationsUsesMainFallback: true,
    liveReady: true,
  });
  assert.equal(telegramConfiguration({ ...env, CANAL2_ID: "" }).liveReady, false);
});

test("envia foto ao canal principal sem expor token no payload", async () => {
  let requestUrl = "";
  let payload = {};
  const result = await sendMainOffer(env, offer, async (url, init) => {
    requestUrl = url;
    payload = JSON.parse(init.body);
    return jsonResponse({ message_id: 41 });
  });
  assert.equal(result.messageId, 41);
  assert.equal(result.messageKind, "photo");
  assert.match(requestUrl, /sendPhoto$/);
  assert.equal(payload.chat_id, env.TELEGRAM_CHAT_ID);
  assert.equal(payload.disable_notification, false);
  assert.equal(JSON.stringify(payload).includes(env.TELEGRAM_TOKEN), false);
});

test("usa a imagem oficial do parceiro quando o detalhe não fornece thumbnail", async () => {
  let payload = {};
  const partnerImageUrl = "https://example.com/hot-park.jpg";
  const result = await sendMainOffer(env, {
    ...offer,
    imageUrl: "",
    cardImageUrl: "",
    partnerImageUrl,
  }, async (url, init) => {
    assert.match(url, /sendPhoto$/);
    payload = JSON.parse(init.body);
    return jsonResponse({ message_id: 46 });
  });
  assert.equal(payload.photo, partnerImageUrl);
  assert.equal(result.messageKind, "photo");
  assert.equal(result.imageStrategy, "remote_url");
});

test("reutiliza file_id antes de qualquer URL e devolve a identidade da foto", async () => {
  let payload;
  const result = await sendMainOffer(env, {
    ...offer,
    telegramPhotoFileId: "cached-file-id",
  }, async (_url, init) => {
    payload = JSON.parse(init.body);
    return jsonResponse({
      message_id: 44,
      photo: [
        { file_id: "small", file_unique_id: "same-photo" },
        { file_id: "largest", file_unique_id: "same-photo" },
      ],
    });
  });
  assert.equal(payload.photo, "cached-file-id");
  assert.equal(result.imageStrategy, "file_id");
  assert.equal(result.photoFileId, "largest");
  assert.equal(result.photoFileUniqueId, "same-photo");
  assert.deepEqual(result.imageAttempts, [{ strategy: "file_id", ok: true, error: "" }]);
});

test("pula estratégia remota aberta e tenta upload diretamente", async () => {
  const calls = [];
  const result = await sendMainOffer(env, {
    ...offer,
    imageStrategies: { remote_url: false },
  }, async (url, init = {}) => {
    calls.push(url);
    if (url === offer.imageUrl) {
      return new Response(new Uint8Array([1, 2, 3]), {
        status: 200,
        headers: { "Content-Type": "image/png" },
      });
    }
    assert.ok(init.body instanceof FormData);
    return jsonResponse({ message_id: 45 });
  });
  assert.deepEqual(calls, [offer.imageUrl, "https://api.telegram.org/bot123456:test-token/sendPhoto"]);
  assert.equal(result.imageStrategy, "upload");
});

test("usa texto quando o Telegram rejeita a imagem", async () => {
  const methods = [];
  let textPayload = {};
  const result = await sendMainOffer(env, offer, async (url, init = {}) => {
    methods.push(url.split("/").pop());
    if (url.endsWith("/sendPhoto")) {
      return new Response(JSON.stringify({ ok: false, description: "bad photo" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }
    if (url.endsWith("/sendMessage")) textPayload = JSON.parse(init.body);
    return jsonResponse({ message_id: 42 });
  });
  assert.deepEqual(methods, ["sendPhoto", "show.jpg", "sendMessage"]);
  assert.equal(result.messageKind, "text");
  assert.equal(result.messageId, 42);
  assert.deepEqual(textPayload.link_preview_options, {
    is_disabled: false,
    url: offer.link,
    prefer_small_media: true,
    show_above_text: true,
  });
});

test("faz upload da imagem quando o Telegram recusa a URL pública", async () => {
  const calls = [];
  const result = await sendMainOffer(env, offer, async (url, init = {}) => {
    calls.push({ url, body: init.body });
    if (url === offer.imageUrl) {
      return new Response(new Uint8Array([1, 2, 3]), {
        status: 200,
        headers: { "Content-Type": "image/png", "Content-Length": "3" },
      });
    }
    if (url.endsWith("/sendPhoto") && typeof init.body === "string") {
      return new Response(JSON.stringify({ ok: false, description: "bad photo URL" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }
    assert.ok(init.body instanceof FormData);
    return jsonResponse({ message_id: 43 });
  });
  assert.equal(calls.length, 3);
  assert.equal(result.messageKind, "photo");
  assert.equal(result.messageId, 43);
  assert.equal(calls[2].body.get("photo").name, "oferta.png");
});

test("corrige MIME PNG incorreto quando os bytes são JPEG", async () => {
  let uploadedPhoto;
  const jpegBytes = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10]);
  const result = await sendMainOffer(env, offer, async (url, init = {}) => {
    if (url === offer.imageUrl) {
      return new Response(jpegBytes, {
        status: 200,
        headers: { "Content-Type": "image/png" },
      });
    }
    if (url.endsWith("/sendPhoto") && typeof init.body === "string") {
      return new Response(JSON.stringify({ ok: false, description: "bad photo URL" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }
    uploadedPhoto = init.body.get("photo");
    return jsonResponse({ message_id: 47 });
  });
  assert.equal(result.messageKind, "photo");
  assert.equal(uploadedPhoto.name, "oferta.jpg");
  assert.equal(uploadedPhoto.type, "image/jpeg");
});

test("encaminha para o canal 2 somente após ter a mensagem principal", async () => {
  let method = "";
  let payload = {};
  const result = await forwardToCanal2(env, 41, async (url, init) => {
    method = url.split("/").pop();
    payload = JSON.parse(init.body);
    return jsonResponse({ message_id: 99 });
  });
  assert.equal(result.messageId, 99);
  assert.equal(method, "copyMessage");
  assert.equal(payload.from_chat_id, env.TELEGRAM_CHAT_ID);
  assert.equal(payload.chat_id, env.CANAL2_ID);
  assert.equal(payload.message_id, 41);
  assert.equal(payload.disable_notification, false);
});

test("edita foto esgotada no canal correto", async () => {
  let method = "";
  let payload = {};
  await editSoldOutMessage(env, {
    chatId: env.CANAL2_ID,
    messageId: 99,
    messageKind: "photo",
    offer,
    soldOutAt: "2026-07-30T12:34:00Z",
  }, async (url, init) => {
    method = url.split("/").pop();
    payload = JSON.parse(init.body);
    return jsonResponse({ message_id: 99 });
  });
  assert.equal(method, "editMessageCaption");
  assert.equal(payload.chat_id, env.CANAL2_ID);
  assert.equal(payload.message_id, 99);
  assert.match(payload.caption, /\[ESGOTADO\]/);
});

test("avisa esgotamento respondendo ao encaminhamento do canal 2", async () => {
  let payload = {};
  const result = await sendSoldOutNotice(env, {
    chatId: env.CANAL2_ID,
    replyToMessageId: 99,
    offer,
  }, async (url, init) => {
    assert.match(url, /sendMessage$/);
    payload = JSON.parse(init.body);
    return jsonResponse({ message_id: 100 });
  });
  assert.equal(result.messageId, 100);
  assert.equal(payload.chat_id, env.CANAL2_ID);
  assert.equal(payload.reply_parameters.message_id, 99);
  assert.equal(payload.reply_parameters.allow_sending_without_reply, true);
  assert.equal(payload.disable_notification, false);
  assert.match(payload.text, /^🚫 \[ESGOTADO\]/);
  assert.equal(payload.link_preview_options.is_disabled, true);
});

test("completa a legenda urgente depois do enriquecimento", async () => {
  let method = "";
  let payload = {};
  await editMainOfferMessage({ ...env, GRUPO_COMENTARIO_ID: "-1003802235343" }, {
    messageId: 41,
    messageKind: "photo",
    offer: {
      ...offer,
      validity: "Benefício válido até 02/08/2026 23:59.",
    },
  }, async (url, init) => {
    method = url.split("/").pop();
    payload = JSON.parse(init.body);
    return jsonResponse({ message_id: 41 });
  });
  assert.equal(method, "editMessageCaption");
  assert.match(payload.caption, /02\/08\/2026/);
  assert.match(payload.caption, /detalhes completos nos comentários/);
});

test("teste de transporte confirma principal e canal 2", async () => {
  const methods = [];
  let mainPayload = {};
  const result = await sendTransportTest(env, async (url, init) => {
    const method = url.split("/").pop();
    methods.push(method);
    if (method === "sendMessage") mainPayload = JSON.parse(init.body);
    return jsonResponse({
      message_id: method === "sendMessage" ? 501 : 502,
    });
  });
  assert.deepEqual(methods, ["sendMessage", "copyMessage"]);
  assert.deepEqual(result, {
    mainMessageId: 501,
    canal2MessageId: 502,
  });
  assert.equal(mainPayload.link_preview_options.prefer_small_media, true);
  assert.equal(mainPayload.link_preview_options.show_above_text, true);
});

test("envia detalhe como resposta à discussão automática", async () => {
  let payload = {};
  const result = await sendDiscussionComment(
    { ...env, GRUPO_COMENTARIO_ID: "-1003802235343" },
    "📋 <b>Oferta</b>",
    778,
    async (_url, init) => {
      payload = JSON.parse(init.body);
      return jsonResponse({ message_id: 779 });
    },
  );
  assert.equal(result.messageId, 779);
  assert.equal(payload.chat_id, "-1003802235343");
  assert.equal(payload.reply_parameters.message_id, 778);
  assert.equal(payload.disable_notification, false);
});

test("atualiza comentário existente quando o detalhe é recuperado", async () => {
  let payload = {};
  const result = await editDiscussionComment(
    { ...env, GRUPO_COMENTARIO_ID: "-1003802235343" },
    "📋 <b>Oferta completa</b>",
    779,
    async (url, init) => {
      assert.match(url, /editMessageText$/);
      payload = JSON.parse(init.body);
      return jsonResponse({ message_id: 779 });
    },
  );
  assert.equal(result.messageId, 779);
  assert.equal(payload.chat_id, "-1003802235343");
  assert.equal(payload.message_id, 779);
  assert.equal(payload.link_preview_options.is_disabled, true);
});

test("registra webhook preservando atualizações pendentes", async () => {
  let payload = {};
  await registerTelegramWebhook({
    ...env,
    PUBLIC_BASE_URL: "https://worker.example",
    TELEGRAM_WEBHOOK_SECRET: "secret-value",
  }, async (_url, init) => {
    payload = JSON.parse(init.body);
    return jsonResponse(true);
  });
  assert.equal(payload.url, "https://worker.example/telegram-webhook");
  assert.equal(payload.secret_token, "secret-value");
  assert.equal(payload.drop_pending_updates, false);
  assert.deepEqual(payload.allowed_updates, ["message"]);
});

test("consulta o estado seguro do webhook", async () => {
  const result = await getTelegramWebhookInfo(env, async () => jsonResponse({
    url: "https://worker.example/telegram-webhook",
    pending_update_count: 2,
    last_error_date: 123,
    last_error_message: "Wrong response from the webhook: 401 Unauthorized",
    allowed_updates: ["message"],
  }));
  assert.deepEqual(result, {
    url: "https://worker.example/telegram-webhook",
    pendingUpdateCount: 2,
    lastErrorDate: 123,
    lastErrorMessage: "Wrong response from the webhook: 401 Unauthorized",
    allowedUpdates: ["message"],
  });
});
