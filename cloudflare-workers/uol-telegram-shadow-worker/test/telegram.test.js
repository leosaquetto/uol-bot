import test from "node:test";
import assert from "node:assert/strict";

import {
  editMainOfferMedia,
  editSoldOutMessage,
  editMainOfferMessage,
  forwardToCanal2,
  sendMainOffer,
  sendOperationsAlert,
  sendDiscussionComment,
  telegramConfiguration,
  registerTelegramWebhook,
  getTelegramWebhookInfo,
  telegramCall,
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
    mainReady: true,
    canal2Ready: true,
    operationsConfigured: true,
    operationsUsesMainFallback: true,
    liveReady: true,
  });
  assert.deepEqual(
    telegramConfiguration({ ...env, CANAL2_ID: "" }),
    {
      tokenConfigured: true,
      mainConfigured: true,
      canal2Configured: false,
      mainReady: true,
      canal2Ready: false,
      operationsConfigured: true,
      operationsUsesMainFallback: true,
      liveReady: false,
    },
  );
});

test("preserva status e retry_after da API do Telegram", async () => {
  await assert.rejects(
    telegramCall(env, "sendMessage", { chat_id: env.TELEGRAM_CHAT_ID }, async () =>
      new Response(JSON.stringify({
        ok: false,
        error_code: 429,
        description: "Too Many Requests",
        parameters: { retry_after: 17 },
      }), {
        status: 429,
        headers: { "Content-Type": "application/json" },
      })),
    (error) => {
      assert.equal(error.transport, "telegram");
      assert.equal(error.operation, "sendMessage");
      assert.equal(error.status, 429);
      assert.equal(error.httpStatus, 429);
      assert.equal(error.retryAfterSeconds, 17);
      assert.equal(error.retry_after, 17);
      assert.equal(error.retryable, true);
      assert.equal(error.ambiguous, false);
      return true;
    },
  );
});

test("classifica timeout de envio como ambíguo e não tenta fallback duplicado", async () => {
  let calls = 0;
  await assert.rejects(
    sendMainOffer(env, offer, async () => {
      calls += 1;
      throw new DOMException("The operation timed out", "TimeoutError");
    }),
    (error) => {
      assert.equal(error.code, "telegram_sendPhoto_timeout_ambiguous");
      assert.equal(error.category, "timeout");
      assert.equal(error.ambiguous, true);
      assert.equal(error.retryable, true);
      return true;
    },
  );
  assert.equal(calls, 1);
});

test("timeout em consulta Telegram não é classificado como entrega ambígua", async () => {
  await assert.rejects(
    telegramCall(env, "getWebhookInfo", {}, async () => {
      throw new DOMException("The operation timed out", "TimeoutError");
    }),
    (error) => {
      assert.equal(error.code, "telegram_getWebhookInfo_timeout");
      assert.equal(error.ambiguous, false);
      return true;
    },
  );
});

test("não faz fallback quando uma resposta Telegram de sucesso é ilegível", async () => {
  let calls = 0;
  await assert.rejects(
    sendMainOffer(env, offer, async () => {
      calls += 1;
      return new Response("not-json", { status: 200 });
    }),
    (error) => {
      assert.equal(error.code, "telegram_sendPhoto_response_ambiguous");
      assert.equal(error.httpStatus, 200);
      assert.equal(error.ambiguous, true);
      return true;
    },
  );
  assert.equal(calls, 1);
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

test("limita tentativa de imagem ao prazo absoluto restante", async () => {
  let requestSignal;
  const imageDeadlineAt = new Date(Date.now() + 30).toISOString();
  await sendMainOffer(env, { ...offer, imageDeadlineAt }, async (_url, init) => {
    requestSignal = init.signal;
    return jsonResponse({ message_id: 52 });
  });
  await new Promise((resolve) => setTimeout(resolve, 60));
  assert.equal(requestSignal.aborted, true);
});

test("adia texto enquanto prazo de imagem ainda está aberto", async () => {
  const calls = [];
  const result = await sendMainOffer(env, {
    ...offer,
    imageUrl: "",
    cardImageUrl: "",
    partnerImageUrl: "",
    deferTextFallback: true,
  }, async (url, init) => {
    calls.push(url);
    return jsonResponse({ message_id: 48 });
  });

  assert.deepEqual(calls, []);
  assert.equal(result.deferred, true);
  assert.equal(result.messageId, 0);
  assert.equal(result.messageKind, "");
  assert.equal(result.imageStrategy, "pending_image");
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

test("edita texto tardio para foto com a legenda completa no mesmo message_id", async () => {
  let requestUrl = "";
  let payload = {};
  const result = await editMainOfferMedia(env, {
    messageId: 48,
    offer,
  }, async (url, init) => {
    requestUrl = url;
    payload = JSON.parse(init.body);
    return jsonResponse({
      message_id: 48,
      photo: [{ file_id: "late-photo", file_unique_id: "late-photo-unique" }],
    });
  });

  assert.match(requestUrl, /editMessageMedia$/);
  assert.equal(payload.chat_id, env.TELEGRAM_CHAT_ID);
  assert.equal(payload.message_id, 48);
  assert.equal(payload.media.type, "photo");
  assert.equal(payload.media.media, offer.imageUrl);
  assert.match(payload.media.caption, /2 ingressos: Show Teste/);
  assert.equal(payload.media.parse_mode, "HTML");
  assert.equal(result.messageId, 48);
  assert.equal(result.messageKind, "photo");
  assert.equal(result.imageStrategy, "remote_url_edit");
  assert.equal(result.photoFileId, "late-photo");
});

test("faz upload ao editar quando Telegram rejeita URL tardia", async () => {
  const calls = [];
  const result = await editMainOfferMedia(env, {
    messageId: 49,
    offer,
  }, async (url, init = {}) => {
    calls.push({ url, body: init.body });
    if (url === offer.imageUrl) {
      return new Response(new Uint8Array([0x89, 0x50, 0x4e, 0x47]), {
        status: 200,
        headers: { "Content-Type": "image/png" },
      });
    }
    if (url.endsWith("/editMessageMedia") && typeof init.body === "string") {
      return new Response(JSON.stringify({ ok: false, description: "bad photo URL" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }
    assert.ok(init.body instanceof FormData);
    const media = JSON.parse(init.body.get("media"));
    assert.equal(media.type, "photo");
    assert.equal(media.media, "attach://photo");
    assert.match(media.caption, /2 ingressos: Show Teste/);
    assert.equal(init.body.get("photo").name, "oferta.png");
    return jsonResponse({
      message_id: 49,
      photo: [{ file_id: "uploaded-late-photo", file_unique_id: "late-unique" }],
    });
  });

  assert.equal(calls.length, 3);
  assert.equal(result.messageId, 49);
  assert.equal(result.imageStrategy, "upload_edit");
  assert.equal(result.photoFileId, "uploaded-late-photo");
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
  assert.deepEqual(textPayload.link_preview_options, { is_disabled: true });
  assert.equal(result.imageStrategy, "text_timeout");
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

test("completa a legenda urgente depois do enriquecimento", async () => {
  let method = "";
  let payload = {};
  await editMainOfferMessage({ ...env, GRUPO_COMENTARIO_ID: "-1000000000001" }, {
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

test("edição de texto vencido continua sem preview", async () => {
  let payload = {};
  await editMainOfferMessage(env, {
    messageId: 42,
    messageKind: "text",
    offer,
  }, async (_url, init) => {
    payload = JSON.parse(init.body);
    return jsonResponse({ message_id: 42 });
  });
  assert.deepEqual(payload.link_preview_options, { is_disabled: true });
});

test("envia detalhe como resposta à discussão automática", async () => {
  let payload = {};
  const result = await sendDiscussionComment(
    { ...env, GRUPO_COMENTARIO_ID: "-1000000000001" },
    "📋 <b>Oferta</b>",
    778,
    async (_url, init) => {
      payload = JSON.parse(init.body);
      return jsonResponse({ message_id: 779 });
    },
  );
  assert.equal(result.messageId, 779);
  assert.equal(payload.chat_id, "-1000000000001");
  assert.equal(payload.reply_parameters.message_id, 778);
  assert.equal(payload.disable_notification, false);
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
