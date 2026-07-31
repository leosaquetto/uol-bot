import test from "node:test";
import assert from "node:assert/strict";

import {
  editSoldOutMessage,
  editMainOfferMessage,
  forwardToCanal2,
  sendMainOffer,
  sendDiscussionComment,
  sendTransportTest,
  telegramConfiguration,
  registerTelegramWebhook,
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

test("configuração live exige token e os dois canais", () => {
  assert.deepEqual(telegramConfiguration(env), {
    tokenConfigured: true,
    mainConfigured: true,
    canal2Configured: true,
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

test("usa texto quando o Telegram rejeita a imagem", async () => {
  const methods = [];
  const result = await sendMainOffer(env, offer, async (url) => {
    methods.push(url.split("/").pop());
    if (url.endsWith("/sendPhoto")) {
      return new Response(JSON.stringify({ ok: false, description: "bad photo" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }
    return jsonResponse({ message_id: 42 });
  });
  assert.deepEqual(methods, ["sendPhoto", "show.jpg", "sendMessage"]);
  assert.equal(result.messageKind, "text");
  assert.equal(result.messageId, 42);
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

test("encaminha para o canal 2 somente após ter a mensagem principal", async () => {
  let payload = {};
  const result = await forwardToCanal2(env, 41, async (_url, init) => {
    payload = JSON.parse(init.body);
    return jsonResponse({ message_id: 99 });
  });
  assert.equal(result.messageId, 99);
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
  const result = await sendTransportTest(env, async (url) => {
    const method = url.split("/").pop();
    methods.push(method);
    return jsonResponse({
      message_id: method === "sendMessage" ? 501 : 502,
    });
  });
  assert.deepEqual(methods, ["sendMessage", "forwardMessage"]);
  assert.deepEqual(result, {
    mainMessageId: 501,
    canal2MessageId: 502,
  });
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

test("registra webhook descartando atualizações antigas", async () => {
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
  assert.equal(payload.drop_pending_updates, true);
  assert.deepEqual(payload.allowed_updates, ["message"]);
});
