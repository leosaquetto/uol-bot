import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDiscordPayload,
  cacheDiscordOfferImage,
  discordConfiguration,
  getDiscordMessageImageProxy,
  sendDiscordOffer,
  sendDiscordOperationsAlert,
} from "../src/discord.js";

const offer = {
  title: "2 INGRESSOS: João Rock",
  link: "https://clube.uol.com.br/campanhasdeingresso/pAC-2-joao-rock",
  cardImageUrl: "https://example.com/thumb.jpg",
};

test("mantém o formato aprovado do Discord com thumbnail", () => {
  const payload = buildDiscordPayload(offer);
  assert.deepEqual(payload.allowed_mentions, { parse: [] });
  assert.equal(payload.embeds[0].title, offer.title);
  assert.equal(payload.embeds[0].url, offer.link);
  assert.equal(payload.embeds[0].image.url, offer.cardImageUrl);
  assert.equal(payload.content, `🚨 **${offer.title}**`);
});

test("envia pelo webhook consolidado e confirma o ID", async () => {
  const env = { DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/123/token" };
  assert.equal(discordConfiguration(env).configured, true);
  let payload = {};
  const result = await sendDiscordOffer(env, offer, async (url, init) => {
    assert.match(url, /wait=true/);
    payload = JSON.parse(init.body);
    return new Response(JSON.stringify({
      id: "discord-1",
      embeds: [{ image: { proxy_url: "https://media.discordapp.net/proxy.jpg" } }],
    }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  });
  assert.equal(result.messageId, "discord-1");
  assert.equal(result.imageProxyUrl, "https://media.discordapp.net/proxy.jpg");
  assert.equal(payload.embeds[0].image.url, offer.cardImageUrl);
});

test("recupera a imagem já cacheada pelo Discord", async () => {
  const env = { DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/123/token" };
  const proxyUrl = await getDiscordMessageImageProxy(env, "discord-1", async (url) => {
    assert.equal(url, "https://discord.com/api/webhooks/123/token/messages/discord-1");
    return new Response(JSON.stringify({
      embeds: [{ image: { proxy_url: "https://media.discordapp.net/proxy.jpg" } }],
    }), { headers: { "Content-Type": "application/json" } });
  });
  assert.equal(proxyUrl, "https://media.discordapp.net/proxy.jpg");
});

test("canal privado cacheia imagem de oferta comum sem publicar no canal de ingressos", async () => {
  const env = {
    DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/tickets/token",
    DISCORD_IMAGE_CACHE_WEBHOOK_URL: "https://discord.com/api/webhooks/cache/token",
  };
  assert.equal(discordConfiguration(env).imageCacheConfigured, true);
  const result = await cacheDiscordOfferImage(env, offer, async (url, init) => {
    assert.match(url, /webhooks\/cache\/token\?wait=true$/);
    const payload = JSON.parse(init.body);
    assert.equal(payload.embeds[0].image.url, offer.cardImageUrl);
    return new Response(JSON.stringify({
      id: "cache-1",
      embeds: [{ image: { proxy_url: "https://media.discordapp.net/cache.jpg" } }],
    }), { headers: { "Content-Type": "application/json" } });
  });
  assert.deepEqual(result, {
    messageId: "cache-1",
    imageProxyUrl: "https://media.discordapp.net/cache.jpg",
  });
});

test("consulta proxy no webhook privado quando solicitado", async () => {
  const proxy = await getDiscordMessageImageProxy(
    { DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/tickets/token" },
    "cache-1",
    async (url) => {
      assert.equal(url, "https://discord.com/api/webhooks/cache/token/messages/cache-1");
      return new Response(JSON.stringify({
        embeds: [{ image: { proxy_url: "https://media.discordapp.net/private.jpg" } }],
      }), { headers: { "Content-Type": "application/json" } });
    },
    "https://discord.com/api/webhooks/cache/token",
  );
  assert.equal(proxy, "https://media.discordapp.net/private.jpg");
});

test("preserva status e retry_after de erro do Discord", async () => {
  const env = { DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/123/token" };
  await assert.rejects(
    sendDiscordOffer(env, offer, async () => new Response(JSON.stringify({
      message: "You are being rate limited.",
      retry_after: 2.5,
    }), {
      status: 429,
      headers: { "Content-Type": "application/json" },
    })),
    (error) => {
      assert.equal(error.transport, "discord");
      assert.equal(error.operation, "sendWebhook");
      assert.equal(error.status, 429);
      assert.equal(error.retryAfterSeconds, 2.5);
      assert.equal(error.retry_after, 2.5);
      assert.equal(error.retryable, true);
      assert.equal(error.ambiguous, false);
      return true;
    },
  );
});

test("classifica timeout do webhook Discord como entrega ambígua", async () => {
  const env = { DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/123/token" };
  await assert.rejects(
    sendDiscordOffer(env, offer, async () => {
      throw new DOMException("The operation timed out", "TimeoutError");
    }),
    (error) => {
      assert.equal(error.code, "discord_sendWebhook_timeout_ambiguous");
      assert.equal(error.category, "timeout");
      assert.equal(error.ambiguous, true);
      assert.equal(error.retryable, true);
      return true;
    },
  );
});

test("classifica resposta Discord aceita sem ID como ambígua", async () => {
  const env = { DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/123/token" };
  await assert.rejects(
    sendDiscordOffer(env, offer, async () => new Response("{}", {
      status: 200,
      headers: { "Content-Type": "application/json" },
    })),
    (error) => {
      assert.equal(error.code, "discord_sendWebhook_response_ambiguous");
      assert.equal(error.httpStatus, 200);
      assert.equal(error.ambiguous, true);
      return true;
    },
  );
});

test("envia alerta operacional por webhook independente", async () => {
  let body;
  const result = await sendDiscordOperationsAlert({
    DISCORD_OPS_WEBHOOK_URL: "https://discord.com/api/webhooks/ops/token",
  }, "Falha crítica", async (url, init) => {
    assert.equal(url, "https://discord.com/api/webhooks/ops/token");
    body = JSON.parse(init.body);
    return new Response(null, { status: 204 });
  });
  assert.equal(result.ok, true);
  assert.equal(body.content, "Falha crítica");
  assert.deepEqual(body.allowed_mentions, { parse: [] });
});
