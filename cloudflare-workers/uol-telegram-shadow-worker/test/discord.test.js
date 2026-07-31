import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDiscordPayload,
  discordConfiguration,
  sendDiscordOffer,
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
});

test("envia pelo webhook consolidado e confirma o ID", async () => {
  const env = { DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/123/token" };
  assert.equal(discordConfiguration(env).configured, true);
  let payload = {};
  const result = await sendDiscordOffer(env, offer, async (url, init) => {
    assert.match(url, /wait=true/);
    payload = JSON.parse(init.body);
    return new Response(JSON.stringify({ id: "discord-1" }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  });
  assert.equal(result.messageId, "discord-1");
  assert.equal(payload.embeds[0].image.url, offer.cardImageUrl);
});
