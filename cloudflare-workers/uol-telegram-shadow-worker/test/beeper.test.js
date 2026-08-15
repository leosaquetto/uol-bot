import assert from "node:assert/strict";
import test from "node:test";

import {
  beeperDeliveryIdempotencyKey,
  beeperDestinationKey,
  beeperGatewayConfiguration,
  buildBeeperOfferText,
  sendBeeperOffer,
} from "../src/beeper.js";

const offer = {
  id: "offer-1",
  title: "Ingresso Clube UOL",
  link: "https://clube.uol.com.br/campanhasdeingresso/offer-1",
  description: "Dois ingressos para o show.",
  discordImageProxyUrl: "https://media.discordapp.net/proxy.jpg",
};

test("separa a idempotência por destino Beeper", () => {
  assert.equal(
    beeperDestinationKey({ BEEPER_DESTINATION_KEY: " WhatsApp-Personal " }),
    "whatsapp-personal",
  );
  assert.equal(
    beeperDeliveryIdempotencyKey("offer-1", "whatsapp-group"),
    "uol:offer-1:whatsapp-group:v1",
  );
  assert.throws(() => beeperDestinationKey({}), /beeper_destination_key_invalid/);
});

test("Beeper fica opt-in e exige URL e token", () => {
  assert.deepEqual(beeperGatewayConfiguration({}), {
    enabled: false,
    configured: false,
    ready: true,
    url: "",
  });
  assert.equal(beeperGatewayConfiguration({ BEEPER_DELIVERY_ENABLED: "true" }).ready, false);
});

test("monta texto curto com URL para o preview nativo", () => {
  assert.equal(
    buildBeeperOfferText(offer),
    "🚨 Ingresso Clube UOL\nhttps://clube.uol.com.br/campanhasdeingresso/offer-1",
  );
});

test("envia ao gateway autenticado e idempotente", async () => {
  const env = {
    BEEPER_DELIVERY_ENABLED: "true",
    BEEPER_DESTINATION_KEY: "whatsapp-personal",
    BEEPER_GATEWAY_URL: "https://beeper.example/v1/send-offer",
    BEEPER_GATEWAY_TOKEN: "secret",
  };
  const result = await sendBeeperOffer(env, offer, {
    idempotencyKey: "uol:offer-1:v1",
  }, async (url, init) => {
    assert.equal(url, env.BEEPER_GATEWAY_URL);
    assert.equal(init.headers.Authorization, "Bearer secret");
    assert.equal(init.headers["Idempotency-Key"], "uol:offer-1:v1");
    const body = JSON.parse(init.body);
    assert.equal(body.link, offer.link);
    assert.match(body.text, /Ingresso Clube UOL/);
    assert.deepEqual(body.preview, {
      title: offer.title,
      summary: offer.description,
      imageUrl: offer.discordImageProxyUrl,
    });
    return new Response(JSON.stringify({ pendingMessageID: "pending-1" }), {
      status: 202,
      headers: { "Content-Type": "application/json" },
    });
  });
  assert.deepEqual(result, { pendingMessageId: "pending-1", replayed: false });
});

test("preserva entrega interna ambígua sem duplicar", async () => {
  await assert.rejects(
    sendBeeperOffer({
      BEEPER_DESTINATION_KEY: "whatsapp-personal",
      BEEPER_GATEWAY_URL: "https://beeper.example/v1/send-offer",
      BEEPER_GATEWAY_TOKEN: "secret",
    }, offer, { idempotencyKey: "uol:offer-1:v1" }, async () =>
      new Response(JSON.stringify({ code: "delivery_unknown" }), {
        status: 409,
        headers: { "Content-Type": "application/json" },
      })),
    (error) => error.transport === "beeper" && error.ambiguous === true,
  );
});
