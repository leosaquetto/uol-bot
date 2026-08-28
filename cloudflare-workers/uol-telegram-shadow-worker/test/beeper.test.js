import assert from "node:assert/strict";
import test from "node:test";

import {
  beeperDeliveryOfferIds,
  beeperDeliveryIdempotencyKey,
  beeperDestinationKey,
  beeperGatewayConfiguration,
  beeperGatewayReadinessUrl,
  buildBeeperOfferText,
  probeBeeperGateway,
  sendBeeperOffer,
} from "../src/beeper.js";

const offer = {
  id: "offer-1",
  title: "Ingresso Clube UOL",
  link: "https://clube.uol.com.br/campanhasdeingresso/offer-1",
  description: "Dois ingressos para o show.",
  imageUrl: "https://ddrxgn8ucibei.cloudfront.net/beneficios/offer.jpg",
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
    destinationKey: "",
    offerIds: [],
    filterActive: false,
  });
  assert.equal(beeperGatewayConfiguration({ BEEPER_DELIVERY_ENABLED: "true" }).ready, false);
});

test("valida filtro de recuperação e deriva probe autenticado sem envio", async () => {
  const env = {
    BEEPER_DELIVERY_ENABLED: "true",
    BEEPER_DESTINATION_KEY: "whatsapp-group",
    BEEPER_DELIVERY_OFFER_IDS: "offer-b,offer-a,offer-a",
    BEEPER_GATEWAY_URL: "https://beeper.example/v1/send-offer",
    BEEPER_GATEWAY_TOKEN: "secret",
  };
  assert.deepEqual(beeperDeliveryOfferIds(env), ["offer-a", "offer-b"]);
  assert.equal(beeperGatewayReadinessUrl(env), "https://beeper.example/v1/readyz");
  assert.throws(
    () => beeperGatewayReadinessUrl({
      BEEPER_GATEWAY_URL: "https://beeper.example/wrong-path",
    }),
    /beeper_gateway_url_invalid/,
  );
  assert.throws(
    () => beeperGatewayReadinessUrl({
      BEEPER_GATEWAY_URL: "https://beeper.example/v1/send-offer?token=bad",
    }),
    /beeper_gateway_url_invalid/,
  );
  assert.throws(
    () => beeperDeliveryOfferIds({ BEEPER_DELIVERY_OFFER_IDS: "offer ok" }),
    /beeper_delivery_offer_ids_invalid/,
  );
  const result = await probeBeeperGateway(env, async (url, init) => {
    assert.equal(url, "https://beeper.example/v1/readyz");
    assert.equal(init.method, "GET");
    assert.equal(init.headers.Authorization, "Bearer secret");
    return Response.json({
      ok: true,
      deliveryConfirmation: "confirmed_by_whatsapp_bridge",
    });
  });
  assert.equal(result.ok, true);
  assert.equal(result.status, 200);
  assert.equal(result.code, "ready");

  const staleGateway = await probeBeeperGateway(env, async () => Response.json({
    ok: true,
    deliveryConfirmation: "accepted_by_beeper_transport",
  }));
  assert.equal(staleGateway.ok, false);
  assert.equal(staleGateway.status, 200);
  assert.equal(staleGateway.code, "delivery_contract_mismatch");
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
      imageUrl: offer.imageUrl,
    });
    return new Response(JSON.stringify({
      pendingMessageID: "pending-1",
      deliveryState: "confirmed_by_whatsapp_bridge",
    }), {
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

test("mantém delivery_pending retryável e conflito definitivo terminal", async () => {
  const baseEnv = {
    BEEPER_DESTINATION_KEY: "whatsapp-group",
    BEEPER_GATEWAY_URL: "https://beeper.example/v1/send-offer",
    BEEPER_GATEWAY_TOKEN: "secret",
  };
  await assert.rejects(
    sendBeeperOffer(baseEnv, offer, { idempotencyKey: "uol:offer-1:v1" }, async () =>
      Response.json({ code: "delivery_pending" }, { status: 409 })),
    (error) => error.retryable === true && error.ambiguous === false,
  );
  await assert.rejects(
    sendBeeperOffer(baseEnv, offer, { idempotencyKey: "uol:offer-1:v1" }, async () =>
      Response.json({ code: "idempotency_conflict" }, { status: 409 })),
    (error) => error.retryable === false && error.ambiguous === false,
  );
});

test("resposta aceita sem pendingMessageID permanece ambígua", async () => {
  await assert.rejects(
    sendBeeperOffer({
      BEEPER_DESTINATION_KEY: "whatsapp-group",
      BEEPER_GATEWAY_URL: "https://beeper.example/v1/send-offer",
      BEEPER_GATEWAY_TOKEN: "secret",
    }, offer, { idempotencyKey: "uol:offer-1:v1" }, async () =>
      Response.json({ accepted: true }, { status: 202 })),
    (error) => error.transport === "beeper" && error.ambiguous === true,
  );
});

test("rejeita confirmação sintética do transporte privado", async () => {
  await assert.rejects(
    sendBeeperOffer({
      BEEPER_DESTINATION_KEY: "whatsapp-group",
      BEEPER_GATEWAY_URL: "https://beeper.example/v1/send-offer",
      BEEPER_GATEWAY_TOKEN: "secret",
    }, offer, { idempotencyKey: "uol:offer-1:v1" }, async () =>
      Response.json({
        pendingMessageID: "synthetic-pending",
        deliveryState: "accepted_by_beeper_transport",
      }, { status: 202 })),
    (error) => error.transport === "beeper" && error.ambiguous === true,
  );
});
