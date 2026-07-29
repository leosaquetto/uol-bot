import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDiscordPayload,
  cleanText,
  dedupeOffers,
  isTicketCampaignLink,
  normalizeOfferId,
  pruneStateOffers,
  runCollector,
} from "../src/worker.js";

function memoryKv(initial = null) {
  let value = initial;
  let puts = 0;
  return {
    async get(_key, type) {
      if (value == null) return null;
      return type === "json" ? JSON.parse(value) : value;
    },
    async put(_key, next) {
      value = next;
      puts += 1;
    },
    snapshot() {
      return value ? JSON.parse(value) : null;
    },
    puts() {
      return puts;
    },
  };
}

const ticketA = {
  link: "https://clube.uol.com.br/campanhasdeingresso/pA4-2-ingressos-show-sp",
  title: "  2 INGRESSOS: Show SP  ",
  category: "Ingressos Exclusivos",
  imageUrl: "https://example.com/show.png",
};

test("normaliza texto e ID estável", () => {
  assert.equal(cleanText("  Olá \n mundo  "), "Olá mundo");
  assert.equal(normalizeOfferId(ticketA.link), "pa4-2-ingressos-show-sp");
});

test("aceita somente campanhas de ingresso do Clube UOL", () => {
  assert.equal(isTicketCampaignLink(ticketA.link), true);
  assert.equal(isTicketCampaignLink("https://clube.uol.com.br/loja/cupom"), false);
  assert.equal(isTicketCampaignLink("https://example.com/campanhasdeingresso/x"), false);
});

test("deduplica ingressos e ignora benefícios comuns", () => {
  const offers = dedupeOffers([
    ticketA,
    { ...ticketA, title: "duplicado" },
    { link: "https://clube.uol.com.br/loja/cupom", title: "Cupom" },
  ]);
  assert.equal(offers.length, 1);
  assert.equal(offers[0].title, "2 INGRESSOS: Show SP");
});

test("payload do Discord não permite mentions e contém link direto", () => {
  const payload = buildDiscordPayload(dedupeOffers([ticketA])[0]);
  assert.deepEqual(payload.allowed_mentions, { parse: [] });
  assert.equal(payload.embeds[0].url, ticketA.link);
  assert.equal(payload.embeds[0].image.url, ticketA.imageUrl);
});

test("primeira rodada cria baseline sem enviar", async () => {
  const kv = memoryKv();
  const result = await runCollector(
    { UOL_TICKETS_STATE: kv, DELIVERY_MODE: "live", MAX_STATE_OFFERS: "200" },
    {
      offers: dedupeOffers([ticketA]),
      sendEnabled: true,
      fetchImpl: async () => {
        throw new Error("não deveria enviar");
      },
    },
  );
  assert.equal(result.outcome, "baseline_created");
  assert.equal(result.sent, 0);
  assert.equal(kv.snapshot().offers["pa4-2-ingressos-show-sp"].status, "baseline");
});

test("dry-run registra nova oferta como pendente sem webhook", async () => {
  const initialized = JSON.stringify({
    version: 1,
    initializedAt: "2026-07-29T00:00:00.000Z",
    updatedAt: "2026-07-29T00:00:00.000Z",
    offers: {},
  });
  const kv = memoryKv(initialized);
  const result = await runCollector(
    { UOL_TICKETS_STATE: kv, DELIVERY_MODE: "dry-run", MAX_STATE_OFFERS: "200" },
    { offers: dedupeOffers([ticketA]), sendEnabled: false },
  );
  assert.equal(result.outcome, "new_offers_dry_run");
  assert.equal(result.newOffers, 1);
  assert.equal(kv.snapshot().offers["pa4-2-ingressos-show-sp"].status, "pending");
});

test("modo live envia pendente e persiste ID da mensagem", async () => {
  const initialized = JSON.stringify({
    version: 1,
    initializedAt: "2026-07-29T00:00:00.000Z",
    updatedAt: "2026-07-29T00:00:00.000Z",
    offers: {},
  });
  const kv = memoryKv(initialized);
  const fetchImpl = async (_url, init) => {
    assert.equal(init.method, "POST");
    return new Response(JSON.stringify({ id: "discord-message-1" }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };
  const result = await runCollector(
    {
      UOL_TICKETS_STATE: kv,
      DELIVERY_MODE: "live",
      DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/123/token",
      MAX_STATE_OFFERS: "200",
    },
    { offers: dedupeOffers([ticketA]), sendEnabled: true, fetchImpl },
  );
  assert.equal(result.sent, 1);
  assert.equal(result.failed, 0);
  assert.equal(kv.snapshot().offers["pa4-2-ingressos-show-sp"].status, "sent");
  assert.equal(kv.snapshot().offers["pa4-2-ingressos-show-sp"].discordMessageId, "discord-message-1");
});

test("poda mantém entradas mais recentes", () => {
  const pruned = pruneStateOffers({
    old: { firstSeenAt: "2026-01-01T00:00:00Z" },
    newest: { firstSeenAt: "2026-07-29T00:00:00Z" },
  }, 1);
  assert.deepEqual(Object.keys(pruned), ["newest"]);
});
