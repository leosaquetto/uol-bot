import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDiscordPayload,
  cleanText,
  collectorEnabled,
  dedupeOffers,
  isTicketCampaignLink,
  normalizeOfferId,
  offerSourceKey,
  pruneStateOffers,
  reconcileStateOffers,
  runCollector,
} from "../src/worker.js";

test("coletor de rollback exige ativação explícita", () => {
  assert.equal(collectorEnabled({}), false);
  assert.equal(collectorEnabled({ COLLECTOR_ENABLED: "false" }), false);
  assert.equal(collectorEnabled({ COLLECTOR_ENABLED: "true" }), true);
});

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
  assert.equal(offerSourceKey(ticketA.link), "campanhasdeingresso|pa4");
});

test("deduplica correções do slug pela origem estável", () => {
  const malformed = {
    ...ticketA,
    link: "https://clube.uol.com.br/campanhasdeingresso/pAC-2-ribeirao-preto-sp-joao-rock",
  };
  const corrected = {
    ...ticketA,
    link: "https://clube.uol.com.br/campanhasdeingresso/pAC-2-ribeiro-preto-sp-joao-rock",
  };
  assert.equal(dedupeOffers([malformed, corrected]).length, 1);
  assert.equal(offerSourceKey(malformed.link), offerSourceKey(corrected.link));
});

test("reconcilia aliases antigos preservando a entrega mais antiga", () => {
  const reconciled = reconcileStateOffers({
    antigo: {
      id: "pac-2-ribeirao",
      link: "https://clube.uol.com.br/campanhasdeingresso/pAC-2-ribeirao-preto-sp-joao-rock",
      status: "sent",
      firstSeenAt: "2026-07-31T13:40:00.000Z",
      discordMessageId: "first",
    },
    corrigido: {
      id: "pac-2-ribeiro",
      link: "https://clube.uol.com.br/campanhasdeingresso/pAC-2-ribeiro-preto-sp-joao-rock",
      status: "sent",
      firstSeenAt: "2026-07-31T13:42:00.000Z",
      discordMessageId: "duplicate",
    },
  });
  assert.equal(Object.keys(reconciled).length, 1);
  assert.equal(Object.values(reconciled)[0].discordMessageId, "first");
});

test("não reenvia quando a mesma origem reaparece com slug corrigido", async () => {
  const initialized = JSON.stringify({
    version: 1,
    initializedAt: "2026-07-29T00:00:00.000Z",
    updatedAt: "2026-07-29T00:00:00.000Z",
    offers: {
      antigo: {
        id: "pac-2-ribeirao",
        link: "https://clube.uol.com.br/campanhasdeingresso/pAC-2-ribeirao-preto-sp-joao-rock",
        title: "João Rock",
        status: "sent",
        firstSeenAt: "2026-07-31T13:40:00.000Z",
        sentAt: "2026-07-31T13:40:02.000Z",
      },
    },
  });
  const kv = memoryKv(initialized);
  const corrected = dedupeOffers([{
    ...ticketA,
    link: "https://clube.uol.com.br/campanhasdeingresso/pAC-2-ribeiro-preto-sp-joao-rock",
    title: "João Rock corrigido",
  }]);
  const result = await runCollector(
    { UOL_TICKETS_STATE: kv, DELIVERY_MODE: "live", MAX_STATE_OFFERS: "200" },
    {
      offers: corrected,
      sendEnabled: true,
      fetchImpl: async () => { throw new Error("não deveria reenviar"); },
    },
  );
  assert.equal(result.newOffers, 0);
  assert.equal(result.sent, 0);
  assert.equal(Object.keys(kv.snapshot().offers).length, 1);
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
