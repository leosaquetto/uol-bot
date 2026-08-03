import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDedupeKeys,
  buildDiscussionCommentChunks,
  buildTelegramCaption,
  canonicalKey,
  decideShadowDelivery,
  dedupeCards,
  evaluateDetailQuality,
  extractValidity,
  estimateDailyRowWrites,
  normalizeOfferId,
  observationFreshnessMinutes,
  offerIdentityKeys,
  offerSourceKey,
  parseRuntimeSnapshot,
  parseValidityWindow,
  shouldTouchObservation,
  shouldPersistRunSummary,
  slugTailVariants,
  shouldSendToCanal2,
} from "../src/core.js";

test("snapshot de runtime aceita objeto e falha fechado para JSON inválido", () => {
  assert.deepEqual(parseRuntimeSnapshot('{"lastOffersSeen":12}'), { lastOffersSeen: 12 });
  assert.deepEqual(parseRuntimeSnapshot("[1,2]"), {});
  assert.deepEqual(parseRuntimeSnapshot("invalido"), {});
});

test("observação só toca armazenamento após janela configurada", () => {
  const observedAt = "2026-08-03T12:15:00.000Z";
  assert.equal(shouldTouchObservation("", observedAt, 15), true);
  assert.equal(
    shouldTouchObservation("2026-08-03T12:05:01.000Z", observedAt, 15),
    false,
  );
  assert.equal(
    shouldTouchObservation("2026-08-03T12:00:00.000Z", observedAt, 15),
    true,
  );
});

test("saúde mantém observação válida durante a janela de toque", () => {
  assert.equal(observationFreshnessMinutes(15), 20);
  assert.equal(observationFreshnessMinutes(1), 6);
});

test("cadência rápida mantém orçamento estável abaixo do plano gratuito", () => {
  const budget = estimateDailyRowWrites({
    pollIntervalSeconds: 15,
    maintenanceIntervalSeconds: 60,
    htmlIntervalSeconds: 60,
    observationTouchMinutes: 15,
    offerTouchMinutes: 15,
    apiCards: 48,
    listingCards: 48,
  });

  assert.equal(budget.limit, 100_000);
  assert.equal(budget.withinFreeTier, true);
  assert.ok(budget.projected < 75_000);
  assert.ok(budget.headroom > 25_000);
});

test("histórico grava eventos e só amostra ciclos sem mudança", () => {
  const now = "2026-08-03T12:15:00.000Z";
  assert.equal(shouldPersistRunSummary({ outcome: "telegram_delivered" }, null, now), true);
  assert.equal(shouldPersistRunSummary(
    { outcome: "no_change" },
    { outcome: "failed", finishedAt: "2026-08-03T12:14:30.000Z" },
    now,
  ), true);
  assert.equal(shouldPersistRunSummary(
    { outcome: "no_change" },
    { outcome: "no_change", finishedAt: "2026-08-03T12:05:01.000Z" },
    now,
  ), false);
  assert.equal(shouldPersistRunSummary(
    { outcome: "no_change" },
    { outcome: "no_change", finishedAt: "2026-08-03T12:00:00.000Z" },
    now,
  ), true);
});

const showOffer = {
  link: "https://clube.uol.com.br/campanhasdeingresso/pA4-2-ingressos-show-sp",
  title: "2 INGRESSOS: Show em São Paulo",
  previewTitle: "2 INGRESSOS: Show em São Paulo",
  category: "Ingressos Exclusivos",
  description: "Assinante UOL, resgate um par de ingressos para o show.",
};

test("normaliza a identidade a partir do slug público", () => {
  assert.equal(normalizeOfferId(showOffer.link), "pa4-2-ingressos-show-sp");
});

test("porta as correções canônicas do consumidor legado", () => {
  assert.equal(canonicalKey("SeleÃ§Ã£o grÃ¡tis"), "selecao-gratis");
  assert.equal(canonicalKey("seleÃ§Ã£o grÃ¡tis"), "selecao-gratis");
  assert.equal(canonicalKey("seleção grtis"), "selecao-gratis");
  assert.equal(canonicalKey("Pós-graduação até 70%"), "pos-graduacao-ate-70");
  assert.deepEqual(
    slugTailVariants("https://clube.uol.com.br/parceiro/p01-show-de-joao"),
    ["p01-show-de-joao", "p01-show-de-joo", "p01-show-joao"],
  );
});

test("mantém identidade estável quando o texto acentuado do slug muda", () => {
  const malformed = "https://clube.uol.com.br/evino/p70-r-30-off-frete-grtis-acima-de-r-199";
  const corrected = "https://clube.uol.com.br/evino/p70-r-30-off-frete-gratis-acima-de-r-199";
  assert.equal(normalizeOfferId(malformed), normalizeOfferId(corrected));
  assert.equal(offerSourceKey(malformed), "evino|p70");
  assert.equal(offerSourceKey(malformed), offerSourceKey(corrected));
  assert.ok(
    offerIdentityKeys(malformed).some((key) => offerIdentityKeys(corrected).includes(key)),
  );
});

test("identidade parceiro mais código cobre letras perdidas fora das correções conhecidas", () => {
  const malformed = "https://clube.uol.com.br/dominos/p7T-40-off-no-cardpio-de-pizzas-delivery";
  const corrected = "https://clube.uol.com.br/dominos/p7T-40-off-no-cardapio-de-pizzas-delivery";
  assert.notEqual(normalizeOfferId(malformed), normalizeOfferId(corrected));
  assert.equal(offerSourceKey(malformed), offerSourceKey(corrected));
});

test("deduplica cards repetidos da listagem", () => {
  const cards = dedupeCards([
    { ...showOffer, title: showOffer.title },
    { ...showOffer, title: "duplicado" },
  ]);
  assert.equal(cards.length, 1);
  assert.equal(cards[0].previewTitle, showOffer.title);
});

test("deduplica duas variantes do mesmo parceiro e código na listagem", () => {
  const cards = dedupeCards([
    {
      link: "https://clube.uol.com.br/centauro/p8f-tnis-de-racket-15-off",
      title: "Tênis de Racket + 15% OFF",
    },
    {
      link: "https://clube.uol.com.br/centauro/p8f-tenis-de-racket-15-off",
      title: "Tênis de Racket + 15% OFF",
    },
  ]);
  assert.equal(cards.length, 1);
});

test("canal principal recebe toda oferta elegível e canal 2 recebe show", () => {
  const decision = decideShadowDelivery(showOffer, {
    now: new Date("2026-07-30T12:00:00Z"),
  });
  assert.equal(decision.wouldSendMain, true);
  assert.equal(decision.wouldSendCanal2, true);
});

test("canal 2 bloqueia teatro, stand-up e esporte sem bloquear o canal principal", () => {
  for (const title of [
    "2 ingressos Teatro Claro",
    "Ingressos para stand-up",
    "Ingressos para jogo de futebol",
  ]) {
    const offer = { ...showOffer, title, previewTitle: title };
    assert.equal(shouldSendToCanal2(offer), false);
    assert.equal(decideShadowDelivery(offer).wouldSendMain, true);
  }
});

test("benefício comum não entra no canal 2", () => {
  const common = {
    ...showOffer,
    link: "https://clube.uol.com.br/nike/desconto",
    category: "Moda",
  };
  assert.equal(shouldSendToCanal2(common), false);
});

test("extrai e interpreta a janela de validade", () => {
  const validity = extractValidity(
    "Benefício válido de 29/07/2026 11:12 até 12/08/2026 23:59. Regras.",
  );
  const window = parseValidityWindow(validity);
  assert.match(validity, /29\/07\/2026/);
  assert.equal(window.start?.toISOString(), "2026-07-29T14:12:00.000Z");
  assert.equal(window.end?.toISOString(), "2026-08-13T02:59:00.000Z");
});

test("interpreta uma única data após até como fim da validade", () => {
  const window = parseValidityWindow("Benefício válido até 01/08/2026.");
  assert.equal(window.start, null);
  assert.equal(window.end?.toISOString(), "2026-08-02T02:59:59.999Z");

  const timedWindow = parseValidityWindow("Benefício válido até 01/08/2026 12:30.");
  assert.equal(timedWindow.start, null);
  assert.equal(timedWindow.end?.toISOString(), "2026-08-01T15:30:00.000Z");
});

test("não inventa início quando o texto contém apenas limites finais", () => {
  const window = parseValidityWindow(
    "Válido até 01/08/2026 para o primeiro lote e até 03/08/2026 para o segundo.",
  );
  assert.equal(window.start, null);
  assert.equal(window.end?.toISOString(), "2026-08-04T02:59:59.999Z");
});

test("usa início e fim do dia de São Paulo quando o intervalo não informa hora", () => {
  const window = parseValidityWindow("Benefício válido de 01/08/2026 até 03/08/2026.");
  assert.equal(window.start?.toISOString(), "2026-08-01T03:00:00.000Z");
  assert.equal(window.end?.toISOString(), "2026-08-04T02:59:59.999Z");
});

test("descarta uma validade até já encerrada em vez de tratá-la como início recente", () => {
  const decision = decideShadowDelivery({
    ...showOffer,
    validity: "Válido até 01/08/2026 23:59.",
  }, {
    now: new Date("2026-08-02T12:00:00Z"),
  });
  assert.equal(decision.eligible, false);
  assert.equal(decision.discardReason, "validade_expirada");
});

test("rejeita datas de validade inexistentes", () => {
  const window = parseValidityWindow("Benefício válido de 31/02/2026 até 32/03/2026.");
  assert.equal(window.start, null);
  assert.equal(window.end, null);
});

test("descarta oferta cuja validade terminou", () => {
  const decision = decideShadowDelivery({
    ...showOffer,
    validity: "Benefício válido de 01/07/2026 10:00 até 02/07/2026 23:59.",
  }, {
    now: new Date("2026-07-30T12:00:00Z"),
  });
  assert.equal(decision.eligible, false);
  assert.equal(decision.discardReason, "validade_expirada");
});

test("classifica detalhe completo e gera dedupe estável", async () => {
  const detail = {
    ...showOffer,
    validity: "Benefício válido de 29/07/2026 11:12 até 12/08/2026 23:59.",
    description: "Descrição suficientemente completa para representar as regras e o benefício divulgado pelo Clube UOL.",
    imageUrl: "https://example.com/oferta.png",
  };
  assert.equal(evaluateDetailQuality(detail), "complete");
  assert.deepEqual(await buildDedupeKeys(detail), await buildDedupeKeys({ ...detail }));
});

test("gera legenda Telegram segura e destaca ingressos", () => {
  const caption = buildTelegramCaption({
    ...showOffer,
    title: "Show <Especial> & Convidados",
    validity: "Benefício válido até 31/08/2026 23:59",
  });
  assert.match(caption, /‼️ Show &lt;Especial&gt; &amp; Convidados ‼️/);
  assert.match(caption, /#campanhasdeingresso/);
  assert.match(caption, /31\/08\/2026/);
  assert.match(caption, /https:\/\/clube\.uol\.com\.br/);
});

test("preserva endereço com abreviação pontuada", () => {
  const caption = buildTelegramCaption({
    ...showOffer,
    description: "Data: 02 de Agosto. Local: Av. Infante Dom Henrique, S/N - Glória, Rio de Janeiro - RJ Importante: sujeito a estoque.",
  });
  assert.match(caption, /📍 Av\. Infante Dom Henrique, S\/N - Glória, Rio de Janeiro - RJ/);
  assert.doesNotMatch(caption, /📍 Av(?:\s|$)/);
});

test("gera edição de esgotamento sem perder o link", () => {
  const caption = buildTelegramCaption(showOffer, {
    soldOutAt: "2026-07-30T12:34:00Z",
  });
  assert.match(caption, /\[ESGOTADO\]/);
  assert.match(caption, /<s>/);
  assert.match(caption, /Oferta esgotada às 09:34/);
  assert.match(caption, /https:\/\/clube\.uol\.com\.br/);
});

test("formata e divide o histórico completo para a discussão", () => {
  const chunks = buildDiscussionCommentChunks({
    ...showOffer,
    description: "Assinante UOL, resgate seu par. Data: 02 de Agosto. Local: Av. Paulista, 1000 - SP. Importante: sujeito a estoque. REGRAS DE RESGATE: Uma oferta por CPF.",
    validity: "Benefício válido até 02/08/2026 23:59.",
  });
  assert.ok(chunks.length >= 1);
  assert.match(chunks.join("\n"), /📋 <b>2 INGRESSOS/);
  assert.match(chunks.join("\n"), /📍 <b>Local:<\/b>/);
  assert.match(chunks.join("\n"), /📌 <b>REGRAS DE RESGATE:<\/b>/);
  assert.match(chunks.at(-1), /clube\.uol\.com\.br/);
  assert.ok(chunks.every((chunk) => chunk.length <= 3_800));
});
