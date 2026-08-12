import test from "node:test";
import assert from "node:assert/strict";

import {
  buildApiSnapshotFingerprint,
  buildApiHealthSnapshot,
  buildDedupeKeys,
  buildDiscussionCommentChunks,
  buildTelegramCaption,
  canonicalKey,
  decideShadowDelivery,
  dedupeCards,
  evaluateDetailQuality,
  extractValidity,
  estimateDailyRowWrites,
  formatOfferDuration,
  maintenanceRetryAt,
  normalizeTicketProbeAt,
  normalizeOfferId,
  observationFreshnessMinutes,
  offerIdentityKeys,
  offerSourceKey,
  parseRuntimeSnapshot,
  parseValidityWindow,
  rollingReadEstimate,
  shouldTouchObservation,
  shouldPersistRunSummary,
  shouldReconcileHtmlSnapshot,
  storageReadBudget,
  slugTailVariants,
  shouldSendToCanal2,
} from "../src/core.js";

test("fingerprint da API é estável, ordenado e ignora observação", async () => {
  const cards = [
    {
      id: "b",
      link: "https://clube.uol.com.br/b/pb",
      title: "Oferta B",
      previewTitle: "Oferta B",
      description: "Detalhe B",
      observedAt: "2026-08-05T12:00:00.000Z",
    },
    {
      id: "a",
      link: "https://clube.uol.com.br/a/pa",
      title: "Oferta A",
      previewTitle: "Oferta A",
      description: "Detalhe A",
      observedAt: "2026-08-05T12:00:01.000Z",
    },
  ];
  const reordered = [
    { ...cards[1], observedAt: "2026-08-05T13:00:00.000Z" },
    { ...cards[0], observedAt: "2026-08-05T13:00:01.000Z" },
  ];
  assert.equal(
    await buildApiSnapshotFingerprint(cards),
    await buildApiSnapshotFingerprint(reordered),
  );
  assert.notEqual(
    await buildApiSnapshotFingerprint(cards),
    await buildApiSnapshotFingerprint([
      { ...cards[0], description: "Detalhe A alterado" },
      cards[1],
    ]),
  );
  assert.equal(typeof await buildApiSnapshotFingerprint([]), "string");
});

test("fingerprint completa invalida qualquer campo relevante da listagem HTML", async () => {
  const card = {
    id: "oferta-1",
    link: "https://clube.uol.com.br/beneficios/oferta-1",
    previewTitle: "Oferta 1",
    title: "Oferta completa",
    category: "gastronomia",
    cardImageUrl: "https://img.example/card.jpg",
    partnerImageUrl: "https://img.example/partner.jpg",
    partnerName: "Parceiro",
    imageUrl: "https://img.example/detail.jpg",
    validity: "31/08/2026",
    description: "Descrição",
  };
  const original = await buildApiSnapshotFingerprint([card]);
  for (const field of [
    "link", "previewTitle", "title", "category", "cardImageUrl",
    "partnerImageUrl", "partnerName", "imageUrl", "validity", "description",
  ]) {
    assert.notEqual(
      await buildApiSnapshotFingerprint([{ ...card, [field]: `${card[field]} alterado` }]),
      original,
      `campo não invalidou fingerprint: ${field}`,
    );
  }
});

test("reconciliação HTML só pula fotografia completa, idêntica e ainda fresca", () => {
  const base = {
    fingerprint: "a".repeat(64),
    previousFingerprint: "a".repeat(64),
    lastReconciledAt: "2026-08-10T20:00:00.000Z",
    now: new Date("2026-08-10T20:14:59.000Z"),
    refreshIntervalSeconds: 900,
    complete: true,
    initialized: true,
  };
  assert.deepEqual(shouldReconcileHtmlSnapshot(base), {
    reconcile: false,
    reason: "unchanged_fresh",
  });
  for (const override of [
    { complete: false, reason: "incomplete" },
    { initialized: false, reason: "uninitialized" },
    { previousFingerprint: "", reason: "missing_fingerprint" },
    { previousFingerprint: "invalid", reason: "invalid_fingerprint" },
    { previousFingerprint: "b".repeat(64), reason: "changed" },
    { lastReconciledAt: "", reason: "missing_reconciled_at" },
    {
      lastReconciledAt: "2026-08-10T20:15:00.000Z",
      reason: "invalid_reconciled_at",
    },
    { now: new Date("2026-08-10T20:15:00.000Z"), reason: "periodic_refresh" },
  ]) {
    const { reason, ...values } = override;
    assert.deepEqual(shouldReconcileHtmlSnapshot({ ...base, ...values }), {
      reconcile: true,
      reason,
    });
  }
});

test("timestamp de probe inválido falha seguro para execução imediata", () => {
  const fallback = new Date("2026-08-10T20:00:00.000Z");
  assert.equal(
    normalizeTicketProbeAt("2026-08-10T20:01:02.345Z", fallback),
    "2026-08-10T20:01:02.345Z",
  );
  assert.equal(normalizeTicketProbeAt("invalid", fallback), fallback.toISOString());
  assert.equal(normalizeTicketProbeAt("9999", fallback), fallback.toISOString());
  assert.equal(normalizeTicketProbeAt("", fallback), fallback.toISOString());
});

test("snapshot de saúde da API preserva cards válidos fora das observações SQL", () => {
  assert.deepEqual(buildApiHealthSnapshot([
    {
      id: "b",
      link: " https://clube.uol.com.br/campanhasdeingresso/pb ",
      previewTitle: "  Ingresso B  ",
      category: "campanhasdeingresso",
      observedAt: "2026-08-06T02:00:00.000Z",
    },
    {
      id: "a",
      link: "https://clube.uol.com.br/campanhasdeingresso/pa",
      apiDetail: { previewTitle: "Ingresso A" },
      category: "campanhasdeingresso",
    },
    { id: "sem-link" },
  ]), [
    {
      id: "a",
      link: "https://clube.uol.com.br/campanhasdeingresso/pa",
      previewTitle: "Ingresso A",
      category: "campanhasdeingresso",
    },
    {
      id: "b",
      link: "https://clube.uol.com.br/campanhasdeingresso/pb",
      previewTitle: "Ingresso B",
      category: "campanhasdeingresso",
    },
  ]);
});

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

test("reserva leituras críticas e corta manutenção antes do limite diário", () => {
  const healthy = storageReadBudget({
    rowsRead: 250_000,
    primaryEstimatedRowsRead: 64,
    now: new Date("2026-08-03T12:00:00.000Z"),
  });
  assert.equal(healthy.limit, 5_000_000);
  assert.equal(healthy.criticalReserve, 1_000_000);
  assert.equal(healthy.maintenanceAllowed, true);

  const protectedState = storageReadBudget({
    rowsRead: 4_000_001,
    primaryEstimatedRowsRead: 64,
    now: new Date("2026-08-03T12:00:00.000Z"),
  });
  assert.equal(protectedState.maintenanceAllowed, false);
  assert.equal(protectedState.withinFreeTier, true);
  assert.ok(protectedState.remaining >= protectedState.criticalReserve - 1);
});

test("estimativa sustentada aumenta reserva do polling restante", () => {
  const budget = storageReadBudget({
    rowsRead: 100_000,
    primaryEstimatedRowsRead: 500,
    now: new Date("2026-08-03T00:00:00.000Z"),
  });
  assert.equal(budget.remainingPrimaryScans, 5_760);
  assert.equal(budget.criticalReserve, 4_320_000);
  assert.equal(budget.maintenanceAllowed, true);
  assert.equal(budget.maintenanceCeiling, 680_000);
  assert.equal(budget.recommendedPollIntervalSeconds, 15);
});

test("polling desacelera quando o custo crítico medido cresce", () => {
  const budget = storageReadBudget({
    rowsRead: 100_000,
    primaryEstimatedRowsRead: 1_000,
    now: new Date("2026-08-03T00:00:00.000Z"),
  });
  assert.equal(budget.recommendedPollIntervalSeconds, 27);
  assert.equal(budget.maintenanceAllowed, false);
});

test("polling pausa antes de ultrapassar o limite e aponta o reset UTC", () => {
  const budget = storageReadBudget({
    rowsRead: 4_999_950,
    primaryEstimatedRowsRead: 64,
    now: new Date("2026-08-03T23:59:00.000Z"),
  });
  assert.equal(budget.primaryAllowed, false);
  assert.equal(budget.affordablePrimaryScans, 0);
  assert.equal(budget.resetAt, "2026-08-04T00:00:00.000Z");
});

test("estimativa móvel reage a custo sustentado sem eternizar pico isolado", () => {
  assert.equal(rollingReadEstimate(100, 3_400), 1_750);
  assert.equal(rollingReadEstimate(1_750, 100), 1_585);
  assert.equal(rollingReadEstimate(0, 88), 88);
  assert.equal(rollingReadEstimate(0, 3_400), 512);
});

test("backoff da manutenção cresce, limita e respeita o reset UTC", () => {
  const now = new Date("2026-08-05T12:00:00.000Z");
  assert.equal(
    maintenanceRetryAt({
      now,
      resetAt: "2026-08-06T00:00:00.000Z",
      skipped: 0,
    }),
    "2026-08-05T12:01:00.000Z",
  );
  assert.equal(
    maintenanceRetryAt({
      now,
      resetAt: "2026-08-06T00:00:00.000Z",
      skipped: 8,
    }),
    "2026-08-05T12:15:00.000Z",
  );
  assert.equal(
    maintenanceRetryAt({
      now: new Date("2026-08-05T23:59:45.000Z"),
      resetAt: "2026-08-06T00:00:00.000Z",
      skipped: 0,
    }),
    "2026-08-06T00:00:01.000Z",
  );
  assert.equal(
    maintenanceRetryAt({
      now,
      resetAt: "2026-08-06T00:00:00.000Z",
      skipped: 0,
      deferUntilReset: true,
    }),
    "2026-08-06T00:00:01.000Z",
  );
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
  firstSeenAt: "2026-07-30T12:28:00Z",
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

test("canal 2 recebe toda campanha de ingresso, incluindo teatro e esporte", () => {
  for (const title of [
    "2 ingressos Teatro Claro",
    "Ingressos para stand-up",
    "Ingressos para jogo de futebol",
  ]) {
    const offer = { ...showOffer, title, previewTitle: title };
    assert.equal(shouldSendToCanal2(offer), true);
    assert.equal(decideShadowDelivery(offer).wouldSendMain, true);
    assert.equal(decideShadowDelivery(offer).wouldSendCanal2, true);
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
  const caption = buildTelegramCaption(showOffer, { soldOutAt: "2026-07-30T12:34:00Z" });
  assert.match(caption, /\[ESGOTADO\]/);
  assert.match(caption, /<s>/);
  assert.match(
    caption,
    /📸 Oferta capturada às 09:28\.\n\n❌ Oferta esgotada às 09:34\.\n\n⏱️ Ficou no ar por 6 min\./,
  );
  assert.match(caption, /https:\/\/clube\.uol\.com\.br/);
});

test("formata duração curta, longa e inválida sem bloquear a edição", () => {
  assert.equal(
    formatOfferDuration("2026-07-30T12:00:00Z", "2026-07-30T12:00:20Z"),
    "menos de 1 min",
  );
  assert.equal(
    formatOfferDuration("2026-07-30T12:00:00Z", "2026-07-30T13:12:00Z"),
    "1h 12min",
  );
  assert.equal(
    formatOfferDuration("2026-07-30T12:00:00Z", "2026-08-01T14:00:00Z"),
    "2d 2h",
  );
  assert.equal(formatOfferDuration("invalido", "2026-07-30T12:00:00Z"), "");
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
