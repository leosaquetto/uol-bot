import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDedupeKeys,
  decideShadowDelivery,
  dedupeCards,
  evaluateDetailQuality,
  extractValidity,
  normalizeOfferId,
  parseValidityWindow,
  shouldSendToCanal2,
} from "../src/core.js";

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

test("deduplica cards repetidos da listagem", () => {
  const cards = dedupeCards([
    { ...showOffer, title: showOffer.title },
    { ...showOffer, title: "duplicado" },
  ]);
  assert.equal(cards.length, 1);
  assert.equal(cards[0].previewTitle, showOffer.title);
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
