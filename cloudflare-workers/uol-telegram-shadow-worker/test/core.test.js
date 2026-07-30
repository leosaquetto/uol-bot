import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDedupeKeys,
  buildTelegramCaption,
  canonicalKey,
  decideShadowDelivery,
  dedupeCards,
  evaluateDetailQuality,
  extractValidity,
  normalizeOfferId,
  offerIdentityKeys,
  offerSourceKey,
  parseValidityWindow,
  slugTailVariants,
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

test("gera edição de esgotamento sem perder o link", () => {
  const caption = buildTelegramCaption(showOffer, {
    soldOutAt: "2026-07-30T12:34:00Z",
  });
  assert.match(caption, /\[ESGOTADO\]/);
  assert.match(caption, /<s>/);
  assert.match(caption, /Oferta esgotada às 09:34/);
  assert.match(caption, /https:\/\/clube\.uol\.com\.br/);
});
