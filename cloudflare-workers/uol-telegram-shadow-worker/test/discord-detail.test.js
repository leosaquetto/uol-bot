import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDiscordDetail,
  fetchDiscordOfferDetail,
  mergeDiscordOfferDetail,
} from "../src/discord-detail.js";

const offer = {
  title: "20% off em produtos selecionados",
  previewTitle: "20% off em produtos selecionados",
  link: "https://clube.uol.com.br/beneficios/pbt-20-desconto",
  category: "beneficios",
  cardImageUrl: "https://example.com/thumb.jpg",
};

test("normaliza o detalhe público para o card do Discord", () => {
  const detail = buildDiscordDetail({
    title: "20% off em produtos selecionados",
    description: "Benefício exclusivo para assinantes com desconto em produtos selecionados. Regras: válido para uma compra por CPF.",
    validityText: "Benefício válido de 04/08/2026 12:00 até 04/08/2028 23:59.",
    fallbackTitle: offer.previewTitle,
  });

  assert.deepEqual(detail, {
    title: "20% off em produtos selecionados",
    validity: "Benefício válido de 04/08/2026 12:00 até 04/08/2028 23:59.",
    description: "Benefício exclusivo para assinantes com desconto em produtos selecionados. Regras: válido para uma compra por CPF.",
  });
});

test("descarta detalhe vazio ou curto demais", () => {
  assert.equal(buildDiscordDetail({ description: "curto" }), null);
  assert.equal(buildDiscordDetail({ description: "", metaDescription: "" }), null);
});

test("faz merge somente do texto e preserva thumbnail/metadados do card", () => {
  const merged = mergeDiscordOfferDetail(offer, {
    title: "Oferta detalhada",
    validity: "Válido até 31/08/2026",
    description: "Descrição completa da oferta comum para exibição no card do Discord.",
  });

  assert.deepEqual(merged, {
    ...offer,
    title: "Oferta detalhada",
    validity: "Válido até 31/08/2026",
    description: "Descrição completa da oferta comum para exibição no card do Discord.",
  });
});

test("não bloqueia quando a página não é HTML ou redireciona", async () => {
  let requestUrl = "";
  const nonHtml = await fetchDiscordOfferDetail(offer, async (url, init) => {
    requestUrl = url;
    assert.equal(init.headers.Accept, "text/html,application/xhtml+xml");
    assert.equal(init.headers["Cache-Control"], "no-cache, no-store, max-age=0");
    return new Response("{}", {
      headers: { "Content-Type": "application/json" },
    });
  });
  assert.equal(nonHtml, null);
  assert.match(requestUrl, /_uol_discord_detail_ts=/);

  const redirected = new Response("<html><body>redirect</body></html>", {
    headers: { "Content-Type": "text/html" },
  });
  Object.defineProperty(redirected, "url", {
    value: "https://clube.uol.com.br/beneficios/pbz-outra-oferta",
  });
  assert.equal(
    await fetchDiscordOfferDetail(offer, async () => redirected),
    null,
  );
});
