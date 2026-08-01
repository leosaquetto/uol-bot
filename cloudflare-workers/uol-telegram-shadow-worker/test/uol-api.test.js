import test from "node:test";
import assert from "node:assert/strict";

import {
  fetchTicketOffersFromApi,
  mapTicketApiItem,
  mapTicketApiPayload,
  mergeOfferCards,
  ticketApiConfiguration,
} from "../src/uol-api.js";

const apiItem = {
  id: "59931",
  titulo: "2 INGRESSOS: 02/08 Shopping Cidade SP",
  descricao: "Assinante UOL, resgate um par de ingressos para a exposição.",
  inicio: "2026-07-31 11:25:00",
  fim: "2026-08-02 23:59:00",
  url: "/campanhasdeingresso/pAD-2-ingressos-02-08-shopping-cidade-sp",
  imagem: "https://cdn.example.com/beneficios/oferta.png",
  parceiro: JSON.stringify({
    titulo: "Campanhas de ingressos",
    imagem: "https://cdn.example.com/parceiros/uol.png",
  }),
};

test("mapeia ingresso da API para a mesma identidade da URL pública", () => {
  const card = mapTicketApiItem(apiItem);
  assert.equal(card.id, "pad-2-ingressos-02-08-shopping-cidade-sp");
  assert.equal(
    card.link,
    "https://clube.uol.com.br/campanhasdeingresso/pAD-2-ingressos-02-08-shopping-cidade-sp",
  );
  assert.equal(card.category, "campanhasdeingresso");
  assert.equal(card.apiDetail.validity, "Benefício válido de 31/07/2026 11:25 até 02/08/2026 23:59.");
  assert.equal(card.apiDetail.imageUrl, apiItem.imagem);
});

test("deduplica a resposta e preserva o detalhe rico da API", () => {
  const cards = mapTicketApiPayload({ beneficios: [apiItem, apiItem] });
  assert.equal(cards.length, 1);
  assert.match(cards[0].apiDetail.description, /resgate um par/);
});

test("API ganha do HTML ao consolidar a mesma oferta", () => {
  const apiCard = mapTicketApiItem(apiItem);
  const htmlCard = {
    ...apiCard,
    previewTitle: "Título do HTML",
    cardImageUrl: "https://public.example.com/beneficios/oferta.png",
    apiDetail: undefined,
  };
  const merged = mergeOfferCards([apiCard], [htmlCard]);
  assert.equal(merged.length, 1);
  assert.ok(merged[0].apiDetail);
  assert.equal(merged[0].previewTitle, apiCard.previewTitle);
  assert.equal(merged[0].cardImageUrl, htmlCard.cardImageUrl);
});

test("envia os dois cabeçalhos e restringe a categoria a ingressos", async () => {
  let request;
  const cards = await fetchTicketOffersFromApi({
    UOL_API_AUTHORIZATION: "api-token",
    UOL_OAUTH_AUTHORIZATION: "Bearer personal-token",
  }, async (url, init) => {
    request = { url: new URL(url), init };
    return new Response(JSON.stringify({ beneficios: [apiItem] }), {
      headers: { "content-type": "application/json" },
    });
  });
  assert.equal(cards.length, 1);
  assert.equal(request.url.searchParams.get("category_id"), "162");
  assert.equal(request.init.headers.Authorization, "Bearer api-token");
  assert.equal(request.init.headers["X-Authorization"], "Bearer personal-token");
});

test("status de configuração nunca expõe tokens", () => {
  assert.deepEqual(ticketApiConfiguration({
    UOL_API_AUTHORIZATION: "segredo-1",
    UOL_OAUTH_AUTHORIZATION: "segredo-2",
  }), { configured: true });
});
