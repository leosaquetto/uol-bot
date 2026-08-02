import test from "node:test";
import assert from "node:assert/strict";

import {
  fetchOffersFromApi,
  fetchTicketOffersFromApi,
  mapTicketApiItem,
  mapTicketApiPayload,
  mergeOfferCards,
  probeCouponAuthentication,
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

test("mescla duas listagens já normalizadas sem descartar os demais cards", () => {
  const ticket = {
    id: "pa1-ingresso",
    link: "https://clube.uol.com.br/campanhasdeingresso/pA1-ingresso",
    previewTitle: "2 INGRESSOS",
    category: "campanhasdeingresso",
    cardImageUrl: "https://img.example/ticket.jpg",
  };
  const common = {
    id: "pb2-beneficio",
    link: "https://clube.uol.com.br/parceiro/pB2-beneficio",
    previewTitle: "20% OFF",
    category: "descontos",
    cardImageUrl: "https://img.example/common.jpg",
  };
  assert.deepEqual(
    mergeOfferCards([ticket], [ticket, common]).map((card) => card.id),
    [ticket.id, common.id],
  );
});

test("consulta ingressos somente com a credencial técnica", async () => {
  let request;
  const cards = await fetchTicketOffersFromApi({
    UOL_API_AUTHORIZATION: "api-token",
    UOL_OAUTH_AUTHORIZATION: "personal-token-ignored",
  }, async (url, init) => {
    request = { url: new URL(url), init };
    return new Response(JSON.stringify({ beneficios: [apiItem] }), {
      headers: { "content-type": "application/json" },
    });
  });
  assert.equal(cards.length, 1);
  assert.equal(request.url.searchParams.get("category_id"), "162");
  assert.equal(request.init.headers.Authorization, "Bearer api-token");
  assert.equal(request.init.headers["X-Authorization"], undefined);
});

test("consulta a API geral sob demanda e preserva a categoria da URL", async () => {
  let requestedUrl;
  const commonItem = {
    ...apiItem,
    titulo: "36% OFF na caixa de trufas",
    url: "/cacaushow/p8u-36-off-na-caixa-de-trufas",
  };
  const cards = await fetchOffersFromApi({
    UOL_API_AUTHORIZATION: "api-token",
    UOL_OAUTH_AUTHORIZATION: "personal-token",
  }, async (url) => {
    requestedUrl = new URL(url);
    return new Response(JSON.stringify({ beneficios: [commonItem] }), {
      headers: { "content-type": "application/json" },
    });
  });
  assert.equal(requestedUrl.searchParams.has("category_id"), false);
  assert.equal(cards[0].category, "cacaushow");
  assert.match(cards[0].apiDetail.description, /resgate um par/);
});

test("status de configuração nunca expõe tokens", () => {
  assert.deepEqual(ticketApiConfiguration({
    UOL_API_AUTHORIZATION: "segredo-1",
  }), { configured: true, personalAuthorizationRequired: false });
});

test("diagnóstico testa combinações sem devolver credenciais", async () => {
  const seenHeaders = [];
  const result = await probeCouponAuthentication({
    UOL_API_AUTHORIZATION: "application-secret",
    UOL_OAUTH_AUTHORIZATION: "personal-secret",
  }, async (_url, init) => {
    seenHeaders.push(init.headers);
    const accepted = Boolean(init.headers.Authorization) && !init.headers["X-Authorization"];
    return new Response(JSON.stringify(accepted
      ? { beneficios: [apiItem] }
      : { error: "unauthorized" }), {
      status: accepted ? 200 : 401,
      headers: { "content-type": "application/json" },
    });
  });
  assert.deepEqual(result.map((item) => item.name), [
    "both", "application_only", "personal_only", "none",
  ]);
  assert.equal(result.find((item) => item.name === "application_only").offers, 1);
  assert.equal(JSON.stringify(result).includes("application-secret"), false);
  assert.equal(seenHeaders[1].Authorization, "Bearer application-secret");
  assert.equal(seenHeaders[1]["X-Authorization"], undefined);
});
