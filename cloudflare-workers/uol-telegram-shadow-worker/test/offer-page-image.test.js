import test from "node:test";
import assert from "node:assert/strict";

import {
  extractOfferPageImage,
  offerPageUrl,
  resolveOfferPageImage,
} from "../src/offer-page-image.js";

const LINK = "https://clube.uol.com.br/campanhasdeingresso/pYy-2-ingressos-22-11-teatro-uol-sp";
const API_IMAGE = "https://ddrxgn8ucibei.cloudfront.net/beneficios/6afc371703617_268d9b63116b15afe2ddd2618ca702f9.png";
const PAGE_IMAGE = "https://ddrxgn8ucibei.cloudfront.net/beneficios/cb91a74333d3abf77a4f67291087f16f.png";

// Recorte da página real de 17/11 (só as imagens relevantes).
const PAGE_HTML = `<!DOCTYPE html><html lang="pt-BR"><head>
<meta charset="utf-8">
<meta property="og:url" content="${LINK}">
<meta property="og:image" content="${PAGE_IMAGE}">
<title>Clube UOL - Teatro</title></head><body>
<img src="https://d310rdc9v8uuq.cloudfront.net/beneficios_categorias/0b31e54ad4def1df288c491e48ab0ce7.png">
<img src="https://d310rdc9v8uuq.cloudfront.net/beneficios_categorias/3808e419d2ed5e48b5e1a25a4e298290.png">
<div id="beneficio"><h2>2 INGRESSOS: Teatro UOL</h2>
<div id="ilustracoes"><img src="${PAGE_IMAGE}"></div>
<img src="https://ddrxgn8ucibei.cloudfront.net/parceiros/b262fba623c914d0172deefd9292be0b.png">
<p>Promoção válida em São Paulo</p></div></body></html>`;

function latin1Response(html, init = {}) {
  const bytes = Uint8Array.from(html, (char) => char.charCodeAt(0) & 0xff);
  return new Response(bytes, {
    status: 200,
    headers: { "Content-Type": "text/html; charset=ISO-8859-1" },
    ...init,
  });
}

test("aceita só páginas públicas do Clube UOL", () => {
  assert.equal(offerPageUrl(LINK), LINK);
  assert.equal(offerPageUrl("https://clube.uol.com.br/categoria/oferta-x/"), "https://clube.uol.com.br/categoria/oferta-x/");
  for (const bad of [
    "",
    "http://clube.uol.com.br/campanhasdeingresso/pYy-x",
    "https://evil.example/campanhasdeingresso/pYy-x",
    "https://clube.uol.com.br.evil.example/campanhasdeingresso/pYy-x",
    "https://clube.uol.com.br:8443/campanhasdeingresso/pYy-x",
    "https://user@clube.uol.com.br/campanhasdeingresso/pYy-x",
    "https://clube.uol.com.br/campanhasdeingresso/pYy-x?redirect=1",
    "https://clube.uol.com.br/campanhasdeingresso/pYy-x/resgatar",
    "não é URL",
  ]) {
    assert.equal(offerPageUrl(bad), "", bad);
  }
});

test("extrai a única arte em /beneficios/ e ignora /beneficios_categorias/", () => {
  assert.equal(extractOfferPageImage(PAGE_HTML), PAGE_IMAGE);
});

test("não devolve a própria imagem que já falhou", () => {
  const html = `<img src="${API_IMAGE}">`;
  assert.equal(extractOfferPageImage(html, { excludeUrl: API_IMAGE }), "");
  assert.equal(extractOfferPageImage(`${html}<img src="${PAGE_IMAGE}">`, { excludeUrl: API_IMAGE }), PAGE_IMAGE);
});

test("com várias artes só aceita a que também é og:image", () => {
  const other = "https://ddrxgn8ucibei.cloudfront.net/beneficios/outra.jpg";
  assert.equal(
    extractOfferPageImage(`<meta content="${PAGE_IMAGE}" property="og:image"><img src="${other}"><img src="${PAGE_IMAGE}">`),
    PAGE_IMAGE,
  );
  assert.equal(extractOfferPageImage(`<img src="${other}"><img src="${PAGE_IMAGE}">`), "");
});

test("ignora prefixos parecidos e extensões que não são imagem", () => {
  assert.equal(extractOfferPageImage(`
    <img src="https://ddrxgn8ucibei.cloudfront.net/beneficios_categorias/a.png">
    <img src="https://ddrxgn8ucibei.cloudfront.net/beneficios/a.svg">
    <img src="https://exemplo.com/beneficios/a.png">
    <img src="http://ddrxgn8ucibei.cloudfront.net/beneficios/a.png">`), "");
});

test("busca a página da oferta e devolve a arte", async () => {
  const calls = [];
  const imageUrl = await resolveOfferPageImage(LINK, { excludeUrl: API_IMAGE }, async (url, init) => {
    calls.push({ url, accept: init.headers.Accept, userAgent: init.headers["User-Agent"] });
    return latin1Response(PAGE_HTML);
  });
  assert.equal(imageUrl, PAGE_IMAGE);
  // Sem User-Agent o Clube responde 405 ao Worker (falha real de 17/11 após o #154).
  assert.deepEqual(calls, [{ url: LINK, accept: "text/html", userAgent: "UOLTelegramCloudflare/1.0" }]);
});

test("falhas da página viram códigos offer_page_*", async () => {
  await assert.rejects(
    resolveOfferPageImage("https://evil.example/x/y", {}, async () => assert.fail("não deve buscar")),
    /offer_page_link_invalid/,
  );
  await assert.rejects(
    resolveOfferPageImage(LINK, {}, async () => new Response("bloqueado", { status: 403 })),
    /offer_page_http_403/,
  );
  await assert.rejects(
    resolveOfferPageImage(LINK, {}, async () => latin1Response("<html>sem arte</html>")),
    /offer_page_image_missing/,
  );
  await assert.rejects(
    resolveOfferPageImage(LINK, {}, async () => new Response("x", {
      headers: { "Content-Length": String(2 * 1024 * 1024) },
    })),
    /offer_page_too_large/,
  );
  const redirected = latin1Response(PAGE_HTML);
  Object.defineProperty(redirected, "url", { value: "https://evil.example/pagina" });
  await assert.rejects(resolveOfferPageImage(LINK, {}, async () => redirected), /offer_page_redirect_foreign/);
});
