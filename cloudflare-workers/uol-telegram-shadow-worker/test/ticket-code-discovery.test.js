import test from "node:test";
import assert from "node:assert/strict";
import { buildTicketCodeCandidates, fetchTicketCodeOffer, ticketCodeOfferUrl } from "../src/ticket-code-discovery.js";

const origin = "https://clube.uol.com.br";
const card = (code, partner = "loja") => ({ link: `${origin}/${partner}/${code}-oferta` });

test("prioriza lacunas ancoradas e preserva maiusculas", () => {
  const codes = buildTicketCodeCandidates([card("pPT"), card("pPP"), card("pPw"), card("pPv")]);
  assert.deepEqual(codes.slice(0, 3), ["pPQ", "pPR", "pPS"]);
  assert.ok(codes.includes("pPu"));
  assert.ok(codes.includes("pPV"));
  for (const seen of ["pPT", "pPP", "pPw", "pPv"]) assert.ok(!codes.includes(seen));
});

test("limita prefixos, intervalos e vizinhos sem supor rollover", () => {
  const codes = buildTicketCodeCandidates([card("pQB"), card("pQD"), card("pPa"), card("pPz"), card("pAB"), card("pAD")]);
  assert.ok(codes.includes("pQC"));
  assert.ok(!codes.includes("pAC"));
  assert.ok(!codes.includes("pPm"));
  assert.ok(!codes.includes("pPA"));
  assert.ok(codes.length <= 64);
  assert.deepEqual(buildTicketCodeCandidates([card("pPQ"), card("pQa")]), []);
});

test("ignora ancoras externas, relativas, codificadas ou com ponto", () => {
  const invalid = [
    `${origin}/loja/p.PP-oferta`, `${origin}/loja/p%50P-oferta`,
    `${origin}/loja/pPP-oferta?x=1`, `${origin}/loja/pPP-oferta/resgatar`,
    "https://evil.test/loja/pPP-oferta", "/loja/pPP-oferta",
    "https://clube.uol.com.br:443/loja/pPP-oferta",
  ].map((link) => ({ link }));
  assert.deepEqual(buildTicketCodeCandidates([...invalid, card("pPT")]), []);
});

test("alias exige codigo exato e caminho publico completo", () => {
  const expected = `${origin}/campanhasdeingresso/pPS-2-ingressos`;
  assert.equal(ticketCodeOfferUrl(expected, "pPS", { full: true }), expected);
  assert.equal(ticketCodeOfferUrl("//clubeuol.clubeben.com.br/campanhasdeingresso/pPS-2-ingressos", "pPS", { full: true, legacy: true }), expected);
  assert.equal(ticketCodeOfferUrl("//clubeuol.clubeben.com.br/campanhasdeingresso/pPS-2-ingressos", "pPS", { full: true }), "");
  for (const bad of [
    `${origin}/campanhasdeingresso/pPs-2-ingressos`, `${expected}/resgatar`, `${expected}?x=1`, `${expected}#x`,
    `${origin}/campanhasdeingresso/p%50S-2-ingressos`, `${origin}/campanhasdeingresso/p.PS-2-ingressos`,
    `https://user@clube.uol.com.br/campanhasdeingresso/pPS-2-ingressos`, `https://clube.uol.com.br:443/campanhasdeingresso/pPS-2-ingressos`,
    `${origin}/other/../campanhasdeingresso/pPS-2-ingressos`, "https://evil.test/campanhasdeingresso/pPS-2-ingressos",
  ]) assert.equal(ticketCodeOfferUrl(bad, "pPS", { full: true, legacy: true }), "", bad);
  assert.equal(ticketCodeOfferUrl(`${origin}/campanhasdeingresso/pPS`, "pPS", { full: true }), "");
});

test("nunca segue redirecionamento ou acessa resgate", async () => {
  const calls = [];
  const result = await fetchTicketCodeOffer("pPS", async (url, options) => {
    calls.push({ url, options });
    return new Response(null, { status: 302, headers: { location: `${url}/resgatar` } });
  });
  assert.deepEqual(result, { status: "unknown", reason: "unexpected_redirect", requests: 1 });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, `${origin}/campanhasdeingresso/pPS`);
  assert.equal(calls[0].options.redirect, "manual");
  assert.equal(calls[0].options.headers.Cookie, undefined);
  assert.equal(calls[0].options.headers.Authorization, undefined);
  assert.ok(calls[0].options.signal instanceof AbortSignal);
});

test("somente redirecionamentos para a home oficial contam como ausencia", async () => {
  for (const location of ["/", "/index.html", `${origin}/`]) {
    assert.deepEqual(await fetchTicketCodeOffer("pPS", async () => new Response(null, { status: 302, headers: { location } })), { status: "absent", reason: "home_redirect", requests: 1 });
  }
  for (const location of ["/auth/uol/login", "/challenge", "https://evil.test/", `${origin}/?challenge=1`, `${origin}:443/`]) {
    assert.deepEqual(await fetchTicketCodeOffer("pPS", async () => new Response(null, { status: 302, headers: { location } })), { status: "unknown", reason: "unexpected_redirect", requests: 1 });
  }
});

test("classifica falhas sem expor corpo nem mensagem de rede", async () => {
  for (const [status, expectedStatus, reason] of [[404, "absent", "not_found"], [410, "absent", "not_found"], [429, "unknown", "rate_limited"], [503, "unknown", "upstream_error"]]) {
    assert.deepEqual(await fetchTicketCodeOffer("pPS", async () => new Response("private", { status })), { status: expectedStatus, reason, requests: 1 });
  }
  assert.deepEqual(await fetchTicketCodeOffer("pPS", async () => new Response("private", { headers: { "content-type": "application/json" } })), { status: "unknown", reason: "non_html", requests: 1 });
  assert.deepEqual(await fetchTicketCodeOffer("pPS", async () => { throw new Error("private"); }), { status: "unknown", reason: "network_or_timeout", requests: 1 });
  assert.deepEqual(await fetchTicketCodeOffer("p.PS", async () => assert.fail("must not fetch")), { status: "unknown", reason: "invalid_code", requests: 0 });
});

test("limite real do stream funciona sem Content-Length", async () => {
  let reads = 0;
  let cancelled = false;
  const stream = new ReadableStream({
    pull(controller) { reads += 1; controller.enqueue(new Uint8Array(256 * 1024)); },
    cancel() { cancelled = true; },
  });
  const result = await fetchTicketCodeOffer("pPS", async () => new Response(stream, { headers: { "content-type": "text/html" } }));
  assert.deepEqual(result, { status: "unknown", reason: "body_too_large", requests: 1 });
  assert.ok(cancelled);
  assert.ok(reads <= 6);
});

test("nao le um corpo que ja excede o limite declarado", async () => {
  const result = await fetchTicketCodeOffer("pPS", async () => new Response("x", { headers: { "content-type": "text/html", "content-length": String(1024 * 1024 + 1) } }));
  assert.deepEqual(result, { status: "unknown", reason: "body_too_large", requests: 1 });
});
