import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createGateway } from "../src/gateway.js";

const id = "2102952373022851247";
const link = `https://x.com/tvglobo/status/${id}`;
const payload = {
  link,
  text: `📺 TV Globo\nLegenda\n${link}`,
  preview: { summary: "Legenda", imageUrl: "https://pbs.twimg.com/media/test.jpg" },
};
function setup(overrides = {}) {
  const sent = [];
  const handler = createGateway({
    token: "uol-token", tvgloboToken: "tvglobo-token", chatId: "group",
    selfChatId: "self", accountId: "account", beeperAccessToken: "beeper-token",
    databasePath: join(mkdtempSync(join(tmpdir(), "tvglobo-test-")), "ledger.sqlite"),
    fetchImpl: async () => new Response(new Uint8Array([0xff, 0xd8, 0xff, 0xd9]), { headers: { "Content-Type": "image/jpeg" } }),
    sendMessageImpl: async message => { sent.push(message); return { pendingMessageID: "pending-tvglobo" }; },
    confirmDeliveryImpl: async delivery => {
      assert.equal(delivery.chatId, "self");
      assert.equal(delivery.requirePreview, true);
      return { state: "delivered" };
    },
    logger: { info() {}, warn() {} }, ...overrides,
  });
  const request = (body = payload, token = "tvglobo-token", path = "/v1/send-tvglobo", key = `tvglobo:${id}:self:v1`) => new Request(`https://gateway.test${path}`, {
    method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json", "Idempotency-Key": key },
    body: JSON.stringify(body),
  });
  return { handler, sent, request };
}

test("TV Globo envia cartão ao chat fixo e não duplica o mesmo post", async () => {
  const { handler, sent, request } = setup();
  assert.equal((await handler(request({ ...payload, chatId: "group" }))).status, 202);
  assert.equal(sent[0].chatId, "self");
  assert.equal(sent[0].preview.title, "TV Globo • @tvglobo");
  assert.equal(sent[0].preview.summary, "Legenda");
  assert.match(sent[0].preview.img, /^file:/);
  assert.equal((await handler(request())).status, 200);
  assert.equal(sent.length, 1);
});

test("tokens de TV Globo e UOL não são intercambiáveis", async () => {
  const { handler, sent, request } = setup();
  assert.equal((await handler(request(payload, "uol-token"))).status, 401);
  assert.equal((await handler(request(payload, "tvglobo-token", "/v1/send-offer"))).status, 401);
  assert.equal((await handler(request(payload, "tvglobo-token", "/v1/send-buyticket"))).status, 401);
  assert.equal(sent.length, 0);
});

test("TV Globo rejeita outro perfil, imagem externa, URL com credenciais e chave incorreta", async () => {
  const { handler, sent, request } = setup();
  for (const body of [
    { ...payload, link: link.replace("tvglobo", "outra") },
    { ...payload, preview: { ...payload.preview, imageUrl: "https://evil.test/photo.jpg" } },
    { ...payload, preview: { ...payload.preview, imageUrl: "https://secret@pbs.twimg.com/media/test.jpg" } },
    { ...payload, preview: { ...payload.preview, imageUrl: "" } },
  ]) assert.equal((await handler(request(body))).status, 400);
  assert.equal((await handler(request(payload, "tvglobo-token", "/v1/send-tvglobo", "uol:wrong:v1"))).status, 400);
  assert.equal(sent.length, 0);
});

test("rota TV Globo desativada sem configuração ou com destino igual ao grupo", async () => {
  for (const overrides of [{ tvgloboToken: "" }, { selfChatId: "" }, { selfChatId: "group" }]) {
    const { handler, sent, request } = setup(overrides);
    assert.equal((await handler(request())).status, 404);
    assert.equal(sent.length, 0);
  }
});

test("entrega ambígua não reenvia ao repetir a chamada", async () => {
  const { handler, sent, request } = setup({ confirmDeliveryImpl: async () => ({ state: "unknown" }) });
  assert.equal((await handler(request())).status, 503);
  assert.equal((await handler(request())).status, 409);
  assert.equal(sent.length, 1);
});

test("rota geral aceita outro perfil e mantém o destino próprio", async () => {
  const { handler, sent, request } = setup();
  const otherLink = link.replace("tvglobo", "outro_perfil");
  const body = { ...payload, link: otherLink, text: `Legenda\n${otherLink}`, chatId: "group" };
  const make = () => {
    const req = request(body, "tvglobo-token", "/v1/send-x-post");
    req.headers.delete("Idempotency-Key");
    return req;
  };
  assert.equal((await handler(make())).status, 202);
  assert.equal(sent[0].chatId, "self");
  assert.equal(sent[0].preview.title, "X • @outro_perfil");
  assert.equal(sent[0].preview.link, otherLink);
  assert.equal((await handler(make())).status, 202);
  assert.equal(sent.length, 2);
});

test("rota geral reenvia TV Globo sem alterar a deduplicação da rota antiga", async () => {
  const { handler, sent, request } = setup();
  assert.equal((await handler(request())).status, 202);
  const response = await handler(request(payload, "tvglobo-token", "/v1/send-x-post"));
  assert.equal(response.status, 202);
  assert.equal((await handler(request())).status, 200);
  assert.equal(sent.length, 2);
});

test("rota geral rejeita URL inválida, credenciais, perfis fora do formato e token UOL", async () => {
  const { handler, sent, request } = setup();
  for (const badLink of [
    link.replace("x.com", "evil.test"),
    link.replace("x.com", "secret@x.com"),
    link.replace("tvglobo", "longusernameover15"),
    link.replace("tvglobo", "outro-perfil"),
    link + "?chat=group",
  ]) {
    const body = { ...payload, link: badLink, text: badLink };
    assert.equal((await handler(request(body, "tvglobo-token", "/v1/send-x-post", `x:${id}:self:v1`))).status, 400);
  }
  assert.equal((await handler(request(payload, "uol-token", "/v1/send-x-post"))).status, 401);
  assert.equal(sent.length, 0);
});
