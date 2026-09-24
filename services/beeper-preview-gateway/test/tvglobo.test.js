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
      assert.equal(delivery.requirePreview, Boolean(sent.at(-1)?.preview?.img));
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

test("rota geral preserva título e texto longo, acrescentando URL para o WhatsApp exibir o cartão", async () => {
  const { handler, sent, request } = setup();
  const body = { ...payload, text: "Texto integral ❤️\n".repeat(4000).trim(),
    preview: { ...payload.preview, title: "Nome (@tvglobo) no X", summary: "Resumo curto…" } };
  assert.ok(Buffer.byteLength(JSON.stringify(body)) > 64 * 1024);
  assert.equal((await handler(request(body, "tvglobo-token", "/v1/send-x-post"))).status, 202);
  assert.equal(sent[0].text, body.text + "\n\n" + link);
  assert.equal(sent[0].preview.title, body.preview.title);
  assert.equal(sent[0].preview.link, link);
  assert.equal(sent[0].preview.summary, "Resumo curto…");
  assert.equal((await handler(request({ ...body, text: body.text + "\n\n" + link }, "tvglobo-token", "/v1/send-x-post"))).status, 202);
  assert.equal(sent[1].text, sent[0].text);
  assert.equal((await handler(request({ ...payload, text: "Sem link" }))).status, 400);
  assert.equal((await handler(request({ ...payload, text: "a".repeat(8001) + link }))).status, 400);
});

test("cliente antigo recebe URL antes da assinatura e crédito atualizado para push", async () => {
  const { handler, sent, request } = setup();
  const body = { ...payload, text: "Texto\n\n`@tvglobo via X, 00:34`\n`powered by leo saquetto sync`" };
  assert.equal((await handler(request(body, "tvglobo-token", "/v1/send-x-post"))).status, 202);
  assert.equal(sent[0].text, "Texto\n\n`@tvglobo via X, 00:34`\n" + link + "\n`push by @leosaquetto`");
});

test("avatar de cliente antigo é removido; mídia continua com imagem", async () => {
  const { handler, sent, request } = setup();
  const body = { ...payload, preview: { ...payload.preview, imageUrl: "https://pbs.twimg.com/profile_images/123/avatar_x96.jpg" } };
  assert.equal((await handler(request(body, "tvglobo-token", "/v1/send-x-post"))).status, 202);
  assert.equal(sent[0].preview, undefined);
  assert.equal((await handler(request(payload, "tvglobo-token", "/v1/send-x-post"))).status, 202);
  assert.equal(sent[1].preview.imgSize, undefined);
  assert.match(sent[1].preview.img, /^file:/);
});

test("post sem mídia não baixa imagem nem envia cartão", async () => {
  let downloads = 0;
  const { handler, sent, request } = setup({ fetchImpl: async () => { downloads++; throw new Error("unexpected download"); } });
  for (const imageUrl of [undefined, "", "https://pbs.twimg.com/profile_images/123/avatar_400x400.jpg"]) {
    const body = { ...payload, preview: { ...payload.preview, title: "Nome (@tvglobo) no X", imageUrl } };
    assert.equal((await handler(request(body, "tvglobo-token", "/v1/send-x-post"))).status, 202);
    assert.equal(sent.at(-1).preview, undefined);
    assert.equal(sent.at(-1).text, body.text);
  }
  assert.equal(downloads, 0);
});

test("formatação WhatsApp passa intacta apenas na rota pessoal geral", async () => {
  const { handler, sent, request } = setup();
  const body = { ...payload, format: "whatsapp", text: "*Título*\n```Texto```\n🔗 `" + link + "`\n`push by @leosaquetto`", preview: { summary: "Texto" } };
  assert.equal((await handler(request(body, "tvglobo-token", "/v1/send-x-post"))).status, 202);
  assert.equal(sent[0].text, body.text);
  assert.equal(sent[0].formatText, false);
  assert.equal(sent[0].preview, undefined);
  assert.equal((await handler(request({ ...payload, format: "whatsapp" }))).status, 202);
  assert.equal(sent[1].formatText, undefined);
  assert.ok(sent[1].preview.img);
});

test("modelo nativo mantém link sem protocolo, sem repetir URL, e associa a mídia ao link exibido", async () => {
  const { handler, sent, request } = setup();
  const displayedLink = link.replace("https://", "");
  const body = { ...payload, format: "whatsapp", text: "> ```Texto```\n> 𝕏 ```Nome (@tvglobo) no X, 23:44```\n> ```" + displayedLink + "```" };
  assert.equal((await handler(request(body, "tvglobo-token", "/v1/send-x-post"))).status, 202);
  assert.equal(sent[0].text, body.text);
  assert.equal(sent[0].preview.link, displayedLink);
  assert.ok(sent[0].preview.img);
});
