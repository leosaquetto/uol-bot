import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createGateway } from "../src/gateway.js";

const token = "test-token";
const beeperAccessToken = "beeper-test-token";
const chatId = "!personal:local-whatsapp.localhost";
const link = "https://clube.uol.com.br/campanhasdeingresso/teste";

function gateway(fetchImpl) {
  const directory = mkdtempSync(join(tmpdir(), "beeper-gateway-test-"));
  return createGateway({
    token,
    chatId,
    beeperAccessToken,
    databasePath: join(directory, "deliveries.sqlite"),
    fetchImpl,
    now: () => new Date("2026-08-14T20:00:00.000Z"),
  });
}

function request(key = "uol:offer-1:v1", text = `Oferta\n${link}`) {
  return new Request("http://gateway.test/v1/send-offer", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "Idempotency-Key": key,
    },
    body: JSON.stringify({ link, text }),
  });
}

test("envia pelo endpoint oficial do Beeper sem desabilitar preview", async () => {
  let calls = 0;
  const handler = gateway(async (url, init) => {
    calls += 1;
    assert.match(url, /\/v1\/chats\/.*\/messages$/);
    assert.equal(init.headers.Authorization, `Bearer ${beeperAccessToken}`);
    assert.deepEqual(JSON.parse(init.body), { text: `Oferta\n${link}` });
    return new Response(JSON.stringify({ pendingMessageID: "pending-1" }), {
      status: 202,
      headers: { "Content-Type": "application/json" },
    });
  });
  const response = await handler(request());
  assert.equal(response.status, 202);
  assert.equal(calls, 1);
  assert.equal((await response.json()).pendingMessageID, "pending-1");
});

test("só fica pronto quando o chat configurado está acessível", async () => {
  const seen = [];
  const handler = gateway(async (url, init) => {
    seen.push({ url, authorization: init.headers.Authorization });
    return new Response("{}", { status: 200 });
  });
  const response = await handler(new Request("http://gateway.test/readyz"));
  assert.equal(response.status, 200);
  assert.match(seen[0].url, /\/v1\/chats\/.*personal/);
  assert.equal(seen[0].authorization, `Bearer ${beeperAccessToken}`);

  const unavailable = gateway(async () => new Response("{}", { status: 500 }));
  assert.equal(
    (await unavailable(new Request("http://gateway.test/readyz"))).status,
    503,
  );
});

test("repete resposta sem reenviar a mesma oferta", async () => {
  let calls = 0;
  const handler = gateway(async () => {
    calls += 1;
    return new Response(JSON.stringify({ pendingMessageID: "pending-1" }), {
      status: 202,
      headers: { "Content-Type": "application/json" },
    });
  });
  assert.equal((await handler(request())).status, 202);
  const replay = await handler(request());
  assert.equal(replay.status, 200);
  assert.equal((await replay.json()).replayed, true);
  assert.equal(calls, 1);
});

test("fecha falhas ambíguas para impedir duplicação", async () => {
  const handler = gateway(async () => {
    throw new Error("connection reset");
  });
  assert.equal((await handler(request())).status, 503);
  const retry = await handler(request());
  assert.equal(retry.status, 409);
  assert.equal((await retry.json()).code, "delivery_unknown");
});

test("rejeita destino e autenticação fora do contrato", async () => {
  const handler = gateway(async () => new Response("{}", { status: 202 }));
  const unauthorized = request();
  unauthorized.headers.set("Authorization", "Bearer wrong");
  assert.equal((await handler(unauthorized)).status, 401);
  const invalid = new Request("http://gateway.test/v1/send-offer", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "Idempotency-Key": "uol:offer-2:v1",
    },
    body: JSON.stringify({
      link: "https://example.com/fora-do-contrato",
      text: "fora",
    }),
  });
  assert.equal((await handler(invalid)).status, 400);
});
