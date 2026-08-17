import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { createGateway } from "../src/gateway.js";

const token = "test-token";
const beeperAccessToken = "beeper-test-token";
const chatId = "!personal:local-whatsapp.localhost";
const link = "https://clube.uol.com.br/campanhasdeingresso/teste";
const silentLogger = {
  info() {},
  log() {},
  warn() {},
  error() {},
};

function gateway(fetchImpl, overrides = {}) {
  const directory = mkdtempSync(join(tmpdir(), "beeper-gateway-test-"));
  return createGateway({
    token,
    chatId,
    beeperAccessToken,
    databasePath: join(directory, "deliveries.sqlite"),
    fetchImpl,
    now: () => new Date("2026-08-14T20:00:00.000Z"),
    logger: silentLogger,
    ...overrides,
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

test("envia card nativo pelo transporte interno com imagem", async () => {
  let sent;
  const handler = gateway(async (url, init) => {
    assert.equal(url, "https://ddrxgn8ucibei.cloudfront.net/offer.jpg");
    assert.equal(init.redirect, "error");
    return new Response(new Uint8Array([0xff, 0xd8, 0xff, 0xd9]), {
      status: 200,
      headers: { "Content-Type": "image/jpeg" },
    });
  }, {
    sendMessageImpl: async (message) => {
      sent = message;
      return { pendingMessageID: "pending-preview-1" };
    },
  });
  const previewRequest = new Request("http://gateway.test/v1/send-offer", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "Idempotency-Key": "uol:offer-preview:v1",
    },
    body: JSON.stringify({
      link,
      text: `Oferta\n${link}`,
      preview: {
        title: "Oferta Clube UOL",
        summary: "Resumo da oferta",
        imageUrl: "https://ddrxgn8ucibei.cloudfront.net/offer.jpg",
      },
    }),
  });
  const response = await handler(previewRequest);
  assert.equal(response.status, 202);
  assert.equal((await response.json()).pendingMessageID, "pending-preview-1");
  assert.equal(sent.preview.title, "Oferta Clube UOL");
  assert.equal(sent.preview.imgType, "image/jpeg");
  assert.match(sent.preview.img, /^file:\/\//);
});

test("omite proxy de imagem não permitido sem bloquear a oferta", async () => {
  let sent;
  let imageFetches = 0;
  const handler = gateway(async () => {
    imageFetches += 1;
    return new Response("unexpected", { status: 500 });
  }, {
    sendMessageImpl: async (message) => {
      sent = message;
      return { pendingMessageID: "pending-without-image" };
    },
  });
  const response = await handler(new Request("http://gateway.test/v1/send-offer", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "Idempotency-Key": "uol:offer-discord-proxy:v1",
    },
    body: JSON.stringify({
      link,
      text: `Oferta\n${link}`,
      preview: {
        title: "Oferta Clube UOL",
        summary: "Resumo",
        imageUrl: "https://images-ext-1.discordapp.net/external/example/offer.jpg",
      },
    }),
  }));
  assert.equal(response.status, 202);
  assert.equal(imageFetches, 0);
  assert.equal(sent.preview.img, undefined);
  assert.equal(sent.preview.imgType, undefined);
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

test("valida autenticação sem enviar e expõe saúde sanitizada do ledger", async () => {
  const records = [];
  const handler = gateway(async () => new Response("{}", { status: 200 }), {
    logger: {
      info: (record) => records.push(record),
      warn: (record) => records.push(record),
    },
  });
  const unauthorized = await handler(new Request("http://gateway.test/v1/readyz"));
  assert.equal(unauthorized.status, 401);
  assert.deepEqual(records, []);

  const authorized = await handler(new Request("http://gateway.test/v1/readyz", {
    headers: { Authorization: `Bearer ${token}` },
  }));
  assert.equal(authorized.status, 200);
  assert.deepEqual(await authorized.json(), {
    ok: true,
    components: { transport: true, beeperApi: true, ledger: true },
    deliveryConfirmation: "accepted_by_beeper_transport",
    ledger: { total: 0, accepted: 0, failed: 0, unknown: 0, pending: 0 },
  });
  assert.equal(records.length, 1);
  assert.match(records[0], /"path":"\/v1\/readyz"/);
});

test("repete resposta sem reenviar a mesma oferta", async () => {
  let calls = 0;
  const directory = mkdtempSync(join(tmpdir(), "beeper-gateway-ledger-test-"));
  const databasePath = join(directory, "deliveries.sqlite");
  const handler = gateway(async () => {
    calls += 1;
    return new Response(JSON.stringify({ pendingMessageID: "pending-1" }), {
      status: 202,
      headers: { "Content-Type": "application/json" },
    });
  }, { databasePath });
  assert.equal((await handler(request())).status, 202);
  const database = new DatabaseSync(databasePath);
  assert.equal(
    database.prepare("SELECT status FROM deliveries WHERE idempotency_key = ?").get("uol:offer-1:v1").status,
    "accepted",
  );
  database.close();
  const replay = await handler(request());
  assert.equal(replay.status, 200);
  assert.deepEqual(await replay.json(), {
    accepted: true,
    pendingMessageID: "pending-1",
    deliveryState: "accepted_by_beeper_transport",
    replayed: true,
  });
  assert.equal(calls, 1);
});

test("fecha resposta sem comprovante local como entrega ambígua", async () => {
  let calls = 0;
  const handler = gateway(async () => new Response("{}", { status: 202 }), {
    sendMessageImpl: async () => {
      calls += 1;
      return { accepted: true };
    },
  });
  const first = await handler(request("uol:offer-no-receipt:v1"));
  assert.equal(first.status, 503);
  assert.equal((await first.json()).code, "delivery_unknown");
  const retry = await handler(request("uol:offer-no-receipt:v1"));
  assert.equal(retry.status, 409);
  assert.equal(calls, 1);
});

test("libera nova tentativa apenas depois de rejeição inequívoca", async () => {
  let calls = 0;
  const handler = gateway(async () => new Response("{}", { status: 202 }), {
    sendMessageImpl: async () => {
      calls += 1;
      if (calls === 1) throw new Error("rejected before transport acceptance");
      return { pendingMessageID: "pending-after-retry" };
    },
  });
  assert.equal((await handler(request("uol:offer-safe-retry:v1"))).status, 502);
  assert.equal((await handler(request("uol:offer-safe-retry:v1"))).status, 202);
  assert.equal(calls, 2);
});

test("não reutiliza chave idempotente com outro conteúdo", async () => {
  let calls = 0;
  const handler = gateway(async () => {
    calls += 1;
    return new Response(JSON.stringify({ pendingMessageID: "pending-1" }), {
      status: 202,
      headers: { "Content-Type": "application/json" },
    });
  });
  assert.equal((await handler(request("uol:offer-conflict:v1"))).status, 202);
  const conflict = await handler(request(
    "uol:offer-conflict:v1",
    `Oferta alterada\n${link}`,
  ));
  assert.equal(conflict.status, 409);
  assert.equal((await conflict.json()).code, "idempotency_conflict");
  assert.equal(calls, 1);
});

test("registra só decisões autenticadas sem token, conteúdo, link ou destino", async () => {
  const records = [];
  const logger = {
    info: (record) => records.push(record),
    warn: (record) => records.push(record),
    error: (record) => records.push(record),
  };
  const handler = gateway(async () => new Response(JSON.stringify({
    pendingMessageID: "pending-audit",
  }), {
    status: 202,
    headers: { "Content-Type": "application/json" },
  }), { logger });
  const unauthorized = request("uol:private-offer:v1", `segredo\n${link}`);
  unauthorized.headers.set("Authorization", "Bearer super-secret-token");
  assert.equal((await handler(unauthorized)).status, 401);
  assert.deepEqual(records, []);

  assert.equal((await handler(request(
    "uol:private-offer:v1",
    `segredo\n${link}`,
  ))).status, 202);

  const joined = records.join("\n");
  assert.match(joined, /"event":"beeper_gateway_request"/);
  assert.match(joined, /"code":"ok"/);
  assert.match(joined, /"deliveryState":"accepted_by_beeper_transport"/);
  assert.doesNotMatch(joined, /super-secret-token|segredo|clube\.uol\.com|personal|private-offer/);
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
