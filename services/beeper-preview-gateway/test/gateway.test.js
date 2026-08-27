import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { createGateway } from "../src/gateway.js";

const token = "test-token";
const beeperAccessToken = "beeper-test-token";
const accountId = "local-whatsapp_ba_example";
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
    accountId,
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

test("usa somente o endpoint oficial mesmo quando recebe metadados de preview", async () => {
  let sent;
  const handler = gateway(async (url, init) => {
    sent = { url, init };
    return Response.json({ pendingMessageID: "pending-preview-1" });
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
  assert.match(sent.url, /\/v1\/chats\/.*\/messages$/);
  assert.deepEqual(JSON.parse(sent.init.body), { text: `Oferta\n${link}` });
});

test("só fica pronto com conta conectada e chat compatível", async () => {
  const seen = [];
  const handler = gateway(async (url, init) => {
    seen.push({ url, authorization: init.headers.Authorization });
    if (url.endsWith("/v1/accounts")) {
      return Response.json([{
        accountID: accountId,
        network: "WhatsApp",
        status: "connected",
      }]);
    }
    return Response.json({
      id: chatId,
      accountID: accountId,
      network: "WhatsApp",
      isReadOnly: false,
    });
  });
  const response = await handler(new Request("http://gateway.test/readyz"));
  assert.equal(response.status, 200);
  assert.equal(seen.length, 2);
  assert.ok(seen.some(({ url }) => url.endsWith("/v1/accounts")));
  assert.ok(seen.some(({ url }) => /\/v1\/chats\/.*personal/.test(url)));
  assert.ok(seen.every(({ authorization }) => authorization === `Bearer ${beeperAccessToken}`));

  const unavailable = gateway(async () => new Response("{}", { status: 500 }));
  assert.equal(
    (await unavailable(new Request("http://gateway.test/readyz"))).status,
    503,
  );

  const disconnected = gateway(async (url) => url.endsWith("/v1/accounts")
    ? Response.json([{ accountID: accountId, network: "WhatsApp", status: "disconnected" }])
    : Response.json({ id: chatId, accountID: accountId, network: "WhatsApp" }));
  const disconnectedResponse = await disconnected(new Request("http://gateway.test/readyz"));
  assert.equal(disconnectedResponse.status, 503);
  assert.equal((await disconnectedResponse.json()).code, "beeper_destination_not_ready");
});

test("valida autenticação sem enviar e expõe saúde sanitizada do ledger", async () => {
  const records = [];
  const handler = gateway(async (url) => url.endsWith("/v1/accounts")
    ? Response.json([{ accountID: accountId, network: "WhatsApp", status: "connected" }])
    : Response.json({ id: chatId, accountID: accountId, network: "WhatsApp" }), {
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
    deliveryConfirmation: "accepted_by_beeper_api",
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
    deliveryState: "accepted_by_beeper_api",
    replayed: true,
  });
  assert.equal(calls, 1);
});

test("preserva idempotência do ledger criado pela versão anterior", async () => {
  let calls = 0;
  const directory = mkdtempSync(join(tmpdir(), "beeper-gateway-upgrade-test-"));
  const databasePath = join(directory, "deliveries.sqlite");
  const database = new DatabaseSync(databasePath);
  database.exec(`
    CREATE TABLE deliveries (
      idempotency_key TEXT PRIMARY KEY,
      request_hash TEXT NOT NULL,
      status TEXT NOT NULL,
      response_json TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  const text = `Oferta\n${link}`;
  const legacyHash = createHash("sha256").update(JSON.stringify({
    link,
    text,
    preview: {
      link,
      title: "Clube UOL",
      summary: "",
      type: "website",
      imageUrl: "",
    },
  })).digest("hex");
  database.prepare(`
    INSERT INTO deliveries
      (idempotency_key, request_hash, status, response_json, created_at, updated_at)
    VALUES (?, ?, 'failed', '', ?, ?)
  `).run(
    "uol:offer-pre-upgrade:v1",
    legacyHash,
    "2026-08-14T19:00:00.000Z",
    "2026-08-14T19:00:00.000Z",
  );
  database.close();

  const handler = gateway(async () => {
    calls += 1;
    return Response.json({ pendingMessageID: "pending-after-upgrade" });
  }, { databasePath });
  const response = await handler(request("uol:offer-pre-upgrade:v1", text));
  assert.equal(response.status, 202);
  assert.equal(calls, 1);
});

test("fecha resposta sem comprovante local como entrega ambígua", async () => {
  let calls = 0;
  const handler = gateway(async () => {
    calls += 1;
    return new Response("{}", { status: 202 });
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
  const handler = gateway(async () => {
    calls += 1;
    if (calls === 1) return Response.json({ code: "not_logged_in" }, { status: 502 });
    return Response.json({ pendingMessageID: "pending-after-retry" });
  });
  assert.equal((await handler(request("uol:offer-safe-retry:v1"))).status, 502);
  assert.equal((await handler(request("uol:offer-safe-retry:v1"))).status, 202);
  assert.equal(calls, 2);
});

test("fecha erro HTTP ambíguo sem permitir possível duplicação", async () => {
  let calls = 0;
  const handler = gateway(async () => {
    calls += 1;
    return Response.json({ code: "upstream_failure" }, { status: 502 });
  });
  const first = await handler(request("uol:offer-upstream-failure:v1"));
  assert.equal(first.status, 503);
  assert.equal((await first.json()).code, "delivery_unknown");
  const retry = await handler(request("uol:offer-upstream-failure:v1"));
  assert.equal(retry.status, 409);
  assert.equal(calls, 1);
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
  assert.match(joined, /"deliveryState":"accepted_by_beeper_api"/);
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
