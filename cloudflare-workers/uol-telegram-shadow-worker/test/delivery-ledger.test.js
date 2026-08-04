import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import {
  deliveryEventKey,
  boundedReconciliationCandidates,
  recordDeliveryEvent,
  summarizeDeliveryTimeline,
  trimDeliveryEvents,
} from "../src/delivery-ledger.js";

function databaseWithLedger() {
  const database = new DatabaseSync(":memory:");
  database.exec(`
    CREATE TABLE delivery_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dedupe_key TEXT NOT NULL UNIQUE,
      offer_id TEXT NOT NULL,
      target TEXT NOT NULL,
      operation TEXT NOT NULL,
      state TEXT NOT NULL,
      attempt INTEGER NOT NULL DEFAULT 0,
      generation INTEGER NOT NULL DEFAULT 1,
      occurred_at TEXT NOT NULL,
      external_id TEXT NOT NULL DEFAULT '',
      error TEXT NOT NULL DEFAULT ''
    );
    CREATE INDEX delivery_events_offer_idx
      ON delivery_events(offer_id, occurred_at DESC, id DESC);
  `);
  return database;
}

function execute(database, sql, ...params) {
  return database.prepare(sql).run(...params);
}

test("gera chave idempotente por oferta, destino, operação e tentativa", () => {
  const first = deliveryEventKey({
    offerId: "oferta-1",
    target: "main",
    operation: "send",
    attempt: 2,
    generation: 3,
  });
  const second = deliveryEventKey({
    offerId: "oferta-1",
    target: "main",
    operation: "send",
    attempt: 2,
    generation: 3,
  });

  assert.equal(first, second);
  assert.equal(first, "oferta-1|main|send|2|3");
});

test("grava evento uma vez e limita erro e identificador externo", () => {
  const database = databaseWithLedger();
  const longError = "erro\n" + "x".repeat(400);
  const longExternal = "m".repeat(300);
  const event = {
    offerId: "oferta-1",
    target: "discord",
    operation: "send",
    state: "failed",
    attempt: 1,
    generation: 2,
    occurredAt: "2026-08-04T20:00:00.000Z",
    externalId: longExternal,
    error: longError,
  };

  assert.equal(recordDeliveryEvent(execute.bind(null, database), event), true);
  assert.equal(recordDeliveryEvent(execute.bind(null, database), event), false);
  const row = database.prepare("SELECT * FROM delivery_events").get();
  assert.equal(row.dedupe_key, "oferta-1|discord|send|1|2|failed");
  assert.equal(row.error.length, 240);
  assert.equal(row.external_id.length, 180);
  assert.equal(row.error.includes("\n"), false);
  database.close();
});

test("mantém eventos recentes por oferta e resume timeline sanitizada", () => {
  const database = databaseWithLedger();
  const insert = (occurredAt, operation, state, attempt) => recordDeliveryEvent(
    execute.bind(null, database),
    {
      offerId: "oferta-1",
      target: "main",
      operation,
      state,
      attempt,
      generation: 1,
      occurredAt,
    },
  );
  insert("2026-08-04T20:00:00.000Z", "send", "attempt_started", 1);
  insert("2026-08-04T20:00:01.000Z", "send", "failed", 1);
  insert("2026-08-04T20:00:02.000Z", "send", "sent", 2);
  trimDeliveryEvents(execute.bind(null, database), "oferta-1", 2);

  const rows = database.prepare(
    "SELECT * FROM delivery_events WHERE offer_id = ? ORDER BY occurred_at ASC",
  ).all("oferta-1");
  assert.equal(rows.length, 2);
  assert.deepEqual(summarizeDeliveryTimeline(rows), [
    {
      target: "main",
      operation: "send",
      state: "sent",
      attempt: 2,
      generation: 1,
      occurredAt: "2026-08-04T20:00:02.000Z",
      externalId: "",
      error: "",
    },
    {
      target: "main",
      operation: "send",
      state: "failed",
      attempt: 1,
      generation: 1,
      occurredAt: "2026-08-04T20:00:01.000Z",
      externalId: "",
      error: "",
    },
  ]);
  database.close();
});

test("seleciona no máximo 32 ofertas únicas para reconciliação, na ordem do último evento", () => {
  const rows = Array.from({ length: 40 }, (_, index) => ({
    event_id: 100 - index,
    offer_id: `oferta-${index % 35}`,
    status: index % 2 ? "partial_delivery" : "delivery_unknown",
  }));
  const candidates = boundedReconciliationCandidates(rows, 32);
  assert.equal(candidates.length, 32);
  assert.equal(candidates[0].event_id, 100);
  assert.equal(new Set(candidates.map((row) => row.offer_id)).size, 32);
  assert.equal(candidates.at(-1).offer_id, "oferta-31");
});
