import assert from "node:assert/strict";
import test from "node:test";
import { chooseDeliveryBudget, summarizeQueueSlo } from "../src/queue-policy.js";

const now = new Date("2026-08-04T20:00:00.000Z");

test("resume idade da fila e separa principal crítico de secundários", () => {
  const result = summarizeQueueSlo([
    {
      id: "nova",
      status: "delivery_pending",
      first_seen_at: "2026-08-04T19:59:50.000Z",
      main_sent_at: "",
    },
    {
      id: "secundaria",
      status: "partial_delivery",
      first_seen_at: "2026-08-04T19:40:00.000Z",
      main_sent_at: "2026-08-04T19:40:01.000Z",
    },
  ], now);

  assert.equal(result.pending, 2);
  assert.equal(result.criticalPending, 1);
  assert.equal(result.secondaryPending, 1);
  assert.equal(result.oldestAgeMs, 20 * 60_000);
  assert.equal(result.p95AgeMs, 20 * 60_000);
});

test("mantém lote e concorrência quando a fila está saudável", () => {
  const result = chooseDeliveryBudget({
    storageReadBudget: { maintenanceAllowed: true, primaryAllowed: true },
    queueSlo: { pending: 1, criticalPending: 1, secondaryPending: 0, oldestAgeMs: 10_000 },
    configuredBatch: 4,
    configuredConcurrency: 6,
  });

  assert.deepEqual(result, {
    batchSize: 4,
    concurrency: 6,
    deferSecondary: false,
    allowPrioritySecondary: false,
    reason: "healthy",
  });
});

test("cede secundários sob backlog crítico antigo sem reduzir caminho principal", () => {
  const result = chooseDeliveryBudget({
    storageReadBudget: { maintenanceAllowed: true, primaryAllowed: true },
    queueSlo: { pending: 5, criticalPending: 2, secondaryPending: 3, oldestAgeMs: 46_000 },
    configuredBatch: 8,
    configuredConcurrency: 6,
  });

  assert.equal(result.batchSize, 4);
  assert.equal(result.concurrency, 6);
  assert.equal(result.deferSecondary, true);
  assert.equal(result.allowPrioritySecondary, false);
  assert.equal(result.reason, "critical_backlog");
});

test("cede secundários quando a reserva de leituras está ativa", () => {
  const result = chooseDeliveryBudget({
    storageReadBudget: { maintenanceAllowed: false, primaryAllowed: true },
    queueSlo: { pending: 0, criticalPending: 0, secondaryPending: 2, oldestAgeMs: 0 },
    configuredBatch: 4,
    configuredConcurrency: 6,
  });

  assert.equal(result.deferSecondary, true);
  assert.equal(result.allowPrioritySecondary, false);
  assert.equal(result.reason, "quota_reserve");
  assert.equal(result.batchSize, 4);
  assert.equal(result.concurrency, 6);
});

test("não aumenta limites configurados por causa de fila antiga", () => {
  const result = chooseDeliveryBudget({
    storageReadBudget: { maintenanceAllowed: true, primaryAllowed: true },
    queueSlo: { pending: 20, criticalPending: 20, secondaryPending: 0, oldestAgeMs: 600_000 },
    configuredBatch: 99,
    configuredConcurrency: 99,
  });

  assert.equal(result.batchSize, 4);
  assert.equal(result.concurrency, 6);
  assert.equal(result.deferSecondary, true);
  assert.equal(result.allowPrioritySecondary, false);
});

test("reserva secundários apenas para IDs novos da API", () => {
  const result = chooseDeliveryBudget({
    storageReadBudget: { maintenanceAllowed: false, primaryAllowed: true },
    queueSlo: { pending: 1, criticalPending: 1, secondaryPending: 0, oldestAgeMs: 0 },
    configuredBatch: 4,
    configuredConcurrency: 6,
    priorityCount: 2,
  });

  assert.equal(result.deferSecondary, true);
  assert.equal(result.allowPrioritySecondary, true);
  assert.equal(result.batchSize, 4);
  assert.equal(result.concurrency, 6);
});
