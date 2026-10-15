import assert from "node:assert/strict";
import test from "node:test";
import {
  chooseDeliveryBudget,
  isRecentDeliveryDecision,
  summarizeQueueSlo,
} from "../src/queue-policy.js";

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
  assert.equal(result.actionableMainPending, 1);
  assert.equal(result.oldestActionableMainAgeMs, 10_000);
});

test("incidentes terminais no principal não bloqueiam nova entrega secundária", () => {
  for (const terminal of [
    {
      status: "delivery_unknown",
      main_delivery_unknown_at: "2026-08-04T19:00:00.000Z",
    },
    { status: "delivery_dead_letter", main_delivery_attempts: 10 },
    { status: "delivery_blocked_configuration", mainDeliveryReady: false },
  ]) {
    const queueSlo = summarizeQueueSlo([
      {
        ...terminal,
        first_seen_at: "2026-08-04T19:00:00.000Z",
        main_sent_at: "",
      },
      {
        status: "partial_delivery",
        first_seen_at: "2026-08-04T19:59:55.000Z",
        main_sent_at: "2026-08-04T19:59:56.000Z",
      },
    ], now, { mainDeliveryReady: terminal.mainDeliveryReady ?? true });
    const budget = chooseDeliveryBudget({ queueSlo });

    // Incidents remain visible; they are excluded only from scheduling pressure.
    assert.equal(queueSlo.pending, 2);
    assert.equal(queueSlo.criticalPending, 1);
    assert.equal(queueSlo.secondaryPending, 1);
    assert.equal(queueSlo.oldestAgeMs, 60 * 60_000);
    assert.equal(queueSlo.actionableMainPending, 0);
    assert.equal(queueSlo.oldestActionableMainAgeMs, 0);
    assert.equal(budget.deferSecondary, false, terminal.status);
    assert.equal(budget.reason, "healthy", terminal.status);
  }
});

test("principal pendente elegível há 45 segundos mantém prioridade", () => {
  const queueSlo = summarizeQueueSlo([{
    status: "delivery_pending",
    first_seen_at: "2026-08-04T19:59:15.000Z",
    main_sent_at: "",
    main_delivery_attempts: 1,
    main_delivery_next_attempt_at: "2026-08-04T20:00:00.000Z",
  }], now);

  assert.equal(queueSlo.actionableMainPending, 1);
  assert.equal(queueSlo.oldestActionableMainAgeMs, 45_000);
  assert.equal(chooseDeliveryBudget({ queueSlo }).reason, "critical_backlog");
  assert.equal(chooseDeliveryBudget({ queueSlo }).deferSecondary, true);
});

test("secundário antigo não empresta sua idade ao principal recente", () => {
  const queueSlo = summarizeQueueSlo([
    {
      status: "delivery_pending",
      first_seen_at: "2026-08-04T19:59:50.000Z",
      main_sent_at: "",
    },
    {
      status: "partial_delivery",
      first_seen_at: "2026-08-04T19:00:00.000Z",
      main_sent_at: "2026-08-04T19:00:01.000Z",
    },
  ], now);

  assert.equal(queueSlo.oldestAgeMs, 60 * 60_000);
  assert.equal(queueSlo.oldestActionableMainAgeMs, 10_000);
  assert.equal(chooseDeliveryBudget({ queueSlo }).deferSecondary, false);
});

test("backoff e envio em voo não pressionam o orçamento secundário", () => {
  for (const delivery of [
    { main_delivery_next_attempt_at: "2026-08-04T20:00:01.000Z" },
    { main_delivery_in_flight_at: "2026-08-04T19:59:59.000Z" },
    { main_delivery_in_flight_at: "2026-08-04T19:58:00.000Z" },
    {
      delivery_unknown_target: "main",
      delivery_unknown_at: "2026-08-04T19:00:00.000Z",
    },
  ]) {
    const queueSlo = summarizeQueueSlo([{
      ...delivery,
      status: "delivery_pending",
      first_seen_at: "2026-08-04T19:00:00.000Z",
      main_sent_at: "",
    }], now);

    assert.equal(queueSlo.criticalPending, 1);
    assert.equal(queueSlo.actionableMainPending, 0);
    assert.equal(chooseDeliveryBudget({ queueSlo }).deferSecondary, false);
  }
});

test("geração anterior mantém incidente visível sem bloquear a geração atual", () => {
  const queueSlo = summarizeQueueSlo([{
    status: "delivery_pending",
    first_seen_at: "2026-08-04T19:00:00.000Z",
    main_sent_at: "",
    delivery_generation: 1,
  }], now, { currentGeneration: 2 });

  assert.equal(queueSlo.pending, 1);
  assert.equal(queueSlo.criticalPending, 1);
  assert.equal(queueSlo.actionableMainPending, 0);
  assert.equal(chooseDeliveryBudget({ queueSlo }).deferSecondary, false);
});

test("linha sem intenção ou decisão de envio não pressiona secundários", () => {
  for (const eligibility of [
    { would_send_main: 0, decision_at: "2026-08-04T19:00:00.000Z" },
    { would_send_main: 1, decision_at: "" },
  ]) {
    const queueSlo = summarizeQueueSlo([{
      ...eligibility,
      status: "delivery_pending",
      first_seen_at: "2026-08-04T19:00:00.000Z",
      main_sent_at: "",
    }], now);

    assert.equal(queueSlo.criticalPending, 1);
    assert.equal(queueSlo.actionableMainPending, 0);
    assert.equal(chooseDeliveryBudget({ queueSlo }).deferSecondary, false);
  }
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

test("mantém prioridade secundária somente para decisões da última hora", () => {
  assert.equal(isRecentDeliveryDecision({
    decision_at: "2026-08-04T19:00:00.000Z",
  }, now), true);
  assert.equal(isRecentDeliveryDecision({
    decision_at: "2026-08-04T18:59:59.999Z",
  }, now), false);
  assert.equal(isRecentDeliveryDecision({
    decision_at: "2026-08-04T20:00:00.001Z",
  }, now), false);
  assert.equal(isRecentDeliveryDecision({ decision_at: "inválido" }, now), false);
  assert.equal(isRecentDeliveryDecision({}, now), false);
});
