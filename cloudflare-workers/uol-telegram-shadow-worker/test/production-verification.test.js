import assert from "node:assert/strict";
import test from "node:test";

import { validateProductionHealth } from "../src/production-verification.js";

const NOW = new Date("2026-08-10T15:00:00.000Z");
const EXPECTED_VERSION_ID = "version-current";
const WORKER_NAME = "uol-telegram-shadow-pilot";

function healthySnapshot() {
  return {
    livenessStatus: 200,
    readinessStatus: 200,
    liveness: {
      ok: true,
      worker: WORKER_NAME,
      versionId: EXPECTED_VERSION_ID,
    },
    readiness: {
      ok: true,
      worker: WORKER_NAME,
      versionId: EXPECTED_VERSION_ID,
      mode: "live",
      lastScanAt: "2026-08-10T14:59:45.000Z",
      checks: {
        alarmFresh: true,
        scanFresh: true,
        maintenanceFresh: true,
        maintenanceDeferred: false,
        deliveryConfigured: true,
        criticalIncidents: 0,
        deadLetters: 0,
        unknown: 0,
        blockedConfiguration: 0,
        maintenanceDeadLetters: 0,
        storageReadBudgetHealthy: true,
        storageWriteBudgetHealthy: true,
      },
      queueSlo: {
        pending: 0,
        criticalPending: 0,
        secondaryPending: 0,
      },
      storageReadBudget: {
        rowsWritten: 37_000,
        primaryLastSuccessfulAt: "2026-08-10T14:59:45.000Z",
        primaryLastSuccessfulVersionId: EXPECTED_VERSION_ID,
        primaryLastSuccessfulRowsRead: 160,
        primaryEstimatedRowsRead: 80,
        observedPrimaryEstimate: 80,
        primaryRowsWithSafety: 120,
        primaryMaxRowsRead: 2_000,
        recommendedPollIntervalSeconds: 15,
        primaryAllowed: true,
        withinFreeTier: true,
      },
      storageWriteBudget: {
        limit: 100_000,
        projected: 62_000,
        headroom: 38_000,
        withinFreeTier: true,
      },
    },
  };
}

function validate(snapshot) {
  return validateProductionHealth(snapshot, {
    expectedVersionId: EXPECTED_VERSION_ID,
    expectedMode: "live",
    maxScanAgeMs: 180_000,
    now: NOW,
  });
}

test("aceita readiness segura com manutenção adiada e sem mutar o snapshot", () => {
  const snapshot = healthySnapshot();
  snapshot.readiness.checks.maintenanceDeferred = true;
  const original = structuredClone(snapshot);

  const summary = validate(snapshot);

  assert.equal(summary.ok, true);
  assert.equal(summary.versionId, EXPECTED_VERSION_ID);
  assert.equal(summary.headlessState, "degraded");
  assert.deepEqual(summary.headlessReasons, ["maintenance_deferred"]);
  assert.deepEqual(snapshot, original);
});

test("exige a versão esperada", () => {
  assert.throws(
    () => validateProductionHealth(healthySnapshot(), { now: NOW }),
    /expected_version_id_required/,
  );
});

test("rejeita identidade, readiness, versão e scan inválidos", () => {
  const cases = [
    {
      error: "liveness_worker_identity_mismatch",
      mutate: (snapshot) => { snapshot.liveness.worker = "other-worker"; },
    },
    {
      error: "readiness_worker_identity_mismatch",
      mutate: (snapshot) => { snapshot.readiness.worker = "other-worker"; },
    },
    {
      error: "not_ready:",
      mutate: (snapshot) => {
        snapshot.readinessStatus = 503;
        snapshot.readiness.ok = false;
      },
    },
    {
      error: "deployed_version_id_mismatch",
      mutate: (snapshot) => {
        snapshot.liveness.versionId = "version-old";
        snapshot.readiness.versionId = "version-old";
      },
    },
    {
      error: "scan_missing_or_stale",
      mutate: (snapshot) => { snapshot.readiness.lastScanAt = "2026-08-10T14:56:59.000Z"; },
    },
  ];

  for (const { error, mutate } of cases) {
    const snapshot = healthySnapshot();
    mutate(snapshot);
    assert.throws(() => validate(snapshot), new RegExp(error));
  }
});

test("exige fila crítica zerada", () => {
  const pending = healthySnapshot();
  pending.readiness.queueSlo.criticalPending = 1;
  assert.throws(() => validate(pending), /critical_queue_pending:1/);

  const missing = healthySnapshot();
  delete missing.readiness.queueSlo.criticalPending;
  assert.throws(() => validate(missing), /critical_queue_missing/);
});

test("exige recomendação de polling positiva e de no máximo 15 segundos", () => {
  const slow = healthySnapshot();
  slow.readiness.storageReadBudget.recommendedPollIntervalSeconds = 16;
  assert.throws(() => validate(slow), /recommended_poll_interval_too_slow:16/);

  const missing = healthySnapshot();
  delete missing.readiness.storageReadBudget.recommendedPollIntervalSeconds;
  assert.throws(() => validate(missing), /recommended_poll_interval_missing/);
});

test("exige estimativa primária limitada e margem conservadora", () => {
  const high = healthySnapshot();
  high.readiness.storageReadBudget.primaryEstimatedRowsRead = 513;
  high.readiness.storageReadBudget.observedPrimaryEstimate = 513;
  high.readiness.storageReadBudget.primaryRowsWithSafety = 770;
  assert.throws(() => validate(high), /primary_estimate_too_high:513/);

  const inconsistent = healthySnapshot();
  inconsistent.readiness.storageReadBudget.primaryEstimatedRowsRead = 100;
  inconsistent.readiness.storageReadBudget.observedPrimaryEstimate = 80;
  assert.throws(() => validate(inconsistent), /primary_estimate_inconsistent/);

  const optimistic = healthySnapshot();
  optimistic.readiness.storageReadBudget.primaryEstimatedRowsRead = 100;
  optimistic.readiness.storageReadBudget.observedPrimaryEstimate = 100;
  optimistic.readiness.storageReadBudget.primaryRowsWithSafety = 149;
  assert.throws(() => validate(optimistic), /primary_safety_estimate_not_conservative/);

  const singleCycleRegression = healthySnapshot();
  singleCycleRegression.readiness.storageReadBudget.primaryMaxRowsRead = 4_097;
  assert.throws(
    () => validate(singleCycleRegression),
    /primary_single_cycle_too_high:4097/,
  );
});

test("exige ciclo primário bem-sucedido da versão publicada", () => {
  const cases = [
    {
      error: "primary_cycle_version_mismatch",
      mutate: (snapshot) => {
        snapshot.readiness.storageReadBudget.primaryLastSuccessfulVersionId = "version-old";
      },
    },
    {
      error: "primary_cycle_missing",
      mutate: (snapshot) => {
        snapshot.readiness.storageReadBudget.primaryLastSuccessfulRowsRead = 0;
      },
    },
    {
      error: "primary_cycle_stale",
      mutate: (snapshot) => {
        snapshot.readiness.storageReadBudget.primaryLastSuccessfulAt =
          "2026-08-10T14:56:59.000Z";
      },
    },
  ];

  for (const { error, mutate } of cases) {
    const snapshot = healthySnapshot();
    mutate(snapshot);
    assert.throws(() => validate(snapshot), new RegExp(error));
  }
});

test("rejeita outage e orçamento de leitura incoerente mesmo com HTTP 200", () => {
  const outage = healthySnapshot();
  outage.readiness.checks.alarmFresh = false;
  assert.throws(() => validate(outage), /headless_outage:alarm_stale/);

  const unsafe = healthySnapshot();
  unsafe.readiness.storageReadBudget.primaryAllowed = false;
  unsafe.readiness.storageReadBudget.withinFreeTier = false;
  assert.throws(() => validate(unsafe), /storage_read_budget_not_healthy/);
});

test("exige projeção e uso real de escritas com margem do free tier", () => {
  const projected = healthySnapshot();
  projected.readiness.storageWriteBudget.projected = 80_001;
  projected.readiness.storageWriteBudget.headroom = 19_999;
  assert.throws(() => validate(projected), /write_projection_too_high:80001/);

  const actual = healthySnapshot();
  actual.readiness.storageReadBudget.rowsWritten = 100_000;
  assert.throws(() => validate(actual), /write_limit_reached:100000/);

  const lowHeadroom = healthySnapshot();
  lowHeadroom.readiness.storageReadBudget.rowsWritten = 80_001;
  assert.throws(() => validate(lowHeadroom), /write_usage_too_high:80001/);

  const missing = healthySnapshot();
  delete missing.readiness.storageWriteBudget;
  assert.throws(() => validate(missing), /write_budget_missing/);
});
