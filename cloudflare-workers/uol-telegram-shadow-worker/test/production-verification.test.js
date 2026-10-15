import assert from "node:assert/strict";
import test from "node:test";

import { validateProductionHealth, productionBaseline, productionDebtRegressions, reportProductionHealth } from "../src/production-verification.js";

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

function debtSnapshot(snapshot) {
  const keys = ["criticalIncidents", "deadLetters", "unknown", "maintenanceDeadLetters"];
  return {
    worker: WORKER_NAME,
    versionId: snapshot.readiness.versionId,
    checkedAt: NOW.toISOString(),
    checks: Object.fromEntries(keys.map(key => [key, snapshot.readiness.checks[key]])),
    identities: Object.fromEntries(keys.map(key => [key,
      Array.from({ length: snapshot.readiness.checks[key] }, (_, index) => `${key}:${index}`)])),
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
      error: "liveness_worker_identity_mismatch",
      mutate: (snapshot) => { snapshot.liveness.worker = ""; },
    },
    {
      error: "readiness_worker_identity_mismatch",
      mutate: (snapshot) => { snapshot.readiness.worker = ""; },
    },
    {
      error: "version_metadata_missing",
      mutate: (snapshot) => { snapshot.liveness.versionId = ""; },
    },
    {
      error: "version_metadata_missing",
      mutate: (snapshot) => { snapshot.readiness.versionId = ""; },
    },
    {
      error: "version_metadata_mismatch",
      mutate: (snapshot) => { snapshot.liveness.versionId = "version-old"; },
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

test("exige recomendação de polling positiva e de no máximo 30 segundos", () => {
  const slow = healthySnapshot();
  slow.readiness.storageReadBudget.recommendedPollIntervalSeconds = 31;
  assert.throws(() => validate(slow), /recommended_poll_interval_too_slow:31/);

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
  unsafe.readinessStatus = 503;
  unsafe.readiness.ok = false;
  unsafe.readiness.checks.storageReadBudgetHealthy = false;
  unsafe.readiness.storageReadBudget.primaryAllowed = false;
  unsafe.readiness.storageReadBudget.withinFreeTier = false;
  assert.throws(() => validate(unsafe), /storage_read_budget_not_healthy/);
});

test("falha fechado para readiness 503 sem motivo degradado reconhecido", () => {
  const snapshot = healthySnapshot();
  snapshot.readinessStatus = 503;
  snapshot.readiness.ok = false;

  assert.throws(
    () => validate(snapshot),
    /not_ready:outage:readiness_protocol/,
  );
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


test("relatório distingue runtime válido de dívida histórica sem alterar readiness", () => {
  const snapshot = healthySnapshot();
  snapshot.readinessStatus = 503;
  snapshot.readiness.ok = false;
  snapshot.readiness.checks.criticalIncidents = 3;
  snapshot.readiness.checks.unknown = 4;
  const baseline = productionBaseline(debtSnapshot(snapshot));
  const options = { expectedVersionId: EXPECTED_VERSION_ID, expectedMode: "live", now: NOW };
  const result = reportProductionHealth(snapshot, options, baseline, debtSnapshot(snapshot));
  assert.equal(result.published, true);
  assert.equal(result.runtimeVerified, true);
  assert.equal(result.ready, false);
  assert.equal(result.ok, true);
  assert.equal(result.pending.length, 2);
  assert.throws(() => validate(snapshot), /not_ready:degraded/);
  assert.equal(reportProductionHealth(snapshot, options, null, debtSnapshot(snapshot)).ok, false);
  snapshot.readiness.checks.unknown += 1;
  assert.deepEqual(reportProductionHealth(snapshot, options, baseline, debtSnapshot(snapshot)).regressions, ["unknown:increased", "unknown:new_identities:1"]);
});

test("dívida histórica não esconde regressão de versão, ciclo ou orçamento", () => {
  const snapshot = healthySnapshot();
  snapshot.readinessStatus = 503;
  snapshot.readiness.ok = false;
  snapshot.readiness.checks.deadLetters = 2;
  const baseline = productionBaseline(debtSnapshot(snapshot));
  snapshot.readiness.storageReadBudget.primaryLastSuccessfulVersionId = "previous";
  const result = reportProductionHealth(snapshot, { expectedVersionId: EXPECTED_VERSION_ID, now: NOW }, baseline, debtSnapshot(snapshot));
  assert.equal(result.published, true);
  assert.equal(result.runtimeVerified, false);
  assert.equal(result.ok, false);
  assert.match(result.runtimeError, /primary_cycle_version_mismatch/);
});


test("baseline rejeita contadores ausentes ou inválidos e identidades inconsistentes", () => {
  const baseline = debtSnapshot(healthySnapshot());
  for (const value of [undefined, -1, 0.5, "0", NaN, Number.MAX_SAFE_INTEGER + 1]) {
    const invalid = structuredClone(baseline);
    invalid.checks.unknown = value;
    assert.throws(() => productionBaseline(invalid), /baseline_checks_invalid:unknown/);
  }
  for (const values of [["duplicado", "duplicado"], [], [""], ["   "]]) {
    const invalid = structuredClone(baseline);
    invalid.checks.unknown = 1;
    invalid.identities.unknown = values;
    assert.throws(() => productionBaseline(invalid), /baseline_identities_invalid:unknown/);
  }
  const granular = structuredClone(baseline);
  granular.checks.maintenanceDeadLetters = 1;
  granular.identities.maintenanceDeadLetters = ["offer:main", "offer:canal2"];
  assert.equal(productionBaseline(granular).identities.maintenanceDeadLetters.length, 2);
});

test("nova identidade bloqueia mesmo quando total fica igual ou diminui", () => {
  const snapshot = healthySnapshot();
  snapshot.readinessStatus = 503;
  snapshot.readiness.ok = false;
  snapshot.readiness.checks.unknown = 2;
  const baseline = productionBaseline(debtSnapshot(snapshot));
  const current = debtSnapshot(snapshot);
  current.identities.unknown = ["unknown:0", "unknown:novo"];
  const options = { expectedVersionId: EXPECTED_VERSION_ID, now: NOW };
  const sameTotal = reportProductionHealth(snapshot, options, baseline, current);
  assert.equal(sameTotal.debtVerified, true);
  assert.equal(sameTotal.ok, false);
  assert.deepEqual(sameTotal.regressions, ["unknown:new_identities:1"]);
  snapshot.readiness.checks.unknown = 1;
  current.checks.unknown = 1;
  current.identities.unknown = ["unknown:novo"];
  assert.equal(reportProductionHealth(snapshot, options, baseline, current).ok, false);
  assert.deepEqual(baseline.identities.unknown, ["unknown:0", "unknown:1"]);
});

test("contador de readiness inválido impede o gate mesmo sem dívida", () => {
  const snapshot = healthySnapshot();
  const baseline = debtSnapshot(snapshot);
  snapshot.readiness.checks.deadLetters = "0";
  const result = reportProductionHealth(snapshot, { expectedVersionId: EXPECTED_VERSION_ID, now: NOW }, baseline, baseline);
  assert.equal(result.runtimeVerified, false);
  assert.equal(result.ok, false);
  assert.match(result.runtimeError, /readiness_checks_invalid:deadLetters/);
  assert.throws(() => validate(snapshot), /readiness_checks_invalid:deadLetters/);
});

test("bootstrap exige snapshot pós válido e informa dívida ainda não verificada", () => {
  const snapshot = healthySnapshot();
  const options = { expectedVersionId: EXPECTED_VERSION_ID, now: NOW, bootstrapDebt: true };
  const result = reportProductionHealth(snapshot, options, null, debtSnapshot(snapshot));
  assert.equal(result.ok, true);
  assert.equal(result.debtVerified, false);
  assert.equal(reportProductionHealth(snapshot, options).ok, false);
  const stale = debtSnapshot(snapshot);
  stale.versionId = "previous";
  assert.deepEqual(reportProductionHealth(snapshot, options, null, stale).regressions, ["debt_version_mismatch"]);
  const mismatch = debtSnapshot(snapshot);
  mismatch.checks.deadLetters = 1;
  mismatch.identities.deadLetters = ["new"];
  assert.deepEqual(reportProductionHealth(snapshot, options, null, mismatch).regressions, ["debt_readiness_mismatch:deadLetters"]);
});

test("baseline original também impede aumento de contagem com identidades granulares conhecidas", () => {
  const snapshot = healthySnapshot();
  snapshot.readiness.checks.maintenanceDeadLetters = 1;
  const baseline = debtSnapshot(snapshot);
  baseline.identities.maintenanceDeadLetters.push("maintenance:second");
  const current = structuredClone(baseline);
  current.checks.maintenanceDeadLetters = 2;
  assert.deepEqual(productionDebtRegressions(baseline, current), ["maintenanceDeadLetters:increased"]);
});
