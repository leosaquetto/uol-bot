import test from "node:test";
import assert from "node:assert/strict";
import { classifyHeadlessHealth } from "../src/headless-health.js";

const now = new Date("2026-08-04T20:00:00.000Z");

function healthyReadiness(overrides = {}) {
  return {
    status: 200,
    body: {
      ok: true,
      worker: "uol-telegram-shadow-pilot",
      versionId: "version-1",
      mode: "live",
      lastScanAt: "2026-08-04T19:59:50.000Z",
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
      ...overrides,
    },
  };
}

function healthyLiveness(overrides = {}) {
  return {
    status: 200,
    body: {
      ok: true,
      worker: "uol-telegram-shadow-pilot",
      versionId: "version-1",
      ...overrides,
    },
  };
}

test("classifica Worker vivo e pronto como healthy", () => {
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness: healthyReadiness(),
    now,
  });

  assert.equal(result.state, "healthy");
  assert.equal(result.hardFailure, false);
  assert.deepEqual(result.reasons, []);
});

test("classifica manutenção adiada pela reserva como degraded, não outage", () => {
  const readiness = healthyReadiness({
    checks: {
      ...healthyReadiness().body.checks,
      maintenanceDeferred: true,
    },
  });
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness,
    now,
  });

  assert.equal(result.state, "degraded");
  assert.equal(result.hardFailure, false);
  assert.deepEqual(result.reasons, ["maintenance_deferred"]);
});

test("classifica projeção de escrita fora do free tier como degraded", () => {
  const readiness = healthyReadiness({
    status: 503,
    ok: false,
    checks: {
      ...healthyReadiness().body.checks,
      storageWriteBudgetHealthy: false,
    },
  });
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness,
    now,
  });

  assert.equal(result.state, "degraded");
  assert.equal(result.hardFailure, false);
  assert.deepEqual(result.reasons, ["write_quota_risk"]);
});

test("classifica incidentes históricos com scan fresco como degraded", () => {
  const readiness = healthyReadiness({
    status: 503,
    ok: false,
    checks: {
      alarmFresh: true,
      scanFresh: true,
      maintenanceFresh: true,
      deliveryConfigured: true,
      criticalIncidents: 1,
      deadLetters: 0,
      unknown: 1,
      blockedConfiguration: 0,
      maintenanceDeadLetters: 0,
      storageReadBudgetHealthy: true,
    },
  });
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness,
    now,
  });

  assert.equal(result.state, "degraded");
  assert.equal(result.hardFailure, false);
  assert.deepEqual(result.reasons, ["critical_incidents", "unknown_deliveries"]);
});

test("classifica scan antigo como outage mesmo com liveness vivo", () => {
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness: healthyReadiness({
      status: 503,
      ok: false,
      lastScanAt: "2026-08-04T19:50:00.000Z",
      checks: {
        alarmFresh: false,
        scanFresh: false,
        maintenanceFresh: true,
        deliveryConfigured: true,
        criticalIncidents: 0,
        deadLetters: 0,
        unknown: 0,
        blockedConfiguration: 0,
        maintenanceDeadLetters: 0,
        storageReadBudgetHealthy: true,
      },
    }),
    now,
    maxScanAgeMs: 180_000,
  });

  assert.equal(result.state, "outage");
  assert.equal(result.hardFailure, true);
  assert.deepEqual(result.reasons, ["alarm_stale", "scan_stale"]);
});

test("classifica resposta de liveness ausente como outage", () => {
  const result = classifyHeadlessHealth({
    liveness: { status: 503, body: null },
    readiness: null,
    now,
  });

  assert.equal(result.state, "outage");
  assert.equal(result.hardFailure, true);
  assert.deepEqual(result.reasons, ["liveness_http", "liveness_body", "readiness_missing"]);
});

test("classifica configuração não live como outage e preserva snapshot sanitizado", () => {
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness({ versionId: "version-1" }),
    readiness: healthyReadiness({
      status: 503,
      ok: false,
      mode: "shadow",
      checks: {
        alarmFresh: true,
        scanFresh: true,
        maintenanceFresh: true,
        deliveryConfigured: false,
        criticalIncidents: 0,
        deadLetters: 0,
        unknown: 0,
        blockedConfiguration: 0,
        maintenanceDeadLetters: 0,
        storageReadBudgetHealthy: true,
      },
    }),
    now,
  });

  assert.equal(result.state, "outage");
  assert.equal(result.hardFailure, true);
  assert.ok(result.reasons.includes("mode_not_live"));
  assert.ok(result.reasons.includes("delivery_unconfigured"));
  assert.deepEqual(Object.keys(result.snapshot).sort(), [
    "checks",
    "lastScanAt",
    "mode",
    "readinessStatus",
    "versionId",
  ]);
});
