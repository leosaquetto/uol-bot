import test from "node:test";
import assert from "node:assert/strict";
import { classifyHeadlessHealth } from "../src/headless-health.js";
import { readinessChecksOk } from "../src/health-contract.js";

const now = new Date("2026-08-04T20:00:00.000Z");

function healthyReadiness(overrides = {}) {
  const { checks: checkOverrides = {}, ...bodyOverrides } = overrides;
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
        ...checkOverrides,
      },
      ...bodyOverrides,
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

test("decisão produtora mantém manutenção adiada pronta, porém observável", () => {
  const checks = {
    ...healthyReadiness().body.checks,
    maintenanceDeferred: true,
  };

  assert.equal(readinessChecksOk("live", checks), true);
  assert.equal(readinessChecksOk("shadow", checks), false);
  assert.equal(readinessChecksOk("live", { ...checks, scanFresh: false }), false);
  assert.equal(
    readinessChecksOk("live", { ...checks, storageReadBudgetHealthy: false }),
    false,
  );
  assert.equal(readinessChecksOk("live", { ...checks, criticalIncidents: 1 }), false);
});

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
    ok: false,
    checks: {
      storageWriteBudgetHealthy: false,
    },
  });
  readiness.status = 503;
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
    ok: false,
    checks: {
      criticalIncidents: 1,
      unknown: 1,
    },
  });
  readiness.status = 503;
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
  const readiness = healthyReadiness({
    ok: false,
    lastScanAt: "2026-08-04T19:50:00.000Z",
    checks: {
      alarmFresh: false,
      scanFresh: false,
    },
  });
  readiness.status = 503;
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness,
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
  assert.deepEqual(result.reasons, [
    "liveness_http",
    "liveness_body",
    "liveness_worker",
    "liveness_version_missing",
    "readiness_missing",
  ]);
});

test("classifica configuração não live como outage e preserva snapshot sanitizado", () => {
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness({ versionId: "version-1" }),
    readiness: healthyReadiness({
      ok: false,
      mode: "shadow",
      checks: {
        deliveryConfigured: false,
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
    "livenessOk",
    "livenessStatus",
    "mode",
    "readinessOk",
    "readinessStatus",
    "versionId",
    "worker",
  ]);
});

test("falha fechado para HTTP 503 sem degradação reconhecida", () => {
  const readiness = healthyReadiness({ ok: false });
  readiness.status = 503;

  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness,
    now,
  });

  assert.equal(result.state, "outage");
  assert.equal(result.hardFailure, true);
  assert.deepEqual(result.reasons, ["readiness_protocol"]);
});

test("aceita shadow intencional somente como 503/not-ready", () => {
  const readiness = healthyReadiness({ ok: false, mode: "shadow" });
  readiness.status = 503;

  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness,
    expectedMode: "shadow",
    now,
  });

  assert.equal(result.state, "degraded");
  assert.equal(result.hardFailure, false);
  assert.deepEqual(result.reasons, ["intentional_shadow"]);
});

test("exige identidades e versões nos dois endpoints", () => {
  const cases = [
    [healthyLiveness({ worker: "" }), healthyReadiness(), "liveness_worker"],
    [healthyLiveness(), healthyReadiness({ worker: "" }), "readiness_worker"],
    [healthyLiveness({ versionId: "" }), healthyReadiness(), "liveness_version_missing"],
    [healthyLiveness(), healthyReadiness({ versionId: "" }), "readiness_version_missing"],
  ];

  for (const [liveness, readiness, expectedReason] of cases) {
    const result = classifyHeadlessHealth({ liveness, readiness, now });
    assert.equal(result.state, "outage");
    assert.ok(result.reasons.includes(expectedReason));
  }
});

test("rejeita divergência de versão entre liveness e readiness", () => {
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness({ versionId: "version-old" }),
    readiness: healthyReadiness(),
    now,
  });

  assert.equal(result.state, "outage");
  assert.deepEqual(result.reasons, ["version_mismatch"]);
});

test("preserva reserva de leitura explicitamente reconhecida como degraded", () => {
  const readiness = healthyReadiness({
    ok: false,
    checks: { storageReadBudgetHealthy: false },
  });
  readiness.status = 503;

  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness,
    now,
  });

  assert.equal(result.state, "degraded");
  assert.equal(result.hardFailure, false);
  assert.deepEqual(result.reasons, ["quota_reserve"]);
});

test("falha fechado se shadow alegar readiness 200/ok", () => {
  const result = classifyHeadlessHealth({
    liveness: healthyLiveness(),
    readiness: healthyReadiness({ mode: "shadow" }),
    expectedMode: "shadow",
    now,
  });

  assert.equal(result.state, "outage");
  assert.equal(result.hardFailure, true);
  assert.deepEqual(result.reasons, ["readiness_protocol"]);
});
