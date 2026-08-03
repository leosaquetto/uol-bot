import assert from "node:assert/strict";
import test from "node:test";

import {
  buildIncidentSignals,
  buildLatencyMetrics,
  buildOperationsAlert,
} from "../src/operations.js";

test("resume latências ponta a ponta sem aceitar relógio invertido", () => {
  const metrics = buildLatencyMetrics([
    {
      id: "nova",
      title: "2 ingressos",
      first_seen_at: "2026-08-01T12:00:00.000Z",
      discord_sent_at: "2026-08-01T12:00:01.000Z",
      main_sent_at: "2026-08-01T12:00:02.000Z",
      canal2_sent_at: "2026-08-01T12:00:03.000Z",
      comment_sent_at: "2026-08-01T12:00:04.000Z",
    },
    {
      id: "anterior",
      title: "outra oferta",
      first_seen_at: "2026-08-01T11:00:00.000Z",
      discord_sent_at: "",
      main_sent_at: "2026-08-01T10:59:59.000Z",
      canal2_sent_at: "",
      comment_sent_at: "",
    },
  ], new Date("2026-08-01T12:05:00.000Z"));

  assert.equal(metrics.telegram.samples, 1);
  assert.equal(metrics.telegram.latestMs, 2_000);
  assert.equal(metrics.discordToTelegram.p95Ms, 1_000);
  assert.equal(metrics.latest[0].commentMs, 4_000);
});

test("só abre incidente de API genérico após três falhas", () => {
  assert.deepEqual(buildIncidentSignals({
    apiError: "network timeout",
    apiFailureStreak: 2,
  }), []);
  const [signal] = buildIncidentSignals({
    apiError: "network timeout",
    apiFailureStreak: 3,
  });
  assert.equal(signal.key, "ticket-api");
  assert.equal(signal.severity, "warning");
});

test("falha de autorização é crítica imediatamente e sinais são deduplicáveis", () => {
  const signals = buildIncidentSignals({
    apiError: "uol_api_http_401",
    apiFailureStreak: 1,
    webhookPendingUpdates: 2,
    failedRunStreak: 3,
    ticketIssues: [{ id: "pAZ", title: "2 INGRESSOS", missingPhoto: true }],
  });
  assert.deepEqual(signals.map((signal) => signal.key), [
    "ticket-api",
    "telegram-webhook",
    "failed-scans",
    "ticket-delivery:pAZ",
  ]);
  assert.equal(signals[0].severity, "critical");
  assert.match(buildOperationsAlert(signals[0]), /Alerta do monitor Clube UOL/);
  assert.match(buildOperationsAlert(signals[0], { recovered: true }), /Incidente resolvido/);
});

test("abre incidente quando API e HTML permanecem divergentes", () => {
  const signals = buildIncidentSignals({
    sourceDivergenceStreak: 5,
    secondsSinceFullSourceSuccess: 90,
    sourceDetails: "API 3, HTML 0",
  });
  assert.equal(signals[0].key, "source-health");
  assert.match(signals[0].details, /divergências: 5/);
});

test("avisa antes de a credencial técnica vencer sem tratar o HTML como indisponível", () => {
  const now = new Date("2026-08-02T00:00:00.000Z");
  const signals = buildIncidentSignals({
    apiAuthorizationExpiresAt: "2026-08-12T00:00:00.000Z",
    now,
  });
  assert.equal(signals[0].key, "ticket-api-expiry");
  assert.equal(signals[0].severity, "warning");
  assert.match(signals[0].details, /duas fontes HTML públicas continuarão ativas/);
  assert.deepEqual(buildIncidentSignals({
    apiAuthorizationExpiresAt: "2026-08-20T00:00:00.000Z",
    now,
  }), []);
});

test("abre incidentes distintos para dead letter, entrega incerta e configuração", () => {
  const signals = buildIncidentSignals({
    deliveryIssues: [
      { id: "p1", title: "Oferta 1", target: "main", state: "dead_letter" },
      { id: "p2", title: "Oferta 2", target: "discord", state: "unknown" },
      { id: "p3", title: "Oferta 3", target: "canal2", state: "blocked_configuration" },
    ],
  });
  assert.deepEqual(signals.map((signal) => signal.severity), [
    "critical",
    "critical",
    "warning",
  ]);
  assert.deepEqual(signals.map((signal) => signal.key), [
    "delivery-queue:p1:main",
    "delivery-queue:p2:discord",
    "delivery-queue:p3:canal2",
  ]);
});
