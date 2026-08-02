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
