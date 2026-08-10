import assert from "node:assert/strict";
import test from "node:test";

import {
  classifyDeliveryRow,
  deliveryConfiguration,
  deliveryRetryAt,
  envFlag,
  isAmbiguousDeliveryError,
} from "../src/delivery-state.js";

const now = new Date("2026-08-02T12:00:00.000Z");

function row(overrides = {}) {
  return {
    main_sent_at: "",
    main_delivery_attempts: 0,
    main_delivery_next_attempt_at: "",
    main_delivery_in_flight_at: "",
    main_delivery_unknown_at: "",
    would_send_canal2: 0,
    canal2_sent_at: "",
    canal2_delivery_attempts: 0,
    canal2_delivery_next_attempt_at: "",
    canal2_delivery_in_flight_at: "",
    canal2_delivery_unknown_at: "",
    discord_sent_at: "",
    discord_delivery_attempts: 0,
    discord_delivery_next_attempt_at: "",
    discord_delivery_in_flight_at: "",
    discord_delivery_unknown_at: "",
    delivery_unknown_at: "",
    delivery_unknown_target: "",
    ...overrides,
  };
}

const ready = {
  main: { enabled: true, ready: true },
  canal2: { enabled: true, ready: true },
  discord: { enabled: true, ready: true },
};

test("separa configuração principal, Canal 2 e Discord", () => {
  const configuration = deliveryConfiguration({
    CANAL2_DELIVERY_ENABLED: "true",
    DISCORD_DELIVERY_ENABLED: "true",
  }, {
    tokenConfigured: true,
    mainConfigured: true,
    canal2Configured: false,
    mainReady: true,
    canal2Ready: false,
  }, { configured: false });
  assert.equal(configuration.main.ready, true);
  assert.equal(configuration.canal2.ready, false);
  assert.equal(configuration.discord.ready, false);
});

test("destino principal acionável não é bloqueado por secundários", () => {
  const configuration = {
    main: { enabled: true, ready: true },
    canal2: { enabled: true, ready: false },
    discord: { enabled: true, ready: false },
  };
  const result = classifyDeliveryRow(row({ would_send_canal2: 1 }), configuration, {
    ticket: true,
    now,
  });
  assert.equal(result.state, "actionable");
  assert.deepEqual(result.actionable.map((target) => target.target), ["main"]);
});

test("tentativas esgotadas viram dead letter sem ocupar lote acionável", () => {
  const exhausted = classifyDeliveryRow(row({ main_delivery_attempts: 10 }), ready, {
    maxAttempts: 10,
    now,
  });
  const next = classifyDeliveryRow(row(), ready, { maxAttempts: 10, now });
  assert.equal(exhausted.state, "dead_letter");
  assert.equal(next.state, "actionable");
});

test("in-flight persistido é tratado como resultado externo incerto", () => {
  const result = classifyDeliveryRow(row({
    main_delivery_in_flight_at: "2026-08-02T11:59:00.000Z",
  }), ready, { now });
  assert.deepEqual(result, {
    state: "unknown",
    target: "main",
    targets: ["main"],
    staleUnknownTargets: ["main"],
    actionable: [],
  });
});

test("in-flight recente continua ativo e não vira unknown prematuro", () => {
  const result = classifyDeliveryRow(row({
    main_delivery_in_flight_at: "2026-08-02T11:59:45.000Z",
  }), ready, { now, inFlightStaleSeconds: 60 });
  assert.deepEqual(result, {
    state: "in_flight",
    targets: ["main"],
    actionable: [],
  });
});

test("lane principal ignora estado secundário sem alterá-lo", () => {
  const result = classifyDeliveryRow(row({
    discord_delivery_unknown_at: "2026-08-02T11:59:00.000Z",
  }), ready, { ticket: true, now, targetNames: ["main"] });
  assert.equal(result.state, "actionable");
  assert.deepEqual(result.actionable.map((target) => target.target), ["main"]);
  assert.deepEqual(result.unknownTargets, []);
});

test("resultado incerto de um destino não bloqueia outro destino independente", () => {
  const result = classifyDeliveryRow(row({
    main_delivery_unknown_at: "2026-08-02T11:59:00.000Z",
  }), ready, { ticket: true, now });
  assert.equal(result.state, "actionable");
  assert.deepEqual(result.actionable.map((target) => target.target), ["discord"]);
  assert.deepEqual(result.unknownTargets, ["main"]);
});

test("dependência impossível vira dead letter em vez de backoff eterno", () => {
  const result = classifyDeliveryRow(row({
    would_send_canal2: 1,
    main_delivery_attempts: 10,
  }), ready, { maxAttempts: 10, now });
  assert.equal(result.state, "dead_letter");
});

test("backoff respeita retry_after e jitter limitado", () => {
  const retryAt = deliveryRetryAt({ retryAfterSeconds: 120 }, 4, now, 0.5);
  assert.equal(retryAt, "2026-08-02T12:02:09.000Z");
  assert.equal(
    deliveryRetryAt({ retryAfterSeconds: 120 }, 4, now, 0),
    "2026-08-02T12:02:00.000Z",
  );
  assert.equal(classifyDeliveryRow(row({
    main_delivery_next_attempt_at: retryAt,
  }), ready, { now }).state, "backoff");
  assert.equal(
    deliveryRetryAt({ retryAfterSeconds: 7_200 }, 20, now, 0),
    "2026-08-02T14:00:00.000Z",
  );
  const extreme = deliveryRetryAt({ retryAfterSeconds: Number.MAX_VALUE }, 20, now, 1);
  assert.equal(Number.isFinite(Date.parse(extreme)), true);
  assert.equal(Date.parse(extreme) >= now.getTime(), true);
});

test("classifica timeout como ambíguo e flags explicitamente", () => {
  assert.equal(isAmbiguousDeliveryError(new DOMException("timed out", "TimeoutError")), true);
  assert.equal(envFlag("false", true), false);
  assert.equal(envFlag("true", false), true);
});
