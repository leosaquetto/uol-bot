import assert from "node:assert/strict";
import test from "node:test";
import {
  completeOperationsEvent, enqueueOperationsEvent, normalizeOperationsOutbox,
  reserveNextOperationsEvent,
} from "../src/operations-outbox.js";

const start = new Date("2026-10-15T12:00:00.000Z");
const later = (ms) => new Date(start.getTime() + ms);
const input = (id, extra = {}) => ({ id, kind: "source-health", severity: "warning", recovered: false, ...extra });
const enqueue = (state, id, extra = {}, now = start) => enqueueOperationsEvent(state, input(id, extra), now).state;

test("dedup immutable metadata only and stable no-op normalization", () => {
  const state = enqueueOperationsEvent(undefined, { ...input("episode:opened"), body: "PRIVATE", unknown: "PRIVATE" }, start).state;
  const copy = JSON.stringify(state);
  const duplicate = enqueueOperationsEvent(state, input("episode:opened"), later(1));
  assert.equal(duplicate.changed, false);
  assert.equal(duplicate.enqueued, false);
  assert.equal(duplicate.reason, "duplicate");
  assert.equal(duplicate.state, state);
  assert.equal(normalizeOperationsOutbox(state, start).changed, false);
  assert.equal(JSON.stringify(state), copy);
  assert.equal(JSON.stringify(state).includes("PRIVATE"), false);
});

test("reserved survives regular reads; explicit crash recovery makes uncertainty terminal", () => {
  const state = enqueue(undefined, "episode:opened");
  const reserved = reserveNextOperationsEvent(state, start);
  assert.equal(state.events[0].status, "pending");
  assert.equal(reserved.event.attempts, 1);
  assert.equal(reserveNextOperationsEvent(reserved.state, start).event, null);
  assert.equal(normalizeOperationsOutbox(reserved.state, start).changed, false);
  const restarted = normalizeOperationsOutbox(JSON.parse(JSON.stringify(reserved.state)), later(1), { recoverReserved: true });
  assert.equal(restarted.state.events[0].status, "uncertain");
  assert.equal(reserveNextOperationsEvent(restarted.state, later(1_000_000)).event, null);
  assert.equal(completeOperationsEvent(restarted.state, "episode:opened", { status: "accepted" }, later(2)).changed, false);
});

test("retry after is respected, delays are one/five minutes, third rejection exhausts", () => {
  let state = enqueue(undefined, "episode:opened");
  state = reserveNextOperationsEvent(state, start).state;
  state = completeOperationsEvent(state, "episode:opened", { status: "retryable", retryAfterMs: 120_000 }, start).state;
  assert.equal(reserveNextOperationsEvent(state, later(60_000)).event, null);
  state = reserveNextOperationsEvent(state, later(120_000)).state;
  state = completeOperationsEvent(state, "episode:opened", { status: "retryable" }, later(120_000)).state;
  assert.equal(reserveNextOperationsEvent(state, later(419_999)).event, null);
  state = reserveNextOperationsEvent(state, later(420_000)).state;
  state = completeOperationsEvent(state, "episode:opened", { status: "retryable" }, later(420_000)).state;
  assert.equal(state.events[0].attempts, 3);
  assert.equal(state.events[0].status, "exhausted");
  assert.equal(reserveNextOperationsEvent(state, later(999_999)).event, null);
});

test("accepted, uncertain and permanent are terminal and never retry", () => {
  for (const status of ["accepted", "uncertain", "permanent"]) {
    const reservation = reserveNextOperationsEvent(enqueue(undefined, "episode:opened"), start);
    const completed = completeOperationsEvent(reservation.state, "episode:opened", { status }, start);
    assert.equal(completed.state.events[0].status, status);
    assert.equal(reserveNextOperationsEvent(completed.state, later(10 * 60_000)).event, null);
    assert.equal(completeOperationsEvent(completed.state, "episode:opened", { status }, start).changed, false);
  }
});

test("provider Retry-After blocks every incident and survives restart and terminal exhaustion", () => {
  let state = enqueue(enqueue(undefined, "episode:opened"), "other:opened", { severity: "critical" });
  state = reserveNextOperationsEvent(state, start).state;
  state = completeOperationsEvent(state, "episode:opened", { status: "retryable", retryAfterMs: 120_000 }, start).state;
  assert.equal(state.retryAt, later(120_000).toISOString());
  const restarted = normalizeOperationsOutbox(JSON.parse(JSON.stringify(state)), later(1), { recoverReserved: true });
  assert.equal(restarted.changed, false);
  assert.equal(reserveNextOperationsEvent(restarted.state, later(119_999)).event, null);
  state = reserveNextOperationsEvent(restarted.state, later(120_000)).state;
  state = completeOperationsEvent(state, "episode:opened", { status: "retryable" }, later(120_000)).state;
  state = reserveNextOperationsEvent(state, later(420_000)).state;
  state = completeOperationsEvent(state, "episode:opened", { status: "retryable", retryAfterMs: 180_000 }, later(420_000)).state;
  assert.equal(state.events[0].status, "exhausted");
  assert.equal(state.retryAt, later(600_000).toISOString());
  assert.equal(reserveNextOperationsEvent(state, later(599_999)).event, null);
  assert.equal(enqueueOperationsEvent(state, input("other:opened"), later(500_000)).reason, "duplicate");
  assert.equal(reserveNextOperationsEvent(state, later(600_000)).event.id, "other:opened");
  assert.equal(normalizeOperationsOutbox({ ...state, retryAt: "invalid" }, later(1)).state.retryAt, undefined);
});

test("20 rolling attempts with five protected for critical and recovery", () => {
  let state;
  for (let index = 0; index < 15; index++) {
    state = enqueue(state, `ordinary_${index}`);
    const reservation = reserveNextOperationsEvent(state, start);
    assert.ok(reservation.event);
    state = completeOperationsEvent(reservation.state, reservation.event.id, { status: "accepted" }, start).state;
  }
  state = enqueue(state, "ordinary_blocked");
  assert.equal(reserveNextOperationsEvent(state, start).event, null);
  for (let index = 0; index < 5; index++) {
    state = enqueue(state, `priority_${index}`, index % 2 ? { recovered: true } : { severity: "critical" });
    const reservation = reserveNextOperationsEvent(state, start);
    assert.ok(reservation.event.id.startsWith("priority_"));
    state = completeOperationsEvent(reservation.state, reservation.event.id, { status: "accepted" }, start).state;
  }
  state = enqueue(state, "critical_blocked", { severity: "critical" });
  assert.equal(reserveNextOperationsEvent(state, later(24 * 60 * 60_000 - 1)).event, null);
  assert.equal(reserveNextOperationsEvent(state, later(24 * 60 * 60_000)).event.id, "ordinary_blocked");
});

test("critical attempts do not consume the ordinary fifteen-attempt allocation", () => {
  let state;
  for (let index = 0; index < 20; index++) {
    state = enqueue(state, `event_${index}`, index < 5 ? { severity: "critical" } : {});
    const reservation = reserveNextOperationsEvent(state, start);
    assert.ok(reservation.event);
    state = completeOperationsEvent(reservation.state, reservation.event.id, { status: "accepted" }, start).state;
  }
  assert.equal(state.attempts.length, 20);
});

test("recovery remains independent and pending after failed opening", () => {
  let state = enqueue(undefined, "episode:opened");
  state = reserveNextOperationsEvent(state, start).state;
  state = completeOperationsEvent(state, "episode:opened", { status: "permanent" }, start).state;
  state = enqueue(state, "episode:recovered", { recovered: true });
  assert.equal(state.events[0].status, "permanent");
  assert.equal(state.events[1].status, "pending");
  assert.equal(reserveNextOperationsEvent(state, start).event.id, "episode:recovered");
});

test("capacity preserves pending and seven-day terminal identities", () => {
  let state;
  for (let index = 0; index < 128; index++) state = enqueue(state, `event_${index}`);
  assert.equal(enqueueOperationsEvent(state, input("overflow"), start).reason, "capacity");
  assert.equal(normalizeOperationsOutbox(state, later(8 * 24 * 60 * 60_000)).state.events.length, 128);
  const reservation = reserveNextOperationsEvent(state, start);
  state = completeOperationsEvent(reservation.state, reservation.event.id, { status: "accepted" }, start).state;
  assert.equal(enqueueOperationsEvent(state, input("event_0"), later(7 * 24 * 60 * 60_000 - 1)).reason, "duplicate");
  const after = enqueueOperationsEvent(state, input("new"), later(7 * 24 * 60 * 60_000));
  assert.equal(after.enqueued, true);
  assert.equal(after.state.events.length, 128);
  assert.equal(after.state.events.filter((event) => event.status === "pending").length, 128);
});

test("retained terminals do not consume pending capacity or lose replay protection", () => {
  let state;
  for (let index = 0; index < 140; index++) {
    state = enqueue(state, `terminal_${index}`);
    state = {
      ...state,
      events: state.events.map((event) => event.id === `terminal_${index}` ? {
        ...event, status: index < 128 ? "accepted" : "uncertain", finishedAt: start.toISOString(),
      } : event),
    };
  }
  const added = enqueueOperationsEvent(state, input("new_critical", { severity: "critical" }), start);
  assert.equal(added.enqueued, true);
  assert.equal(added.state.events.length, 141);
  assert.equal(added.state.events.filter((event) => event.status === "pending").length, 1);
  assert.equal(enqueueOperationsEvent(added.state, input("terminal_0"), later(1)).reason, "duplicate");
  assert.equal(enqueueOperationsEvent(added.state, input("terminal_139"), later(1)).reason, "duplicate");
  assert.equal(reserveNextOperationsEvent(added.state, start).event.id, "new_critical");
  assert.equal(normalizeOperationsOutbox(added.state, later(7 * 24 * 60 * 60_000 - 1)).state.events.length, 141);
});

test("capacity counts pending and reserved independently of retained terminals", () => {
  let state;
  for (let index = 0; index < 128; index++) state = enqueue(state, `pending_${index}`);
  state = reserveNextOperationsEvent(state, start).state;
  state = {
    ...state,
    events: [...state.events, {
      ...state.events[0], id: "old_terminal", status: "accepted", finishedAt: start.toISOString(),
    }],
  };
  assert.equal(state.events.filter((event) => event.status === "pending").length, 127);
  assert.equal(state.events.filter((event) => event.status === "reserved").length, 1);
  assert.equal(enqueueOperationsEvent(state, input("new_critical", { severity: "critical" }), start).reason, "capacity");
  state = completeOperationsEvent(state, "pending_0", { status: "accepted" }, start).state;
  const added = enqueueOperationsEvent(state, input("new_critical", { severity: "critical" }), start);
  assert.equal(added.enqueued, true);
  assert.equal(added.state.events.length, 130);
});

test("outcome diagnostics keep only enumerated reasons and HTTP status metadata", () => {
  for (const [status, reason, httpStatus] of [
    ["accepted", "http_accepted", 200], ["uncertain", "timeout", undefined],
    ["uncertain", "network_error", undefined], ["retryable", "rate_limited", 429],
    ["retryable", "server_error", 503], ["permanent", "http_rejected", 403],
  ]) {
    const reserved = reserveNextOperationsEvent(enqueue(undefined, "episode:opened"), start);
    const completed = completeOperationsEvent(reserved.state, "episode:opened", {
      status, reason, httpStatus, body: "PRIVATE", error: "PRIVATE",
    }, start);
    const event = completed.state.events[0];
    assert.equal(event.lastReason, reason);
    assert.equal(event.lastHttpStatus, httpStatus);
    assert.equal(normalizeOperationsOutbox(completed.state, start).changed, false);
    assert.equal(JSON.stringify(completed.state).includes("PRIVATE"), false);
  }
  for (const httpStatus of [99, 600, 200.5, "200", null]) {
    const reserved = reserveNextOperationsEvent(enqueue(undefined, "episode:opened"), start);
    const completed = completeOperationsEvent(reserved.state, "episode:opened", {
      status: "uncertain", reason: "PRIVATE", httpStatus,
    }, start);
    assert.equal(completed.state.events[0].lastReason, undefined);
    assert.equal(completed.state.events[0].lastHttpStatus, undefined);
    assert.equal(JSON.stringify(completed.state).includes("PRIVATE"), false);
    const untrusted = { ...completed.state, events: [{ ...completed.state.events[0], lastReason: "PRIVATE", lastHttpStatus: httpStatus }] };
    const normalized = normalizeOperationsOutbox(untrusted, start).state.events[0];
    assert.equal(normalized.lastReason, undefined);
    assert.equal(normalized.lastHttpStatus, undefined);
  }
});

test("legacy uncertainty stays unchanged and new completion never retains stale diagnostics", () => {
  const reserved = reserveNextOperationsEvent(enqueue(undefined, "episode:opened"), start);
  const legacy = completeOperationsEvent(reserved.state, "episode:opened", { status: "uncertain" }, start).state;
  assert.equal(normalizeOperationsOutbox(legacy, later(1)).changed, false);
  assert.equal(legacy.events[0].lastReason, undefined);
  assert.equal(legacy.events[0].lastHttpStatus, undefined);
  assert.equal(reserveNextOperationsEvent(legacy, later(60_000)).event, null);

  const retry = completeOperationsEvent(reserved.state, "episode:opened", {
    status: "retryable", reason: "rate_limited", httpStatus: 429,
  }, start).state;
  const next = reserveNextOperationsEvent(retry, later(60_000));
  const completed = completeOperationsEvent(next.state, "episode:opened", {
    status: "uncertain", reason: "network_error",
  }, later(60_000)).state;
  assert.equal(completed.events[0].lastReason, "network_error");
  assert.equal(completed.events[0].lastHttpStatus, undefined);
  const recovered = normalizeOperationsOutbox(next.state, later(60_001), { recoverReserved: true }).state;
  assert.equal(recovered.events[0].status, "uncertain");
  assert.equal(recovered.events[0].lastReason, undefined);
  assert.equal(recovered.events[0].lastHttpStatus, undefined);
});
