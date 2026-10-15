const DAY_MS = 24 * 60 * 60_000;
const RETENTION_MS = 7 * DAY_MS;
const MAX_EVENTS = 128;
const MAX_ATTEMPTS = 20;
const MAX_ORDINARY_ATTEMPTS = 15;
const KINDS = new Set([
  "ticket-api", "ticket-api-contract", "ticket-api-expiry", "telegram-webhook",
  "failed-scans", "source-health", "delivery-slo", "ticket-delivery",
  "delivery-queue", "instagram-monitor", "monitor",
]);
const STATUSES = new Set(["pending", "reserved", "accepted", "uncertain", "permanent", "exhausted"]);
const TERMINAL = new Set(["accepted", "uncertain", "permanent", "exhausted"]);
const OUTCOMES = new Set(["accepted", "uncertain", "permanent", "retryable"]);

function timestamp(now) {
  const value = now instanceof Date ? now.getTime() : Date.parse(now);
  if (!Number.isFinite(value)) throw new TypeError("operations_outbox_invalid_time");
  return value;
}

function iso(value) {
  return new Date(value).toISOString();
}

function validTime(value, fallback) {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? iso(parsed) : fallback;
}

function result(original, state, extra = {}) {
  const changed = JSON.stringify(original) !== JSON.stringify(state);
  return { state: changed ? state : original, changed, ...extra };
}

// Only fixed metadata is retained. Notification text and provider responses do
// not belong in this durable record. Recovery is explicit, once per DO start;
// normal reads must never terminalize an in-flight reservation.
export function normalizeOperationsOutbox(original, now = new Date(), { recoverReserved = false } = {}) {
  const at = timestamp(now);
  const nowIso = iso(at);
  const events = [];
  const seen = new Set();
  for (const input of Array.isArray(original?.events) ? original.events : []) {
    if (!input || typeof input.id !== "string" || !/^[A-Za-z0-9:_-]{1,180}$/.test(input.id) || seen.has(input.id)) continue;
    seen.add(input.id);
    const event = {
      id: input.id,
      kind: KINDS.has(input.kind) ? input.kind : "monitor",
      severity: ["critical", "warning", "info"].includes(input.severity) ? input.severity : "warning",
      recovered: input.recovered === true,
      createdAt: validTime(input.createdAt, nowIso),
      status: STATUSES.has(input.status) ? input.status : "uncertain",
      attempts: Math.max(0, Math.min(3, Math.trunc(Number(input.attempts) || 0))),
      nextAt: validTime(input.nextAt, nowIso),
    };
    if (input.lastAttemptAt) event.lastAttemptAt = validTime(input.lastAttemptAt, nowIso);
    if (OUTCOMES.has(input.lastOutcome)) event.lastOutcome = input.lastOutcome;
    if (input.finishedAt) event.finishedAt = validTime(input.finishedAt, nowIso);
    if (recoverReserved && event.status === "reserved") {
      event.status = "uncertain";
      event.lastOutcome = "uncertain";
      event.finishedAt = nowIso;
    }
    if (TERMINAL.has(event.status) && !event.finishedAt) event.finishedAt = nowIso;
    if (TERMINAL.has(event.status) && at - Date.parse(event.finishedAt) >= RETENTION_MS) continue;
    events.push(event);
  }
  const attempts = (Array.isArray(original?.attempts) ? original.attempts : [])
    .filter((attempt) => Number.isFinite(Date.parse(attempt?.at)) && Date.parse(attempt.at) > at - DAY_MS)
    .map((attempt) => ({ at: validTime(attempt.at, nowIso), privileged: attempt.privileged === true }));
  const state = { version: 1, events, attempts };
  const retryAt = Date.parse(original?.retryAt);
  if (Number.isFinite(retryAt) && retryAt > at) state.retryAt = iso(retryAt);
  return result(original, state);
}

export function enqueueOperationsEvent(original, input, now = new Date()) {
  const normalized = normalizeOperationsOutbox(original, now);
  const state = normalized.state;
  if (!input || typeof input.id !== "string" || !/^[A-Za-z0-9:_-]{1,180}$/.test(input.id)) {
    return result(original, state, { enqueued: false, reason: "invalid_id" });
  }
  if (state.events.some((event) => event.id === input.id)) {
    return result(original, state, { enqueued: false, reason: "duplicate" });
  }
  if (state.events.length >= MAX_EVENTS) return result(original, state, { enqueued: false, reason: "capacity" });
  const nowIso = iso(timestamp(now));
  const event = {
    id: input.id,
    kind: KINDS.has(input.kind) ? input.kind : "monitor",
    severity: ["critical", "warning", "info"].includes(input.severity) ? input.severity : "warning",
    recovered: input.recovered === true,
    createdAt: nowIso,
    status: "pending",
    attempts: 0,
    nextAt: nowIso,
  };
  return result(original, { ...state, events: [...state.events, event] }, { enqueued: true, event });
}

export function reserveNextOperationsEvent(original, now = new Date()) {
  const normalized = normalizeOperationsOutbox(original, now);
  const state = normalized.state;
  const at = timestamp(now);
  if (state.retryAt && Date.parse(state.retryAt) > at) {
    return result(original, state, { event: null });
  }
  const ordinary = state.attempts.filter((attempt) => !attempt.privileged).length;
  const eligible = state.events.find((event) => event.status === "pending" &&
    event.attempts < 3 && Date.parse(event.nextAt) <= at &&
    state.attempts.length < MAX_ATTEMPTS &&
    (event.recovered || event.severity === "critical" || ordinary < MAX_ORDINARY_ATTEMPTS));
  if (!eligible) return result(original, state, { event: null });
  const nowIso = iso(at);
  const event = {
    ...eligible, status: "reserved", attempts: eligible.attempts + 1,
    lastAttemptAt: nowIso,
  };
  return result(original, {
    ...state,
    events: state.events.map((value) => value.id === event.id ? event : value),
    attempts: [...state.attempts, { at: nowIso, privileged: event.recovered || event.severity === "critical" }],
  }, { event });
}

export function completeOperationsEvent(original, id, outcome, now = new Date()) {
  const normalized = normalizeOperationsOutbox(original, now);
  const state = normalized.state;
  const current = state.events.find((event) => event.id === id);
  if (!current || current.status !== "reserved") return result(original, state);
  const at = timestamp(now);
  const status = OUTCOMES.has(outcome?.status) ? outcome.status : "uncertain";
  const event = { ...current, lastOutcome: status };
  const retryAfter = Number.isFinite(Number(outcome?.retryAfterMs))
    ? Math.max(0, Number(outcome.retryAfterMs)) : 0;
  if (status === "retryable" && current.attempts < 3) {
    const delay = current.attempts === 1 ? 60_000 : 5 * 60_000;
    event.status = "pending";
    event.nextAt = iso(at + Math.max(delay, retryAfter));
  } else {
    event.status = status === "retryable" ? "exhausted" : status;
    event.finishedAt = iso(at);
  }
  const nextState = { ...state, events: state.events.map((value) => value.id === id ? event : value) };
  // A provider cooldown applies to the destination, including other incidents
  // and a final rejected attempt. It survives restarts independently of events.
  if (status === "retryable" && retryAfter > 0) {
    nextState.retryAt = iso(Math.max(at + retryAfter, Date.parse(state.retryAt) || 0));
  }
  return result(original, nextState);
}
