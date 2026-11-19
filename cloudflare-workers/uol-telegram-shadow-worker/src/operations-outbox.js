const DAY_MS = 24 * 60 * 60_000;
const RETENTION_MS = 7 * DAY_MS;
// An operational alert that could not be sent within two days (the send
// budget rolls every 24 h) is stale: the
// incident it describes has moved on. Leaving it pending forever filled the
// outbox (128) and blocked health processing and incident recovery.
const PENDING_MAX_AGE_MS = 2 * DAY_MS;
const MAX_PENDING_EVENTS = 128;
const MAX_ATTEMPTS = 20;
const MAX_ORDINARY_ATTEMPTS = 15;
const KINDS = new Set([
  "ticket-api", "ticket-api-contract", "ticket-api-expiry", "telegram-webhook",
  "failed-scans", "source-health", "delivery-slo", "ticket-delivery",
  "delivery-queue", "instagram-monitor", "digest", "monitor",
]);
const STATUSES = new Set(["pending", "reserved", "accepted", "uncertain", "permanent", "exhausted"]);
const TERMINAL = new Set(["accepted", "uncertain", "permanent", "exhausted"]);
const OUTCOMES = new Set(["accepted", "uncertain", "permanent", "retryable"]);
const OUTCOME_REASONS = new Set([
  "timeout", "http_accepted", "rate_limited", "stale", "server_error", "http_rejected", "network_error",
  "request_invalid", "fetch_invocation_error", "fetch_rejected", "response_invalid",
  "transport_unconfigured", "transport_retired", "superseded",
]);
const RECOVERED_SUFFIX = ":recovered";
const OPENED_SUFFIX = ":opened";

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
    // The incident key (no text) lets the sender look up what to say.
    if (typeof input.key === "string" && /^[A-Za-z0-9:_.-]{1,180}$/.test(input.key)) event.key = input.key;
    if (input.lastAttemptAt) event.lastAttemptAt = validTime(input.lastAttemptAt, nowIso);
    if (OUTCOMES.has(input.lastOutcome)) event.lastOutcome = input.lastOutcome;
    if (OUTCOME_REASONS.has(input.lastReason)) event.lastReason = input.lastReason;
    if (Number.isInteger(input.lastHttpStatus) && input.lastHttpStatus >= 100 && input.lastHttpStatus <= 599) {
      event.lastHttpStatus = input.lastHttpStatus;
    }
    if (input.finishedAt) event.finishedAt = validTime(input.finishedAt, nowIso);
    if (recoverReserved && event.status === "reserved") {
      event.status = "uncertain";
      event.lastOutcome = "uncertain";
      delete event.lastReason;
      delete event.lastHttpStatus;
      event.finishedAt = nowIso;
    }
    if (event.status === "pending" && at - Date.parse(event.createdAt) > PENDING_MAX_AGE_MS) {
      event.status = "exhausted";
      event.lastReason = "stale";
      event.finishedAt = nowIso;
    }
    if (TERMINAL.has(event.status) && !event.finishedAt) event.finishedAt = nowIso;
    if (TERMINAL.has(event.status) && at - Date.parse(event.finishedAt) >= RETENTION_MS) continue;
    events.push(event);
  }
  // An incident that recovered before its opening alert left the queue is not
  // news: the daily budget delayed such pairs by hours (18/11) and the phone got
  // "atenção" and "recuperado" for something that had healed long before.
  // Drop both while neither has been attempted; an opening that may have been
  // delivered (accepted/uncertain) keeps its recovery.
  const byId = new Map(events.map((event) => [event.id, event]));
  for (const event of events) {
    if (!event.id.endsWith(RECOVERED_SUFFIX) || event.status !== "pending") continue;
    const opened = byId.get(event.id.slice(0, -RECOVERED_SUFFIX.length) + OPENED_SUFFIX);
    if (!opened || opened.status !== "pending" || opened.attempts > 0) continue;
    for (const value of [opened, event]) {
      value.status = "exhausted";
      value.lastReason = "superseded";
      value.finishedAt = nowIso;
    }
  }
  const attempts = (Array.isArray(original?.attempts) ? original.attempts : [])
    .filter((attempt) => Number.isFinite(Date.parse(attempt?.at)) && Date.parse(attempt.at) > at - DAY_MS)
    .map((attempt) => ({ at: validTime(attempt.at, nowIso), privileged: attempt.privileged === true }));
  const state = { version: 1, events, attempts };
  const retryAt = Date.parse(original?.retryAt);
  if (Number.isFinite(retryAt) && retryAt > at) state.retryAt = iso(retryAt);
  return result(original, state);
}

// A transport change retires queued work: alerts written for the previous
// transport describe incidents that have moved on and must not arrive as a burst.
// Terminal records are kept as deduplication evidence.
export function retireOperationsBacklog(original, now = new Date()) {
  const normalized = normalizeOperationsOutbox(original, now);
  const nowIso = iso(timestamp(now));
  let retired = 0;
  const events = normalized.state.events.map((event) => {
    if (event.status !== "pending" && event.status !== "reserved") return event;
    retired += 1;
    return { ...event, status: "exhausted", lastReason: "transport_retired", finishedAt: nowIso };
  });
  const state = { ...normalized.state, events };
  delete state.retryAt;
  return result(original, state, { retired });
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
  // Retained terminal identities are deduplication evidence, not queued work.
  // The 20-attempt daily budget normally bounds them to 140 over seven days;
  // capacity pressure must not evict an identity and allow an old alert to replay.
  if (state.events.filter((event) => !TERMINAL.has(event.status)).length >= MAX_PENDING_EVENTS) {
    return result(original, state, { enqueued: false, reason: "capacity" });
  }
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
  if (typeof input.key === "string" && /^[A-Za-z0-9:_.-]{1,180}$/.test(input.key)) event.key = input.key;
  // Normalize again so a recovery supersedes its unsent opening right away.
  const next = normalizeOperationsOutbox({ ...state, events: [...state.events, event] }, now).state;
  return result(original, next, { enqueued: true, event });
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
  delete event.lastReason;
  delete event.lastHttpStatus;
  if (OUTCOME_REASONS.has(outcome?.reason)) event.lastReason = outcome.reason;
  if (Number.isInteger(outcome?.httpStatus) && outcome.httpStatus >= 100 && outcome.httpStatus <= 599) {
    event.lastHttpStatus = outcome.httpStatus;
  }
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
