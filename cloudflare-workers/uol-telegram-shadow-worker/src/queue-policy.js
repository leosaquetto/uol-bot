import { classifyDeliveryRow } from "./delivery-state.js";

const PENDING_STATUSES = new Set([
  "delivery_pending",
  "partial_delivery",
  "delivery_unknown",
  "delivery_dead_letter",
  "delivery_blocked_configuration",
]);
const CRITICAL_BACKLOG_AGE_MS = 45_000;
export const RECENT_DELIVERY_PRIORITY_MS = 60 * 60_000;

function timestamp(value) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : null;
}

export function isRecentDeliveryDecision(row, now = new Date()) {
  const decisionAt = timestamp(row?.decision_at || row?.decisionAt);
  const nowMs = now instanceof Date ? now.getTime() : Number(now);
  if (decisionAt === null || !Number.isFinite(nowMs)) return false;
  const ageMs = nowMs - decisionAt;
  return ageMs >= 0 && ageMs <= RECENT_DELIVERY_PRIORITY_MS;
}

function percentile(values, ratio) {
  if (!values.length) return 0;
  const ordered = [...values].sort((left, right) => left - right);
  return ordered[Math.max(0, Math.ceil(ordered.length * ratio) - 1)];
}

export function summarizeQueueSlo(rows = [], now = new Date(), {
  mainDeliveryReady = true,
  maxAttempts = 10,
  inFlightStaleSeconds = 60,
  currentGeneration = null,
} = {}) {
  const nowMs = now instanceof Date ? now.getTime() : Number(now);
  const safeNowMs = Number.isFinite(nowMs) ? nowMs : Date.now();
  const instant = new Date(safeNowMs);
  const pendingRows = rows.filter((row) => PENDING_STATUSES.has(String(row.status || "")));
  const age = (row) => {
    const seenAt = timestamp(row.first_seen_at || row.firstSeenAt || row.decision_at);
    return seenAt === null ? 0 : Math.max(0, safeNowMs - seenAt);
  };
  const ages = pendingRows.map(age);
  const actionableMainRows = pendingRows.filter((row) => (
    currentGeneration === null || Number(row.delivery_generation) === Number(currentGeneration)
  ) && (
    row.would_send_main === undefined || Number(row.would_send_main) === 1
  ) && (
    row.decision_at === undefined || timestamp(row.decision_at) !== null
  ) && classifyDeliveryRow(row, {
    main: { ready: mainDeliveryReady },
    canal2: { enabled: false, ready: false },
    discord: { enabled: false, ready: false },
  }, {
    now: instant,
    maxAttempts,
    inFlightStaleSeconds,
    targetNames: ["main"],
  }).state === "actionable");
  return {
    pending: pendingRows.length,
    criticalPending: pendingRows.filter((row) => !String(row.main_sent_at || "").trim()).length,
    secondaryPending: pendingRows.filter((row) => String(row.main_sent_at || "").trim()).length,
    oldestAgeMs: ages.length ? Math.max(...ages) : 0,
    p95AgeMs: percentile(ages, 0.95),
    actionableMainPending: actionableMainRows.length,
    oldestActionableMainAgeMs: actionableMainRows.length
      ? Math.max(...actionableMainRows.map(age))
      : 0,
  };
}

export function chooseDeliveryBudget({
  storageReadBudget = {},
  queueSlo = {},
  configuredBatch = 4,
  configuredConcurrency = 6,
  priorityCount = 0,
} = {}) {
  const boundedBatch = Math.min(8, Math.max(1, Number.parseInt(String(configuredBatch), 10) || 4));
  const boundedConcurrency = Math.min(
    6,
    Math.max(1, Number.parseInt(String(configuredConcurrency), 10) || 6),
  );
  const quotaReserve = storageReadBudget.maintenanceAllowed === false ||
    storageReadBudget.primaryAllowed === false;
  // Keep legacy callers compatible, but never substitute aggregate incident
  // counts or secondary ages when the explicit actionable-main metrics exist.
  const actionableMainPending = queueSlo.actionableMainPending ?? queueSlo.criticalPending;
  const oldestActionableMainAgeMs = queueSlo.oldestActionableMainAgeMs ?? queueSlo.oldestAgeMs;
  const criticalBacklog = Number(actionableMainPending || 0) > 0 &&
    Number(oldestActionableMainAgeMs || 0) >= CRITICAL_BACKLOG_AGE_MS;
  if (quotaReserve) {
    return {
      batchSize: boundedBatch,
      concurrency: boundedConcurrency,
      deferSecondary: true,
      allowPrioritySecondary: Number(priorityCount || 0) > 0,
      reason: "quota_reserve",
    };
  }
  if (criticalBacklog) {
    return {
      batchSize: Math.min(4, boundedBatch),
      concurrency: boundedConcurrency,
      deferSecondary: true,
      allowPrioritySecondary: false,
      reason: "critical_backlog",
    };
  }
  return {
    batchSize: boundedBatch,
    concurrency: boundedConcurrency,
    deferSecondary: false,
    allowPrioritySecondary: false,
    reason: "healthy",
  };
}
