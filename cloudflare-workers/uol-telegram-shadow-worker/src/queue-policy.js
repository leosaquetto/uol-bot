const PENDING_STATUSES = new Set([
  "delivery_pending",
  "partial_delivery",
  "delivery_unknown",
  "delivery_dead_letter",
  "delivery_blocked_configuration",
]);
const CRITICAL_BACKLOG_AGE_MS = 45_000;

function timestamp(value) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function percentile(values, ratio) {
  if (!values.length) return 0;
  const ordered = [...values].sort((left, right) => left - right);
  return ordered[Math.max(0, Math.ceil(ordered.length * ratio) - 1)];
}

export function summarizeQueueSlo(rows = [], now = new Date()) {
  const nowMs = now instanceof Date ? now.getTime() : Number(now);
  const safeNowMs = Number.isFinite(nowMs) ? nowMs : Date.now();
  const pendingRows = rows.filter((row) => PENDING_STATUSES.has(String(row.status || "")));
  const ages = pendingRows.map((row) => {
    const seenAt = timestamp(row.first_seen_at || row.firstSeenAt || row.decision_at);
    return seenAt === null ? 0 : Math.max(0, safeNowMs - seenAt);
  });
  return {
    pending: pendingRows.length,
    criticalPending: pendingRows.filter((row) => !String(row.main_sent_at || "").trim()).length,
    secondaryPending: pendingRows.filter((row) => String(row.main_sent_at || "").trim()).length,
    oldestAgeMs: ages.length ? Math.max(...ages) : 0,
    p95AgeMs: percentile(ages, 0.95),
  };
}

export function chooseDeliveryBudget({
  storageReadBudget = {},
  queueSlo = {},
  configuredBatch = 4,
  configuredConcurrency = 6,
} = {}) {
  const boundedBatch = Math.min(8, Math.max(1, Number.parseInt(String(configuredBatch), 10) || 4));
  const boundedConcurrency = Math.min(
    6,
    Math.max(1, Number.parseInt(String(configuredConcurrency), 10) || 6),
  );
  const quotaReserve = storageReadBudget.maintenanceAllowed === false ||
    storageReadBudget.primaryAllowed === false;
  const criticalBacklog = Number(queueSlo.criticalPending || 0) > 0 &&
    Number(queueSlo.oldestAgeMs || 0) >= CRITICAL_BACKLOG_AGE_MS;
  if (quotaReserve) {
    return {
      batchSize: boundedBatch,
      concurrency: boundedConcurrency,
      deferSecondary: true,
      reason: "quota_reserve",
    };
  }
  if (criticalBacklog) {
    return {
      batchSize: Math.min(4, boundedBatch),
      concurrency: boundedConcurrency,
      deferSecondary: true,
      reason: "critical_backlog",
    };
  }
  return {
    batchSize: boundedBatch,
    concurrency: boundedConcurrency,
    deferSecondary: false,
    reason: "healthy",
  };
}

