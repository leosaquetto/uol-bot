const WORKER_NAME = "uol-telegram-shadow-pilot";
const DEFAULT_MAX_SCAN_AGE_MS = 180_000;

const HARD_REASONS = new Set([
  "liveness_http",
  "liveness_body",
  "liveness_worker",
  "readiness_missing",
  "readiness_http",
  "readiness_worker",
  "version_mismatch",
  "alarm_stale",
  "scan_stale",
  "maintenance_stale",
  "delivery_unconfigured",
  "mode_not_live",
]);

const DEGRADED_CHECKS = [
  ["criticalIncidents", "critical_incidents"],
  ["deadLetters", "dead_letters"],
  ["unknown", "unknown_deliveries"],
  ["blockedConfiguration", "blocked_configuration"],
  ["maintenanceDeadLetters", "maintenance_dead_letters"],
];

function bodyOf(response) {
  if (!response || typeof response !== "object") return null;
  return response.body && typeof response.body === "object"
    ? response.body
    : response.payload && typeof response.payload === "object"
      ? response.payload
      : response;
}

function addReason(reasons, reason) {
  if (!reasons.includes(reason)) reasons.push(reason);
}

function finiteTimestamp(value) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : null;
}

export function classifyHeadlessHealth({
  liveness,
  readiness,
  now = new Date(),
  maxScanAgeMs = DEFAULT_MAX_SCAN_AGE_MS,
} = {}) {
  const live = bodyOf(liveness);
  const ready = bodyOf(readiness);
  const reasons = [];
  const nowMs = now instanceof Date ? now.getTime() : Number(now);
  const safeNowMs = Number.isFinite(nowMs) ? nowMs : Date.now();
  const boundedMaxScanAgeMs = Math.max(30_000, Number(maxScanAgeMs) || DEFAULT_MAX_SCAN_AGE_MS);

  if (!liveness || Number(liveness.status) !== 200) addReason(reasons, "liveness_http");
  if (live?.ok !== true) addReason(reasons, "liveness_body");
  if (live?.worker && live.worker !== WORKER_NAME) addReason(reasons, "liveness_worker");
  if (!ready) {
    addReason(reasons, "readiness_missing");
  } else {
    if (![200, 503].includes(Number(readiness.status))) addReason(reasons, "readiness_http");
    if (ready.worker !== WORKER_NAME) addReason(reasons, "readiness_worker");
    if (live?.versionId && ready.versionId && live.versionId !== ready.versionId) {
      addReason(reasons, "version_mismatch");
    }

    const checks = ready.checks || {};
    if (checks.alarmFresh !== true) addReason(reasons, "alarm_stale");
    if (checks.scanFresh !== true) addReason(reasons, "scan_stale");
    if (checks.maintenanceFresh !== true) addReason(reasons, "maintenance_stale");
    if (checks.deliveryConfigured !== true) addReason(reasons, "delivery_unconfigured");
    if (checks.storageReadBudgetHealthy === false) addReason(reasons, "quota_reserve");

    const lastScanAt = finiteTimestamp(ready.lastScanAt);
    if (lastScanAt === null || safeNowMs - lastScanAt > boundedMaxScanAgeMs) {
      addReason(reasons, "scan_stale");
    }
    if (ready.mode !== "live") addReason(reasons, "mode_not_live");
    for (const [field, reason] of DEGRADED_CHECKS) {
      if (Number(checks[field] || 0) > 0) addReason(reasons, reason);
    }
  }

  const hardFailure = reasons.some((reason) => HARD_REASONS.has(reason));
  const degraded = reasons.some((reason) => !HARD_REASONS.has(reason));
  const state = hardFailure ? "outage" : degraded ? "degraded" : "healthy";
  const checks = ready?.checks && typeof ready.checks === "object"
    ? {
      alarmFresh: ready.checks.alarmFresh === true,
      scanFresh: ready.checks.scanFresh === true,
      maintenanceFresh: ready.checks.maintenanceFresh === true,
      deliveryConfigured: ready.checks.deliveryConfigured === true,
      criticalIncidents: Number(ready.checks.criticalIncidents || 0),
      deadLetters: Number(ready.checks.deadLetters || 0),
      unknown: Number(ready.checks.unknown || 0),
      blockedConfiguration: Number(ready.checks.blockedConfiguration || 0),
      maintenanceDeadLetters: Number(ready.checks.maintenanceDeadLetters || 0),
      storageReadBudgetHealthy: ready.checks.storageReadBudgetHealthy === true,
    }
    : {};

  return {
    state,
    hardFailure,
    reasons,
    snapshot: {
      readinessStatus: Number(readiness?.status || 0),
      versionId: String(ready?.versionId || live?.versionId || ""),
      mode: String(ready?.mode || ""),
      lastScanAt: String(ready?.lastScanAt || ""),
      checks,
    },
  };
}
