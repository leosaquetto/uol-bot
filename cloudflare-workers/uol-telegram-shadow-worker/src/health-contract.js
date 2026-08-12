export const HEALTH_WORKER_NAME = "uol-telegram-shadow-pilot";
export const DEFAULT_MAX_SCAN_AGE_MS = 180_000;

const HARD_REASONS = new Set([
  "liveness_http",
  "liveness_body",
  "liveness_worker",
  "liveness_version_missing",
  "readiness_missing",
  "readiness_http",
  "readiness_body",
  "readiness_protocol",
  "readiness_unexplained",
  "readiness_worker",
  "readiness_version_missing",
  "version_mismatch",
  "checks_missing",
  "alarm_stale",
  "scan_stale",
  "maintenance_stale",
  "delivery_unconfigured",
  "mode_invalid",
  "mode_not_live",
  "mode_not_expected",
]);

const DEGRADED_CHECKS = [
  ["criticalIncidents", "critical_incidents"],
  ["deadLetters", "dead_letters"],
  ["unknown", "unknown_deliveries"],
  ["blockedConfiguration", "blocked_configuration"],
  ["maintenanceDeadLetters", "maintenance_dead_letters"],
];

const BOOLEAN_CHECKS = [
  "alarmFresh",
  "scanFresh",
  "maintenanceFresh",
  "maintenanceDeferred",
  "deliveryConfigured",
  "storageReadBudgetHealthy",
  "storageWriteBudgetHealthy",
];

const VALID_MODES = new Set(["live", "shadow"]);

export function readinessChecksOk(mode, checks = {}) {
  return Boolean(
    mode === "live" && checks.alarmFresh && checks.scanFresh &&
    checks.maintenanceFresh && checks.deliveryConfigured &&
    Number(checks.criticalIncidents || 0) === 0 &&
    Number(checks.deadLetters || 0) === 0 &&
    Number(checks.unknown || 0) === 0 &&
    Number(checks.blockedConfiguration || 0) === 0 &&
    Number(checks.maintenanceDeadLetters || 0) === 0 &&
    checks.storageReadBudgetHealthy && checks.storageWriteBudgetHealthy,
  );
}

function bodyOf(response) {
  if (!response || typeof response !== "object") return null;
  if (Object.hasOwn(response, "body")) {
    return response.body && typeof response.body === "object" ? response.body : null;
  }
  if (Object.hasOwn(response, "payload")) {
    return response.payload && typeof response.payload === "object" ? response.payload : null;
  }
  return response;
}

function addReason(reasons, reason) {
  if (!reasons.includes(reason)) reasons.push(reason);
}

function finiteTimestamp(value) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeChecks(rawChecks) {
  const source = rawChecks && typeof rawChecks === "object" ? rawChecks : {};
  const checks = {};
  let complete = rawChecks && typeof rawChecks === "object";

  for (const field of BOOLEAN_CHECKS) {
    if (typeof source[field] !== "boolean") complete = false;
    checks[field] = source[field] === true;
  }
  for (const [field] of DEGRADED_CHECKS) {
    const value = source[field];
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) complete = false;
    checks[field] = typeof value === "number" && Number.isFinite(value) && value >= 0
      ? value
      : 0;
  }

  return { checks, complete };
}

export function evaluateHealthContract({
  liveness,
  readiness,
  now = new Date(),
  maxScanAgeMs = DEFAULT_MAX_SCAN_AGE_MS,
  expectedWorker = HEALTH_WORKER_NAME,
  expectedMode = "live",
} = {}) {
  const live = bodyOf(liveness);
  const ready = bodyOf(readiness);
  const reasons = [];
  const nowMs = now instanceof Date ? now.getTime() : Number(now);
  const safeNowMs = Number.isFinite(nowMs) ? nowMs : Date.now();
  const requestedMaxScanAgeMs = Number(maxScanAgeMs);
  const effectiveMaxScanAgeMs = Number.isFinite(requestedMaxScanAgeMs) &&
      requestedMaxScanAgeMs > 0
    ? requestedMaxScanAgeMs
    : DEFAULT_MAX_SCAN_AGE_MS;
  const normalizedExpectedWorker = String(expectedWorker || HEALTH_WORKER_NAME).trim();
  const normalizedExpectedMode = String(expectedMode || "live").trim().toLowerCase();
  const livenessStatus = Number(liveness?.status || 0);
  const readinessStatus = Number(readiness?.status || 0);
  const liveWorker = String(live?.worker || "").trim();
  const readyWorker = String(ready?.worker || "").trim();
  const liveVersionId = String(live?.versionId || "").trim();
  const readyVersionId = String(ready?.versionId || "").trim();
  const mode = String(ready?.mode || "").trim().toLowerCase();
  const normalizedChecks = normalizeChecks(ready?.checks);

  if (livenessStatus !== 200) addReason(reasons, "liveness_http");
  if (live?.ok !== true) addReason(reasons, "liveness_body");
  if (liveWorker !== normalizedExpectedWorker) addReason(reasons, "liveness_worker");
  if (!liveVersionId) addReason(reasons, "liveness_version_missing");

  if (!ready) {
    addReason(reasons, "readiness_missing");
  } else {
    if (![200, 503].includes(readinessStatus)) addReason(reasons, "readiness_http");
    if (typeof ready.ok !== "boolean") addReason(reasons, "readiness_body");
    if (readyWorker !== normalizedExpectedWorker) addReason(reasons, "readiness_worker");
    if (!readyVersionId) addReason(reasons, "readiness_version_missing");
    if (liveVersionId && readyVersionId && liveVersionId !== readyVersionId) {
      addReason(reasons, "version_mismatch");
    }
    if (!normalizedChecks.complete) addReason(reasons, "checks_missing");

    const checks = normalizedChecks.checks;
    if (!checks.alarmFresh) addReason(reasons, "alarm_stale");
    if (!checks.scanFresh) addReason(reasons, "scan_stale");
    if (!checks.maintenanceFresh) addReason(reasons, "maintenance_stale");
    if (checks.maintenanceDeferred) addReason(reasons, "maintenance_deferred");
    if (!checks.deliveryConfigured) addReason(reasons, "delivery_unconfigured");
    if (ready.checks?.storageReadBudgetHealthy === false) {
      addReason(reasons, "quota_reserve");
    }
    if (ready.checks?.storageWriteBudgetHealthy === false) {
      addReason(reasons, "write_quota_risk");
    }

    const lastScanAt = finiteTimestamp(ready.lastScanAt);
    if (lastScanAt === null || safeNowMs - lastScanAt > effectiveMaxScanAgeMs) {
      addReason(reasons, "scan_stale");
    }

    if (!VALID_MODES.has(mode)) {
      addReason(reasons, "mode_invalid");
    } else if (mode !== normalizedExpectedMode) {
      addReason(
        reasons,
        normalizedExpectedMode === "live" ? "mode_not_live" : "mode_not_expected",
      );
    }

    for (const [field, reason] of DEGRADED_CHECKS) {
      if (normalizedChecks.checks[field] > 0) addReason(reasons, reason);
    }

    const expectedReady = readinessChecksOk(mode, normalizedChecks.checks);
    const protocolMatches = expectedReady
      ? readinessStatus === 200 && ready.ok === true
      : readinessStatus === 503 && ready.ok === false;
    if (!protocolMatches) {
      addReason(reasons, "readiness_protocol");
    } else if (!expectedReady) {
      const hardReasonPresent = reasons.some((reason) => HARD_REASONS.has(reason));
      const degradedReasonPresent = reasons.some((reason) => !HARD_REASONS.has(reason));
      if (
        normalizedExpectedMode === "shadow" && mode === "shadow" &&
        !hardReasonPresent && !degradedReasonPresent
      ) {
        addReason(reasons, "intentional_shadow");
      } else if (!hardReasonPresent && !degradedReasonPresent) {
        addReason(reasons, "readiness_unexplained");
      }
    }
  }

  const hardFailure = reasons.some((reason) => HARD_REASONS.has(reason));
  const degraded = reasons.some((reason) => !HARD_REASONS.has(reason));

  return {
    state: hardFailure ? "outage" : degraded ? "degraded" : "healthy",
    hardFailure,
    reasons,
    snapshot: {
      worker: readyWorker || liveWorker,
      livenessStatus,
      livenessOk: live?.ok === true,
      readinessStatus,
      readinessOk: ready?.ok === true,
      versionId: readyVersionId || liveVersionId,
      mode,
      lastScanAt: String(ready?.lastScanAt || ""),
      checks: ready ? normalizedChecks.checks : {},
    },
  };
}
