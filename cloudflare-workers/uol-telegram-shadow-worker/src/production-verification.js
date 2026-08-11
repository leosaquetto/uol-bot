import { classifyHeadlessHealth } from "./headless-health.js";

const WORKER_NAME = "uol-telegram-shadow-pilot";
const DEFAULT_MAX_SCAN_AGE_MS = 180_000;
const MAX_RECOMMENDED_POLL_INTERVAL_SECONDS = 15;
const MAX_PRIMARY_ESTIMATED_ROWS_READ = 512;
const MAX_PRIMARY_SINGLE_CYCLE_ROWS_READ = 4_096;
const PRIMARY_ESTIMATE_SAFETY_FACTOR = 1.5;
const MAX_PROJECTED_ROWS_WRITTEN = 80_000;
const MIN_WRITE_HEADROOM = 20_000;

function readinessFailure(checks = {}) {
  const failed = Object.entries(checks)
    .filter(([, value]) => value === false || (typeof value === "number" && value > 0))
    .map(([name]) => name);
  return failed.length > 0 ? failed.join(",") : "unknown";
}

function finiteNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

export function validateProductionHealth({
  livenessStatus,
  readinessStatus,
  liveness,
  readiness,
} = {}, {
  expectedVersionId = "",
  expectedMode = "",
  maxScanAgeMs = DEFAULT_MAX_SCAN_AGE_MS,
  now = new Date(),
} = {}) {
  const normalizedExpectedVersionId = String(expectedVersionId || "").trim();
  const normalizedExpectedMode = String(expectedMode || "").trim().toLowerCase();
  if (!normalizedExpectedVersionId) throw new Error("expected_version_id_required");
  if (normalizedExpectedMode && !["live", "shadow"].includes(normalizedExpectedMode)) {
    throw new Error("expected_delivery_mode_invalid");
  }

  const nowMs = now instanceof Date ? now.getTime() : Number(now);
  if (!Number.isFinite(nowMs)) throw new Error("verification_now_invalid");
  const normalizedMaxScanAgeMs = Number(maxScanAgeMs);
  if (!Number.isFinite(normalizedMaxScanAgeMs) || normalizedMaxScanAgeMs <= 0) {
    throw new Error("max_scan_age_invalid");
  }

  const headless = classifyHeadlessHealth({
    liveness: { status: livenessStatus, body: liveness },
    readiness: { status: readinessStatus, body: readiness },
    now: nowMs,
    maxScanAgeMs: normalizedMaxScanAgeMs,
  });
  if (livenessStatus !== 200 || liveness?.ok !== true) throw new Error("liveness_not_ok");
  if (liveness.worker !== WORKER_NAME) {
    throw new Error("liveness_worker_identity_mismatch");
  }
  const intentionalShadow = normalizedExpectedMode === "shadow" && readinessStatus === 503 &&
    readiness?.ok === false && readiness?.mode === "shadow" &&
    readinessFailure(readiness?.checks) === "unknown";
  if (!intentionalShadow && (readinessStatus !== 200 || readiness?.ok !== true)) {
    throw new Error(
      `not_ready:${headless.state}:${headless.reasons.join(",") || readinessFailure(readiness?.checks)}`,
    );
  }
  if (readiness.worker !== WORKER_NAME) {
    throw new Error("readiness_worker_identity_mismatch");
  }
  if (readiness.mode !== "live" && readiness.mode !== "shadow") {
    throw new Error("delivery_mode_invalid");
  }
  if (normalizedExpectedMode && readiness.mode !== normalizedExpectedMode) {
    throw new Error(`delivery_mode_${readiness.mode}_expected_${normalizedExpectedMode}`);
  }
  if (!liveness.versionId || !readiness.versionId) {
    throw new Error("version_metadata_missing");
  }
  if (String(liveness.versionId || "") !== String(readiness.versionId || "")) {
    throw new Error("version_metadata_mismatch");
  }
  if (String(readiness.versionId) !== normalizedExpectedVersionId) {
    throw new Error("deployed_version_id_mismatch");
  }

  const lastScanAt = Date.parse(readiness.lastScanAt || "");
  if (!Number.isFinite(lastScanAt) || nowMs - lastScanAt > normalizedMaxScanAgeMs) {
    throw new Error("scan_missing_or_stale");
  }
  if (!intentionalShadow && headless.hardFailure) {
    throw new Error(`headless_outage:${headless.reasons.join(",") || "unknown"}`);
  }

  const criticalPending = readiness.queueSlo?.criticalPending;
  if (!finiteNumber(criticalPending) || criticalPending < 0) {
    throw new Error("critical_queue_missing");
  }
  if (criticalPending > 0) {
    throw new Error(`critical_queue_pending:${criticalPending}`);
  }

  const budget = readiness.storageReadBudget;
  if (readiness.checks?.storageReadBudgetHealthy !== true ||
      budget?.primaryAllowed !== true || budget?.withinFreeTier !== true) {
    throw new Error("storage_read_budget_not_healthy");
  }
  const recommendedPollIntervalSeconds = budget?.recommendedPollIntervalSeconds;
  if (!finiteNumber(recommendedPollIntervalSeconds) || recommendedPollIntervalSeconds <= 0) {
    throw new Error("recommended_poll_interval_missing");
  }
  if (recommendedPollIntervalSeconds > MAX_RECOMMENDED_POLL_INTERVAL_SECONDS) {
    throw new Error(`recommended_poll_interval_too_slow:${recommendedPollIntervalSeconds}`);
  }

  const primaryLastSuccessfulVersionId = String(
    budget?.primaryLastSuccessfulVersionId || "",
  );
  if (primaryLastSuccessfulVersionId !== normalizedExpectedVersionId) {
    throw new Error(
      `primary_cycle_version_mismatch:${primaryLastSuccessfulVersionId || "missing"}`,
    );
  }
  const primaryLastSuccessfulRowsRead = budget?.primaryLastSuccessfulRowsRead;
  if (!finiteNumber(primaryLastSuccessfulRowsRead) || primaryLastSuccessfulRowsRead <= 0) {
    throw new Error("primary_cycle_missing");
  }
  const primaryLastSuccessfulAt = Date.parse(budget?.primaryLastSuccessfulAt || "");
  if (!Number.isFinite(primaryLastSuccessfulAt) || primaryLastSuccessfulAt > nowMs ||
      nowMs - primaryLastSuccessfulAt > normalizedMaxScanAgeMs) {
    throw new Error("primary_cycle_stale");
  }

  const primaryEstimatedRowsRead = budget?.primaryEstimatedRowsRead;
  const observedPrimaryEstimate = budget?.observedPrimaryEstimate;
  const primaryRowsWithSafety = budget?.primaryRowsWithSafety;
  if (!finiteNumber(primaryEstimatedRowsRead) || primaryEstimatedRowsRead < 0 ||
      !finiteNumber(observedPrimaryEstimate) || observedPrimaryEstimate <= 0) {
    throw new Error("primary_estimate_missing");
  }
  const highestEstimate = Math.max(primaryEstimatedRowsRead, observedPrimaryEstimate);
  if (highestEstimate > MAX_PRIMARY_ESTIMATED_ROWS_READ) {
    throw new Error(`primary_estimate_too_high:${highestEstimate}`);
  }
  if (observedPrimaryEstimate < Math.max(64, primaryEstimatedRowsRead)) {
    throw new Error("primary_estimate_inconsistent");
  }
  const conservativeSafetyEstimate = Math.ceil(
    observedPrimaryEstimate * PRIMARY_ESTIMATE_SAFETY_FACTOR,
  );
  if (!finiteNumber(primaryRowsWithSafety) ||
      primaryRowsWithSafety < conservativeSafetyEstimate) {
    throw new Error("primary_safety_estimate_not_conservative");
  }
  const primaryMaxRowsRead = budget?.primaryMaxRowsRead;
  if (!finiteNumber(primaryMaxRowsRead) || primaryMaxRowsRead < 0) {
    throw new Error("primary_single_cycle_missing");
  }
  if (primaryMaxRowsRead > MAX_PRIMARY_SINGLE_CYCLE_ROWS_READ) {
    throw new Error(`primary_single_cycle_too_high:${primaryMaxRowsRead}`);
  }

  const rowsWritten = budget?.rowsWritten;
  const writeBudget = readiness.storageWriteBudget;
  if (!finiteNumber(rowsWritten) || rowsWritten < 0) {
    throw new Error("rows_written_missing");
  }
  if (!writeBudget || !finiteNumber(writeBudget.limit) ||
      !finiteNumber(writeBudget.projected) || !finiteNumber(writeBudget.headroom) ||
      typeof writeBudget.withinFreeTier !== "boolean") {
    throw new Error("write_budget_missing");
  }
  if (writeBudget.limit !== 100_000) throw new Error("write_budget_limit_invalid");
  if (rowsWritten >= writeBudget.limit) {
    throw new Error(`write_limit_reached:${rowsWritten}`);
  }
  if (rowsWritten > MAX_PROJECTED_ROWS_WRITTEN) {
    throw new Error(`write_usage_too_high:${rowsWritten}`);
  }
  if (!writeBudget.withinFreeTier || writeBudget.projected > MAX_PROJECTED_ROWS_WRITTEN ||
      writeBudget.headroom < MIN_WRITE_HEADROOM) {
    throw new Error(`write_projection_too_high:${writeBudget.projected}`);
  }

  return {
    ok: true,
    worker: readiness.worker,
    versionId: readiness.versionId,
    mode: readiness.mode,
    lastScanAt: readiness.lastScanAt,
    checks: readiness.checks,
    headlessState: headless.state,
    headlessReasons: headless.reasons,
    projectedRowsWritten: writeBudget.projected,
    writeHeadroom: writeBudget.headroom,
  };
}
