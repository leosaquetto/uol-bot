const DEFAULT_BASE_URL = "https://uol-telegram-shadow-pilot.leosaquetto.workers.dev";

function positiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function help() {
  console.log(`Uso: npm run postdeploy:check

Variável obrigatória:
  EXPECTED_VERSION_ID        Version ID retornado pelo deploy recém-concluído

Variáveis opcionais:
  UOL_WORKER_URL              URL base do Worker
  EXPECTED_DELIVERY_MODE      live ou shadow
  UOL_MAX_SCAN_AGE_SECONDS    idade máxima do último scan (padrão: 180)
  UOL_VERIFY_ATTEMPTS         tentativas de liveness/readiness (padrão: 6)
  UOL_VERIFY_INTERVAL_MS      intervalo entre tentativas (padrão: 5000)`);
}

if (process.argv.includes("--help")) {
  help();
  process.exit(0);
}

const baseUrl = String(process.env.UOL_WORKER_URL || DEFAULT_BASE_URL).replace(/\/+$/, "");
const expectedMode = String(process.env.EXPECTED_DELIVERY_MODE || "").trim().toLowerCase();
const expectedVersionId = String(process.env.EXPECTED_VERSION_ID || "").trim();
const maxScanAgeMs = positiveInteger(process.env.UOL_MAX_SCAN_AGE_SECONDS, 180) * 1_000;
const attempts = positiveInteger(process.env.UOL_VERIFY_ATTEMPTS, 6);
const intervalMs = positiveInteger(process.env.UOL_VERIFY_INTERVAL_MS, 5_000);

if (expectedMode && expectedMode !== "live" && expectedMode !== "shadow") {
  throw new Error("EXPECTED_DELIVERY_MODE deve ser live ou shadow");
}
if (!expectedVersionId) {
  throw new Error("EXPECTED_VERSION_ID é obrigatório no pós-deploy");
}

async function readHealth() {
  const verificationId = Date.now();
  const [livenessResponse, readinessResponse] = await Promise.all([
    fetch(`${baseUrl}/livez?verify=${verificationId}`, {
      headers: { Accept: "application/json", "Cache-Control": "no-cache" },
      signal: AbortSignal.timeout(10_000),
    }),
    fetch(`${baseUrl}/readyz?verify=${verificationId}`, {
      headers: { Accept: "application/json", "Cache-Control": "no-cache" },
      signal: AbortSignal.timeout(10_000),
    }),
  ]);
  const [liveness, readiness] = await Promise.all([
    livenessResponse.json(),
    readinessResponse.json(),
  ]);
  return {
    livenessStatus: livenessResponse.status,
    readinessStatus: readinessResponse.status,
    liveness,
    readiness,
  };
}

function readinessFailure(checks = {}) {
  const failed = Object.entries(checks)
    .filter(([, value]) => value === false || (typeof value === "number" && value > 0))
    .map(([name]) => name);
  return failed.length > 0 ? failed.join(",") : "unknown";
}

function validateHealth({ livenessStatus, readinessStatus, liveness, readiness }) {
  const now = Date.now();
  if (livenessStatus !== 200 || liveness?.ok !== true) throw new Error("liveness_not_ok");
  if (liveness.worker !== "uol-telegram-shadow-pilot") {
    throw new Error("liveness_worker_identity_mismatch");
  }
  const intentionalShadow = expectedMode === "shadow" && readinessStatus === 503 &&
    readiness?.ok === false && readiness?.mode === "shadow" &&
    readinessFailure(readiness?.checks) === "unknown";
  if (!intentionalShadow && (readinessStatus !== 200 || readiness?.ok !== true)) {
    throw new Error(`not_ready:${readinessFailure(readiness?.checks)}`);
  }
  if (readiness.worker !== "uol-telegram-shadow-pilot") {
    throw new Error("readiness_worker_identity_mismatch");
  }
  if (readiness.mode !== "live" && readiness.mode !== "shadow") {
    throw new Error("delivery_mode_invalid");
  }
  if (expectedMode && readiness.mode !== expectedMode) {
    throw new Error(`delivery_mode_${readiness.mode}_expected_${expectedMode}`);
  }
  if (!liveness.versionId || !readiness.versionId) {
    throw new Error("version_metadata_missing");
  }
  if (String(liveness.versionId || "") !== String(readiness.versionId || "")) {
    throw new Error("version_metadata_mismatch");
  }
  if (String(readiness.versionId) !== expectedVersionId) {
    throw new Error("deployed_version_id_mismatch");
  }

  const lastScanAt = Date.parse(readiness.lastScanAt || "");
  if (!Number.isFinite(lastScanAt) || now - lastScanAt > maxScanAgeMs) {
    throw new Error("scan_missing_or_stale");
  }

  return {
    ok: true,
    worker: readiness.worker,
    versionId: readiness.versionId,
    mode: readiness.mode,
    lastScanAt: readiness.lastScanAt,
    checks: readiness.checks,
  };
}

let lastError;
for (let attempt = 1; attempt <= attempts; attempt += 1) {
  try {
    const summary = validateHealth(await readHealth());
    console.log(JSON.stringify(summary));
    process.exit(0);
  } catch (error) {
    lastError = error;
    if (attempt < attempts) {
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }
}

console.error(`postdeploy_check_failed: ${lastError?.message || String(lastError)}`);
process.exit(1);
