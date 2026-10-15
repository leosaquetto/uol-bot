import { reportProductionHealth } from "../src/production-verification.js";
import { loadReleaseDebtBaseline, saveInitialReleaseDebtBaseline } from "./release.mjs";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

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
  UOL_VERIFY_ATTEMPTS         tentativas de liveness/readiness (padrão: 12)
  UOL_VERIFY_INTERVAL_MS      intervalo entre tentativas (padrão: 5000)
  UOL_RELEASE_BASELINE_PATH   baseline privado original (padrão: ~/.config/uol-ticket-research/release-debt-baseline.json)

O release fornece o bootstrap explicitamente no primeiro lote. O token administrativo
é lido de ~/.config/uol-ticket-research/shadow-admin-token, sem exibi-lo.`);
}

if (process.argv.includes("--help")) {
  help();
  process.exit(0);
}

const baseUrl = String(process.env.UOL_WORKER_URL || DEFAULT_BASE_URL).replace(/\/+$/, "");
const expectedMode = String(process.env.EXPECTED_DELIVERY_MODE || "").trim().toLowerCase();
const expectedVersionId = String(process.env.EXPECTED_VERSION_ID || "").trim();
const maxScanAgeMs = positiveInteger(process.env.UOL_MAX_SCAN_AGE_SECONDS, 180) * 1_000;
const attempts = positiveInteger(process.env.UOL_VERIFY_ATTEMPTS, 12);
const intervalMs = positiveInteger(process.env.UOL_VERIFY_INTERVAL_MS, 5_000);

if (expectedMode && expectedMode !== "live" && expectedMode !== "shadow") {
  throw new Error("EXPECTED_DELIVERY_MODE deve ser live ou shadow");
}
if (!expectedVersionId) {
  throw new Error("EXPECTED_VERSION_ID é obrigatório no pós-deploy");
}

const bootstrapDebt = process.env.UOL_RELEASE_BOOTSTRAP_DEBT === "1";
const baselinePath = process.env.UOL_RELEASE_BASELINE_PATH || join(homedir(), ".config", "uol-ticket-research", "release-debt-baseline.json");
const baseline = loadReleaseDebtBaseline(baselinePath, { bootstrap: bootstrapDebt });
const predeployDebt = process.env.UOL_PREDEPLOY_DEBT_PATH
  ? JSON.parse(readFileSync(process.env.UOL_PREDEPLOY_DEBT_PATH, "utf8")) : null;
const token = readFileSync(join(homedir(), ".config", "uol-ticket-research", "shadow-admin-token"), "utf8").trim();
if (!token) throw new Error("release_admin_token_missing");


async function readHealth() {
  const verificationId = Date.now();
  const [livenessResponse, readinessResponse, debtResponse] = await Promise.all([
    fetch(`${baseUrl}/livez?verify=${verificationId}`, {
      headers: { Accept: "application/json", "Cache-Control": "no-cache" },
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    }),
    fetch(`${baseUrl}/readyz?verify=${verificationId}`, {
      headers: { Accept: "application/json", "Cache-Control": "no-cache" },
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    }),
    fetch(`${baseUrl}/release-debt?verify=${verificationId}`, {
      headers: { Accept: "application/json", "Cache-Control": "no-cache", Authorization: `Bearer ${token}` },
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    }),
  ]);
  if (debtResponse.status !== 200) throw new Error(`release_debt_http:${debtResponse.status}`);
  const [liveness, readiness, debt] = await Promise.all([
    livenessResponse.json(),
    readinessResponse.json(),
    debtResponse.json(),
  ]);
  return {
    livenessStatus: livenessResponse.status,
    readinessStatus: readinessResponse.status,
    liveness,
    readiness,
    debt,
  };
}

let lastError;
let lastReport;
for (let attempt = 1; attempt <= attempts; attempt += 1) {
  try {
    const snapshot = await readHealth();
    const summary = reportProductionHealth(snapshot, {
      expectedVersionId,
      expectedMode,
      maxScanAgeMs,
      now: Date.now(),
      bootstrapDebt,
    }, baseline || predeployDebt, snapshot.debt);
    lastReport = summary;
    if (!summary.ok) throw new Error(summary.runtimeError || summary.regressions.join(","));
    if (bootstrapDebt) saveInitialReleaseDebtBaseline(baselinePath, snapshot.debt);
    console.log(JSON.stringify(summary));
    process.exit(0);
  } catch (error) {
    lastError = error;
    if (attempt < attempts) {
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }
}

if (lastReport) console.log(JSON.stringify(lastReport));
console.error(`postdeploy_check_failed: ${lastError?.message || String(lastError)}`);
process.exit(1);
