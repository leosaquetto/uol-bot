import { baselineWithApprovedWhatsAppUnknown, reportProductionHealth } from "../src/production-verification.js";
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
  UOL_VERIFY_ATTEMPTS         tentativas de liveness/readiness (padrão: 30)
  UOL_VERIFY_PROPAGATION_SECONDS  quanto esperar a versão nova propagar (padrão: 600)
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
// 12 × 5 s não bastou para a versão nova propagar no release do #156 (17/11): 30 × 5 s = 150 s.
const attempts = positiveInteger(process.env.UOL_VERIFY_ATTEMPTS, 30);
const intervalMs = positiveInteger(process.env.UOL_VERIFY_INTERVAL_MS, 5_000);
// Nem 150 s bastaram no release do #162 (18/11): o /readyz passa pelo Durable
// Object, que segue na versão anterior até reiniciar. Enquanto o único erro for
// a versão ainda propagando, a espera vai até este prazo sem gastar tentativas.
const propagationMs = positiveInteger(process.env.UOL_VERIFY_PROPAGATION_SECONDS, 600) * 1_000;
const PROPAGATION_ERRORS = new Set(["version_metadata_mismatch", "debt_version_mismatch"]);

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
const approvedWhatsAppUnknown = process.env.UOL_RELEASE_APPROVED_WHATSAPP_UNKNOWN;
if (approvedWhatsAppUnknown && (!baseline || !predeployDebt || bootstrapDebt)) {
  throw new Error("release_whatsapp_exception_requires_predeploy_and_baseline");
}
const comparisonBaseline = approvedWhatsAppUnknown
  ? baselineWithApprovedWhatsAppUnknown(baseline, predeployDebt, approvedWhatsAppUnknown)
  : baseline || predeployDebt;
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
const propagationDeadline = Date.now() + propagationMs;
for (let attempt = 1; attempt <= attempts; attempt += 1) {
  try {
    const snapshot = await readHealth();
    const summary = reportProductionHealth(snapshot, {
      expectedVersionId,
      expectedMode,
      maxScanAgeMs,
      now: Date.now(),
      bootstrapDebt,
    }, comparisonBaseline, snapshot.debt);
    lastReport = summary;
    if (!summary.ok) throw new Error(summary.runtimeError || summary.regressions.join(","));
    if (bootstrapDebt) saveInitialReleaseDebtBaseline(baselinePath, snapshot.debt);
    console.log(JSON.stringify(summary));
    process.exit(0);
  } catch (error) {
    lastError = error;
    const propagating = PROPAGATION_ERRORS.has(String(error?.message || "")) &&
      Date.now() + intervalMs < propagationDeadline;
    if (propagating) attempt -= 1;
    if (propagating || attempt < attempts) {
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }
}

if (lastReport) console.log(JSON.stringify(lastReport));
console.error(`postdeploy_check_failed: ${lastError?.message || String(lastError)}`);
process.exit(1);
