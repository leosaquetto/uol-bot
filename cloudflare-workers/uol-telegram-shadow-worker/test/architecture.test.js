import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const workerSource = await readFile(new URL("../src/worker.js", import.meta.url), "utf8");
const workerConfig = await readFile(new URL("../wrangler.jsonc", import.meta.url), "utf8");
const discordRollbackSource = await readFile(
  new URL("../../uol-ingressos-discord-worker/src/worker.js", import.meta.url),
  "utf8",
);

function methodSource(start, end) {
  const startIndex = workerSource.indexOf(start);
  const endIndex = workerSource.indexOf(end, startIndex + start.length);
  assert.notEqual(startIndex, -1, `método ausente: ${start}`);
  assert.notEqual(endIndex, -1, `limite ausente: ${end}`);
  return workerSource.slice(startIndex, endIndex);
}

test("polling crítico usa somente API e envio principal com prazo de imagem", () => {
  const scan = methodSource("  async scan(", "  async runMaintenanceTick(");
  assert.match(scan, /this\.fetchAllApi\(\)/);
  assert.match(scan, /waitForMainImage:\s*true/);
  assert.match(scan, /targetNames:\s*\["main"\]/);
  assert.doesNotMatch(scan, /fetchListing\(/);
  assert.doesNotMatch(scan, /ensureTelegramWebhook\(/);
  assert.doesNotMatch(scan, /processDiscussionComments\(/);
  assert.doesNotMatch(scan, /processSoldOutSync\(/);
  assert.ok(
    scan.indexOf('recordSourceCards("api"') > scan.indexOf("processDeliveryQueue("),
    "telemetria de fonte deve rodar após a tentativa principal",
  );
  assert.match(scan, /uol_source_observation_failed/);
});

test("HTML e destinos secundários ficam isolados na manutenção", () => {
  const maintenance = methodSource("  async runMaintenanceTick(", "  async alarm(");
  assert.match(maintenance, /fetchListing\(/);
  assert.match(maintenance, /targetNames:\s*\["canal2",\s*"discord"\]/);
  assert.doesNotMatch(maintenance, /targetNames:\s*\["main"\]/);
});

test("telemetria frequente usa snapshots e observações limitadas", () => {
  const scan = methodSource("  async scan(", "  async runMaintenanceTick(");
  const maintenance = methodSource("  async runMaintenanceTick(", "  async alarm(");
  const observations = methodSource("  recordSourceCards(", "  updateSourceHealth(");

  assert.match(scan, /setRuntimeSnapshot\("api"/);
  assert.doesNotMatch(scan, /setMetadata\("api_/);
  assert.match(scan, /shouldPersistRunSummary\(/);
  assert.match(maintenance, /setRuntimeSnapshot\("html"/);
  assert.match(maintenance, /setRuntimeSnapshot\("maintenance"/);
  assert.match(observations, /shouldTouchObservation\(/);
  assert.ok(
    scan.indexOf("this.scanInFlight = false") < scan.indexOf('setRuntimeSnapshot("api"'),
    "falha de telemetria não pode manter o polling travado",
  );
  assert.ok(
    maintenance.indexOf("this.maintenanceInFlight = false") <
      maintenance.indexOf('setRuntimeSnapshot("maintenance"'),
    "falha de telemetria não pode manter a manutenção travada",
  );
  assert.match(scan, /uol_api_poll_telemetry_failed/);
  assert.match(maintenance, /uol_maintenance_telemetry_failed/);
});

test("configuração gratuita preserva polling rápido e limita manutenção e conexões", () => {
  assert.match(workerConfig, /"ALARM_INTERVAL_SECONDS":\s*"15"/);
  assert.match(workerConfig, /"MAINTENANCE_INTERVAL_SECONDS":\s*"60"/);
  assert.match(workerConfig, /"MAIN_IMAGE_WAIT_SECONDS":\s*"60"/);
  assert.match(workerConfig, /"DELIVERY_CONCURRENCY":\s*"6"/);
});

test("cada alarme periódico rearma uma vez por execução", () => {
  const primary = methodSource("  async alarm() {", "  reconcileUnknownMainFromForward(");
  const maintenanceClass = workerSource.slice(workerSource.indexOf("export class UolTelegramMaintenance"));
  const maintenanceAlarmStart = maintenanceClass.indexOf("  async alarm() {");
  const maintenanceAlarmEnd = maintenanceClass.indexOf("\n  }\n}\n\nexport default", maintenanceAlarmStart);
  const maintenanceAlarm = maintenanceClass.slice(maintenanceAlarmStart, maintenanceAlarmEnd);

  assert.equal((primary.match(/setAlarm\(/g) || []).length, 1);
  assert.equal((maintenanceAlarm.match(/setAlarm\(/g) || []).length, 1);
});

test("coletor Discord legado falha fechado sem configuração explícita", () => {
  assert.match(
    discordRollbackSource,
    /function collectorEnabled\(env\)[\s\S]*?env\.COLLECTOR_ENABLED \|\| "false"/,
  );
});
