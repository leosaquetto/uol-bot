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
  assert.doesNotMatch(scan, /upgradeTimedOutMainImages\(/);
  assert.ok(
    scan.indexOf('recordSourceCards("api"') > scan.indexOf("processDeliveryQueue("),
    "telemetria de fonte deve rodar após a tentativa principal",
  );
  assert.match(scan, /uol_source_observation_failed/);
});

test("HTML e destinos secundários ficam isolados na manutenção", () => {
  const maintenance = methodSource("  async runMaintenanceTick(", "  async alarm(");
  assert.match(maintenance, /fetchListing\(/);
  assert.match(maintenance, /targetNames:\s*\["discord"\]/);
  assert.match(maintenance, /targetNames:\s*\["canal2"\]/);
  assert.match(maintenance, /primePendingDiscordImageCache\(/);
  assert.match(maintenance, /upgradeTimedOutMainImages\(/);
  assert.ok(
    maintenance.indexOf("this.processDeliveryQueue(") <
      maintenance.indexOf("this.upgradeTimedOutMainImages("),
    "Discord deve fornecer proxy antes do upgrade tardio de imagem",
  );
  assert.ok(
    maintenance.lastIndexOf("this.processDeliveryQueue(") >
      maintenance.indexOf("this.upgradeTimedOutMainImages("),
    "Canal 2 deve encaminhar depois do upgrade de imagem",
  );
  assert.doesNotMatch(maintenance, /targetNames:\s*\["main"\]/);
});

test("lote tardio filtra imagem e backoff antes do limite", () => {
  const upgrade = methodSource(
    "  async upgradeTimedOutMainImages(",
    "  getImageDeliveryHealth(",
  );
  const limitIndex = upgrade.indexOf("LIMIT ?");
  assert.ok(limitIndex > 0);
  for (const filter of [
    "COALESCE(NULLIF(telegram_photo_file_id, ''), NULLIF(image_url, ''),",
    "main_image_upgrade_next_attempt_at = ''",
    "main_image_upgrade_next_attempt_at <= ?",
  ]) {
    const filterIndex = upgrade.indexOf(filter);
    assert.ok(filterIndex > 0 && filterIndex < limitIndex, `filtro tardio fora do SQL: ${filter}`);
  }
  assert.match(upgrade, /discordImageProxyForOffer\(row, originalOffer\)/);
  assert.match(upgrade, /telegramImageRemoteStrategy:\s*"discord_proxy"/);
});

test("proxy Discord aquece próximo envio principal sem bloquear primeiro scan", () => {
  const delivery = methodSource("  async processDeliveryQueue(", "  async processDiscussionComments(");
  const primaryAlarm = methodSource("  async alarm() {", "  reconcileUnknownMainFromForward(");
  assert.match(delivery, /row\.discord_image_proxy_url/);
  assert.match(delivery, /telegramImageRemoteStrategy:\s*"discord_proxy"/);
  assert.match(delivery, /discord_image_proxy_url = COALESCE\(NULLIF\(\?, ''\)/);
  assert.match(delivery, /if \(result\.deferred\) \{[\s\S]*recordImageDelivery/);
  assert.match(primaryAlarm, /result\.newOffers \|\| result\.mainSent/);
  assert.match(primaryAlarm, /ensureMaintenanceAlarm\(maintenanceUrgent\)/);
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
  assert.ok(
    primary.indexOf("setAlarm(") < primary.indexOf('this.scan("alarm")'),
    "alarme crítico deve existir antes de qualquer leitura do scan",
  );
  assert.ok(
    maintenanceAlarm.indexOf("setAlarm(") < maintenanceAlarm.indexOf("runMaintenanceTick"),
    "alarme de manutenção deve existir antes do RPC pesado",
  );
});

test("polling usa aliases indexados e mede rowsRead reais", () => {
  const primary = methodSource("  async alarm() {", "  reconcileUnknownMainFromForward(");
  const resolution = methodSource("  resolveListingCards(", "  async processPending(");
  const lookup = methodSource("  findIdentityRows(", "  chooseIdentityKeeper(");
  const tracking = methodSource("  trackSqlCursor(", "  loadStorageUsage(");
  const maintenance = methodSource("  async runMaintenanceTick(", "  async alarm(");

  assert.match(resolution, /this\.findIdentityRows\(card\)/);
  assert.doesNotMatch(resolution, /SELECT[\s\S]*FROM offers[\s\S]*\.toArray\(\)/);
  assert.match(lookup, /offer_identity_aliases AS a/);
  assert.match(tracking, /cursor\.rowsRead/);
  assert.match(tracking, /cursor\.rowsWritten/);
  assert.match(maintenance, /storage_read_budget_guard/);
  assert.match(workerSource, /primaryEstimatedRowsRead/);
  assert.match(primary, /budget\.recommendedPollIntervalSeconds/);
  assert.match(primary, /!budget\.primaryAllowed/);
});

test("mensagem Telegram ausente encerra sold-out e republica restock", () => {
  const soldOut = methodSource("  async processSoldOutSync(", "  async processRestockSync(");
  const restock = methodSource("  async processRestockSync(", "  evaluateSoldOut(");

  assert.match(soldOut, /isTelegramMessageMissingError\(error\)/);
  assert.match(soldOut, /main_sold_out_synced_at = \?/);
  assert.match(soldOut, /canal2_sold_out_synced_at = \?/);
  assert.match(soldOut, /resolveIncidentWithoutAlert/);
  assert.match(restock, /isTelegramMessageMissingError\(editError\)/);
  assert.match(restock, /sendMainOffer\(this\.env, telegramState\.offer\)/);
  assert.match(restock, /forwardToCanal2\(this\.env, mainMessageId\)/);
  assert.match(restock, /ambiguous \? maxAttempts : attempts/);
});

test("coletor Discord legado falha fechado sem configuração explícita", () => {
  assert.match(
    discordRollbackSource,
    /function collectorEnabled\(env\)[\s\S]*?env\.COLLECTOR_ENABLED \|\| "false"/,
  );
});
