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

test("polling crítico usa API e agenda três destinos sem bloquear a próxima coleta", () => {
  const scan = methodSource("  async scan(", "  async runMaintenanceTick(");
  assert.match(scan, /this\.fetchAllApi\(\)/);
  assert.match(scan, /this\.fetchTicketListing\(\)/);
  assert.match(scan, /Promise\.all\(/);
  assert.match(scan, /mergeOfferCards\(apiCards, ticketListingCards\)/);
  assert.match(scan, /buildApiSnapshotFingerprint\(/);
  assert.match(scan, /discoverySnapshotChanged/);
  assert.match(scan, /buildApiHealthSnapshot\(/);
  assert.ok(
    scan.indexOf("buildApiSnapshotFingerprint(") < scan.indexOf("resolveListingCards("),
    "fingerprint deve ser calculada antes da reconciliação completa",
  );
  assert.match(scan, /waitForMainImage:\s*true/);
  assert.match(scan, /targetNames:\s*\["main", "canal2"\]/);
  const mainTargets = [...scan.matchAll(/targetNames:\s*\["main", "canal2"\]/g)];
  const discordSchedules = [...scan.matchAll(/this\.scheduleDiscordDelivery\(/g)];
  assert.equal(mainTargets.length, 3);
  assert.equal(discordSchedules.length, 3);
  for (let index = 0; index < mainTargets.length; index += 1) {
    assert.ok(
      mainTargets[index].index < discordSchedules[index].index,
      "principal/canal2 deve preceder Discord em todo ramo crítico",
    );
  }
  assert.equal((scan.match(/rows:\s*delivered\.selectedRows/g) || []).length, 3);
  assert.doesNotMatch(scan, /await this\.scheduleDiscordDelivery/);
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

test("HTML crítico de ingressos compartilha o teto de 10s da API", () => {
  const criticalTicket = methodSource("  async fetchTicketListing(", "  currentDeliveryMode(");
  assert.match(criticalTicket, /TICKET_LIST_URL/);
  assert.match(criticalTicket, /10_000/);
});

test("HTML, retries secundários e ciclo de disponibilidade ficam na manutenção", () => {
  const maintenance = methodSource("  async runMaintenanceTick(", "  async alarm(");
  assert.match(maintenance, /fetchListing\(/);
  assert.match(maintenance, /targetNames:\s*\["discord"\]/);
  assert.match(maintenance, /targetNames:\s*\["canal2"\]/);
  assert.match(maintenance, /primePendingDiscordImageCache\(/);
  assert.match(maintenance, /upgradeTimedOutMainImages\(/);
  assert.match(maintenance, /processDiscordAvailabilitySync\(/);
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
  assert.match(maintenance, /runtime:html_snapshot_fingerprint/);
  assert.match(maintenance, /runtime:html_snapshot_reconciled_at/);
  assert.match(maintenance, /shouldReconcileHtmlSnapshot\(/);
  assert.ok(
    maintenance.indexOf("buildApiSnapshotFingerprint(") <
      maintenance.indexOf("resolveListingCards("),
    "fingerprint HTML deve anteceder a reconciliação do ledger",
  );
  assert.ok(
    maintenance.indexOf("this.updateSourceHealth(") <
      maintenance.indexOf('this.setRuntimeSnapshot("html"'),
    "contagem HTML bem-sucedida só pode mudar após validar a fonte",
  );
});

test("disponibilidade Discord parte da fila indexada, sem OR scan em offers", () => {
  const availability = methodSource(
    "  async processDiscordAvailabilitySync(",
    "  async processRestockSync(",
  );
  assert.match(availability, /FROM discord_availability_sync AS d/);
  assert.match(availability, /JOIN offers AS o ON o\.id = d\.offer_id/);
  assert.match(availability, /sold_out_synced_at = ''/);
  assert.match(availability, /restock_synced_at = ''/);
  assert.match(availability, /INDEXED BY discord_avail_sold_due_v21/);
  assert.match(availability, /INDEXED BY discord_avail_restock_due_v21/);
  assert.match(availability, /ORDER BY o\.sold_out_at ASC/);
  assert.match(availability, /ORDER BY o\.restocked_at ASC/);
  assert.doesNotMatch(availability, /ORDER BY d\.sold_out_next_attempt_at/);
  assert.doesNotMatch(availability, /LEFT JOIN discord_availability_sync/);
  assert.doesNotMatch(availability, /\) OR \(/);
});

test("probe crítico fica restrito a ingressos e sincroniza os mesmos canais", () => {
  const scan = methodSource("  async scan(", "  async runMaintenanceTick(");
  const probes = methodSource("  async processTicketAvailabilityProbes(", "  evaluateSoldOut(");
  assert.match(scan, /processTicketAvailabilityProbes\(/);
  assert.match(probes, /link LIKE '%\/campanhasdeingresso\/%'/);
  assert.match(probes, /TICKET_SOLD_OUT_PROBE_DAILY_LIMIT/);
  assert.match(probes, /TICKET_SOLD_OUT_PROBES_PER_SCAN/);
  assert.match(probes, /probeTicketOfferWithControl\(/);
  assert.match(probes, /control\?\.link/);
  assert.match(probes, /o\.missing_since <> ''/);
  assert.match(probes, /s\.last_result = 'listing_absence_pending'/);
  assert.match(probes, /processSoldOutSync\(now, \{ onlyIds: \[row\.id\] \}/);
  assert.match(probes, /processDiscordAvailabilitySync\(now, 1, \{[\s\S]*onlyIds: \[row\.id\]/);
  assert.match(probes, /s\.attempts < \?/);
  assert.match(probes, /INDEXED BY ticket_probe_due_v21/);
  assert.doesNotMatch(probes, /link NOT LIKE '%\/campanhasdeingresso\/%'/);
  assert.match(workerSource, /normalizeTicketProbeAt\(nextAt\)/);
  assert.match(workerSource, /scheduleTicketAbsenceProbe\(/);
  assert.match(workerSource, /clearTicketAbsenceProbe\(/);
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
    "main_image_upgrade_next_attempt_at <= ?",
  ]) {
    const filterIndex = upgrade.indexOf(filter);
    assert.ok(filterIndex > 0 && filterIndex < limitIndex, `filtro tardio fora do SQL: ${filter}`);
  }
  assert.match(upgrade, /WHEN discord_image_proxy_url <> '' THEN 0/);
  assert.match(upgrade, /WHEN discord_message_id <> '' THEN 1/);
  assert.match(upgrade, /first_seen_at DESC/);
  assert.match(upgrade, /discordImageProxyForOffer\(row, originalOffer\)/);
  assert.match(upgrade, /telegramImageRemoteStrategy:\s*"discord_proxy"/);
  assert.match(upgrade, /INDEXED BY offers_main_image_upgrade_due_v25/);
});

test("cache de imagem não repete oferta que já tem proxy ou tentativa terminal", () => {
  const prime = methodSource(
    "  async primePendingDiscordImageCache(",
    "  async upgradeTimedOutMainImages(",
  );
  assert.match(prime, /discord_image_cache_attempts < \?/);
  assert.match(prime, /discord_image_proxy_url = ''/);
  assert.match(prime, /discord_image_cache_next_attempt_at <= \?/);
  assert.match(prime, /INDEXED BY offers_discord_image_cache_due_v25/);
});

test("proxy Discord alimenta o envio Telegram com fallback tardio", () => {
  const delivery = methodSource("  async processDeliveryQueue(", "  async processDiscussionComments(");
  const primaryAlarm = methodSource("  async alarm() {", "  reconcileUnknownMainFromForward(");
  assert.match(delivery, /row\.discord_image_proxy_url/);
  assert.match(delivery, /telegramImageRemoteStrategy:\s*"discord_proxy"/);
  assert.match(delivery, /discord_image_proxy_url = COALESCE\(NULLIF\(\?, ''\)/);
  assert.match(delivery, /if \(result\.deferred\) \{[\s\S]*recordImageDelivery/);
  assert.match(primaryAlarm, /result\.newOffers \|\| result\.mainSent/);
  assert.match(primaryAlarm, /BEEPER_RECOVERY_METADATA_KEY/);
  assert.match(primaryAlarm, /beeperRecovery\.filterActive/);
  assert.match(
    primaryAlarm,
    /\(maintenanceUrgent \|\| beeperRecoveryUrgent\) &&[\s\S]*maintenanceBudget\.maintenanceAllowed/,
  );
});

test("WhatsApp crítico independe da manutenção e não sonda gateway sem fila vencida", () => {
  const primaryAlarm = methodSource("  async alarm() {", "  reconcileUnknownMainFromForward(");
  const maintenance = methodSource("  async runMaintenanceTick(", "  async alarm(");
  const criticalBeeper = methodSource(
    "  beeperQueueHasDueWork(",
    "  async processDiscussionComments(",
  );
  const beeperDiagnostics = methodSource(
    "  beeperQueueDiagnostics(",
    "  beeperQueueHasDueWork(",
  );
  const discordSchedule = methodSource("  scheduleDiscordDelivery(", "  storageUsageSnapshot(");
  const beeperSchedule = methodSource(
    "  scheduleCriticalBeeperDelivery(",
    "  storageUsageSnapshot(",
  );

  assert.match(primaryAlarm, /scheduleCriticalBeeperDelivery\("alarm"\)/);
  assert.doesNotMatch(primaryAlarm, /await this\.scheduleCriticalBeeperDelivery/);
  assert.doesNotMatch(maintenance, /processBeeperDeliveryQueue\(/);
  assert.doesNotMatch(maintenance, /processCriticalBeeperDeliveryQueue\(/);
  assert.match(discordSchedule, /discordSent[\s\S]*processCriticalBeeperDeliveryQueue\(/);
  assert.match(beeperSchedule, /withDetachedStorageCycle\(/);
  assert.match(beeperSchedule, /this\.ctx\.waitUntil\(task\)/);
  assert.match(criticalBeeper, /INDEXED BY beeper_delivery_due_v23/);
  assert.match(criticalBeeper, /INDEXED BY beeper_delivery_inflight_v24/);
  assert.match(beeperDiagnostics, /INDEXED BY beeper_delivery_pending_v25/);
  assert.match(criticalBeeper, /if \(rows\.length\)/);
  assert.doesNotMatch(criticalBeeper, /rows\.length \|\| previousGateway\.gatewayOk/);
});

test("saúde operacional e reparos pesados compartilham gate de cinco minutos", () => {
  const maintenance = methodSource("  async runMaintenanceTick(", "  async alarm(");
  const healthGate = methodSource("  operationalHealthDue(", "  async processOperationalHealth(");

  assert.match(healthGate, /OPS_HEALTH_INTERVAL_SECONDS/);
  assert.match(healthGate, /ops_health_checked_at/);
  assert.match(
    maintenance,
    /if \(operationalHealthDue\) \{[\s\S]*repairKnownMaintenanceDeadLetters\(/,
  );
  assert.match(
    maintenance,
    /if \(operationalHealthDue\) \{[\s\S]*processOperationalHealth\(/,
  );
});

test("telemetria frequente usa snapshots e observações limitadas", () => {
  const scan = methodSource("  async scan(", "  async runMaintenanceTick(");
  const maintenance = methodSource("  async runMaintenanceTick(", "  async alarm(");
  const observations = methodSource("  recordSourceCards(", "  updateSourceHealth(");

  assert.match(scan, /setCriticalSourceSnapshot\(/);
  assert.doesNotMatch(scan, /setMetadata\("api_/);
  assert.match(scan, /shouldPersistRunSummary\(/);
  assert.match(maintenance, /setRuntimeSnapshot\("html"/);
  assert.match(maintenance, /setRuntimeSnapshot\("maintenance"/);
  assert.match(observations, /shouldTouchObservation\(/);
  assert.ok(
    scan.indexOf("this.scanInFlight = false") < scan.indexOf("setCriticalSourceSnapshot("),
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
  assert.ok((maintenanceAlarm.match(/setAlarm\(/g) || []).length >= 1);
  assert.ok(
    primary.indexOf("setAlarm(") < primary.indexOf('this.scan("alarm")'),
    "alarme crítico deve existir antes de qualquer leitura do scan",
  );
  assert.ok(
    maintenanceAlarm.indexOf("setAlarm(") < maintenanceAlarm.indexOf("runMaintenanceTick"),
    "alarme de manutenção deve existir antes do RPC pesado",
  );
  assert.match(maintenanceAlarm, /result\?\.retryAt/);
});

test("polling usa aliases indexados e mede rowsRead reais", () => {
  const primary = methodSource("  async alarm() {", "  reconcileUnknownMainFromForward(");
  const resolution = methodSource("  resolveListingCards(", "  async processPending(");
  const lookup = methodSource("  queryIdentityRows(", "  identityRowsForCardRows(");
  const tracking = methodSource("  trackSqlCursor(", "  loadStorageUsage(");
  const maintenance = methodSource("  async runMaintenanceTick(", "  async alarm(");

  assert.match(resolution, /this\.findIdentityRowsForCards\(cards\)/);
  assert.doesNotMatch(resolution, /SELECT[\s\S]*FROM offers[\s\S]*\.toArray\(\)/);
  assert.match(lookup, /offer_identity_aliases AS a/);
  assert.match(tracking, /cursor\.rowsRead/);
  assert.match(tracking, /cursor\.rowsWritten/);
  assert.match(workerSource, /AsyncLocalStorage/);
  assert.match(workerSource, /trackSqlCursor\(cursor, this\.storageContext\.getStore\(\)\)/);
  assert.match(tracking, /recordStorageUsage\([\s\S]*storageContext/);
  assert.match(maintenance, /storage_read_budget_guard/);
  assert.doesNotMatch(maintenance, /reconcileDeliveryLedger/);
  assert.match(workerSource, /primaryEstimatedRowsRead/);
  assert.match(primary, /budget\.recommendedPollIntervalSeconds/);
  assert.match(primary, /!budget\.primaryAllowed/);
});

test("handoff ao Discord contém somente o lote realmente selecionado", () => {
  const delivery = methodSource("  async processDeliveryQueue(", "  async processDiscussionComments(");

  assert.match(delivery, /selectedRows:\s*selected\.map\(\(entry\) => entry\.row\)/);
  assert.doesNotMatch(delivery, /selectedRows:\s*candidates/);
});

test("readiness rearma antes do cache e não repete consultas pesadas por 15s", () => {
  const readiness = methodSource("  async getReadiness(", "  getPublicOffers(");

  assert.ok(
    readiness.indexOf("this.ensureAlarm()") < readiness.indexOf("this.readinessCache"),
    "o cache nunca pode impedir a recuperação do alarme",
  );
  assert.doesNotMatch(readiness, /this\.ctx\.storage\.getAlarm\(\)/);
  assert.doesNotMatch(readiness, /SELECT finished_at FROM runs/);
  assert.match(readiness, /READINESS_CACHE_TTL_MS/);
});

test("quota mantém contadores por etapa no snapshot persistido", () => {
  assert.match(workerSource, /stageReads/);
  assert.match(workerSource, /stageWrites/);
  assert.match(workerSource, /recordStorageStage\(/);
  assert.match(workerSource, /withStorageStage\(\s*"delivery"/);
  assert.match(workerSource, /withStorageStage\(\s*"tickets"/);
  assert.match(workerSource, /withStorageStage\(\s*"images"/);
  assert.match(workerSource, /withStorageStage\(\s*"comments"/);
  assert.match(workerSource, /durableObjectRowsReadToday: storageReadBudget/);
  assert.match(workerSource, /storageReadBudget,\n      storageWriteBudget,\n      lastScanAt/);
  assert.match(workerSource, /storageWriteBudgetHealthy/);
  assert.match(workerSource, /runtime:storage_usage/);
  assert.match(workerSource, /healthCards/);
  assert.match(workerSource, /recentApiCardsForHealth[\s\S]*healthCards/);
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
