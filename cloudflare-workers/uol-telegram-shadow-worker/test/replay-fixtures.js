import { buildDiscordPayload } from "../src/discord.js";
import {
  classifyDeliveryRow,
  isAmbiguousDeliveryError,
} from "../src/delivery-state.js";
import {
  recordDeliveryEvent,
} from "../src/delivery-ledger.js";
import {
  lateImageUpgradeDue,
  mainImageDeliveryOffer,
} from "../src/image-deadline.js";
import { chooseDeliveryBudget, summarizeQueueSlo } from "../src/queue-policy.js";
import {
  classifyTicketProbeResponse,
  nextTicketProbeState,
} from "../src/ticket-soldout-probe.js";
import { isTelegramMessageMissingError } from "../src/transport-error.js";

const BASE_TIME = "2026-08-04T15:00:00.000Z";
const DELIVERY_CONFIGURATION = {
  main: { ready: true },
  canal2: { enabled: true, ready: true },
  discord: { enabled: true, ready: true },
};

function iso(offsetSeconds = 0) {
  return new Date(Date.parse(BASE_TIME) + offsetSeconds * 1_000).toISOString();
}

function offer(id = "oferta-01", firstSeenAt = iso()) {
  return {
    id,
    link: `https://clube.uol.com.br/campanhasdeingresso/${id}`,
    title: "2 INGRESSOS: Replay Clube UOL",
    previewTitle: "2 INGRESSOS: Replay Clube UOL",
    description: "Assinante UOL, resgate seu benefício no evento de teste do Clube UOL.",
    category: "campanhasdeingresso",
    imageUrl: "https://cdn.example.test/oferta.jpg",
    firstSeenAt,
    first_seen_at: firstSeenAt,
    would_send_canal2: 1,
    main_sent_at: "",
    canal2_sent_at: "",
    discord_sent_at: "",
    main_delivery_attempts: 0,
    canal2_delivery_attempts: 0,
    discord_delivery_attempts: 0,
    main_delivery_in_flight_at: "",
    canal2_delivery_in_flight_at: "",
    discord_delivery_in_flight_at: "",
    main_delivery_unknown_at: "",
    canal2_delivery_unknown_at: "",
    discord_delivery_unknown_at: "",
    delivery_unknown_target: "",
    main_message_id: 0,
    canal2_message_id: 0,
    discord_message_id: "",
    telegram_image_strategy: "",
    main_message_kind: "",
    main_image_upgrade_attempts: 0,
    main_image_upgrade_next_attempt_at: "",
    sold_out_synced: false,
    status: "delivery_pending",
  };
}

function createReplayRecorder() {
  const events = [];
  const seen = new Set();
  const calls = [];
  const execute = (sql, ...params) => {
    if (!String(sql).startsWith("INSERT OR IGNORE INTO delivery_events")) {
      return { changes: 1 };
    }
    const [dedupeKey, offerId, target, operation, state, attempt, generation,
      occurredAt, externalId, error] = params;
    if (seen.has(dedupeKey)) return { changes: 0 };
    seen.add(dedupeKey);
    events.push({
      dedupeKey,
      offerId,
      target,
      operation,
      state,
      attempt,
      generation,
      occurredAt,
      externalId,
      error,
    });
    return { changes: 1 };
  };
  const record = (event) => recordDeliveryEvent(execute, event);
  const call = (value) => calls.push({
    method: "external",
    target: "",
    operation: "",
    offerId: "",
    messageId: "",
    elapsedMs: 0,
    ...value,
  });
  return { events, calls, record, call };
}

function targetCall(row, target, operation, now, recorder) {
  const attempt = Number(row[`${target}_delivery_attempts`] || 0) + 1;
  row[`${target}_delivery_attempts`] = attempt;
  const messageId = `${target}-${row.id}`;
  recorder.record({
    offerId: row.id,
    target,
    operation,
    state: "attempt_started",
    attempt,
    generation: 1,
    occurredAt: now,
    externalId: messageId,
  });
  recorder.call({
    target,
    operation,
    offerId: row.id,
    messageId,
    elapsedMs: Math.max(0, Date.parse(now) - Date.parse(row.first_seen_at)),
  });
  row[`${target}_sent_at`] = now;
  row[`${target}_message_id`] = target === "discord" ? messageId : attempt * 100;
  recorder.record({
    offerId: row.id,
    target,
    operation,
    state: "sent",
    attempt,
    generation: 1,
    occurredAt: now,
    externalId: messageId,
  });
}

function replayApiToAll(recorder) {
  const row = offer();
  const now = iso(1);
  const classification = classifyDeliveryRow(row, DELIVERY_CONFIGURATION, {
    ticket: true,
    now: new Date(now),
  });
  if (classification.actionable.some((item) => item.target === "main")) {
    targetCall(row, "main", "send", now, recorder);
  }
  const afterMain = classifyDeliveryRow(row, DELIVERY_CONFIGURATION, {
    ticket: true,
    now: new Date(now),
  });
  for (const target of ["canal2", "discord"]) {
    if (!afterMain.actionable.some((item) => item.target === target)) continue;
    targetCall(row, target, target === "canal2" ? "copy" : "send", now, recorder);
  }
  row.status = classifyDeliveryRow(row, DELIVERY_CONFIGURATION, {
    ticket: true,
    now: new Date(now),
  }).state === "complete" ? "delivered" : "partial_delivery";
  // An idempotent replay of the same success event must not create a second row.
  recorder.record({
    offerId: row.id,
    target: "main",
    operation: "send",
    state: "sent",
    attempt: 1,
    generation: 1,
    occurredAt: now,
    externalId: "main-oferta-01",
  });
  return [row];
}

function replayLateImageUpgrade(recorder) {
  const row = offer();
  const offerWithDeadline = mainImageDeliveryOffer({ firstSeenAt: row.first_seen_at }, new Date(iso(10)), 60);
  row.main_message_id = 101;
  row.main_message_kind = "text";
  row.telegram_image_strategy = "text_timeout";
  row.main_sent_at = iso(60);
  row.image_url = row.imageUrl;
  recorder.call({
    target: "main",
    operation: "send_text",
    offerId: row.id,
    messageId: String(row.main_message_id),
    elapsedMs: 60_000,
  });
  recorder.record({
    offerId: row.id,
    target: "main",
    operation: "send",
    state: "sent",
    attempt: 1,
    generation: 1,
    occurredAt: iso(60),
    externalId: String(row.main_message_id),
  });
  if (offerWithDeadline.deferTextFallback && lateImageUpgradeDue(row, new Date(iso(61)))) {
    recorder.call({
      method: "PATCH",
      target: "main",
      operation: "edit_media",
      offerId: row.id,
      messageId: String(row.main_message_id),
      elapsedMs: 61_000,
    });
    row.telegram_image_strategy = "photo_upgrade";
    row.main_image_upgrade_attempts = 1;
    recorder.record({
      offerId: row.id,
      target: "main",
      operation: "image_upgrade",
      state: "synced",
      attempt: 1,
      generation: 1,
      occurredAt: iso(61),
      externalId: String(row.main_message_id),
    });
  }
  row.status = "delivered";
  return [row];
}

function replayTicketSoldOut(recorder) {
  const row = offer("ingresso-01", iso());
  let goneCount = 0;
  let attempts = 0;
  const requestedUrl = row.link;
  for (const offset of [30, 35]) {
    const observed = classifyTicketProbeResponse({
      requestedUrl,
      finalUrl: requestedUrl,
      status: 404,
    });
    const state = nextTicketProbeState({
      result: observed.result,
      goneCount,
      attempts,
      now: new Date(iso(offset)),
      confirmGoneCount: 2,
      maxAttempts: 2,
      confirmDelaySeconds: 5,
    });
    recorder.call({
      target: "uol",
      operation: "probe",
      offerId: row.id,
      messageId: "",
      elapsedMs: offset * 1_000,
    });
    goneCount = state.goneCount;
    attempts += 1;
    if (state.action === "confirm") {
      const soldOutAt = iso(offset);
      row.status = "sold_out";
      row.sold_out_at = soldOutAt;
      const payload = buildDiscordPayload(row, {
        soldOutAt,
        publishedAt: row.first_seen_at,
      });
      recorder.call({
        method: "PATCH",
        target: "discord",
        operation: "sold_out_edit",
        offerId: row.id,
        messageId: "discord-ingresso-01",
        elapsedMs: offset * 1_000,
        body: payload.embeds[0].description,
      });
      recorder.record({
        offerId: row.id,
        target: "discord",
        operation: "sold_out",
        state: "synced",
        attempt: 1,
        generation: 1,
        occurredAt: soldOutAt,
        externalId: "discord-ingresso-01",
      });
    }
  }
  return [row];
}

function replayRestock(recorder) {
  const row = offer("restock-01", iso());
  row.status = "sold_out";
  row.sold_out_at = iso(120);
  const soldOutPayload = buildDiscordPayload(row, {
    soldOutAt: row.sold_out_at,
    publishedAt: row.first_seen_at,
  });
  recorder.call({
    method: "PATCH",
    target: "discord",
    operation: "sold_out_edit",
    offerId: row.id,
    messageId: "discord-restock-01",
    body: soldOutPayload.embeds[0].description,
  });
  row.status = "delivered";
  row.restocked_at = iso(180);
  const restockPayload = buildDiscordPayload(row);
  recorder.call({
    method: "PATCH",
    target: "discord",
    operation: "restock_edit",
    offerId: row.id,
    messageId: "discord-restock-01",
    body: restockPayload.embeds[0].description,
  });
  return [row];
}

function replayAmbiguousTimeout(recorder) {
  const row = offer("ambiguous-01", iso());
  const timeout = Object.assign(new Error("timeout"), { ambiguous: true });
  recorder.call({
    target: "main",
    operation: "send",
    offerId: row.id,
    messageId: "",
    elapsedMs: 10_000,
  });
  recorder.record({
    offerId: row.id,
    target: "main",
    operation: "send",
    state: isAmbiguousDeliveryError(timeout) ? "unknown" : "failed",
    attempt: 1,
    generation: 1,
    occurredAt: iso(10),
    error: timeout.message,
  });
  row.status = "delivery_unknown";
  row.main_delivery_unknown_at = iso(10);
  row.delivery_unknown_target = "main";
  return [row];
}

function replayDeletedMessage(recorder) {
  const row = offer("deleted-01", iso());
  row.status = "sold_out";
  row.sold_out_at = iso(30);
  const missing = {
    transport: "telegram",
    status: 400,
    operation: "editMessageCaption",
    description: "Bad Request: message to edit not found",
  };
  recorder.call({
    target: "main",
    operation: "sold_out_edit",
    offerId: row.id,
    messageId: "101",
    elapsedMs: 30_000,
  });
  if (isTelegramMessageMissingError(missing)) {
    row.sold_out_synced = true;
    recorder.record({
      offerId: row.id,
      target: "main",
      operation: "sold_out",
      state: "resolved_missing",
      attempt: 1,
      generation: 1,
      occurredAt: iso(30),
      externalId: "101",
    });
  }
  return [row];
}

function replayBurst(recorder) {
  const rows = Array.from({ length: 24 }, (_, index) => offer(
    `burst-${String(index + 1).padStart(2, "0")}`,
    iso(),
  ));
  const queueSlo = summarizeQueueSlo(rows, new Date(iso(1)));
  const budget = chooseDeliveryBudget({
    storageReadBudget: { primaryAllowed: true, maintenanceAllowed: true },
    queueSlo,
    configuredBatch: 4,
    configuredConcurrency: 6,
  });
  rows.forEach((row, index) => {
    const now = iso(index + 1);
    recorder.call({
      target: "main",
      operation: "send",
      offerId: row.id,
      messageId: `main-${row.id}`,
      elapsedMs: (index + 1) * 1_000,
    });
    row.main_sent_at = now;
    row.status = "delivered";
  });
  return { rows, meta: { budget } };
}

export function runReplayScenario(name, overrides = {}) {
  const recorder = createReplayRecorder();
  let finalOffers;
  let meta = {};
  switch (name) {
    case "api-to-all":
      finalOffers = replayApiToAll(recorder);
      break;
    case "late-image-upgrade":
      finalOffers = replayLateImageUpgrade(recorder);
      break;
    case "ticket-sold-out":
      finalOffers = replayTicketSoldOut(recorder);
      break;
    case "restock":
      finalOffers = replayRestock(recorder);
      break;
    case "ambiguous-timeout":
      finalOffers = replayAmbiguousTimeout(recorder);
      break;
    case "deleted-message":
      finalOffers = replayDeletedMessage(recorder);
      break;
    case "burst-24":
      ({ rows: finalOffers, meta } = replayBurst(recorder));
      break;
    default:
      throw new Error(`unknown_replay_scenario:${name}`);
  }
  return {
    name,
    events: recorder.events,
    finalOffers: overrides.finalOffers || finalOffers,
    calls: recorder.calls,
    meta,
  };
}
