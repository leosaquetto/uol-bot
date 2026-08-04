const MAX_ERROR_LENGTH = 240;
const MAX_EXTERNAL_ID_LENGTH = 180;

function safeText(value, limit) {
  return String(value ?? "")
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, limit);
}

function positiveInteger(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

export function deliveryEventKey({
  offerId,
  target,
  operation,
  attempt = 0,
  generation = 1,
  state = "",
} = {}) {
  const parts = [offerId, target, operation, positiveInteger(attempt), positiveInteger(generation, 1)];
  if (state) parts.push(state);
  return parts
    .map((value) => String(value ?? "").trim())
    .join("|");
}

export function normalizeDeliveryEvent(event = {}) {
  const occurredAt = String(event.occurredAt || new Date().toISOString());
  return {
    dedupeKey: String(event.dedupeKey || deliveryEventKey(event)),
    offerId: safeText(event.offerId, 180),
    target: safeText(event.target, 32),
    operation: safeText(event.operation, 48),
    state: safeText(event.state, 48),
    attempt: positiveInteger(event.attempt),
    generation: Math.max(1, positiveInteger(event.generation, 1)),
    occurredAt: Number.isFinite(Date.parse(occurredAt))
      ? occurredAt
      : new Date().toISOString(),
    externalId: safeText(event.externalId, MAX_EXTERNAL_ID_LENGTH),
    error: safeText(event.error, MAX_ERROR_LENGTH),
  };
}

export function recordDeliveryEvent(execute, event) {
  const normalized = normalizeDeliveryEvent(event);
  if (!normalized.offerId || !normalized.target || !normalized.operation || !normalized.state) {
    return false;
  }
  const result = execute(
    `INSERT OR IGNORE INTO delivery_events(
       dedupe_key, offer_id, target, operation, state, attempt, generation,
       occurred_at, external_id, error
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    normalized.dedupeKey,
    normalized.offerId,
    normalized.target,
    normalized.operation,
    normalized.state,
    normalized.attempt,
    normalized.generation,
    normalized.occurredAt,
    normalized.externalId,
    normalized.error,
  );
  return Number(result?.changes || 0) > 0;
}

export function trimDeliveryEvents(execute, offerId, limit = 240) {
  const boundedLimit = Math.min(1_000, Math.max(1, positiveInteger(limit, 240)));
  execute(
    `DELETE FROM delivery_events
     WHERE offer_id = ?
       AND id NOT IN (
         SELECT id FROM delivery_events
         WHERE offer_id = ?
         ORDER BY occurred_at DESC, id DESC
         LIMIT ?
       )`,
    String(offerId || ""),
    String(offerId || ""),
    boundedLimit,
  );
}

export function boundedReconciliationCandidates(rows = [], limit = 32) {
  const boundedLimit = Math.min(32, Math.max(1, positiveInteger(limit, 32)));
  const seen = new Set();
  const candidates = [];
  for (const row of rows || []) {
    const offerId = String(row?.offer_id || row?.offerId || "").trim();
    if (!offerId || seen.has(offerId)) continue;
    seen.add(offerId);
    candidates.push(row);
    if (candidates.length >= boundedLimit) break;
  }
  return candidates;
}

export function summarizeDeliveryTimeline(rows = [], limit = 20) {
  return [...rows]
    .sort((left, right) => (
      String(right.occurred_at || right.occurredAt || "")
        .localeCompare(String(left.occurred_at || left.occurredAt || "")) ||
      Number(right.id || 0) - Number(left.id || 0)
    ))
    .slice(0, Math.min(50, Math.max(1, positiveInteger(limit, 20))))
    .map((row) => ({
      target: safeText(row.target, 32),
      operation: safeText(row.operation, 48),
      state: safeText(row.state, 48),
      attempt: positiveInteger(row.attempt),
      generation: Math.max(1, positiveInteger(row.generation, 1)),
      occurredAt: String(row.occurred_at || row.occurredAt || ""),
      externalId: safeText(row.external_id || row.externalId, MAX_EXTERNAL_ID_LENGTH),
      error: safeText(row.error, MAX_ERROR_LENGTH),
    }));
}
