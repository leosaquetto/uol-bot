import { DatabaseSync } from "node:sqlite";

const DEFAULT_CONFIRMATION_TIMEOUT_MS = 12_000;
const DEFAULT_POLL_INTERVAL_MS = 200;

function parseJson(value) {
  try {
    return JSON.parse(String(value || "{}"));
  } catch {
    return {};
  }
}

export function inspectDeliveryRow(row, pendingMessageID, requirePreview = false) {
  if (!row) return { state: "pending" };
  const sendStatus = parseJson(row.sendStatus);
  if (sendStatus.status === "FAIL_PERMANENT") {
    return { state: "rejected" };
  }
  if (sendStatus.status === "FAIL_RETRIABLE") return { state: "pending" };
  if (sendStatus.status !== "SUCCESS" || row.eventID === pendingMessageID) {
    return { state: "pending" };
  }
  const message = parseJson(row.message);
  const previewPresent = Array.isArray(message.links) &&
    message.links.some((link) => Boolean(link?.img));
  if (requirePreview && !previewPresent) {
    return { state: "unknown", code: "preview_missing" };
  }
  return { state: "delivered", previewPresent };
}

export function createDeliveryConfirmation({
  databasePath,
  chatId,
  timeoutMs = DEFAULT_CONFIRMATION_TIMEOUT_MS,
  pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
  now = () => Date.now(),
  sleep = (duration) => new Promise((resolve) => setTimeout(resolve, duration)),
}) {
  if (!String(databasePath || "").trim()) {
    throw new Error("BEEPER_INDEX_DB_PATH is required");
  }
  if (!String(chatId || "").trim()) throw new Error("BEEPER_CHAT_ID is required");
  const database = new DatabaseSync(databasePath, { readOnly: true });
  const findDelivery = database.prepare(`
    SELECT eventID, sendStatus, message
    FROM mx_room_messages
    WHERE roomID = ? AND (eventID = ? OR echo_echoID = ?)
    ORDER BY timestamp DESC
    LIMIT 1
  `);

  return {
    isReady() {
      try {
        findDelivery.get(chatId, "__ready__", "__ready__");
        return true;
      } catch {
        return false;
      }
    },
    async waitForDelivery({ pendingMessageID, requirePreview = false }) {
      const deadline = now() + timeoutMs;
      while (now() < deadline) {
        let outcome;
        try {
          const row = findDelivery.get(chatId, pendingMessageID, pendingMessageID);
          outcome = inspectDeliveryRow(row, pendingMessageID, requirePreview);
        } catch {
          return { state: "unknown", code: "confirmation_unavailable" };
        }
        if (outcome.state !== "pending") return outcome;
        await sleep(Math.min(pollIntervalMs, Math.max(0, deadline - now())));
      }
      return { state: "unknown", code: "confirmation_timeout" };
    },
    close() {
      database.close();
    },
  };
}
