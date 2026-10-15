const OPERATIONS_TOPIC = "https://ntfy.sh/macXntfy-8130";
const REQUEST_TIMEOUT_MS = 5_000;
const DEFAULT_RETRY_AFTER_MS = 60_000;

const KIND_LABELS = Object.freeze({
  "ticket-api": "API de ofertas",
  "ticket-api-contract": "Contrato da API de ofertas",
  "ticket-api-expiry": "Validade da credencial da API",
  "telegram-webhook": "Webhook do Telegram",
  "failed-scans": "Ciclos de descoberta",
  "source-health": "Saúde das fontes",
  "delivery-slo": "Prazo da fila de entrega",
  "ticket-delivery": "Integridade da entrega de ingressos",
  "delivery-queue": "Fila de entrega",
  monitor: "Monitor",
});

export function buildOperationalNotification(event = {}) {
  const kind = Object.hasOwn(KIND_LABELS, event?.kind) ? event.kind : "monitor";
  const severity = ["critical", "warning", "info"].includes(event?.severity)
    ? event.severity
    : "warning";
  const recovered = event?.recovered === true;
  return {
    body: [
      recovered ? "UOL bot: incidente recuperado." : "UOL bot: atenção necessária.",
      `Tipo: ${KIND_LABELS[kind]}.`,
      `Nível: ${severity}.`,
    ].join("\n"),
    priority: recovered || severity === "info" ? "3" : severity === "critical" ? "5" : "4",
  };
}

function retryAfterMs(response) {
  const value = String(response.headers?.get("Retry-After") || "").trim();
  const now = Date.now();
  const seconds = /^\d+(?:\.\d+)?$/.test(value) ? Number(value) : NaN;
  const parsed = Number.isFinite(seconds)
    ? seconds * 1_000
    : value ? Date.parse(value) - now : NaN;
  if (!Number.isFinite(parsed)) return DEFAULT_RETRY_AFTER_MS;
  const delay = Math.max(1_000, Math.ceil(parsed));
  const retryAt = now + delay;
  // Preserve the server's delay whenever the outbox can store its due date.
  if (!Number.isSafeInteger(delay) || !Number.isSafeInteger(retryAt)
    || !Number.isFinite(new Date(retryAt).getTime())) return DEFAULT_RETRY_AFTER_MS;
  return delay;
}

// The caller reserves and persists each attempt. Network ambiguity must not be
// retried as a confirmed rejection, and HTTP acceptance is not a read receipt.
export async function sendOperationalNotification(event, fetchImpl = fetch) {
  const notification = buildOperationalNotification(event);
  const controller = new AbortController();
  let timedOut = false;
  let timer;
  const deadline = new Promise((resolve) => {
    timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
      resolve(null);
    }, REQUEST_TIMEOUT_MS);
  });
  try {
    const response = await Promise.race([
      Promise.resolve().then(() => fetchImpl(OPERATIONS_TOPIC, {
        method: "POST",
        redirect: "error",
        headers: {
          "Content-Type": "text/plain; charset=utf-8",
          Title: "UOL bot - aviso tecnico",
          Priority: notification.priority,
        },
        body: notification.body,
        signal: controller.signal,
      })),
      deadline,
    ]);
    if (timedOut || !response) return { status: "uncertain", reason: "timeout" };
    const httpStatus = Number(response.status);
    // Do not parse or expose the notification response, including its ID.
    try {
      response.body?.cancel()?.catch(() => {});
    } catch {
      // A response-body cleanup failure cannot change an HTTP acknowledgement.
    }
    if (httpStatus >= 200 && httpStatus < 300) {
      return { status: "accepted", reason: "http_accepted", httpStatus };
    }
    if (httpStatus === 429 || httpStatus >= 500 && httpStatus < 600) {
      return {
        status: "retryable",
        reason: httpStatus === 429 ? "rate_limited" : "server_error",
        httpStatus,
        retryAfterMs: retryAfterMs(response),
      };
    }
    return { status: "permanent", reason: "http_rejected", httpStatus };
  } catch {
    return { status: "uncertain", reason: timedOut ? "timeout" : "network_error" };
  } finally {
    clearTimeout(timer);
  }
}
