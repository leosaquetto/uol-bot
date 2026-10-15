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
    title: event?.test === true ? "UOL bot - teste tecnico" : "UOL bot - aviso tecnico",
    body: [
      event?.test === true ? "UOL bot: TESTE técnico autorizado." :
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
  let timedOut = false;
  let timer;
  let phase = "request";
  try {
    const notification = buildOperationalNotification(event);
    const controller = new AbortController();
    const init = {
      method: "POST",
      // workerd rejects redirect:"error" before egress. Manual also prevents
      // forwarding the private notification to any redirected destination.
      redirect: "manual",
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        Title: notification.title,
        Priority: notification.priority,
      },
      body: notification.body,
      signal: controller.signal,
    };
    // Validate in the native runtime before invoking transport. A local request
    // error is a proven non-send; fetch rejection remains ambiguous.
    new Request(OPERATIONS_TOPIC, init);
    const deadline = new Promise((resolve) => {
      timer = setTimeout(() => {
        timedOut = true;
        controller.abort();
        resolve(null);
      }, REQUEST_TIMEOUT_MS);
    });
    phase = "invocation";
    const request = fetchImpl(OPERATIONS_TOPIC, init);
    phase = "fetch";
    const response = await Promise.race([
      request,
      deadline,
    ]);
    if (timedOut) return { status: "uncertain", reason: "timeout" };
    phase = "response";
    if (!response) return { status: "uncertain", reason: "response_invalid" };
    const httpStatus = Number(response.status);
    if (!Number.isInteger(httpStatus) || httpStatus < 100 || httpStatus > 599) {
      return { status: "uncertain", reason: "response_invalid" };
    }
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
    // Only fixed stage labels escape this boundary; never persist raw errors,
    // private URLs, headers or provider response bodies.
    if (phase === "request") return { status: "permanent", reason: "request_invalid" };
    const reason = timedOut ? "timeout" : phase === "invocation" ? "fetch_invocation_error"
      : phase === "response" ? "response_invalid" : "fetch_rejected";
    return { status: "uncertain", reason };
  } finally {
    clearTimeout(timer);
  }
}
