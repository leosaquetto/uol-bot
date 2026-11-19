// Operational alerts travel through the owner's private ntfy -> saquettoOS
// bridge (same NotificationEnvelope contract as claude-notify and the gcp-box
// ready monitor). The public ntfy.sh topic timed out from Cloudflare egress and
// is not read by saquettoOS, so it is no longer used.
const REQUEST_TIMEOUT_MS = 8_000;
const DEFAULT_TOPIC = "saquettoos";
const ENVELOPE_SOURCE = "saquettoos";
const ENVELOPE_ROUTE = "saquettoOS://home";
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
  digest: "Resumo do dia",
  monitor: "Monitor",
});

// `detail` is the text built at send time (offer title, what happened, what to
// do). Without it the notice falls back to the type and level only.
export function buildOperationalNotification(event = {}, detail = []) {
  const kind = Object.hasOwn(KIND_LABELS, event?.kind) ? event.kind : "monitor";
  const severity = ["critical", "warning", "info"].includes(event?.severity)
    ? event.severity
    : "warning";
  const recovered = event?.recovered === true;
  const lines = (Array.isArray(detail) ? detail : [])
    .map((line) => String(line || "").replace(/[\u0000-\u001f\u007f]+/g, " ").trim())
    .filter(Boolean).slice(0, 12);
  if (kind === "digest") {
    return {
      title: "UOL bot - resumo do dia",
      body: (lines.length ? lines : ["Sem avisos nas últimas 24 h."]).join("\n").slice(0, 1_500),
      priority: "2",
    };
  }
  const legacy = [
    event?.test === true ? "UOL bot: TESTE técnico autorizado." :
      recovered ? "UOL bot: incidente recuperado." : "UOL bot: atenção necessária.",
    `Tipo: ${KIND_LABELS[kind]}.`,
  ];
  // With send-time text: what happened, which offer and what to do. A recovery
  // only names the offer (lines[1]) when there is one.
  const extra = !lines.length ? [`Nível: ${severity}.`]
    : recovered ? lines.slice(1, 2) : lines;
  return {
    title: event?.test === true ? "UOL bot - teste tecnico" : "UOL bot - aviso tecnico",
    body: [...legacy, ...extra].join("\n").slice(0, 1_000),
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

// Returns null when the private bridge is not configured. Never returns the token
// to callers that persist or display state.
export function operationsTransport(env = {}) {
  const token = String(env?.OPS_NTFY_TOKEN || "").trim();
  const topic = String(env?.OPS_NTFY_TOPIC || DEFAULT_TOPIC).trim();
  let server;
  try {
    server = new URL(String(env?.OPS_NTFY_SERVER || "").trim());
  } catch {
    return null;
  }
  if (server.protocol !== "https:" || server.username || server.password || server.search ||
    server.hash || server.pathname !== "/" || !token || /[\u0000-\u001f\u007f]/.test(token) ||
    !/^[A-Za-z0-9_-]{1,64}$/.test(topic)) return null;
  return { url: server.origin + "/", topic, token };
}

export function buildOperationalEnvelope(event = {}, detail = []) {
  const notification = buildOperationalNotification(event, detail);
  const rawId = typeof event?.id === "string" && /^[A-Za-z0-9:_-]{1,110}$/.test(event.id)
    ? event.id : "";
  return {
    notification,
    envelope: {
      id: `uol-ops-${rawId || (event?.test === true ? `test-${Date.now()}` : "unknown")}`,
      source: ENVELOPE_SOURCE,
      title: notification.title,
      body: notification.body,
      route: ENVELOPE_ROUTE,
    },
  };
}

// The caller reserves and persists each attempt. Network ambiguity must not be
// retried as a confirmed rejection, and HTTP acceptance is not a read receipt.
export async function sendOperationalNotification(event, env = {}, fetchImpl = fetch) {
  let timedOut = false;
  let timer;
  let phase = "request";
  try {
    const transport = operationsTransport(env);
    if (!transport) return { status: "permanent", reason: "transport_unconfigured" };
    // `event.detail` is transient send-time text; it is never persisted.
    const { notification, envelope } = buildOperationalEnvelope(event, event?.detail);
    const controller = new AbortController();
    const init = {
      method: "POST",
      // workerd rejects redirect:"error" before egress. Manual also prevents
      // forwarding the private notification to any redirected destination.
      redirect: "manual",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        Authorization: `Bearer ${transport.token}`,
      },
      body: JSON.stringify({
        topic: transport.topic,
        title: notification.title,
        priority: Number(notification.priority),
        message: JSON.stringify(envelope),
      }),
      signal: controller.signal,
    };
    // Validate in the native runtime before invoking transport. A local request
    // error is a proven non-send; fetch rejection remains ambiguous.
    new Request(transport.url, init);
    const deadline = new Promise((resolve) => {
      timer = setTimeout(() => {
        timedOut = true;
        controller.abort();
        resolve(null);
      }, REQUEST_TIMEOUT_MS);
    });
    phase = "invocation";
    const request = fetchImpl(transport.url, init);
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
    if (httpStatus >= 200 && httpStatus < 300) {
      // ntfy confirms with the stored message. Only the event type and topic are
      // checked; the response (including its ID) is never persisted or exposed.
      let receipt = null;
      try {
        const text = await Promise.race([response.text(), deadline]);
        if (timedOut) return { status: "uncertain", reason: "timeout" };
        receipt = text && text.length <= 65_536 ? JSON.parse(text) : null;
      } catch {
        receipt = null;
      }
      if (receipt?.event === "message" && receipt?.topic === transport.topic) {
        return { status: "accepted", reason: "http_accepted", httpStatus };
      }
      return { status: "uncertain", reason: "response_invalid", httpStatus };
    }
    // Do not parse or expose rejection bodies.
    try {
      response.body?.cancel()?.catch(() => {});
    } catch {
      // A response-body cleanup failure cannot change the HTTP outcome.
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
