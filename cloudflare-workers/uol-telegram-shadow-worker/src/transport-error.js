function positiveNumber(value) {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function compactDescription(value) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, 160);
}

export function isTimeoutLikeError(error) {
  const name = String(error?.name || "").toLowerCase();
  const code = String(error?.code || error?.cause?.code || "").toLowerCase();
  const message = String(error?.message || "").toLowerCase();
  return name === "timeouterror" ||
    code === "etimedout" ||
    code.includes("timeout") ||
    /\b(?:timed out|timeout)\b/.test(message);
}

export class DeliveryTransportError extends Error {
  constructor({
    transport,
    operation,
    category = "http",
    status = 0,
    httpStatus = 0,
    retryAfterSeconds = 0,
    ambiguous = false,
    retryable = false,
    description = "",
  }) {
    const normalizedTransport = String(transport || "transport").trim();
    const normalizedOperation = String(operation || "request").trim();
    const normalizedCategory = String(category || "http").trim();
    const normalizedStatus = Number(status || httpStatus || 0);
    const normalizedHttpStatus = Number(httpStatus || 0);
    const normalizedRetryAfter = positiveNumber(retryAfterSeconds);
    const normalizedDescription = compactDescription(description);
    const discriminator = normalizedCategory === "http"
      ? String(normalizedStatus || "error")
      : normalizedCategory;
    const code = [
      normalizedTransport,
      normalizedOperation,
      discriminator,
      ambiguous ? "ambiguous" : "",
    ].filter(Boolean).join("_");
    super(`${code}${normalizedDescription ? `:${normalizedDescription}` : ""}`);
    this.name = "DeliveryTransportError";
    this.code = code;
    this.transport = normalizedTransport;
    this.operation = normalizedOperation;
    this.category = normalizedCategory;
    this.status = normalizedStatus;
    this.httpStatus = normalizedHttpStatus;
    this.retryAfterSeconds = normalizedRetryAfter;
    // Keep the upstream spelling available to retry schedulers without reparsing a message.
    this.retry_after = normalizedRetryAfter;
    this.ambiguous = Boolean(ambiguous);
    this.retryable = Boolean(retryable);
  }
}

export function createHttpTransportError({
  transport,
  operation,
  status,
  httpStatus,
  retryAfterSeconds,
  description,
}) {
  const normalizedStatus = Number(status || httpStatus || 0);
  const normalizedHttpStatus = Number(httpStatus || 0);
  return new DeliveryTransportError({
    transport,
    operation,
    status: normalizedStatus,
    httpStatus: normalizedHttpStatus,
    retryAfterSeconds,
    description,
    retryable: normalizedStatus === 429 ||
      normalizedHttpStatus === 429 ||
      normalizedHttpStatus >= 500,
  });
}

export function createNetworkTransportError({ transport, operation, error, signal, ambiguous }) {
  const timeout = isTimeoutLikeError(error) || isTimeoutLikeError(signal?.reason);
  const result = new DeliveryTransportError({
    transport,
    operation,
    category: timeout ? "timeout" : "network",
    ambiguous,
    retryable: true,
  });
  // Preserve only non-sensitive diagnostics: native fetch errors may include a secret-bearing URL.
  result.sourceErrorName = compactDescription(error?.name || "Error");
  result.sourceErrorCode = compactDescription(error?.code || error?.cause?.code || "");
  return result;
}

export function createAmbiguousResponseTransportError({ transport, operation, httpStatus }) {
  return new DeliveryTransportError({
    transport,
    operation,
    category: "response",
    status: Number(httpStatus || 0),
    httpStatus: Number(httpStatus || 0),
    ambiguous: true,
    retryable: true,
  });
}

export function shouldDeferTransportFallback(error) {
  const status = Number(error?.status || error?.httpStatus || 0);
  return Boolean(error?.ambiguous || error?.retryable || status === 401 || status === 403);
}
