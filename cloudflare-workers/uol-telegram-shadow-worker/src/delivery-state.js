const DEFAULT_RETRY_SECONDS = 15;
const MAX_RETRY_SECONDS = 30 * 60;
const MAX_DATE_MILLISECONDS = 8_640_000_000_000_000;

export function envFlag(value, fallback = false) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized) return Boolean(fallback);
  if (["1", "true", "yes", "on", "enabled"].includes(normalized)) return true;
  if (["0", "false", "no", "off", "disabled"].includes(normalized)) return false;
  return Boolean(fallback);
}

export function isAmbiguousDeliveryError(error) {
  if (error?.ambiguous === true) return true;
  const name = String(error?.name || "").toLowerCase();
  const message = String(error?.message || error || "").toLowerCase();
  return name === "aborterror" || name === "timeouterror" || [
    "timeout",
    "timed out",
    "aborted",
    "network error",
    "connection reset",
    "socket hang up",
  ].some((fragment) => message.includes(fragment));
}

export function deliveryRetryAt(error, attempts, now = new Date(), randomValue = Math.random()) {
  const instant = now instanceof Date && Number.isFinite(now.getTime()) ? now : new Date();
  const retryAfter = Number(error?.retryAfterSeconds || 0);
  const exponential = Math.min(
    MAX_RETRY_SECONDS,
    DEFAULT_RETRY_SECONDS * (2 ** Math.max(0, Number(attempts || 1) - 1)),
  );
  const hasRetryAfter = Number.isFinite(retryAfter) && retryAfter > 0;
  const baseSeconds = hasRetryAfter
    ? retryAfter
    : exponential;
  const boundedRandom = Math.min(1, Math.max(0, Number(randomValue) || 0));
  // retry_after is a floor declared by the provider. Applying downward jitter
  // would deliberately retry before Telegram/Discord allows it.
  const jitter = hasRetryAfter
    ? 1 + boundedRandom * 0.15
    : 0.85 + boundedRandom * 0.3;
  const target = Math.min(
    MAX_DATE_MILLISECONDS,
    instant.getTime() + Math.ceil(baseSeconds * jitter) * 1_000,
  );
  return new Date(target).toISOString();
}

export function deliveryConfiguration(env, telegram, discord) {
  const mainReady = telegram.mainReady ?? Boolean(
    telegram.tokenConfigured && telegram.mainConfigured,
  );
  const canal2Ready = telegram.canal2Ready ?? Boolean(
    telegram.tokenConfigured && telegram.canal2Configured,
  );
  const canal2Enabled = envFlag(env.CANAL2_DELIVERY_ENABLED, telegram.canal2Configured);
  const discordEnabled = envFlag(env.DISCORD_DELIVERY_ENABLED, discord.configured);
  return {
    main: { enabled: true, ready: mainReady },
    canal2: { enabled: canal2Enabled, ready: !canal2Enabled || canal2Ready },
    discord: { enabled: discordEnabled, ready: !discordEnabled || discord.configured },
  };
}

function due(value, now) {
  const timestamp = Date.parse(String(value || ""));
  return !Number.isFinite(timestamp) || timestamp <= now.getTime();
}

function inFlightIsStale(value, now, staleSeconds) {
  const timestamp = Date.parse(String(value || ""));
  if (!Number.isFinite(timestamp)) return Boolean(value);
  return timestamp <= now.getTime() - staleSeconds * 1_000;
}

export function deliveryTargets(
  row,
  configuration,
  { ticket = false, now = new Date(), inFlightStaleSeconds = 60 } = {},
) {
  const staleSeconds = Math.max(15, Number(inFlightStaleSeconds || 60));
  return [
    {
      target: "main",
      required: true,
      ready: configuration.main.ready,
      dependencyMet: true,
      sent: Boolean(row.main_sent_at),
      attempts: Number(row.main_delivery_attempts || 0),
      due: due(row.main_delivery_next_attempt_at, now),
      inFlightAt: String(row.main_delivery_in_flight_at || ""),
      inFlightStale: inFlightIsStale(
        row.main_delivery_in_flight_at,
        now,
        staleSeconds,
      ),
      unknownAt: String(
        row.main_delivery_unknown_at ||
        (row.delivery_unknown_target === "main" ? row.delivery_unknown_at : ""),
      ),
    },
    {
      target: "canal2",
      required: Boolean(row.would_send_canal2 && configuration.canal2.enabled),
      ready: configuration.canal2.ready,
      dependencyMet: Boolean(row.main_sent_at),
      sent: Boolean(row.canal2_sent_at),
      attempts: Number(row.canal2_delivery_attempts || 0),
      due: due(row.canal2_delivery_next_attempt_at, now),
      inFlightAt: String(row.canal2_delivery_in_flight_at || ""),
      inFlightStale: inFlightIsStale(
        row.canal2_delivery_in_flight_at,
        now,
        staleSeconds,
      ),
      unknownAt: String(
        row.canal2_delivery_unknown_at ||
        (row.delivery_unknown_target === "canal2" ? row.delivery_unknown_at : ""),
      ),
    },
    {
      target: "discord",
      required: Boolean(ticket && configuration.discord.enabled),
      ready: configuration.discord.ready,
      dependencyMet: true,
      sent: Boolean(row.discord_sent_at),
      attempts: Number(row.discord_delivery_attempts || 0),
      due: due(row.discord_delivery_next_attempt_at, now),
      inFlightAt: String(row.discord_delivery_in_flight_at || ""),
      inFlightStale: inFlightIsStale(
        row.discord_delivery_in_flight_at,
        now,
        staleSeconds,
      ),
      unknownAt: String(
        row.discord_delivery_unknown_at ||
        (row.delivery_unknown_target === "discord" ? row.delivery_unknown_at : ""),
      ),
    },
  ].filter((target) => target.required);
}

export function classifyDeliveryRow(
  row,
  configuration,
  {
    ticket = false,
    maxAttempts = 10,
    now = new Date(),
    inFlightStaleSeconds = 60,
    targetNames = [],
  } = {},
) {
  const allowedTargets = new Set(targetNames || []);
  const targets = deliveryTargets(row, configuration, {
    ticket,
    now,
    inFlightStaleSeconds,
  }).filter((target) => !allowedTargets.size || allowedTargets.has(target.target));
  const remaining = targets.filter((target) => !target.sent);
  if (!remaining.length) return { state: "complete", actionable: [] };
  const unknown = remaining.filter(
    (target) => target.unknownAt || (target.inFlightAt && target.inFlightStale),
  );
  const activeInFlight = remaining.filter(
    (target) => target.inFlightAt && !target.inFlightStale && !target.unknownAt,
  );
  const actionable = remaining.filter(
    (target) => !target.unknownAt && !target.inFlightAt &&
      target.ready && target.dependencyMet &&
      target.attempts < maxAttempts && target.due,
  );
  if (actionable.length) {
    return {
      state: "actionable",
      actionable,
      unknownTargets: unknown.map((target) => target.target),
      staleUnknownTargets: unknown
        .filter((target) => target.inFlightAt && target.inFlightStale && !target.unknownAt)
        .map((target) => target.target),
    };
  }
  if (unknown.length) {
    return {
      state: "unknown",
      target: unknown[0].target,
      targets: unknown.map((target) => target.target),
      staleUnknownTargets: unknown
        .filter((target) => target.inFlightAt && target.inFlightStale && !target.unknownAt)
        .map((target) => target.target),
      actionable: [],
    };
  }
  if (activeInFlight.length) {
    return {
      state: "in_flight",
      targets: activeInFlight.map((target) => target.target),
      actionable: [],
    };
  }
  if (remaining.some((target) => !target.ready)) {
    return { state: "blocked_configuration", actionable: [] };
  }
  // Um único destino obrigatório esgotado já impede conclusão. A fila ainda
  // pode trabalhar os outros destinos porque dead letters continuam elegíveis
  // para classificação, mas nunca fica presa em "backoff" para sempre.
  if (remaining.some((target) => target.attempts >= maxAttempts)) {
    return { state: "dead_letter", actionable: [] };
  }
  return { state: "backoff", actionable: [] };
}
