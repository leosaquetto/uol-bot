import { cleanText } from "./core.js";
import { contractHealthSignal } from "./uol-contract.js";

function elapsedMs(from, to) {
  const start = Date.parse(String(from || ""));
  const finish = Date.parse(String(to || ""));
  if (!Number.isFinite(start) || !Number.isFinite(finish) || finish < start) return null;
  return finish - start;
}

function percentile(values, ratio) {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.ceil(sorted.length * ratio) - 1];
}

function summarize(values) {
  const valid = values.filter(Number.isFinite);
  return {
    samples: valid.length,
    latestMs: valid[0] ?? null,
    p50Ms: percentile(valid, 0.5),
    p95Ms: percentile(valid, 0.95),
    maxMs: valid.length ? Math.max(...valid) : null,
  };
}

export function buildLatencyMetrics(rows, now = new Date()) {
  const samples = rows.map((row) => ({
    id: row.id,
    title: cleanText(row.title || row.preview_title || "").slice(0, 120),
    firstSeenAt: row.first_seen_at,
    discordMs: elapsedMs(row.first_seen_at, row.discord_sent_at),
    telegramMs: elapsedMs(row.first_seen_at, row.main_sent_at),
    canal2Ms: elapsedMs(row.first_seen_at, row.canal2_sent_at),
    commentMs: elapsedMs(row.first_seen_at, row.comment_sent_at),
    discordToTelegramMs: elapsedMs(row.discord_sent_at, row.main_sent_at),
  }));
  return {
    windowHours: 24,
    calculatedAt: now.toISOString(),
    offers: rows.length,
    discord: summarize(samples.map((sample) => sample.discordMs)),
    telegram: summarize(samples.map((sample) => sample.telegramMs)),
    canal2: summarize(samples.map((sample) => sample.canal2Ms)),
    comment: summarize(samples.map((sample) => sample.commentMs)),
    discordToTelegram: summarize(samples.map((sample) => sample.discordToTelegramMs)),
    latest: samples.slice(0, 5),
  };
}

export function buildIncidentSignals({
  apiError = "",
  apiFailureStreak = 0,
  apiContract = null,
  apiAuthorizationExpiresAt = "",
  webhookUrlMatches = true,
  webhookPendingUpdates = 0,
  webhookError = "",
  failedRunStreak = 0,
  listingFailureStreak = 0,
  listingDropStreak = 0,
  sourceDivergenceStreak = 0,
  secondsSinceFullSourceSuccess = 0,
  sourceDetails = "",
  ticketIssues = [],
  deliveryIssues = [],
  queueSlo = {},
  queueSloBreachStreak = 0,
  now = new Date(),
} = {}) {
  const signals = [];
  const normalizedApiError = cleanText(apiError).slice(0, 180);
  const apiContractFailure = /uol_api_(?:contract_invalid|json_invalido)/i
    .test(normalizedApiError);
  const authorizationFailure = /(?:401|403|unauthor|forbidden|token|authorization)/i
    .test(normalizedApiError);
  const contractSignal = contractHealthSignal(apiContract) || (apiContractFailure
    ? contractHealthSignal({
      ok: false,
      reason: normalizedApiError.match(/uol_api_contract_invalid:([^:]+)/i)?.[1] ||
        (normalizedApiError.includes("json_invalido") ? "json_invalid" : "invalid"),
      total: normalizedApiError.match(/total=(\d+)/i)?.[1] || 0,
      valid: normalizedApiError.match(/valid=(\d+)/i)?.[1] || 0,
      invalid: normalizedApiError.match(/invalid=(\d+)/i)?.[1] || 0,
    })
    : null);
  if (contractSignal) {
    signals.push({
      ...contractSignal,
      details: `${apiFailureStreak} ciclo(s): ${normalizedApiError || contractSignal.details}`,
    });
  } else if (normalizedApiError && (authorizationFailure || apiFailureStreak >= 3)) {
    signals.push({
      key: "ticket-api",
      severity: authorizationFailure ? "critical" : "warning",
      summary: authorizationFailure
        ? "A API de ofertas rejeitou a autorização"
        : "A API de ofertas falhou em ciclos consecutivos",
      details: `${apiFailureStreak} ciclo(s): ${normalizedApiError}`,
    });
  }
  const apiExpiry = Date.parse(apiAuthorizationExpiresAt);
  const millisecondsUntilApiExpiry = apiExpiry - now.getTime();
  if (
    Number.isFinite(apiExpiry) &&
    millisecondsUntilApiExpiry > 0 &&
    millisecondsUntilApiExpiry <= 14 * 86_400_000
  ) {
    const days = Math.max(1, Math.ceil(millisecondsUntilApiExpiry / 86_400_000));
    signals.push({
      key: "ticket-api-expiry",
      severity: "warning",
      summary: "A credencial técnica da API de ofertas vence em breve",
      details: `${days} dia(s), em ${new Date(apiExpiry).toISOString()}; ` +
        "as duas fontes HTML públicas continuarão ativas",
    });
  }
  if (!webhookUrlMatches || webhookPendingUpdates > 0 || webhookError) {
    signals.push({
      key: "telegram-webhook",
      severity: webhookPendingUpdates > 0 ? "critical" : "warning",
      summary: "O webhook dos comentários do Telegram precisa de atenção",
      details: cleanText([
        `URL correta: ${webhookUrlMatches ? "sim" : "não"}`,
        `pendentes: ${Number(webhookPendingUpdates || 0)}`,
        webhookError,
      ].filter(Boolean).join("; ")).slice(0, 240),
    });
  }
  if (failedRunStreak >= 3) {
    signals.push({
      key: "failed-scans",
      severity: "critical",
      summary: "O monitor falhou em três ou mais ciclos consecutivos",
      details: `${failedRunStreak} ciclos consecutivos com falha`,
    });
  }
  if (
    listingFailureStreak >= 3 ||
    listingDropStreak >= 3 ||
    sourceDivergenceStreak >= 5 ||
    secondsSinceFullSourceSuccess >= 180
  ) {
    signals.push({
      key: "source-health",
      severity: listingFailureStreak >= 3 || secondsSinceFullSourceSuccess >= 300
        ? "critical"
        : "warning",
      summary: "As fontes do Clube UOL estão divergentes ou degradadas",
      details: cleanText([
        `falhas HTML: ${listingFailureStreak}`,
        `quedas: ${listingDropStreak}`,
        `divergências: ${sourceDivergenceStreak}`,
        `sem ciclo conjunto: ${Math.round(secondsSinceFullSourceSuccess)}s`,
      sourceDetails,
    ].filter(Boolean).join("; ")).slice(0, 240),
    });
  }
  const queueOldestAgeMs = Number(queueSlo.oldestAgeMs || 0);
  const queueCriticalPending = Number(queueSlo.criticalPending || 0);
  const queueSecondaryPending = Number(queueSlo.secondaryPending || 0);
  const queueBreach = (
    queueCriticalPending > 0 && queueOldestAgeMs >= 45_000
  ) || (
    queueSecondaryPending > 0 && queueOldestAgeMs >= 10 * 60_000
  );
  if (queueBreach && Number(queueSloBreachStreak || 0) >= 3) {
    signals.push({
      key: "delivery-slo",
      severity: queueCriticalPending > 0 ? "critical" : "warning",
      summary: "A fila de entrega ultrapassou o SLA",
      details: cleanText([
        `principal pendente: ${queueCriticalPending}`,
        `secundária pendente: ${queueSecondaryPending}`,
        `mais antiga: ${Math.round(queueOldestAgeMs / 1_000)}s`,
        `ciclos: ${Number(queueSloBreachStreak || 0)}`,
      ].join("; ")).slice(0, 240),
    });
  }
  for (const issue of ticketIssues) {
    const missing = [issue.missingPhoto ? "foto" : "", issue.missingComment ? "comentário" : ""]
      .filter(Boolean).join(" e ");
    signals.push({
      key: `ticket-delivery:${issue.id}`,
      severity: "warning",
      summary: `Ingresso entregue sem ${missing}`,
      details: cleanText(issue.title || issue.id).slice(0, 180),
    });
  }
  for (const issue of deliveryIssues) {
    const target = cleanText(issue.target || "entrega");
    const state = cleanText(issue.state || "falha");
    signals.push({
      key: `delivery-queue:${issue.id}:${target}`,
      severity: state === "blocked_configuration" ? "warning" : "critical",
      summary: state === "dead_letter"
        ? "Uma oferta esgotou as tentativas de entrega"
        : state === "unknown"
          ? "Uma entrega ficou com resultado externo incerto"
          : state === "main_delivery_slow"
            ? "Uma oferta elegível não chegou ao canal principal no prazo"
            : "Uma entrega está bloqueada por configuração",
      details: cleanText([
        issue.title || issue.id,
        `destino: ${target}`,
        issue.error || "",
      ].filter(Boolean).join("; ")).slice(0, 240),
    });
  }
  return signals;
}

export function buildOperationsAlert(signal, { recovered = false } = {}) {
  const icon = recovered ? "✅" : signal.severity === "critical" ? "🚨" : "⚠️";
  const heading = recovered ? "Incidente resolvido" : "Alerta do monitor Clube UOL";
  return [
    `${icon} ${heading}`,
    "",
    signal.summary,
    signal.details ? `Detalhes: ${signal.details}` : "",
    `Chave: ${signal.key}`,
  ].filter(Boolean).join("\n");
}
