import { cleanText } from "./core.js";

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
  webhookUrlMatches = true,
  webhookPendingUpdates = 0,
  webhookError = "",
  failedRunStreak = 0,
  ticketIssues = [],
} = {}) {
  const signals = [];
  const normalizedApiError = cleanText(apiError).slice(0, 180);
  const authorizationFailure = /(?:401|403|unauthor|forbidden|token|authorization)/i
    .test(normalizedApiError);
  if (normalizedApiError && (authorizationFailure || apiFailureStreak >= 3)) {
    signals.push({
      key: "ticket-api",
      severity: authorizationFailure ? "critical" : "warning",
      summary: authorizationFailure
        ? "A API de ingressos rejeitou a autorização"
        : "A API de ingressos falhou em ciclos consecutivos",
      details: `${apiFailureStreak} ciclo(s): ${normalizedApiError}`,
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
