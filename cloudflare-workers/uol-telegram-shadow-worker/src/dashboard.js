function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function duration(value) {
  if (!Number.isFinite(value)) return "—";
  if (value < 1_000) return `${value} ms`;
  return `${(value / 1_000).toFixed(1)} s`;
}

function metric(label, value, tone = "") {
  return `<article class="metric ${tone}"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></article>`;
}

export function renderDashboard(data) {
  const ops = data.operations || {};
  const latency = data.latency || {};
  const source = data.sourceComparison || {};
  const image = data.imageDelivery || {};
  const usage = data.usageEstimate || {};
  const auth = data.browserAuth || {};
  const incidents = (ops.incidents || []).map((incident) => `
    <tr><td>${escapeHtml(incident.severity)}</td><td>${escapeHtml(incident.summary)}</td><td>${escapeHtml(incident.status)}</td><td>${escapeHtml(incident.last_detected_at)}</td></tr>`).join("") ||
    '<tr><td colspan="4">Nenhum incidente registrado.</td></tr>';
  const offers = (data.recent || []).map((offer) => `
    <tr><td>${escapeHtml(offer.title)}</td><td>${escapeHtml(offer.status)}</td><td>${escapeHtml(offer.firstSeenAt)}</td><td>${escapeHtml(offer.detailQuality)}</td></tr>`).join("") ||
    '<tr><td colspan="4">Nenhuma oferta recente.</td></tr>';
  const strategies = (image.strategies || []).map((strategy) => `
    <tr><td>${escapeHtml(strategy.strategy)}</td><td>${escapeHtml(strategy.state)}</td><td>${escapeHtml(strategy.consecutive_failures)}</td><td>${escapeHtml(strategy.opened_until || "—")}</td></tr>`).join("") ||
    '<tr><td colspan="4">Sem histórico de estratégias.</td></tr>';
  const sourceRows = (source.latest || []).map((sample) => `
    <tr><td>${escapeHtml(sample.title)}</td><td>${escapeHtml(sample.winner)}</td><td>${escapeHtml(duration(sample.deltaMs))}</td><td>${escapeHtml(sample.apiFirstSeenAt || sample.listingFirstSeenAt)}</td></tr>`).join("") ||
    '<tr><td colspan="4">Aguardando observações novas das duas fontes.</td></tr>';
  const latencyRows = (latency.latest || []).map((sample) => `
    <tr><td>${escapeHtml(sample.title)}</td><td>${escapeHtml(duration(sample.discordMs))}</td><td>${escapeHtml(duration(sample.telegramMs))}</td><td>${escapeHtml(duration(sample.commentMs))}</td></tr>`).join("") ||
    '<tr><td colspan="4">Aguardando a próxima oferta genuinamente nova.</td></tr>';
  return `<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30"><title>Clube UOL Monitor</title>
<style>
:root{color-scheme:dark;font-family:Inter,ui-sans-serif,system-ui;background:#09090b;color:#fafafa}*{box-sizing:border-box}body{margin:0;padding:24px}.wrap{max-width:1180px;margin:auto}header{display:flex;justify-content:space-between;gap:16px;align-items:flex-end;margin-bottom:24px}h1{margin:0;font-size:clamp(24px,5vw,42px)}p{color:#a1a1aa}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}.metric,.panel{background:#18181b;border:1px solid #27272a;border-radius:16px;padding:16px}.metric span{display:block;color:#a1a1aa;font-size:13px}.metric strong{display:block;font-size:24px;margin-top:8px}.good{border-color:#14532d}.bad{border-color:#7f1d1d}section{margin-top:22px}h2{font-size:18px}table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:10px;border-bottom:1px solid #27272a}th{color:#a1a1aa}.scroll{overflow:auto}.stamp{font-size:12px;color:#71717a}@media(max-width:620px){body{padding:14px}header{display:block}th,td{min-width:120px}}
</style></head><body><main class="wrap">
<header><div><h1>Clube UOL Monitor</h1><p>Coleta, entrega e saúde operacional em um só lugar.</p></div><div class="stamp">Atualizado em ${escapeHtml(new Date().toISOString())}<br>Autoatualização: 30 s</div></header>
<div class="grid">
${metric("Modo", data.mode || "—", data.mode === "live" ? "good" : "")}
${metric("Último scan", data.lastScanAt || "—")}
${metric("Incidentes ativos", ops.activeIncidents ?? 0, ops.activeIncidents ? "bad" : "good")}
${metric("API ingressos", data.ticketApi?.lastOffersSeen ?? "—", data.ticketApi?.lastError ? "bad" : "good")}
${metric("Telegram p50", duration(latency.telegram?.p50Ms))}
${metric("Discord p50", duration(latency.discord?.p50Ms))}
${metric("API vence", source.apiWins ?? 0)}
${metric("HTML vence", source.listingWins ?? 0)}
${metric("Cache file_id", image.cacheEntries ?? 0)}
${metric("Token automático", auth.autoRefreshConfigured ? "ativo" : "indisponível", auth.autoRefreshConfigured ? "good" : "bad")}
${metric("Scans estimados/dia", usage.alarmInvocationsPerDay ?? "—")}
${metric("Scans estimados/30d", usage.alarmInvocationsPer30Days ?? "—")}
</div>
<section class="panel"><h2>Fontes</h2><p>Comparações pareadas: ${escapeHtml(source.paired ?? 0)} · diferença p50: ${escapeHtml(duration(source.deltaP50Ms))} · p95: ${escapeHtml(duration(source.deltaP95Ms))}</p></section>
<section class="panel"><h2>Últimas disputas API × HTML</h2><div class="scroll"><table><thead><tr><th>Oferta</th><th>Vencedora</th><th>Diferença</th><th>Primeira observação</th></tr></thead><tbody>${sourceRows}</tbody></table></div></section>
<section class="panel"><h2>Latência por oferta</h2><div class="scroll"><table><thead><tr><th>Oferta</th><th>Discord</th><th>Telegram</th><th>Comentário</th></tr></thead><tbody>${latencyRows}</tbody></table></div></section>
<section class="panel"><h2>Estratégias de imagem</h2><div class="scroll"><table><thead><tr><th>Estratégia</th><th>Estado</th><th>Falhas seguidas</th><th>Aberta até</th></tr></thead><tbody>${strategies}</tbody></table></div></section>
<section class="panel"><h2>Incidentes</h2><div class="scroll"><table><thead><tr><th>Severidade</th><th>Resumo</th><th>Estado</th><th>Última detecção</th></tr></thead><tbody>${incidents}</tbody></table></div></section>
<section class="panel"><h2>Ofertas recentes</h2><div class="scroll"><table><thead><tr><th>Oferta</th><th>Status</th><th>Detectada</th><th>Detalhe</th></tr></thead><tbody>${offers}</tbody></table></div></section>
</main></body></html>`;
}
