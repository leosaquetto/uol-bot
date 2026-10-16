"use strict";

const elements = Object.fromEntries(["source-status", "monitor-detail", "last-check", "next-check", "budget", "profiles", "deliveries", "feedback", "check", "toggle", "settings"].map(id => [id, document.getElementById(id)]));
const labels = {
  starting: "Iniciando", checking: "Consultando", ready: "Pronto", found: "Stories encontrados", empty: "Sem Stories novos",
  budget_wait: "Aguardando limite", auth_required: "Sessão precisa de atenção", rate_limited: "Limite do Instagram",
  unknown: "Não confirmado", stopped: "Pausado", paused: "Pausado", error: "Falha na consulta",
  pending: "Pendente", queued: "Na fila", delivered: "Entregue", confirmed: "Confirmado", partial: "Parcial",
  in_flight: "Em envio", reconciling: "Conferindo recibo", held: "Em espera", expired: "Expirado", failed_safe: "Aguardando tentativa",
  uncertain: "Envio incerto", blocked: "Bloqueado", accepted: "Recebido pelo Worker",
};
const errors = {
  config_invalid: "Confira as configurações da extensão.", profiles_invalid: "Confira os nomes dos perfis configurados.",
  token_invalid: "Confira a chave do Worker nas configurações.", worker_url_invalid: "O endereço do Worker é inválido.",
  interval_invalid: "Escolha um intervalo disponível nas configurações.", operation_in_progress: "Uma operação está em andamento. Aguarde sua conclusão.",
  auth_required: "Confira a sessão existente no Instagram e clique em Retomar.", daily_budget: "Limite diário atingido. As consultas retomam quando o orçamento do dia for renovado.",
  rate_limited: "O Instagram limitou as consultas. O monitor aguardará antes de consultar novamente.",
  network_error: "Não foi possível concluir a consulta pela rede.", instagram_http_error: "O Instagram não concluiu a consulta.",
  body_limit: "A resposta ultrapassou o limite de leitura. A consulta foi interrompida.", invalid_response: "A resposta recebida não pôde ser validada.",
  profile_unconfirmed: "Não foi possível confirmar a identidade de um dos perfis.", story_structure_unconfirmed: "A resposta de Stories ainda não pôde ser validada.",
  outbox_full: "A fila está cheia. Os registros pendentes foram preservados e as novas consultas aguardam.",
  worker_auth_required: "O Worker recusou a chave. Confira a configuração de envio.", worker_rejected: "O Worker recusou um envio. O registro foi preservado.",
  receipt_unconfirmed: "Um envio continua sem recibo confirmado. Ele não será repetido sem confirmação.",
  operation_failed: "Não foi possível concluir a operação. Confira o estado do monitor.",
};
const targetLabels = { main: "Telegram", canal2: "Canal 2", discord: "Discord", beeper: "WhatsApp" };
let currentState = null;
let busy = false;

function label(value, fallback = "Aguardando") { return labels[value] || fallback; }
function errorText(value) { return errors[value] || "Não foi possível concluir a operação. Confira o estado do monitor."; }
function formatDate(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "—";
  return new Intl.DateTimeFormat("pt-BR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }).format(date);
}
function feedback(message, isError = false) {
  elements.feedback.textContent = message || "";
  elements.feedback.dataset.error = String(isError);
}
function setBusy(value) {
  busy = value;
  elements.check.disabled = busy || !currentState?.configured;
  elements.toggle.disabled = busy || !currentState?.configured;
  elements.check.textContent = busy ? "Aguarde…" : "Verificar agora";
}
function node(tag, className, text) {
  const result = document.createElement(tag);
  if (className) result.className = className;
  if (text !== undefined) result.textContent = text;
  return result;
}
function campaignUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === "https:" && url.hostname === "clube.uol.com.br" &&
      !url.username && !url.password && !url.port && !url.search && !url.hash &&
      /^\/campanhasdeingresso\/[A-Za-z0-9-]+$/.test(url.pathname) ? url.href : "";
  } catch { return ""; }
}
function render(state) {
  currentState = state;
  const enabled = state.enabled === true;
  const paused = !enabled || Boolean(state.pausedReason);
  elements["source-status"].textContent = !state.configured ? "Configure para começar" : !enabled ? "Pausado"
    : state.lastError === "daily_budget" ? "Limite diário atingido" : label(state.sourceStatus);
  elements["source-status"].dataset.tone = ["found", "empty", "ready"].includes(state.sourceStatus) && !paused ? "success"
    : ["auth_required", "rate_limited", "unknown"].includes(state.sourceStatus) ? "warning" : "neutral";
  elements["monitor-detail"].textContent = !state.configured ? "Adicione os perfis e a chave do Worker nas configurações."
    : !enabled ? "As consultas estão pausadas." : `Consultas a cada ${state.intervalMinutes || 5} minutos, dentro do limite diário.`;
  if (state.pausedReason === "auth_required") elements["monitor-detail"].textContent = "Confira a sessão existente no Instagram antes de retomar.";
  elements["last-check"].textContent = formatDate(state.lastCheckAt);
  elements["next-check"].textContent = paused ? "Pausado" : formatDate(state.nextPollAt);
  elements.budget.textContent = `${state.budget?.used ?? 0} / ${state.budget?.limit ?? 300}`;
  elements.toggle.textContent = paused ? "Retomar" : "Pausar";
  elements.profiles.replaceChildren();
  const profiles = Array.isArray(state.profiles) ? state.profiles : [];
  for (const username of profiles) {
    const result = state.lastResults?.find(item => item.username === username);
    const row = node("li");
    const heading = node("div", "row-heading");
    heading.append(node("span", "profile-name", `@${username}`), node("span", "muted", label(result?.status, "Ainda sem consulta")));
    row.append(heading);
    if (Number.isInteger(result?.count)) row.append(node("span", "row-detail", `${result.count} Story(s) · ${result.newCount ?? 0} novo(s)`));
    elements.profiles.append(row);
  }
  if (!profiles.length) elements.profiles.append(node("li", "muted", "Nenhum perfil configurado."));
  elements.deliveries.replaceChildren();
  const deliveries = Array.isArray(state.deliveries) ? [...state.deliveries]
    .sort((left, right) => (Date.parse(right.updatedAt) || 0) - (Date.parse(left.updatedAt) || 0)).slice(0, 3) : [];
  for (const delivery of deliveries) {
    const row = node("li");
    const heading = node("div", "row-heading");
    const url = campaignUrl(delivery.link);
    const title = node(url ? "a" : "span", url ? "delivery-link" : "profile-name", "Story do @clubeuol");
    if (url) { title.href = url; title.target = "_blank"; title.rel = "noopener noreferrer"; }
    heading.append(title, node("span", "muted", label(delivery.deliveryStatus || delivery.status, "Em acompanhamento")));
    row.append(heading, node("span", "row-detail", formatDate(delivery.updatedAt)));
    const targets = node("div", "targets");
    for (const [target, name] of Object.entries(targetLabels)) {
      const receipt = delivery.targets?.[target];
      if (receipt) targets.append(node("span", "", `${name}: ${receipt.status === "confirmed" &&
        (!receipt.imageConfirmed || typeof receipt.messageId !== "string" || !receipt.messageId.trim())
        ? "Confirmação parcial" : label(receipt.status)}`));
    }
    row.append(targets);
    elements.deliveries.append(row);
  }
  if (!deliveries.length) elements.deliveries.append(node("li", "muted", "Nenhum envio registrado."));
  if (state.lastError || state.workerError) feedback([...new Set([state.lastError, state.workerError].filter(Boolean).map(errorText))].join(" "), true);
  setBusy(busy);
}
async function run(action) {
  if (busy) return;
  const previousCheck = currentState?.lastCheckAt;
  setBusy(true);
  feedback("");
  try {
    const result = await chrome.runtime.sendMessage({ action });
    if (!result?.ok) throw new Error(errorText(result?.error));
    if (result.state) render(result.state);
    if (action === "check" && !result.state?.lastError && !result.state?.workerError) {
      feedback(!result.state?.enabled ? "O monitoramento está pausado. Clique em Retomar para reativar as consultas."
        : result.state?.sourceStatus === "budget_wait" || (result.state?.lastCheckAt === previousCheck && result.state?.nextPollAt > Date.now())
          ? "Consulta em espera. O intervalo e o limite diário continuam valendo. Confira o próximo horário acima."
          : "Estado atualizado. Confira o horário da última consulta.");
    }
  } catch (error) { feedback(error.message || "Não foi possível acessar a extensão.", true); }
  finally { setBusy(false); }
}
elements.check.addEventListener("click", () => run("check"));
elements.toggle.addEventListener("click", () => run(!currentState?.enabled || currentState?.pausedReason ? "resume" : "pause"));
elements.settings.addEventListener("click", () => chrome.runtime.openOptionsPage());
run("getState");
