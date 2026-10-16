"use strict";

const WORKER_URL = "https://uol-telegram-shadow-pilot.leosaquetto.workers.dev";
const form = document.getElementById("settings-form");
const fields = Object.fromEntries(["profiles", "interval", "enabled", "notifications", "token", "save", "feedback", "import-file"].map(id => [id, document.getElementById(id)]));
let busy = false;
const errors = {
  config_invalid: "Confira os campos da configuração.", profiles_invalid: "Informe de 1 a 3 nomes de perfil válidos.",
  interval_invalid: "Escolha um dos intervalos disponíveis.", token_invalid: "Informe uma chave válida, com 16 a 4096 caracteres e sem espaços.",
  worker_url_invalid: "O endereço do Worker deve ser o UOL configurado nesta extensão.",
  operation_in_progress: "Uma consulta está em andamento. Aguarde sua conclusão antes de salvar.",
};
function errorText(value) { return errors[value] || "Não foi possível concluir a operação da extensão."; }

function feedback(message, isError = false) {
  fields.feedback.textContent = message;
  fields.feedback.dataset.error = String(isError);
}
function setBusy(value) {
  busy = value;
  for (const field of form.querySelectorAll("input, select, button")) field.disabled = value;
  fields["import-file"].disabled = value;
}
function render(state) {
  fields.profiles.value = Array.isArray(state.profiles) ? state.profiles.join(", ") : "clubeuol";
  fields.interval.value = String(state.intervalMinutes || 5);
  fields.enabled.checked = state.enabled === true;
  fields.notifications.checked = state.notifications === true;
  fields.token.value = "";
}
function validate(config) {
  if (!Array.isArray(config.profiles) || !config.profiles.length || config.profiles.length > 3) throw new Error("Informe de 1 a 3 perfis.");
  config.profiles = [...new Set(config.profiles.map(value => typeof value === "string" ? value.trim().replace(/^@/, "").toLowerCase() : ""))];
  if (config.profiles.some(value => !/^[a-z0-9_][a-z0-9_.]{0,29}$/.test(value))) throw new Error("Use apenas nomes de perfil, separados por vírgula.");
  if (![5, 10, 15, 30, 60].includes(config.intervalMinutes)) throw new Error("Escolha um intervalo de 5, 10, 15, 30 ou 60 minutos.");
  if (typeof config.enabled !== "boolean" || typeof config.notifications !== "boolean") throw new Error("Configuração de monitoramento inválida.");
  if (config.workerUrl !== undefined && config.workerUrl !== WORKER_URL) throw new Error("O endereço do Worker deve ser o UOL configurado nesta extensão.");
  if (config.token !== undefined) {
    if (typeof config.token !== "string" || config.token.length > 4096) throw new Error(errors.token_invalid);
    config.token = config.token.trim();
    if (!config.token) delete config.token;
    else if (config.token.length < 16 || /\s/.test(config.token)) throw new Error(errors.token_invalid);
  }
  return config;
}
function formConfig() {
  return {
    profiles: fields.profiles.value.split(","), intervalMinutes: Number(fields.interval.value),
    enabled: fields.enabled.checked, notifications: fields.notifications.checked,
  };
}
async function save(config) {
  if (busy) return;
  setBusy(true);
  feedback("");
  try {
    const result = await chrome.runtime.sendMessage({ action: "configure", config: validate(config) });
    if (!result?.ok) throw new Error(errorText(result?.error));
    fields.token.value = "";
    if (result.state) render(result.state);
    feedback("Configurações salvas.");
  } catch (error) { feedback(error.message || "Não foi possível salvar a configuração.", true); }
  finally { setBusy(false); }
}
form.addEventListener("submit", event => {
  event.preventDefault();
  const config = formConfig();
  if (fields.token.value.trim()) config.token = fields.token.value;
  save(config);
});
fields["import-file"].addEventListener("change", async () => {
  const file = fields["import-file"].files?.[0];
  if (!file || busy) return;
  setBusy(true);
  feedback("");
  try {
    if (file.size > 16 * 1024) throw new Error("O arquivo de configuração deve ter até 16 KB.");
    let imported;
    try { imported = JSON.parse(await file.text()); } catch { throw new Error("O arquivo precisa conter um JSON válido."); }
    if (!imported || typeof imported !== "object" || Array.isArray(imported)) throw new Error("O arquivo de configuração é inválido.");
    const config = formConfig();
    for (const key of ["token", "profiles", "intervalMinutes", "enabled", "notifications", "workerUrl"]) {
      if (Object.hasOwn(imported, key)) config[key] = imported[key];
    }
    setBusy(false);
    await save(config);
  } catch (error) { feedback(error.message || "Não foi possível importar a configuração.", true); }
  finally { fields["import-file"].value = ""; setBusy(false); }
});
async function load() {
  setBusy(true);
  try {
    const result = await chrome.runtime.sendMessage({ action: "getState" });
    if (!result?.ok || !result.state) throw new Error(errorText(result?.error));
    render(result.state);
  } catch (error) { feedback(error.message || "Não foi possível acessar a extensão.", true); }
  finally { setBusy(false); }
}
load();
