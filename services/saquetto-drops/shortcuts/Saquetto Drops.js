// Variables used by Scriptable.
// icon-color: deep-green; icon-glyph: paper-plane;
// UI belongs to the Saquetto Drops shortcut. This script only returns data.
const DROPS = Object.freeze({
  bootstrap: "Saquetto Drops-config.private.json",
  baseKey: "saquetto-drops.base-url",
  tokenKey: "saquetto-drops.token",
  maxImages: 10,
  maxBytes: 8 * 1024 * 1024,
  maxBatchBytes: 16 * 1024 * 1024,
});

class DropsError extends Error {}

function validateBaseUrl(value) {
  if (typeof value !== "string" || !/^https:\/\/[a-z0-9.-]+(?::[0-9]{1,5})?(?:\/[a-z0-9/_-]*)?$/i.test(value)) {
    throw new DropsError("Configuração inválida: endereço HTTPS necessário.");
  }
  return value.replace(/\/+$/, "");
}

function splitInput(value) {
  if (value == null || value === "") return [];
  if (Array.isArray(value)) return value.flatMap(splitInput);
  if (typeof value !== "string") throw new DropsError("Entrada do atalho inválida.");
  return value.split(/\r?\n/).filter((item) => item !== "");
}

function validateText(value, imageCount) {
  if (typeof value !== "string") throw new DropsError("Texto do atalho inválido.");
  const limit = imageCount ? 1024 : 8000;
  if (value.length > limit) throw new DropsError(`Limite: ${limit} caracteres${imageCount ? " na legenda" : " no texto"}. Edite antes de enviar.`);
  if (!imageCount && !value.trim()) throw new DropsError("Adicione texto ou imagens antes de enviar.");
  return value; // Do not trim, rewrite links, or change line breaks.
}

function parseImages(value) {
  const items = splitInput(value);
  if (items.length > DROPS.maxImages) throw new DropsError("Máximo de 10 imagens por envio.");
  let totalBytes = 0;
  return items.map((raw) => {
    const base64 = raw.trim();
    if (!base64 || base64.length % 4 !== 0 || !/^[A-Za-z0-9+/]+={0,2}$/.test(base64)) {
      throw new DropsError("Imagem inválida. Use o atalho para converter em JPEG.");
    }
    const bytes = base64.length / 4 * 3 - (base64.endsWith("==") ? 2 : base64.endsWith("=") ? 1 : 0);
    if (bytes > DROPS.maxBytes) throw new DropsError("Cada imagem deve ter no máximo 8 MiB.");
    totalBytes += bytes;
    if (totalBytes > DROPS.maxBatchBytes) throw new DropsError("Selecione menos fotos: limite de 16 MiB por execução do atalho.");
    const mime = base64.startsWith("/9j/") ? "image/jpeg" : base64.startsWith("iVBORw0KGgo") ? "image/png" : null;
    if (!mime) throw new DropsError("Somente imagens JPEG e PNG são aceitas.");
    return { base64, mime, bytes };
  });
}

function destinationOptions(value) {
  if (!value || !Array.isArray(value.destinations) || !value.destinations.length) {
    throw new DropsError("Nenhum destino configurado no servidor.");
  }
  const ids = new Set(), names = new Set();
  return value.destinations.map((item) => {
    if (!item || typeof item.id !== "string" || !/^[a-zA-Z0-9_-]+$/.test(item.id) || ids.has(item.id) || typeof item.name !== "string" || !item.name.trim() || /[\r\n]/.test(item.name)) {
      throw new DropsError("Lista de destinos inválida.");
    }
    if (names.has(item.name)) throw new DropsError("Há destinos com nomes iguais. Revise o cadastro antes de enviar.");
    ids.add(item.id); names.add(item.name);
    return { id: item.id, name: item.name, label: item.name };
  });
}

function resolveDestinations(value, options) {
  const selected = [...new Set(splitInput(value))];
  if (!selected.length) throw new DropsError("Selecione pelo menos um destino.");
  return selected.map((label) => {
    const item = options.find((option) => option.label === label);
    if (!item) throw new DropsError("Destino mudou. Abra novamente a lista antes de enviar.");
    return item.id;
  });
}

function validateStatus(value, requestId) {
  if (!value || value.requestId !== requestId || !Array.isArray(value.jobs) || !value.jobs.length || value.jobs.some((job) => !job || typeof job.destination !== "string" || typeof job.state !== "string")) {
    throw new DropsError("Resposta do servidor não confirmada. Consulte o status antes de repetir.");
  }
  return { requestId, jobs: value.jobs.map(({ id, destination, state, code }) => ({ id, destination, state, code })) };
}

function statusMessage(value, options) {
  const labels = { queued: "na fila", dispatching: "enviando", accepted: "aceito pelo WhatsApp", confirmed: "entrega confirmada", failed: "falhou", unknown: "resultado incerto" };
  const lines = value.jobs.map((job) => {
    const name = options.find((option) => option.id === job.destination)?.name || job.destination;
    const reasons = { request_expired: "prazo de envio expirou", pre_dispatch_failure: "falha antes do envio", dispatch_unconfirmed: "aguardando confirmação", awaiting_whatsapp_ack: "aguardando confirmação" };
    const code = reasons[job.code] ? ` — ${reasons[job.code]}` : "";
    return `${name}: ${labels[job.state] || "estado não reconhecido"}${code}`;
  });
  return `${lines.join("\n")}\n\nNa fila não significa entregue. “Aceito pelo WhatsApp” não confirma recebimento pelo destinatário. Em grupos, a confirmação pode ser de um participante.`;
}

function storage() {
  const fm = FileManager.local();
  const directory = fm.joinPath(fm.documentsDirectory(), "Saquetto Drops-private");
  if (!fm.fileExists(directory)) fm.createDirectory(directory, true);
  return {
    read(name) {
      const path = fm.joinPath(directory, name);
      if (!fm.fileExists(path)) return null;
      try { return JSON.parse(fm.readString(path)); }
      catch { throw new DropsError("Registro local ilegível. Não repita o envio: confira o servidor."); }
    },
    write(name, value) { fm.writeString(fm.joinPath(directory, name), JSON.stringify(value)); },
  };
}

async function credentials() {
  if (!Keychain.contains(DROPS.baseKey) || !Keychain.contains(DROPS.tokenKey)) {
    const cloud = FileManager.iCloud();
    const path = cloud.joinPath(cloud.documentsDirectory(), DROPS.bootstrap);
    if (!cloud.fileExists(path)) throw new DropsError("Configuração ausente. Sincronize Saquetto Drops-config.private.json na pasta Scriptable do iCloud.");
    await cloud.downloadFileFromiCloud(path);
    let input;
    try { input = JSON.parse(cloud.readString(path)); }
    catch { throw new DropsError("Arquivo de configuração inválido."); }
    const baseUrl = validateBaseUrl(input.baseUrl);
    if (typeof input.token !== "string" || input.token.length < 32 || /\s/.test(input.token)) throw new DropsError("Token de configuração inválido.");
    Keychain.set(DROPS.baseKey, baseUrl);
    Keychain.set(DROPS.tokenKey, input.token);
    if (Keychain.get(DROPS.baseKey) !== baseUrl || Keychain.get(DROPS.tokenKey) !== input.token) throw new DropsError("Não foi possível salvar a configuração no Keychain.");
    // Remove only this one-time bootstrap, after verifying both stored values.
    cloud.remove(path);
  }
  return { baseUrl: validateBaseUrl(Keychain.get(DROPS.baseKey)), token: Keychain.get(DROPS.tokenKey) };
}

async function api(auth, path, method = "GET", body, mime) {
  const request = new Request(auth.baseUrl + path);
  request.method = method;
  request.timeoutInterval = 45;
  request.allowInsecureRequest = false;
  request.onRedirect = () => null;
  request.headers = { Authorization: `Bearer ${auth.token}`, Accept: "application/json" };
  if (body !== undefined) {
    request.headers["Content-Type"] = mime || "application/json";
    request.body = mime ? body : JSON.stringify(body);
  }
  let raw;
  try { raw = await request.loadString(); }
  catch { throw new DropsError("Servidor sem resposta confirmada. Consulte ou retome o último envio; não crie outro igual."); }
  const status = request.response?.statusCode;
  if (status === 404) return { status, value: null };
  if (status === 401 || status === 403) throw new DropsError("Acesso recusado. Confira a configuração do dispositivo.");
  if (status < 200 || status >= 300 || !status) {
    const error = new DropsError(`Servidor recusou a operação (HTTP ${Number(status) || 0}). Consulte o último envio antes de repetir.`);
    let code;
    try { code = JSON.parse(raw).code; } catch {}
    // A conflict can refer to an existing request; resolve it via status first.
    error.rejected = [400,410,413,415,429,507].includes(status) || (status === 409 && code === "sending_paused");
    throw error;
  }
  let value;
  try { value = JSON.parse(raw); }
  catch { throw new DropsError("Resposta inválida do servidor. Consulte o último envio antes de repetir."); }
  return { status, value };
}

function requireSameServer(record, auth) {
  if (!record || record.baseUrl !== auth.baseUrl || !record.payload || !/^[0-9a-f-]{36}$/i.test(record.payload.requestId)) {
    throw new DropsError("Último envio indisponível para este servidor.");
  }
}

function saveRequest(store, record) {
  // Durable payload before any send; retry never regenerates IDs or media.
  store.write(`request-${record.payload.requestId}.json`, record);
  store.write("last-request.json", { requestId: record.payload.requestId });
}

function lastRequest(store) {
  const last = store.read("last-request.json");
  if (!last) return null;
  if (!/^[0-9a-f-]{36}$/i.test(last.requestId)) throw new DropsError("Registro local inválido. Consulte o servidor.");
  const record = store.read(`request-${last.requestId}.json`);
  if (!record) throw new DropsError("Registro do último envio ausente. Consulte o servidor antes de repetir.");
  return record;
}

async function postSaved(auth, store, record) {
  requireSameServer(record, auth);
  record.uncertain = true;
  saveRequest(store, record);
  let reply;
  try { reply = await api(auth, "/v1/whatsapp/send", "POST", record.payload); }
  catch (error) {
    if (error.rejected) {
      record.uncertain = false;
      record.rejected = true;
      saveRequest(store, record);
    }
    throw error;
  }
  if (reply.status !== 202) throw new DropsError("Envio sem aceite confirmado. Consulte o último envio.");
  const status = validateStatus(reply.value, record.payload.requestId);
  record.uncertain = false;
  record.status = status;
  saveRequest(store, record);
  return { ...status, message: statusMessage(status, record.options) };
}

async function runDrops(input, auth, store) {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw new DropsError("Execute pelo atalho Saquetto Drops.");
  if (input.action === "destinations") {
    const reply = await api(auth, "/v1/whatsapp/destinations");
    const options = destinationOptions(reply.value);
    store.write("destinations.json", { baseUrl: auth.baseUrl, options });
    return { names: options.map((item) => item.label), message: "Destinos atualizados." };
  }
  if (input.action === "review") {
    const record = lastRequest(store);
    if (!record) throw new DropsError("Nenhum envio registrado neste dispositivo.");
    requireSameServer(record, auth);
    return { message: `Retomar o mesmo pedido, sem criar outro:\n\n${record.options.map((item) => item.name).join("\n")}\n\n${record.payload.text || "(sem legenda)"}\n\n${record.payload.mediaIds.length} imagem(ns).` };
  }
  if (input.action === "status" || input.action === "retry") {
    const record = lastRequest(store);
    if (!record) return { message: "Nenhum envio registrado neste dispositivo." };
    requireSameServer(record, auth);
    const reply = await api(auth, `/v1/whatsapp/requests/${record.payload.requestId}`);
    if (reply.status === 404) {
      if (record.rejected) return { message: "O servidor recusou este pedido antes de enfileirar. Corrija os dados e faça um Novo envio." };
      if (input.action === "retry") {
        if (input.confirmed !== "yes") throw new DropsError("Confirme a retomada no atalho.");
        return postSaved(auth, store, record);
      }
      return { requestId: record.payload.requestId, message: "Pedido ainda não localizado. Use Retomar último envio: o mesmo identificador será preservado." };
    }
    const status = validateStatus(reply.value, record.payload.requestId);
    record.uncertain = false;
    record.status = status;
    saveRequest(store, record);
    return { ...status, message: statusMessage(status, record.options) };
  }
  if (input.action !== "send") throw new DropsError("Ação do atalho inválida.");
  if (input.confirmed !== "yes") throw new DropsError("Confirme o envio no atalho.");
  const last = lastRequest(store);
  if (last?.uncertain) throw new DropsError("Há envio sem resposta confirmada. Use Consultar ou Retomar último envio antes de criar outro.");
  const cached = store.read("destinations.json");
  if (!cached || cached.baseUrl !== auth.baseUrl) throw new DropsError("Abra a lista de destinos antes de enviar.");
  const destinations = resolveDestinations(input.destinations, cached.options);
  const images = parseImages(input.imagesBase64);
  const text = validateText(input.text == null ? "" : input.text, images.length);
  const requestId = UUID.string().toLowerCase();
  // Keep confirmed inputs if upload or execution is interrupted. No send yet.
  store.write("last-draft.json", { requestId, destinations, text, imageCount: images.length, preparedAt: new Date().toISOString() });
  const mediaIds = [];
  for (const image of images) {
    const reply = await api(auth, "/v1/whatsapp/media", "POST", Data.fromBase64String(image.base64), image.mime);
    if (typeof reply.value?.mediaId !== "string" || !reply.value.mediaId) throw new DropsError("Upload não confirmado. Nenhuma mensagem foi enviada.");
    mediaIds.push(reply.value.mediaId);
  }
  const record = { baseUrl: auth.baseUrl, payload: { requestId, destinations, text, mediaIds }, options: cached.options.filter((item) => destinations.includes(item.id)), uncertain: true, createdAt: new Date().toISOString() };
  saveRequest(store, record);
  return postSaved(auth, store, record);
}

if (typeof Script !== "undefined" && typeof args !== "undefined") {
  try {
    // Shortcut parameters are read regardless of runsInApp; no inputs discarded.
    const input = args.shortcutParameter;
    const output = await runDrops(input, await credentials(), storage());
    Script.setShortcutOutput(output);
    Script.complete();
  } catch (error) {
    // Never surface native/network error objects, response bodies, or credentials.
    throw new Error(error instanceof DropsError ? error.message : "Saquetto Drops interrompido. Consulte o último envio antes de repetir.");
  }
}
