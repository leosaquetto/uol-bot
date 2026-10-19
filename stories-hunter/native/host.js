// Variables used by Scriptable.
// These must be at the very top of the file. Do not edit.
// icon-color: deep-orange; icon-glyph: ticket-alt;
// =====================================================================
// Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter
// Product: Clube UOL Premium (legacy marker above retained for 5.8.0 updater compatibility)
// =====================================================================
// - Abertura Instantânea com Cache Local e Histórico no iCloud
// - Entrada Card por Card Progressiva com Animação Cascade Stagger
// - Destaque Automático de Palavras-Chave (ex: "Museu") em tom Laranja e Thumb Ampliada
// - Logotipo Oficial Clube UOL no Cabeçalho
// - Perfil de Usuário com Avatar Circular (leosaquetto / gaoriel via Catbox HD)
// - Botão "Meus Resgates" com Sincronização Headless Oficial do Perfil UOL
// - Modal de Ajustes (Priorizar / Despriorizar)
// - Tag de Contagem (ex: 14 Ativas) e Chips de Categorias (Museu, Teatro, Música, Exposições)
// - Toggle de Visualização Agrupada (por Banner Idêntico) vs Lista Detalhada
// - Extração de Descrição Completa (1800+ caracteres sem corte)
// - Frases formatadas em Sentence Case ("Em sentença")
// - Ícones Majesticons Line Interface SVGs nativos
// - Ciclo Completo de Resgate Headless com mapeamento de cota mensal e erros no modal
// - Histórico Persistido com tempo no ar e tags para ofertas esgotadas em P&B
// =====================================================================

const SCRIPT_VERSION = "5.9.2";
const UPDATE_RAW_URL = "https://raw.githubusercontent.com/leosaquetto/uol-bot/main/scriptable_uol_stories_hunter.js";
const NTFY_TOPIC = "leo-macsend-8130";
const HISTORY_FILE_NAME = "uol_offers_history.json";
const SETTINGS_FILE_NAME = "uol_hub_settings.json";
const CREDS_FILE_NAME = "clubeuolscrip.txt";
const COOKIES_FILE_NAME = "uol_session_cookies.json";
const REDEMPTIONS_DB_FILE_NAME = "uol_redemptions_database.json";
const STARTUP_HTML = __BUILD_STARTUP_HTML__;
const HUNTER_LOGO_BASE64 = __BUILD_LOGO__;

const ICONS = __BUILD_ICONS__;
const BUNDLED_COOKIES_LEO = __BUILD_COOKIES_LEO__;
const BUNDLED_COOKIES_GAORIEL = __BUILD_COOKIES_GAORIEL__;
const BUNDLED_COOKIES = BUNDLED_COOKIES_LEO;
const BUNDLED_REDEMPTIONS = __BUILD_REDEMPTIONS__;

__BUILD_COLLECTOR__
__BUILD_INSTAGRAM_BROWSER__
__BUILD_ACCESS__
__BUILD_REDEMPTION__
const UOL_RECEIPT_PARSER = __BUILD_PARSER__;

let nativeAccountManager = null;
function createNativeAccountManager() {
  if (nativeAccountManager) return nativeAccountManager;
  const identityDigests = {"leosaquetto":"37aa92d73c736fe04c4b0cd15a704b3d6883ae51745fff565e74b0e412500cc6","gaoriel":"bfa637efa5334c167f2d2a1cd39f75af599d6e35e071b2d6ee0b8af44973f14d"};
  async function parse(html, kind, url) {
    // The server HTML is data, never a loaded document or executable markup.
    const parser = new WebView();
    await parser.loadHTML('<!doctype html><meta http-equiv="Content-Security-Policy" content="default-src &#39;none&#39;">', "https://clube.uol.com.br");
    parser.shouldAllowRequest = () => false;
    const input = JSON.stringify(html).replace(/</g, "\\u003c");
    const expression = kind === "offer" ? "parseOffer(" + input + "," + JSON.stringify(url) + ")" : "parseHistory(" + input + ")";
    const result = await parser.evaluateJavaScript("(() => {" + UOL_RECEIPT_PARSER + "; return JSON.stringify(" + expression + ");})()");
    return JSON.parse(result);
  }
  nativeAccountManager = HunterRedemption.create({ keychain: Keychain, now: Date.now,
    uuid: () => UUID.string().toLowerCase(), request: url => new Request(url),
    authorizeAccount: async user => {
      if (!Keychain.contains("uol.hunter.shared-account.v1." + user)) return true;
      const access = await createHunterAccess().status({ refresh: true });
      return access.status === "approved" && access.accounts.includes(user);
    },
    loadCookies: async user => {
      const existing = loadSessionCookies(user);
      if (Array.isArray(existing) && existing.length) return existing;
      const config = await createHunterAccess().accountConfig(user);
      if (!config.ok || config.identityDigest !== identityDigests[user]) throw Object.assign(new Error("account_access_required"), { reason: "account_access_required" });
      Keychain.set("uol.hunter.shared-account.v1." + user, config.deviceId);
      return config.cookies;
    }, verifyIdentity: (user, parsed) => HunterRedemption.identityDigest(parsed.signedInName) === identityDigests[user],
    login: async flow => {
      const login = new WebView();
      let callback = null;
      login.shouldAllowRequest = request => {
        const raw = request && request.url;
        if (typeof raw !== "string") return false;
        if (flow.isCallback(raw)) {
          if (!callback) {
            callback = raw;
            Promise.resolve().then(() => login.loadHTML('<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src &#39;none&#39;"><h2>Autenticação recebida</h2><p>Toque em Close/Fechar para voltar ao Hunter e concluir a validação da conta.</p>', "https://clube.uol.com.br")).catch(() => {});
          }
          return false;
        }
        // The official form performs its own login and security checks. No
        // password injection, cookie scraping, logout or redemption here.
        if (raw === "about:blank") return true;
        if (!raw.startsWith("https://")) return false;
        const host = raw.slice(8).split('/')[0].split('?')[0].split('#')[0].toLowerCase();
        return /^[a-z0-9.-]+$/.test(host) && (host === "uol.com.br" || host.endsWith(".uol.com.br")) &&
          !raw.startsWith("https://clube.uol.com.br/auth/uol?") && !/resgatar|logout/i.test(raw);
      };
      try { await login.loadURL(flow.url); } catch (_) { if (!callback) throw Object.assign(new Error("login_unavailable"), { reason: "login_unavailable" }); }
      await login.present();
      return callback;
    },
    parseHistory: html => parse(html, "history"), parseOffer: (html, url) => parse(html, "offer", url) });
  return nativeAccountManager;
}

async function manageNativeAccount(view, action, user, url) {
  const manager = createNativeAccountManager();
  let result;
  if (action === "redeem") {
    const confirm = new Alert(); confirm.title = "Confirmar conta do resgate";
    confirm.message = "Resgatar usando a conta de " + (user === "gaoriel" ? "Gabriel" : "Leonardo") + "? A cota e o voucher pertencem a essa conta.";
    confirm.addAction("Resgatar nesta conta"); confirm.addCancelAction("Cancelar");
    result = await confirm.presentAlert() < 0 ? { status: "cancelled", account: user } : await manager.redeem(user, url);
  } else result = action === "reconnect" ? await manager.reconnect(user) : await manager.history(user);
  const safe = JSON.stringify(result).replace(/</g, "\\u003c");
  await view.evaluateJavaScript("handleNativeAccountResult(" + JSON.stringify(action) + "," + JSON.stringify(user) + "," + safe + ");");
}

__BUILD_UPDATER__

function hunterAction() {
  if (typeof args === "undefined") return "open";
  const shortcut = args.shortcutParameter;
  return String(args.queryParameters?.action || (typeof shortcut === "string" ? shortcut : shortcut?.action) || "open").toLowerCase();
}

// The installed OTA shortcut opens the freshly saved script without an action.
// Record that version locally, show the UI, and leave collection for the next
// opening. Explicit notification/collection actions remain eligible immediately.
function hunterCollectionPausedForUpdate(action) {
  const explicitCollection = action === "collect" || action === "notification";
  try {
    const fm = FileManager.local();
    const file = fm.joinPath(fm.documentsDirectory(), "uol_hunter_loaded_version.json");
    let version = "";
    if (fm.fileExists(file)) {
      try { version = JSON.parse(fm.readString(file)).version || ""; } catch (_) {}
    }
    if (version === SCRIPT_VERSION) return false;
    fm.writeString(file, JSON.stringify({ version: SCRIPT_VERSION }));
    return !explicitCollection;
  } catch (_) {
    // Failed marker persistence must not turn an OTA opening into a source read.
    return !explicitCollection;
  }
}

let nativeInstagramBrowser = null;
function createNativeInstagramBrowser() {
  if (!nativeInstagramBrowser) nativeInstagramBrowser = HunterInstagramBrowser.create({
    webView: () => new WebView(), losslessJSON: HunterCollector.losslessJSON,
    timer: (milliseconds, callback) => Timer.schedule(milliseconds, false, callback)
  });
  return nativeInstagramBrowser;
}

async function runNativeHunterCollector() {
  try {
    const collector = HunterCollector.create({
      fm: FileManager.local(), keychain: Keychain, now: Date.now, instagramBrowser: createNativeInstagramBrowser(),
      uuid: () => UUID.string().toLowerCase(), request: url => new Request(url),
      pause: milliseconds => new Promise(resolve => Timer.schedule(milliseconds, false, resolve))
    });
    // This file is provisioned privately. Never interpolate it into the release
    // artifact or send its contents through the WebView/observation intake.
    for (const fm of [getFileManager(), FileManager.local()]) {
      const bootstrap = fm.joinPath(fm.documentsDirectory(), "uol_hunter_private_bootstrap.json");
      if (!fm.fileExists(bootstrap)) continue;
      try {
        if (fm.isFileDownloaded && !fm.isFileDownloaded(bootstrap)) await fm.downloadFileFromiCloud(bootstrap);
        const privateConfig = JSON.parse(fm.readString(bootstrap));
        if (!Keychain.contains(HunterCollector.KEYS.token) && Keychain.contains(HunterCollector.KEYS.device) &&
            Keychain.get(HunterCollector.KEYS.device) !== privateConfig.deviceId) {
          return { status: "not_configured", reason: "device_mismatch", sources: {} };
        }
        collector.importBootstrap(privateConfig);
        if (Keychain.contains(HunterCollector.KEYS.token)) break;
      } catch (_) {
        // An unavailable cloud file must not prevent an existing local import.
      }
    }
    const access = await createHunterAccess().status();
    if (access.status !== "approved") return { status: access.status === "not_configured" ? "activation_required" : access.status,
      reason: access.reason, access, sources: {} };
    const result = await collector.run();
    return { ...result, access };
  } catch (_) {
    return { status: "unavailable", discovered: 0, received: 0, delivered: 0, pending: 0, sources: {} };
  }
}

function createHunterAccess() {
  return HunterAccess.create({ keychain: Keychain, now: Date.now,
    uuid: () => UUID.string().toLowerCase(), request: url => new Request(url) });
}

async function runNativeInstagramAction(action) {
  let access = await createHunterAccess().status({ refresh: true });
  if (access.status !== 'approved') return { status: access.status, access, sources: {} };
  const browser = createNativeInstagramBrowser();
  const collector = HunterCollector.create({ fm: FileManager.local(), keychain: Keychain, now: Date.now,
    instagramBrowser: { capture: () => browser.capture(), connect: async () => {
      const result = await browser.connect();
      if (result.status !== 'ready') return result;
      access = await createHunterAccess().status({ refresh: true });
      return access.status === 'approved' ? result : { status: 'cancelled' };
    } },
    uuid: () => UUID.string().toLowerCase(), request: url => new Request(url),
    pause: milliseconds => new Promise(resolve => Timer.schedule(milliseconds, false, resolve)) });
  const result = await collector.runInstagram({ connect: action === 'connect' });
  if (access.status !== 'approved') return { status: access.status, access, sources: {} };
  return { ...result, access };
}

async function handleNativeInstagramAction(view, action) {
  const result = await runNativeInstagramAction(action);
  await view.evaluateJavaScript("handleNativeInstagramResult(" + JSON.stringify(action) + "," + JSON.stringify(result).replace(/</g, "\\u003c") + ");");
}

async function hunterNotice(title, message) {
  const alert = new Alert(); alert.title = title; alert.message = message;
  alert.addAction("OK"); await alert.presentAlert();
}

async function manageNativeHunterAccess(view) {
  const manager = createHunterAccess();
  let access = await manager.status({ refresh: true });
  const freshSafe = JSON.stringify(access).replace(/</g, "\\u003c");
  await view.evaluateJavaScript("INITIAL.hunterAccess = " + freshSafe + "; renderHunterAccessUI(); setHunterCollectionStatus({status: INITIAL.hunterAccess.status === 'not_configured' ? 'activation_required' : INITIAL.hunterAccess.status});");
  let requested = false;
  if (access.status === "not_configured" || access.status === "access_expired") {
    const prompt = new Alert(); prompt.title = "Ativar coleta neste iPhone";
    prompt.message = "Escolha um nome para reconhecer este aparelho. A coleta começa após aprovação do proprietário.";
    prompt.addTextField("Nome do aparelho", typeof Device !== "undefined" ? String(Device.name()).slice(0, 60) : "Meu iPhone");
    prompt.addAction("Solicitar acesso"); prompt.addCancelAction("Cancelar");
    if (await prompt.presentAlert() < 0) return;
    access = await manager.requestAccess(prompt.textFieldValue(0));
    requested = true;
  } else if (access.status === "approved" && access.role === "owner") {
    const result = await manager.devices();
    if (!result.ok) { await hunterNotice("Aparelhos", "Não foi possível consultar os aparelhos agora."); return; }
    const devices = result.devices.filter(d => d.role !== "owner" && ["pending", "approved"].includes(d.state));
    if (!devices.length) { await hunterNotice("Aparelhos", "Nenhuma solicitação pendente ou aparelho de amigo ativo."); return; }
    const menu = new Alert(); menu.title = "Gerenciar aparelhos";
    menu.message = "Confira o nome e o código com seu amigo antes de aprovar.";
    devices.forEach(d => menu.addAction(d.label + " • " + d.code + " • " + (d.state === "pending" ? "pendente" : "ativo")));
    menu.addCancelAction("Voltar");
    const index = await menu.presentSheet();
    if (index < 0) return;
    const device = devices[index];
    let approve = device.state === "pending", accounts;
    if (!approve) {
      const operation = new Alert(); operation.title = device.label;
      operation.addAction("Revisar permissões"); operation.addAction("Suspender aparelho"); operation.addCancelAction("Voltar");
      const chosen = await operation.presentSheet();
      if (chosen < 0) return;
      approve = chosen === 0;
    }
    if (approve) {
      const permissions = new Alert(); permissions.title = "Permissões deste aparelho";
      permissions.message = "A coleta usa somente a conta técnica. Libere as contas pessoais apenas para quem também poderá resgatar por vocês.";
      const choices = [[]]; permissions.addAction("Somente coleta");
      for (const account of (access.accounts || [])) {
        choices.push([account]); permissions.addAction("Coleta + resgates de " + (account === "gaoriel" ? "Gabriel" : "Leonardo"));
      }
      if ((access.accounts || []).length === 2) { choices.push(access.accounts); permissions.addAction("Coleta + resgates das duas contas"); }
      permissions.addCancelAction("Voltar");
      const choice = await permissions.presentSheet();
      if (choice < 0) return;
      accounts = choices[choice];
    }
    const confirm = new Alert(); confirm.title = approve ? "Aprovar aparelho?" : "Suspender aparelho?";
    confirm.message = device.label + "\nCódigo: " + device.code + (approve
      ? "\nColeta técnica autorizada." + (accounts.length ? " Também receberá a sessão para consultar vouchers e resgatar por: " + accounts.map(account => account === "gaoriel" ? "Gabriel" : "Leonardo").join(", ") + "." : " Acesso somente à coleta de novidades públicas.")
      : "\nNovas observações desse aparelho serão recusadas pelo servidor.");
    confirm.addAction(approve ? "Aprovar" : "Suspender"); confirm.addCancelAction("Cancelar");
    if (await confirm.presentAlert() < 0) return;
    const decision = await manager.decide(device.deviceId, approve ? "approve" : "revoke", device.code, accounts);
    await hunterNotice("Aparelhos", decision.ok ? (approve ? "Aparelho aprovado." : "Aparelho suspenso.") : "Decisão não confirmada. Consulte novamente antes de repetir.");
  } else {
    await hunterNotice("Coleta neste iPhone", access.status === "approved" ? "Acesso aprovado. A próxima abertura ou notificação pode coletar dentro dos limites."
      : access.status === "access_pending" ? "Solicitação pendente. Confira este código com o proprietário: " + access.code
      : access.status === "access_revoked" ? "Acesso suspenso pelo proprietário."
      : "Não foi possível confirmar o acesso. Tente novamente mais tarde.");
  }
  if (requested && access.status === "access_pending") await hunterNotice("Solicitação enviada", "Aguarde aprovação do proprietário. Código deste aparelho: " + access.code);
  // Only safe state is injected after an explicit user action in the loaded UI.
  const safe = JSON.stringify(access).replace(/</g, "\\u003c");
  await view.evaluateJavaScript("INITIAL.hunterAccess = " + safe + "; renderHunterAccessUI(); setHunterCollectionStatus({status: INITIAL.hunterAccess.status === 'not_configured' ? 'activation_required' : INITIAL.hunterAccess.status});");
}

async function main() {
  console.log("⚡ [Clube UOL Premium] Inicializando...");
  const action = hunterAction();
  if (typeof migrateHunterProductName === "function") {
    const migration = await migrateHunterProductName();
    if (!["not_needed", "migrated"].includes(migration.status)) {
      if (typeof Script.setShortcutOutput === "function") Script.setShortcutOutput(JSON.stringify(migration));
      if (action !== "update-auto") await hunterNotice("Migração não concluída", "O nome antigo foi preservado quando possível. Há conflito ou falha de arquivo; o backup local continua disponível. Nenhum dado pessoal foi alterado.");
      Script.complete(); return;
    }
  }
  if (action === "update" || action === "update-auto") {
    const parameter = typeof args !== "undefined" && typeof args.shortcutParameter === "object" ? args.shortcutParameter : {};
    const requestedChannel = (typeof args !== "undefined" && args.queryParameters?.channel) || parameter?.channel;
    if (requestedChannel !== undefined) configureHunterUpdateChannel(requestedChannel);
    await performSelfUpdate(action === "update", action === "update-auto");
    if (typeof Script.setShortcutOutput === "function" && typeof hunterLastUpdateResult !== "undefined") {
      const result = hunterLastUpdateResult;
      Script.setShortcutOutput(JSON.stringify({status: result.status, reason: result.reason, version: result.version, channel: hunterUpdateChannel(), updaterVersion: HUNTER_UPDATER_BUILD}));
    }
    Script.complete();
    return;
  }
  if (action === "open" && typeof autoUpdateHunterOnOpen === "function" && await autoUpdateHunterOnOpen()) {
    Script.complete(); return;
  }
  if (action === "reconnect-instagram" || action === "collect-instagram") {
    const result = await runNativeInstagramAction(action === "reconnect-instagram" ? "connect" : "collect");
    if (typeof Script.setShortcutOutput === "function") Script.setShortcutOutput(JSON.stringify(result));
    Script.complete(); return;
  }
  const collectionPausedForUpdate = hunterCollectionPausedForUpdate(action);
  if (action === "collect" || action === "notification") {
    const result = await runNativeHunterCollector();
    if (typeof Script.setShortcutOutput === "function") Script.setShortcutOutput(JSON.stringify(result));
    Script.complete();
    return;
  }
  const uiView = new WebView();
  installHunterBrowser(uiView);
  await uiView.loadHTML(buildHunterStartupHTML(), "https://clube.uol.com.br");
  let dismissed = false;
  const presentation = uiView.present(true).then(() => { dismissed = true; });
  // A readable loading state also lets native presentation settle before the
  // one document replacement. No evaluateJavaScript is used during startup.
  const readable = new Promise(resolve => Timer.schedule(800, false, resolve));
  const collection = collectionPausedForUpdate
    ? createHunterAccess().status().then(access => ({ status: "updated", discovered: 0, received: 0, delivered: 0, pending: 0, sources: {}, access }))
    : runNativeHunterCollector();
  try {
    await preparePrivateFiles();
    persistPrivateBootstrap();
    const settings = loadSettings();
    const historyData = loadHistory();
    const credentials = loadCredentials();
    const sessionCookies = loadSessionCookies(settings.currentUser || "leosaquetto");
    const realRedemptions = loadRedemptionsDatabase();
    const collectionResult = await collection;
    const html = buildDashboardHTML(historyData, settings, credentials, sessionCookies, realRedemptions,
      collectionPausedForUpdate, collectionResult);
    await readable;
    if (!dismissed) {
      await uiView.loadHTML(html, "https://clube.uol.com.br");
      if (!dismissed && typeof showHunterStartupUpdateNotice === "function")
        showHunterStartupUpdateNotice(uiView).catch(() => {});
    }
  } catch (_) {
    await readable;
    if (!dismissed) await uiView.loadHTML(buildHunterStartupHTML(true), "https://clube.uol.com.br");
  }
  await presentation;
  await collection;
  if (uiView.hunterActionTask) await uiView.hunterActionTask;
  Script.complete();
}

function resolveHunterAssets(template) {
  return template.replace(/__LOGO_BASE64__/g, () => HUNTER_LOGO_BASE64)
    .replace(/__ICON_([A-Za-z0-9_]+)__/g, (match, key) => ICONS[key] ?? match);
}

function buildHunterStartupHTML(failed = false) {
  return resolveHunterAssets(STARTUP_HTML).replace("__STARTUP_TITLE__", failed ? "Não foi possível abrir" : "Preparando suas ofertas")
    .replace("__STARTUP_DESCRIPTION__", failed
      ? "Seus dados foram preservados. Feche esta janela e tente abrir novamente."
      : "Buscando novidades e carregando seu histórico…")
    .replace('<progress aria-label="Carregando o Clube UOL Premium"></progress>', failed ? "" : '<progress aria-label="Carregando o Clube UOL Premium"></progress>');
}

function installHunterBrowser(view) {
  const internalRoute = "hunter-browser://open?url=";
  const externalRoute = "hunter-browser://external?url=";
  let opening = false;
  view.shouldAllowRequest = request => {
    const raw = request && request.url;
    if (typeof raw !== "string") return true;
    const navigationTheme = /^hunter-theme:\/\/set\?value=(default|purple|orange|white)$/.exec(raw);
    if (navigationTheme && navigationTheme[0] === raw) {
      const theme = navigationTheme[1];
      const saved = saveNavigationTheme(theme);
      Promise.resolve().then(() => view.evaluateJavaScript("handleNavigationThemeSaveResult(" + JSON.stringify(theme) + "," + saved + ");")).catch(() => {});
      return false;
    }
    if (/^hunter-theme:/i.test(raw)) return false;
    if (raw === "hunter-update://check" || raw === "hunter-update://check/") {
      if (!opening) {
        opening = true;
        view.hunterActionTask = Promise.resolve().then(() => performSelfUpdate(true))
          .then(updated => view.evaluateJavaScript("handleNativeUpdateResult(" + (updated ? "true" : "false") + ");").catch(() => {}))
          .finally(() => { opening = false; });
      } else view.evaluateJavaScript("handleNativeUpdateResult(false);").catch(() => {});
      return false;
    }
    const accountAction = /^hunter-account:\/\/(history|redeem|reconnect)\?account=(leosaquetto|gaoriel)(?:&url=([^&]+))?$/.exec(raw);
    if (accountAction) {
      let url = "";
      try { url = accountAction[3] ? decodeURIComponent(accountAction[3]) : ""; } catch (_) { return false; }
      if (accountAction[1] === "redeem" && !/^https:\/\/clube\.uol\.com\.br\/campanhasdeingresso\/[A-Za-z0-9][A-Za-z0-9_-]*$/.test(url)) return false;
      if (!opening) {
        opening = true;
        view.hunterActionTask = Promise.resolve().then(() => manageNativeAccount(view, accountAction[1], accountAction[2], url))
          .catch(() => view.evaluateJavaScript("handleNativeAccountResult(" + JSON.stringify(accountAction[1]) + "," + JSON.stringify(accountAction[2]) + ",{status:'unavailable',account:" + JSON.stringify(accountAction[2]) + "});").catch(() => {}))
          .finally(() => { opening = false; });
      } else {
        view.evaluateJavaScript("handleNativeAccountResult(" + JSON.stringify(accountAction[1]) + "," + JSON.stringify(accountAction[2]) + ",{status:'busy',account:" + JSON.stringify(accountAction[2]) + "});").catch(() => {});
      }
      return false;
    }
    if (raw === "hunter-access://manage" || raw === "hunter-access://manage/") {
      if (!opening) {
        opening = true;
        const task = Promise.resolve().then(() => manageNativeHunterAccess(view)).catch(() => hunterNotice("Coleta neste iPhone", "Não foi possível concluir a configuração agora."))
          .finally(() => { opening = false; });
        view.hunterActionTask = task;
      }
      return false;
    }
    if (/^hunter-instagram:\/\/(?:connect|reconnect|collect)\/?$/.test(raw)) {
      const action = raw.includes('://collect') ? 'collect' : 'connect';
      if (!opening) {
        opening = true;
        view.hunterActionTask = Promise.resolve().then(() => handleNativeInstagramAction(view, action))
          .catch(() => view.evaluateJavaScript("handleNativeInstagramResult(" + JSON.stringify(action) + ",{status:'unavailable'});").catch(() => {}))
          .finally(() => { opening = false; });
      } else view.evaluateJavaScript("handleNativeInstagramResult(" + JSON.stringify(action) + ",{status:'busy'});").catch(() => {});
      return false;
    }
    if (/^hunter-(?:account|access|update|instagram):/i.test(raw)) return false;
    const external = raw.startsWith(externalRoute);
    const route = external ? externalRoute : internalRoute;
    if (!raw.startsWith(route)) return true;
    let url;
    try { url = decodeURIComponent(raw.slice(route.length)); } catch (_) { return false; }
    if (url.length > 4096 || /[\x00-\x20\\]/.test(url) ||
        !/^https:\/\/(?:clube\.uol\.com\.br|clubeuol\.uol\.com\.br)(?:\/|$)/i.test(url)) return false;
    if (opening) return false;
    opening = true;
    // Neither browser path replaces the Hunter document. Closing the in-app
    // sheet or returning from Safari reveals the same Hunter screen.
    Promise.resolve().then(() => external ? Safari.open(url) : Safari.openInApp(url, false))
      .catch(() => { console.warn("Não foi possível abrir o navegador da oferta."); })
      .finally(() => { opening = false; });
    return false;
  };
}

function nativeRedemptionKey(item) {
  if (!item || typeof item !== "object") return "";
  const account = typeof item.user === "string" && item.user.trim() ? item.user.trim().toLowerCase() : "";
  if (!account) return "";
  const identity = typeof item.url === "string" && item.url.trim() ? "url:" + item.url.trim()
    : typeof item.id === "string" && item.id.trim() && !/^redemption-.+-\d+$/.test(item.id.trim()) ? "id:" + item.id.trim()
      : Number.isSafeInteger(item.id) ? "id:" + item.id : "";
  return identity ? account + "|" + identity : "";
}

function loadRedemptionsDatabase() {
  const records = [], byIdentity = new Map();
  function add(item) {
    if (!item || typeof item !== "object") return;
    const key = nativeRedemptionKey(item);
    if (!key || !byIdentity.has(key)) {
      if (key) byIdentity.set(key, records.length);
      records.push({ ...item });
      return;
    }
    const index = byIdentity.get(key), previous = records[index];
    records[index] = { ...previous, ...item };
    if (!item.redeemedAt && previous.redeemedAt) records[index].redeemedAt = previous.redeemedAt;
  }
  for (const item of BUNDLED_REDEMPTIONS) add(item);
  try {
    const fm = getFileManager();
    const dbPath = fm.joinPath(fm.documentsDirectory(), REDEMPTIONS_DB_FILE_NAME);
    if (fm.fileExists(dbPath)) {
      // preparePrivateFiles has awaited the iCloud download before this read.
      const parsed = JSON.parse(fm.readString(dbPath));
      if (!Array.isArray(parsed)) throw new Error("Formato inválido do banco de resgates");
      for (const item of parsed) add(item);
    }
  } catch (_) {
    console.log("Aviso: banco privado indisponível; arquivo original preservado.");
    return BUNDLED_REDEMPTIONS;
  }
  // A display load must not rewrite or repair the private redemption database.
  return records;
}

function saveRedemptionsDatabase(items) {
  try {
    const fm = getFileManager();
    const dbPath = fm.joinPath(fm.documentsDirectory(), REDEMPTIONS_DB_FILE_NAME);
    fm.writeString(dbPath, JSON.stringify(items, null, 2));
  } catch (e) {
    console.log("Aviso ao salvar uol_redemptions_database.json: " + e.message);
  }
}

function saveSettings(settings) {
  try {
    const fm = getFileManager();
    const path = fm.joinPath(fm.documentsDirectory(), SETTINGS_FILE_NAME);
    fm.writeString(path, JSON.stringify(settings, null, 2));
  } catch (e) {
    console.log("Aviso ao salvar uol_hub_settings.json: " + e.message);
  }
}

function getFileManager() {
  try {
    const fm = FileManager.iCloud();
    fm.documentsDirectory();
    return fm;
  } catch (e) {
    return FileManager.local();
  }
}

function loadNavigationTheme() {
  try {
    const fm = FileManager.local();
    const file = fm.joinPath(fm.documentsDirectory(), "uol_navigation_theme.json");
    if (!fm.fileExists(file)) return "default";
    const theme = JSON.parse(fm.readString(file)).theme;
    return ["default", "purple", "orange", "white"].includes(theme) ? theme : "default";
  } catch (_) {
    return "default";
  }
}

function saveNavigationTheme(theme) {
  if (!["default", "purple", "orange", "white"].includes(theme)) return false;
  try {
    const fm = FileManager.local();
    const file = fm.joinPath(fm.documentsDirectory(), "uol_navigation_theme.json");
    fm.writeString(file, JSON.stringify({ theme }));
    return true;
  } catch (_) {
    return false;
  }
}

// Private installs seed local files once. Later public code updates reuse them;
// existing sessions and the redemption database are never overwritten here.
async function preparePrivateFiles() {
  const fm = getFileManager();
  for (const name of [HISTORY_FILE_NAME, SETTINGS_FILE_NAME, COOKIES_FILE_NAME,
    "uol_session_cookies_leosaquetto.json", "uol_session_cookies_gaoriel.json", REDEMPTIONS_DB_FILE_NAME]) {
    const file = fm.joinPath(fm.documentsDirectory(), name);
    if (fm.fileExists(file) && fm.isFileDownloaded && !fm.isFileDownloaded(file)) {
      await fm.downloadFileFromiCloud(file);
      if (!fm.isFileDownloaded(file)) throw new Error("Arquivo privado ainda indisponível no iCloud: " + name);
    }
  }
}

function persistPrivateBootstrap() {
  const fm = getFileManager();
  const entries = [
    ["uol_session_cookies_leosaquetto.json", BUNDLED_COOKIES_LEO],
    ["uol_session_cookies_gaoriel.json", BUNDLED_COOKIES_GAORIEL],
    [REDEMPTIONS_DB_FILE_NAME, BUNDLED_REDEMPTIONS]
  ];
  for (const [name, value] of entries) {
    const destination = fm.joinPath(fm.documentsDirectory(), name);
    if (value.length && !fm.fileExists(destination)) fm.writeString(destination, JSON.stringify(value));
  }
}

function loadHistory() {
  try {
    const fm = getFileManager();
    const path = fm.joinPath(fm.documentsDirectory(), HISTORY_FILE_NAME);
    if (fm.fileExists(path)) {
      if (fm.isFileDownloaded && !fm.isFileDownloaded(path)) {
        fm.downloadFileFromiCloud(path);
      }
      const raw = fm.readString(path);
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") return parsed;
    }
  } catch (e) {
    console.log("Aviso ao ler histórico: " + e.message);
  }
  return { active: [], history: [] };
}

function loadSettings() {
  const defaults = {
    currentUser: "leosaquetto",
    priorizarWords: ["museu"],
    despriorizarWords: ["teatro"],
    groupedView: true,
    myRedemptions: [],
    realRedemptions: BUNDLED_REDEMPTIONS,
    userSessions: {
      leosaquetto: loadSessionCookies("leosaquetto"),
      gaoriel: loadSessionCookies("gaoriel")
    }
  };

  try {
    const fm = getFileManager();
    const path = fm.joinPath(fm.documentsDirectory(), SETTINGS_FILE_NAME);
    if (fm.fileExists(path)) {
      const raw = fm.readString(path);
      const parsed = JSON.parse(raw);
      const s = Object.assign(defaults, parsed);
      if (!s.userSessions) s.userSessions = defaults.userSessions;
      if (!s.userSessions.gaoriel || s.userSessions.gaoriel.length === 0) {
        s.userSessions.gaoriel = loadSessionCookies("gaoriel");
      }
      if (!s.userSessions.leosaquetto || s.userSessions.leosaquetto.length === 0) {
        s.userSessions.leosaquetto = loadSessionCookies("leosaquetto");
      }
      return s;
    }
  } catch (e) {}

  return defaults;
}

function loadSessionCookies(user = "leosaquetto") {
  try {
    const fm = getFileManager();
    const userCookieFileName = "uol_session_cookies_" + user + ".json";
    const userPath = fm.joinPath(fm.documentsDirectory(), userCookieFileName);
    if (fm.fileExists(userPath)) {
      if (fm.isFileDownloaded && !fm.isFileDownloaded(userPath)) {
        fm.downloadFileFromiCloud(userPath);
      }
      const raw = fm.readString(userPath);
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length > 0) return parsed;
      if (parsed && Array.isArray(parsed.cookies)) return parsed.cookies;
    }

    const defaultPath = fm.joinPath(fm.documentsDirectory(), COOKIES_FILE_NAME);
    if (fm.fileExists(defaultPath) && user === "leosaquetto") {
      if (fm.isFileDownloaded && !fm.isFileDownloaded(defaultPath)) {
        fm.downloadFileFromiCloud(defaultPath);
      }
      const raw = fm.readString(defaultPath);
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length > 0) return parsed;
      if (parsed && Array.isArray(parsed.cookies)) return parsed.cookies;
    }
  } catch (e) {
    console.log("Aviso ao ler cookies: " + e.message);
  }
  return user === "gaoriel" ? BUNDLED_COOKIES_GAORIEL : BUNDLED_COOKIES_LEO;
}

async function loadNativeRedemptions(cookies, user = "leosaquetto", existingList = BUNDLED_REDEMPTIONS) {
  const enrichMap = new Map();
  for (const b of (existingList || BUNDLED_REDEMPTIONS)) {
    const key = nativeRedemptionKey(b);
    if (key) enrichMap.set(key, b);
  }

  if (!cookies || cookies.length === 0) return existingList;
  try {
    const cookieHeader = cookies.map(c => c.name + "=" + c.value).join("; ");
    const req = new Request("https://clube.uol.com.br/perfil/beneficios");
    req.headers = {
      "Cookie": cookieHeader,
      "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
    };
    const html = await req.loadString();
    const items = [];
    const blockRegex = /<div[^>]*class="[^"]*beneficio[\s"][^>]*>([\s\S]*?)<\/a>\s*<\/div>/gi;
    let match;
    while ((match = blockRegex.exec(html)) !== null) {
      const block = match[1];
      const imgMatch = block.match(/<img[^>]*src="([^"]+)"/i);
      const partnerMatch = block.match(/<p class="parceiro[^"]*">([^<]+)<\/p>/i);
      const titleMatch = block.match(/<p class="titulo[^"]*"><strong>([^<]+)<\/strong><\/p>/i);
      const linkMatch = block.match(/href="([^"]+)"/i);
      if (titleMatch) {
        const rawTitle = titleMatch[1].trim();
        const fullUrl = linkMatch ? "https://clube.uol.com.br" + linkMatch[1] : "";
        const enriched = enrichMap.get(nativeRedemptionKey({ url: fullUrl, user })) || {};
        items.push(Object.assign({}, enriched, {
          id: enriched.id || ("redemption-" + user + "-" + items.length),
          title: rawTitle,
          partner: partnerMatch ? partnerMatch[1].trim() : (enriched.partner || "Campanhas de Ingressos"),
          thumb: imgMatch ? imgMatch[1] : (enriched.thumb || ""),
          url: fullUrl || enriched.url || "",
          user: user,
          redeemedAt: enriched.redeemedAt || "",
          description: enriched.description || "",
          rules: enriched.rules || "",
          downloadUrl: enriched.downloadUrl || "",
          cupomCode: enriched.cupomCode || "",
          localVoucherFile: enriched.localVoucherFile || "",
          voucherImageDataUrl: enriched.voucherImageDataUrl || ""
        }));
      }
    }
    if (items.length > 0) {
      const otherUserItems = (existingList || BUNDLED_REDEMPTIONS).filter(r => r.user !== user);
      return [...otherUserItems, ...items];
    }
    return existingList;
  } catch (e) {
    console.log("Aviso ao carregar resgates reais: " + e.message);
    return existingList;
  }
}

function loadCredentials() {
  const fm = getFileManager();
  let creds = { 
    email: "", 
    senha: "", 
    source: "none",
    accounts: {
      leosaquetto: { user: "leosaquetto", displayName: "Leonardo Saquetto", login: "", senha: "", source: "none" },
      gaoriel: { user: "gaoriel", displayName: "Gabriel", login: "", senha: "", source: "none" }
    }
  };

  try {
    const docPath = fm.joinPath(fm.documentsDirectory(), CREDS_FILE_NAME);
    if (fm.fileExists(docPath)) {
      if (fm.isFileDownloaded && !fm.isFileDownloaded(docPath)) {
        fm.downloadFileFromiCloud(docPath);
      }
      const content = fm.readString(docPath).trim();
      parseCredentialsText(content, creds);
      creds.source = "iCloud / Documents";
      return creds;
    }

    const localFm = FileManager.local();
    const home = localFm.documentsDirectory().split("/Documents")[0] || "";
    const dlPath = localFm.joinPath(home, "Downloads/" + CREDS_FILE_NAME);
    if (localFm.fileExists(dlPath)) {
      const content = localFm.readString(dlPath).trim();
      parseCredentialsText(content, creds);
      creds.source = "~/Downloads/" + CREDS_FILE_NAME;
      return creds;
    }
  } catch (e) {}

  return creds;
}

function parseCredentialsText(text, target) {
  const lines = text.split(/\r?\n/).map(l => l.trim());
  target.accounts = target.accounts || {
    leosaquetto: { user: "leosaquetto", displayName: "Leonardo Saquetto", login: "", senha: "", source: target.source || "clubeuolscrip.txt" },
    gaoriel: { user: "gaoriel", displayName: "Gabriel", login: "", senha: "", source: target.source || "clubeuolscrip.txt" }
  };

  let currentBlock = null;
  let blockLineIdx = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line || line.startsWith("#")) {
      currentBlock = null;
      blockLineIdx = 0;
      continue;
    }

    const lower = line.toLowerCase();
    if (lower === "leo" || lower === "leosaquetto") {
      currentBlock = "leosaquetto";
      blockLineIdx = 0;
      continue;
    } else if (lower === "gaoriel" || lower === "gao" || lower === "gab" || lower === "gabriel") {
      currentBlock = "gaoriel";
      blockLineIdx = 0;
      continue;
    }

    if (currentBlock) {
      if (blockLineIdx === 0) {
        target.accounts[currentBlock].login = line;
        target.accounts[currentBlock].source = target.source || "clubeuolscrip.txt";
        blockLineIdx++;
      } else if (blockLineIdx === 1) {
        target.accounts[currentBlock].senha = line;
        blockLineIdx++;
      }
      continue;
    }

    if (line.includes("=") || line.includes(":")) {
      const sep = line.includes("=") ? "=" : ":";
      const parts = line.split(sep);
      const k = parts[0].trim().toLowerCase();
      const v = parts.slice(1).join(sep).trim();
      if (k.startsWith("leo") || k.includes("leosaquetto")) {
        if (k.includes("pass") || k.includes("senha")) target.accounts.leosaquetto.senha = v;
        else target.accounts.leosaquetto.login = v;
      } else if (k.startsWith("gao") || k.includes("gab")) {
        if (k.includes("pass") || k.includes("senha")) target.accounts.gaoriel.senha = v;
        else target.accounts.gaoriel.login = v;
      } else {
        if (k.includes("email") || k.includes("usuario") || k.includes("login") || k.includes("user")) {
          target.accounts.leosaquetto.login = v;
        } else if (k.includes("senha") || k.includes("pass")) {
          target.accounts.leosaquetto.senha = v;
        }
      }
    }
  }

  target.email = (target.accounts.leosaquetto && target.accounts.leosaquetto.login) || target.email || "";
  target.senha = (target.accounts.leosaquetto && target.accounts.leosaquetto.senha) || target.senha || "";
}

function buildDashboardHTML(historyData, settings, credentials, sessionCookies, realRedemptions, collectionPausedForUpdate = false, collectionResult = null) {
  const activeUser = settings.currentUser || "leosaquetto";
  const cookiesForActiveUser = (activeUser === "gaoriel") ? BUNDLED_COOKIES_GAORIEL : BUNDLED_COOKIES_LEO;
  const initialData = {
    collectionPausedForUpdate,
    hunterCollection: collectionResult || { status: collectionPausedForUpdate ? "updated" : "starting", discovered: 0, received: 0, delivered: 0, pending: 0, sources: {} },
    historyData: historyData,
    settings: settings,
    navigationBarTheme: loadNavigationTheme(),
    credentials: credentials,
    sessionCookies: (sessionCookies && sessionCookies.length > 0) ? sessionCookies : cookiesForActiveUser,
    sessionCookiesLeo: settings.userSessions.leosaquetto,
    sessionCookiesGaoriel: settings.userSessions.gaoriel,
    realRedemptions: (realRedemptions && realRedemptions.length > 0) ? realRedemptions : BUNDLED_REDEMPTIONS,
    icons: ICONS,
    topic: NTFY_TOPIC,
    hunterAccess: collectionResult?.access || { status: "not_configured", reason: "activation_required" },
    nativeBrowser: true,
    nativeAccounts: true,
    scriptVersion: SCRIPT_VERSION,
    scriptName: typeof hunterInstalledScriptName === "function" ? hunterInstalledScriptName() : ((typeof Script !== "undefined" && Script.name && Script.name()) ? Script.name() : "Clube UOL Premium")
  };
  const jsonStr = JSON.stringify(initialData).replace(/</g, "\\u003c");

  const rawHtml = __BUILD_DASHBOARD_HTML__;
  return resolveHunterAssets(rawHtml).replace("__INITIAL_DATA_JSON__", () => jsonStr);
}

await main();
Script.complete();
