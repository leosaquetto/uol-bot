const fs = require("fs");
const path = require("path");

// Prepare validates a release artifact without installing it or notifying phones.
// Code-only updates preserve the existing iCloud redemption database and vouchers.
const buildOptions = new Set(process.argv.slice(2));
for (const option of buildOptions) {
  if (!["--prepare", "--code-only"].includes(option)) throw new Error("Unknown build option: " + option);
}
const prepareOnly = buildOptions.has("--prepare");
const codeOnly = buildOptions.has("--code-only");

const logoBase64 = fs.readFileSync(path.join(__dirname, "logo_base64.txt"), "utf8").trim();
const startupTemplate = fs.readFileSync(path.join(__dirname, "startup.html"), "utf8").replace(/__LOGO_BASE64__/g, logoBase64);
const svgIcons = JSON.parse(fs.readFileSync(path.join(__dirname, "svg_icons.json"), "utf8"));
let htmlTemplate = fs.readFileSync(path.join(__dirname, "template.html"), "utf8");
const collectorSource = fs.readFileSync(path.join(__dirname, "collector.js"), "utf8");
const accessSource = fs.readFileSync(path.join(__dirname, "access.js"), "utf8");
const redemptionSource = fs.readFileSync(path.join(__dirname, "redemption.js"), "utf8");
// Reuse the scoped receipt/offer parsers already exercised by the sentinel.
const receiptParserSource = fs.readFileSync(path.join(__dirname, "../cloudflare-workers/uol-redemption-sentinel/src/offers.js"), "utf8")
  .replace(/^export /gm, "");

// Load bootstrap cookies safely for Leo and Gabriel
const bootstrapFileLeo = path.join(process.env.HOME, ".config/uol-sentinel/leo-bootstrap.json");
let bundledCookiesLeo = [];
if (fs.existsSync(bootstrapFileLeo)) {
  try {
    const bData = JSON.parse(fs.readFileSync(bootstrapFileLeo, "utf8"));
    if (bData.cookies && Array.isArray(bData.cookies)) {
      bundledCookiesLeo = bData.cookies;
    }
  } catch (e) {}
}

const bootstrapFileGaoriel = path.join(process.env.HOME, ".config/uol-sentinel/gaoriel-bootstrap.json");
let bundledCookiesGaoriel = [];
if (fs.existsSync(bootstrapFileGaoriel)) {
  try {
    const bData = JSON.parse(fs.readFileSync(bootstrapFileGaoriel, "utf8"));
    if (bData.cookies && Array.isArray(bData.cookies)) {
      bundledCookiesGaoriel = bData.cookies;
    }
  } catch (e) {}
}
const bundledCookies = bundledCookiesLeo;


// Load cached redemptions details (timestamps, descriptions, vouchers)
const cachedRedemptionsPath = path.join(__dirname, "cached_redemptions_details.json");
const fallbackRedemptionsPath = path.join(__dirname, "bundled_redemptions.json");
let bundledRedemptions = [];
if (fs.existsSync(cachedRedemptionsPath)) {
  try {
    const rawRedemptions = JSON.parse(fs.readFileSync(cachedRedemptionsPath, "utf8"));
    bundledRedemptions = rawRedemptions.map(r => {
      const copy = { ...r };
      delete copy.voucherImageDataUrl;
      return copy;
    });
  } catch (e) {}
} else if (fs.existsSync(fallbackRedemptionsPath)) {
  try {
    bundledRedemptions = JSON.parse(fs.readFileSync(fallbackRedemptionsPath, "utf8"));
  } catch (e) {}
}

// Format SVGs with currentColor
const downloadsUolSvg = path.join(process.env.HOME, "Downloads/uol_icon_linear_white.svg");
const userPortalSvg = fs.existsSync(downloadsUolSvg) 
  ? fs.readFileSync(downloadsUolSvg, "utf8")
  : fs.readFileSync(path.join(__dirname, "user_portal.svg"), "utf8");
const globePathD = userPortalSvg.match(/d="([^"]+)"/)[1];

svgIcons['globe'] = {
  vb: "15 15 482 482",
  inner: `<path d="${globePathD}" fill="currentColor" stroke="currentColor" stroke-width="12" fill-rule="evenodd" clip-rule="evenodd"/>`
};
// 1. UOL colored ball from Downloads/Logotipo_do_UOL_(2021).svg (removendo o texto "uol")
const uolOfficialPath = path.join(process.env.HOME, "Downloads/Logotipo_do_UOL_(2021).svg");
const uolSvgRaw = fs.existsSync(uolOfficialPath)
  ? fs.readFileSync(uolOfficialPath, "utf8")
  : fs.readFileSync(path.join(__dirname, "uol_official.svg"), "utf8");
const uolPaths = uolSvgRaw.match(/<path[^>]+>/g).slice(1);
const uolBallSvg = `<svg viewBox="0 0 179 181" width="100%" height="100%">${uolPaths.join("")}</svg>`;

// 2. Instagram 2022 colored gradient icon from Downloads/Instagram_logo_2022.svg
const instaSvgPath = path.join(process.env.HOME, "Downloads/Instagram_logo_2022.svg");
const instaRaw = fs.existsSync(instaSvgPath)
  ? fs.readFileSync(instaSvgPath, "utf8")
  : `<svg viewBox="0 0 24 24"><path fill="currentColor" d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z"/></svg>`;

let instaColorSvg;
try {
  const instaDefs = instaRaw.match(/<defs>[\s\S]*?<\/defs>/)[0];
  const instaG = instaRaw.match(/<g[\s\S]*?<\/g>/)[0];
  const instaViewBox = instaRaw.match(/viewBox="([^"]+)"/)[1];
  instaColorSvg = `<svg viewBox="${instaViewBox}" width="100%" height="100%">${instaDefs}${instaG}</svg>`;
} catch (e) {
  instaColorSvg = instaRaw;
}

const formattedSvgs = {};
for (const [k, v] of Object.entries(svgIcons)) {
  let inner = v.inner.replace(/stroke="#000000"/g, 'stroke="currentColor"');
  if (k === 'instagram' || k === 'globe' || k === 'masksTheater' || k === 'stopwatch' || k === 'notes' || k === 'star') {
    formattedSvgs[k] = `<svg viewBox="${v.vb}" fill="currentColor" width="100%" height="100%">${inner}</svg>`;
  } else {
    formattedSvgs[k] = `<svg viewBox="${v.vb}" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" width="100%" height="100%">${inner}</svg>`;
  }
}

formattedSvgs['uolColor'] = uolBallSvg;
formattedSvgs['instagramColor'] = instaColorSvg;

// 3. Bottom Navigation Icons (casa, icone-circular, cartoes-empilhados, mao-com-bilhetes)
const bottomNavIcons = [
  { key: 'navHome', file: 'casa.svg' },
  { key: 'navTickets', file: 'icone-circular.svg' },
  { key: 'navHistory', file: 'cartoes-empilhados.svg' },
  { key: 'navRedemptions', file: 'mao-com-bilhetes.svg' }
];

for (const item of bottomNavIcons) {
  const localPath = path.join(__dirname, item.file);
  const dlPath = path.join(process.env.HOME, "Downloads", item.file);
  let svgRaw = "";
  if (fs.existsSync(localPath)) {
    svgRaw = fs.readFileSync(localPath, "utf8");
  } else if (fs.existsSync(dlPath)) {
    svgRaw = fs.readFileSync(dlPath, "utf8");
  }
  if (svgRaw) {
    const vbMatch = svgRaw.match(/viewBox="([^"]+)"/i);
    const dMatch = svgRaw.match(/d="([^"]+)"/i);
    const vb = vbMatch ? vbMatch[1] : "0 0 500 500";
    const d = dMatch ? dMatch[1] : "";
    formattedSvgs[item.key] = `<svg viewBox="${vb}" width="100%" height="100%"><path fill="currentColor" fill-rule="evenodd" d="${d}"/></svg>`;
  }
}

// Replace logo and icons placeholders in htmlTemplate
htmlTemplate = htmlTemplate.replace(/__LOGO_BASE64__/g, logoBase64);
for (const [k, svg] of Object.entries(formattedSvgs)) {
  const placeholder = new RegExp(`__ICON_${k}__`, "g");
  htmlTemplate = htmlTemplate.replace(placeholder, svg);
}

// Target paths
const targetScriptPath = path.resolve(__dirname, "../scriptable_uol_stories_hunter.js");
const icloudScriptPath = path.join(
  process.env.HOME,
  "Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents/Clube UOL - Stories Hunter.js"
);
const scriptHeader = String.raw`// Variables used by Scriptable.
// These must be at the very top of the file. Do not edit.
// icon-color: deep-orange; icon-glyph: ticket-alt;
// =====================================================================
// Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter
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

const SCRIPT_VERSION = "5.7.8";
const UPDATE_RAW_URL = "https://raw.githubusercontent.com/leosaquetto/uol-bot/main/scriptable_uol_stories_hunter.js";
const NTFY_TOPIC = "leo-macsend-8130";
const HISTORY_FILE_NAME = "uol_offers_history.json";
const SETTINGS_FILE_NAME = "uol_hub_settings.json";
const CREDS_FILE_NAME = "clubeuolscrip.txt";
const COOKIES_FILE_NAME = "uol_session_cookies.json";
const REDEMPTIONS_DB_FILE_NAME = "uol_redemptions_database.json";
const STARTUP_HTML = ${JSON.stringify(startupTemplate)};

const ICONS = ${JSON.stringify(formattedSvgs, null, 2)};
const BUNDLED_COOKIES_LEO = ${JSON.stringify(bundledCookiesLeo)};
const BUNDLED_COOKIES_GAORIEL = ${JSON.stringify(bundledCookiesGaoriel)};
const BUNDLED_COOKIES = BUNDLED_COOKIES_LEO;
const BUNDLED_REDEMPTIONS = ${JSON.stringify(bundledRedemptions, null, 2)};

${collectorSource}
${accessSource}
${redemptionSource}
const UOL_RECEIPT_PARSER = ${JSON.stringify(receiptParserSource)};

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

function hunterVersionParts(value) {
  if (typeof value !== "string" || !/^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$/.test(value)) return null;
  const parts = value.split('.').map(Number);
  return parts.every(Number.isSafeInteger) ? parts : null;
}

function isNewerVersion(remote, local) {
  const remoteParts = hunterVersionParts(remote), localParts = hunterVersionParts(local);
  if (!remoteParts || !localParts) return false;
  for (let i = 0; i < 3; i++) {
    if (remoteParts[i] > localParts[i]) return true;
    if (remoteParts[i] < localParts[i]) return false;
  }
  return false;
}

function hunterUpdateVersion(code, boundDownload = true) {
  if (typeof code !== "string" || boundDownload && unescape(encodeURIComponent(code)).length > 2097152 ||
      !code.startsWith('// Variables used by Scriptable.\n// These must be at the very top of the file.') ||
      !/\/\/ Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter\r?\n/.test(code) ||
      !/\nawait main\(\);\r?\nScript.complete\(\);\s*$/.test(code) ||
      !/^async function main\(\) \{/m.test(code) || !/^function buildDashboardHTML\(/m.test(code) ||
      !/^function hunterCollectionPausedForUpdate\(action\) \{/m.test(code) ||
      !/^const HunterCollector = \(\(\) => \{/m.test(code)) return null;
  const matches = [...code.matchAll(/^const SCRIPT_VERSION = "([^"]+)";$/gm)];
  return matches.length === 1 && hunterVersionParts(matches[0][1]) ? matches[0][1] : null;
}

async function performSelfUpdate(isManual = false) {
  async function notice(title, message) {
    if (!isManual) return;
    try {
      const alert = new Alert(); alert.title = title; alert.message = message;
      alert.addAction("OK"); await alert.presentAlert();
    } catch (_) { /* A dismissed UI cannot change the verified installation result. */ }
  }
  try {
    const req = new Request(UPDATE_RAW_URL);
    req.timeoutInterval = 20; req.onRedirect = () => null;
    req.headers = { Accept: "text/plain, application/javascript", "Cache-Control": "no-cache" };
    const remoteCode = await req.loadString();
    const response = req.response || {};
    const header = name => Object.entries(response.headers || {}).find(([key]) => key.toLowerCase() === name)?.[1] || '';
    const type = header('content-type') || response.mimeType || '';
    const remoteVersion = hunterUpdateVersion(remoteCode);
    if (response.statusCode !== 200 || response.url !== UPDATE_RAW_URL ||
        !/^(?:text\/(?:plain|javascript)|application\/(?:javascript|x-javascript))(?:;|$)/i.test(type) ||
        Number(header('content-length')) > 2097152 || !remoteVersion || !hunterVersionParts(SCRIPT_VERSION)) {
      await notice("Atualização não confirmada", "O arquivo oficial não pôde ser validado. A versão instalada foi preservada.");
      return false;
    }
    const fm = getFileManager(), local = FileManager.local();
    const scriptName = typeof Script !== "undefined" && Script.name && Script.name() ? Script.name() : "Clube UOL - Stories Hunter";
    if (/[\/\\\u0000-\u001f]/.test(scriptName)) throw new Error('script_name_invalid');
    const scriptPath = fm.joinPath(fm.documentsDirectory(), scriptName + ".js");
    if (!fm.fileExists(scriptPath)) throw new Error('installed_script_missing');
    if (fm.isFileDownloaded && !fm.isFileDownloaded(scriptPath)) await fm.downloadFileFromiCloud(scriptPath);
    const previous = fm.readString(scriptPath);
    if (hunterUpdateVersion(previous, false) !== SCRIPT_VERSION) throw new Error('installed_script_unavailable');
    if (!isNewerVersion(remoteVersion, SCRIPT_VERSION)) {
      await notice(remoteVersion === SCRIPT_VERSION ? "Você já está atualizado!" : "Versão instalada preservada",
        remoteVersion === SCRIPT_VERSION ? "A versão oficial é a mesma instalada (" + SCRIPT_VERSION + ")."
          : "O GitHub retornou uma versão anterior. Nenhum downgrade foi aplicado.");
      return false;
    }
    const confirm = new Alert(); confirm.title = "Atualização disponível";
    confirm.message = "Instalar a versão " + remoteVersion + "? Sessões, vouchers e histórico serão preservados.";
    confirm.addAction("Instalar atualização"); confirm.addCancelAction("Cancelar");
    if (await confirm.presentAlert() < 0) return false;
    const backup = local.joinPath(local.documentsDirectory(), "uol_hunter_ota_backup.js");
    const staging = local.joinPath(local.documentsDirectory(), "uol_hunter_ota_staging.js");
    local.writeString(backup, previous);
    if (local.readString(backup) !== previous) throw new Error('backup_unverified');
    local.writeString(staging, remoteCode);
    if (local.readString(staging) !== remoteCode) throw new Error('staging_unverified');
    try {
      fm.writeString(scriptPath, local.readString(staging));
      if (fm.readString(scriptPath) !== remoteCode) throw new Error('installed_script_unverified');
    } catch (_) {
      let restored = false;
      try { fm.writeString(scriptPath, local.readString(backup)); restored = fm.readString(scriptPath) === previous; } catch (_) {}
      await notice("Atualização interrompida", restored ? "A gravação falhou e a versão anterior foi restaurada. Seus dados foram preservados."
        : "Não foi possível confirmar a restauração. A cópia anterior está preservada no armazenamento local: uol_hunter_ota_backup.js.");
      return false;
    }
    await notice("Script atualizado", "Versão " + remoteVersion + " instalada. Toque em OK para abrir; esta primeira abertura não inicia coleta.");
    try { Safari.open("scriptable:///run?scriptName=" + encodeURIComponent(scriptName)); }
    catch (_) { await notice("Atualização salva", "Abra o Stories Hunter novamente para usar a nova versão."); }
    return true;
  } catch (_) {
    await notice("Atualização não confirmada", "Não foi possível concluir a atualização. Nenhum sucesso foi confirmado; a cópia instalada e seus dados foram preservados.");
    return false;
  }
}

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

async function runNativeHunterCollector() {
  try {
    const collector = HunterCollector.create({
      fm: FileManager.local(), keychain: Keychain, now: Date.now,
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
    uuid: () => UUID.string().toLowerCase(), request: url => new Request(url),
    normalizeSession: HunterCollector.normalizeSession });
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
  console.log("⚡ [Clube UOL Hub] Inicializando...");
  const action = hunterAction();
  if (action === "update") {
    await performSelfUpdate(true);
    Script.complete();
    return;
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
    if (!dismissed) await uiView.loadHTML(html, "https://clube.uol.com.br");
  } catch (_) {
    await readable;
    if (!dismissed) await uiView.loadHTML(buildHunterStartupHTML(true), "https://clube.uol.com.br");
  }
  await presentation;
  await collection;
  if (uiView.hunterActionTask) await uiView.hunterActionTask;
  Script.complete();
}

function buildHunterStartupHTML(failed = false) {
  return STARTUP_HTML.replace("__STARTUP_TITLE__", failed ? "Não foi possível abrir" : "Preparando suas ofertas")
    .replace("__STARTUP_DESCRIPTION__", failed
      ? "Seus dados foram preservados. Feche esta janela e tente abrir novamente."
      : "Buscando novidades e carregando seu histórico…")
    .replace('<progress aria-label="Carregando o Stories Hunter"></progress>', failed ? "" : '<progress aria-label="Carregando o Stories Hunter"></progress>');
}

function installHunterBrowser(view) {
  const internalRoute = "hunter-browser://open?url=";
  const externalRoute = "hunter-browser://external?url=";
  let opening = false;
  view.shouldAllowRequest = request => {
    const raw = request && request.url;
    if (typeof raw !== "string") return true;
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
    if (/^hunter-(?:account|access|update):/i.test(raw)) return false;
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
    scriptName: (typeof Script !== "undefined" && Script.name && Script.name()) ? Script.name() : "Clube UOL - Stories Hunter"
  };
  const jsonStr = JSON.stringify(initialData).replace(/</g, "\\u003c");

  const rawHtml = ${JSON.stringify(htmlTemplate)};
  return rawHtml.replace("__INITIAL_DATA_JSON__", () => jsonStr);
}

await main();
Script.complete();
`;

// PUBLIC_ARTIFACT_START
// The GitHub/MacSend code artifact must never embed private accounts or vouchers.
// The private iCloud install below retains the explicitly shared bootstrap.
const publicScriptHeader = scriptHeader
  .replace('const BUNDLED_COOKIES_LEO = ' + JSON.stringify(bundledCookiesLeo) + ';', 'const BUNDLED_COOKIES_LEO = [];')
  .replace('const BUNDLED_COOKIES_GAORIEL = ' + JSON.stringify(bundledCookiesGaoriel) + ';', 'const BUNDLED_COOKIES_GAORIEL = [];')
  .replace('const BUNDLED_REDEMPTIONS = ' + JSON.stringify(bundledRedemptions, null, 2) + ';', 'const BUNDLED_REDEMPTIONS = [];');
if (!publicScriptHeader.includes('const BUNDLED_COOKIES_LEO = [];') ||
    !publicScriptHeader.includes('const BUNDLED_COOKIES_GAORIEL = [];') ||
    !publicScriptHeader.includes('const BUNDLED_REDEMPTIONS = [];')) throw new Error('Public artifact sanitization failed');
// PUBLIC_ARTIFACT_END

fs.writeFileSync(targetScriptPath, publicScriptHeader, "utf8");
console.log("Successfully generated", targetScriptPath);
console.log("File size:", fs.statSync(targetScriptPath).size, "bytes");

if (!prepareOnly && fs.existsSync(path.dirname(icloudScriptPath))) {
  fs.writeFileSync(icloudScriptPath, scriptHeader, "utf8");
  console.log("Successfully synced to iCloud:", icloudScriptPath);

  if (!codeOnly) {
    const icloudDocsDir = path.dirname(icloudScriptPath);
    const icloudDbPath = path.join(icloudDocsDir, "uol_redemptions_database.json");
    fs.writeFileSync(icloudDbPath, JSON.stringify(bundledRedemptions, null, 2), "utf8");
    console.log("Successfully synced database to iCloud:", icloudDbPath);

    const icloudVouchersDir = path.join(icloudDocsDir, "vouchers");
    if (!fs.existsSync(icloudVouchersDir)) fs.mkdirSync(icloudVouchersDir, { recursive: true });
    const localVouchersDir = path.join(__dirname, "vouchers");
    if (fs.existsSync(localVouchersDir)) {
      const vFiles = fs.readdirSync(localVouchersDir);
      for (const vf of vFiles) {
        const src = path.join(localVouchersDir, vf);
        const dst = path.join(icloudVouchersDir, vf);
        if (fs.existsSync(src) && !fs.existsSync(dst)) {
          fs.copyFileSync(src, dst);
        }
      }
      console.log(`Synced ${vFiles.length} voucher files to iCloud vouchers/ folder`);
    }
  }
}

// 4. Dispara NTFY para acionar a automação do Atalhos no iPhone (atualização OTA)
if (!prepareOnly) try {
  const { execSync } = require("child_process");
  try {
    execSync('curl -s "http://127.0.0.1:53318/scriptable/stage?target=clube-uol"', { timeout: 2000 });
  } catch (e) {}
  const bytes = fs.statSync(targetScriptPath).size;
  const ntfyCmd = `curl -s -H "Title: Scriptable" -H "Priority: high" -d "Clube UOL - Stories Hunter.js compilado (${bytes} bytes). Atualizando no iPhone..." https://ntfy.sh/leo-macsend-8130`;
  const res = execSync(ntfyCmd, { encoding: "utf8" });
  console.log("⚡ [NTFY OTA] Notificação enviada para disparar atualização no iPhone:", res.trim());
} catch (e) {
  console.warn("Aviso ao enviar notificação NTFY:", e.message);
}

