const fs = require("fs");
const path = require("path");

const logoBase64 = fs.readFileSync(path.join(__dirname, "logo_base64.txt"), "utf8").trim();
const svgIcons = JSON.parse(fs.readFileSync(path.join(__dirname, "svg_icons.json"), "utf8"));
let htmlTemplate = fs.readFileSync(path.join(__dirname, "template.html"), "utf8");

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
// - Perfil de Usuário com Avatar Circular (leosaquetto / gaoriel via unavatar)
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

const SCRIPT_VERSION = "5.3.0";
const UPDATE_RAW_URL = "https://raw.githubusercontent.com/leosaquetto/uol-bot/main/scriptable_uol_stories_hunter.js";
const NTFY_TOPIC = "leo-macsend-8130";
const HISTORY_FILE_NAME = "uol_offers_history.json";
const SETTINGS_FILE_NAME = "uol_hub_settings.json";
const CREDS_FILE_NAME = "clubeuolscrip.txt";
const COOKIES_FILE_NAME = "uol_session_cookies.json";
const REDEMPTIONS_DB_FILE_NAME = "uol_redemptions_database.json";

const ICONS = ${JSON.stringify(formattedSvgs, null, 2)};
const BUNDLED_COOKIES_LEO = ${JSON.stringify(bundledCookiesLeo)};
const BUNDLED_COOKIES_GAORIEL = ${JSON.stringify(bundledCookiesGaoriel)};
const BUNDLED_COOKIES = BUNDLED_COOKIES_LEO;
const BUNDLED_REDEMPTIONS = ${JSON.stringify(bundledRedemptions, null, 2)};

async function performSelfUpdate(isManual = false) {
  try {
    const fm = getFileManager();
    const scriptName = (typeof Script !== "undefined" && Script.name && Script.name()) ? Script.name() : "Clube UOL - Stories Hunter";
    const scriptPath = fm.joinPath(fm.documentsDirectory(), scriptName + ".js");

    console.log("⚡ [OTA] Verificando atualizações em " + UPDATE_RAW_URL);
    const req = new Request(UPDATE_RAW_URL + "?t=" + Date.now());
    req.timeoutInterval = 10;
    const remoteCode = await req.loadString();

    if (!remoteCode || !remoteCode.includes("Clube UOL - Hub Central")) {
      if (isManual) {
        const a = new Alert();
        a.title = "Falha ao Atualizar";
        a.message = "Não foi possível carregar a versão oficial do GitHub. Verifique sua conexão com a internet.";
        a.addAction("OK");
        await a.presentAlert();
      }
      return false;
    }

    const versionMatch = remoteCode.match(/const SCRIPT_VERSION = "([^"]+)";/);
    const remoteVersion = versionMatch ? versionMatch[1] : null;

    if (remoteVersion && remoteVersion !== SCRIPT_VERSION) {
      fm.writeString(scriptPath, remoteCode);
      console.log("⚡ [OTA] Atualizado com sucesso: v" + SCRIPT_VERSION + " -> v" + remoteVersion);

      const a = new Alert();
      a.title = "✨ Script Atualizado!";
      a.message = "Clube UOL Stories Hunter atualizado para a versão " + remoteVersion + ".\n\nToque em OK para abrir a nova versão.";
      a.addAction("OK");
      await a.presentAlert();

      Safari.open("scriptable:///run?scriptName=" + encodeURIComponent(scriptName));
      return true;
    } else {
      if (isManual) {
        const a = new Alert();
        a.title = "Você já está atualizado!";
        a.message = "Você já está usando a versão mais recente (" + SCRIPT_VERSION + ") do GitHub.";
        a.addAction("OK");
        await a.presentAlert();
      }
      return false;
    }
  } catch (err) {
    console.warn("⚡ [OTA] Erro: " + err.message);
    if (isManual) {
      const a = new Alert();
      a.title = "Erro na Atualização";
      a.message = err.message;
      a.addAction("OK");
      await a.presentAlert();
    }
    return false;
  }
}

async function main() {
  console.log("⚡ [Clube UOL Hub] Inicializando...");

  if (typeof args !== "undefined" && args.queryParameters && args.queryParameters.action === "update") {
    await performSelfUpdate(true);
    Script.complete();
    return;
  }

  const settings = loadSettings();
  const historyData = loadHistory();
  const credentials = loadCredentials();
  const sessionCookies = loadSessionCookies(settings.currentUser || "leosaquetto");
  let realRedemptions = loadRedemptionsDatabase();
  const html = buildDashboardHTML(historyData, settings, credentials, sessionCookies, realRedemptions);

  const uiView = new WebView();
  await uiView.loadHTML(html, "https://clube.uol.com.br");
  await uiView.present(true);
  Script.complete();
}

function loadRedemptionsDatabase() {
  const enrichMap = new Map();
  // 1. Base embutida (zero rede, funciona sempre mesmo sem iCloud)
  for (const b of BUNDLED_REDEMPTIONS) {
    const key = b.url || b.id || b.title;
    if (key) enrichMap.set(key, b);
  }

  // 2. Lê do arquivo permanente no iCloud Drive
  try {
    const fm = getFileManager();
    const dbPath = fm.joinPath(fm.documentsDirectory(), REDEMPTIONS_DB_FILE_NAME);
    if (fm.fileExists(dbPath)) {
      if (fm.isFileDownloaded && !fm.isFileDownloaded(dbPath)) {
        fm.downloadFileFromiCloud(dbPath);
      }
      const raw = fm.readString(dbPath);
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        for (const item of parsed) {
          const key = item.url || item.id || item.title;
          if (key) {
            const existing = enrichMap.get(key) || {};
            enrichMap.set(key, Object.assign({}, existing, item));
          }
        }
      }
    }
  } catch (e) {
    console.log("Aviso ao ler uol_redemptions_database.json: " + e.message);
  }

  return Array.from(enrichMap.values());
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
      leosaquetto: BUNDLED_COOKIES_LEO,
      gaoriel: BUNDLED_COOKIES_GAORIEL
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
        s.userSessions.gaoriel = BUNDLED_COOKIES_GAORIEL;
      }
      if (!s.userSessions.leosaquetto || s.userSessions.leosaquetto.length === 0) {
        s.userSessions.leosaquetto = BUNDLED_COOKIES_LEO;
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
    if (b.url) enrichMap.set(b.url, b);
    if (b.title) enrichMap.set(b.title.trim().toLowerCase(), b);
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
        const enriched = enrichMap.get(fullUrl) || enrichMap.get(rawTitle.toLowerCase()) || {};
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
      const otherUserItems = (existingList || BUNDLED_REDEMPTIONS).filter(r => (r.user || "leosaquetto") !== user);
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

function buildDashboardHTML(historyData, settings, credentials, sessionCookies, realRedemptions) {
  const activeUser = settings.currentUser || "leosaquetto";
  const cookiesForActiveUser = (activeUser === "gaoriel") ? BUNDLED_COOKIES_GAORIEL : BUNDLED_COOKIES_LEO;
  const initialData = {
    historyData: historyData,
    settings: settings,
    credentials: credentials,
    sessionCookies: (sessionCookies && sessionCookies.length > 0) ? sessionCookies : cookiesForActiveUser,
    sessionCookiesLeo: BUNDLED_COOKIES_LEO,
    sessionCookiesGaoriel: BUNDLED_COOKIES_GAORIEL,
    realRedemptions: (realRedemptions && realRedemptions.length > 0) ? realRedemptions : BUNDLED_REDEMPTIONS,
    icons: ICONS,
    topic: NTFY_TOPIC,
    scriptVersion: SCRIPT_VERSION,
    scriptName: (typeof Script !== "undefined" && Script.name && Script.name()) ? Script.name() : "Clube UOL - Stories Hunter"
  };
  const jsonStr = JSON.stringify(initialData).replace(/</g, "\\u003c");

  const rawHtml = ${JSON.stringify(htmlTemplate)};
  return rawHtml.replace("__INITIAL_DATA_JSON__", jsonStr);
}

await main();
Script.complete();
`;

fs.writeFileSync(targetScriptPath, scriptHeader, "utf8");
console.log("Successfully generated", targetScriptPath);
console.log("File size:", fs.statSync(targetScriptPath).size, "bytes");

if (fs.existsSync(path.dirname(icloudScriptPath))) {
  fs.writeFileSync(icloudScriptPath, scriptHeader, "utf8");
  console.log("Successfully synced to iCloud:", icloudScriptPath);

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

// 4. Dispara NTFY para acionar a automação do Atalhos no iPhone (atualização OTA)
try {
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

