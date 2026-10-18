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
const startupTemplate = fs.readFileSync(path.join(__dirname, "startup.html"), "utf8");
const formattedSvgs = JSON.parse(fs.readFileSync(path.join(__dirname, "assets/icons.json"), "utf8"));
let htmlTemplate = require("./assemble-ui.cjs").assembleUI();
const collectorSource = fs.readFileSync(path.join(__dirname, "collector.js"), "utf8");
const accessSource = fs.readFileSync(path.join(__dirname, "access.js"), "utf8");
const redemptionSource = fs.readFileSync(path.join(__dirname, "redemption.js"), "utf8");
// Reuse the scoped receipt/offer parsers already exercised by the sentinel.
const receiptParserSource = fs.readFileSync(path.join(__dirname, "../cloudflare-workers/uol-redemption-sentinel/src/offers.js"), "utf8")
  .replace(/^export /gm, "");

// Load bootstrap cookies safely for Leo and Gabriel
const bootstrapFileLeo = path.join(process.env.HOME, ".config/uol-sentinel/leo-bootstrap.json");
let bundledCookiesLeo = [];
if (!prepareOnly && fs.existsSync(bootstrapFileLeo)) {
  try {
    const bData = JSON.parse(fs.readFileSync(bootstrapFileLeo, "utf8"));
    if (bData.cookies && Array.isArray(bData.cookies)) {
      bundledCookiesLeo = bData.cookies;
    }
  } catch (e) {}
}

const bootstrapFileGaoriel = path.join(process.env.HOME, ".config/uol-sentinel/gaoriel-bootstrap.json");
let bundledCookiesGaoriel = [];
if (!prepareOnly && fs.existsSync(bootstrapFileGaoriel)) {
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
if (!prepareOnly && fs.existsSync(cachedRedemptionsPath)) {
  try {
    const rawRedemptions = JSON.parse(fs.readFileSync(cachedRedemptionsPath, "utf8"));
    bundledRedemptions = rawRedemptions.map(r => {
      const copy = { ...r };
      delete copy.voucherImageDataUrl;
      return copy;
    });
  } catch (e) {}
} else if (!prepareOnly && fs.existsSync(fallbackRedemptionsPath)) {
  try {
    bundledRedemptions = JSON.parse(fs.readFileSync(fallbackRedemptionsPath, "utf8"));
  } catch (e) {}
}

// Target paths
const targetScriptPath = path.resolve(__dirname, "../scriptable_uol_stories_hunter.js");
const icloudScriptPath = path.join(
  process.env.HOME,
  "Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents/Clube UOL - Stories Hunter.js"
);
const substitutions = {
  __BUILD_STARTUP_HTML__: JSON.stringify(startupTemplate),
  __BUILD_LOGO__: JSON.stringify(logoBase64),
  __BUILD_ICONS__: JSON.stringify(formattedSvgs, null, 2),
  __BUILD_COOKIES_LEO__: JSON.stringify(bundledCookiesLeo),
  __BUILD_COOKIES_GAORIEL__: JSON.stringify(bundledCookiesGaoriel),
  __BUILD_REDEMPTIONS__: JSON.stringify(bundledRedemptions, null, 2),
  __BUILD_COLLECTOR__: collectorSource,
  __BUILD_ACCESS__: accessSource,
  __BUILD_REDEMPTION__: redemptionSource,
  __BUILD_PARSER__: JSON.stringify(receiptParserSource),
  __BUILD_DASHBOARD_HTML__: JSON.stringify(htmlTemplate)
};
const hostSource = fs.readFileSync(path.join(__dirname, "native/host.js"), "utf8");
const scriptHeader = hostSource.replace(/__BUILD_[A-Z_]+__/g, marker => {
  if (!Object.prototype.hasOwnProperty.call(substitutions, marker)) throw new Error("Unknown build marker: " + marker);
  return substitutions[marker];
});
require("./distribute.cjs")({ fs, path, targetScriptPath, icloudScriptPath, scriptHeader,
  bundledCookiesLeo, bundledCookiesGaoriel, bundledRedemptions, prepareOnly, codeOnly, sourceDirectory: __dirname });
