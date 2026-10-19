const fs = require("node:fs");
const path = require("node:path");

// Reproducible public assembly: repository inputs only, no install/network/private reads.
function assembleHunter({ bundledCookiesLeo = [], bundledCookiesGaoriel = [], bundledRedemptions = [] } = {}) {
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

const substitutions = {
  __BUILD_UPDATER__: fs.readFileSync(path.join(__dirname, "native/updater.js"), "utf8"),
  __BUILD_STARTUP_HTML__: JSON.stringify(startupTemplate),
  __BUILD_LOGO__: JSON.stringify(logoBase64),
  __BUILD_ICONS__: JSON.stringify(formattedSvgs, null, 2),
  __BUILD_COOKIES_LEO__: JSON.stringify(bundledCookiesLeo),
  __BUILD_COOKIES_GAORIEL__: JSON.stringify(bundledCookiesGaoriel),
  __BUILD_REDEMPTIONS__: JSON.stringify(bundledRedemptions, null, 2),
  __BUILD_COLLECTOR__: collectorSource,
  __BUILD_INSTAGRAM_BROWSER__: fs.readFileSync(path.join(__dirname, "native/instagram-browser.js"), "utf8"),
  __BUILD_ACCESS__: accessSource,
  __BUILD_REDEMPTION__: redemptionSource,
  __BUILD_PARSER__: JSON.stringify(receiptParserSource),
  __BUILD_DASHBOARD_HTML__: JSON.stringify(htmlTemplate)
};
const hostSource = fs.readFileSync(path.join(__dirname, "native/host.js"), "utf8");
return hostSource.replace(/__BUILD_[A-Z_]+__/g, marker => {
  if (!Object.prototype.hasOwnProperty.call(substitutions, marker)) throw new Error("Unknown build marker: " + marker);
  return substitutions[marker];
});
}
module.exports = { assembleHunter };
