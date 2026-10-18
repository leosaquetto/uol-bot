const fs = require("node:fs");
const path = require("node:path");
// Compatibility-only private installation inputs. Public assembly never imports this module.
module.exports = function privateInstallData() {
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

return { bundledCookiesLeo, bundledCookiesGaoriel, bundledRedemptions };
};
