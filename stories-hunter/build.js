const fs = require("node:fs");
const path = require("node:path");
const buildOptions = new Set(process.argv.slice(2));
for (const option of buildOptions) {
  if (!["--prepare", "--code-only", "--install"].includes(option)) throw new Error("Unknown build option: " + option);
}
const prepareOnly = buildOptions.has("--prepare");
const codeOnly = buildOptions.has("--code-only");
const installLocal = !prepareOnly && (buildOptions.has("--install") || !codeOnly);

// BUILD_OPTIONS_END
const { assembleHunter } = require("./assemble.cjs");
const targetScriptPath = path.join(__dirname, "dist/Hunter.js");
fs.mkdirSync(path.dirname(targetScriptPath), { recursive: true });
const publicCode = assembleHunter();
if (!prepareOnly) {
  const manifest = JSON.parse(fs.readFileSync(path.join(__dirname, "releases/stable.json"), "utf8"));
  const hash = require("node:crypto").createHash("sha256").update(publicCode).digest("hex");
  if (manifest.sha256 !== hash || manifest.size !== Buffer.byteLength(publicCode))
    throw new Error("Publish/promote these exact bytes to stable before distributing");
  const legacy = fs.readFileSync(path.join(__dirname, "../scriptable_uol_stories_hunter.js"), "utf8");
  if (legacy !== publicCode) throw new Error("Legacy stable mirror differs from promoted artifact");
}
const privateData = !installLocal ? { bundledCookiesLeo: [], bundledCookiesGaoriel: [], bundledRedemptions: [] }
  : require("./private-install.cjs")();
const scriptHeader = !installLocal ? publicCode : assembleHunter(privateData);
const icloudScriptPath = path.join(process.env.HOME,
  "Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents/Clube UOL Premium.js");
require("./distribute.cjs")({ fs, path, targetScriptPath, icloudScriptPath, scriptHeader,
  ...privateData, prepareOnly, codeOnly, installLocal, sourceDirectory: __dirname });
