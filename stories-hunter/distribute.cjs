// Distribution side effects are isolated from source assembly.
module.exports = function distribute({ fs, path, targetScriptPath, icloudScriptPath, scriptHeader,
  bundledCookiesLeo, bundledCookiesGaoriel, bundledRedemptions, prepareOnly, codeOnly, sourceDirectory }) {
  const __dirname = sourceDirectory;
// DISTRIBUTION_BODY_START
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

// DISTRIBUTION_BODY_END
};
