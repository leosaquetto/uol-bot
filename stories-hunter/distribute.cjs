// Distribution side effects are isolated from source assembly.
module.exports = function distribute({ fs, path, targetScriptPath, icloudScriptPath, scriptHeader,
  bundledCookiesLeo, bundledCookiesGaoriel, bundledRedemptions, prepareOnly, codeOnly, installLocal, sourceDirectory }) {
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

if (installLocal && fs.existsSync(path.dirname(icloudScriptPath))) {
  require('./install-code.cjs')({fs, path, target:icloudScriptPath, code:scriptHeader,
    backupDirectory:path.join(process.env.HOME, '.config/uol-ticket-research/premium-code-backups')});
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

// Dedicated title never matches the generic "Scriptable" automation.
// Do not stage /scriptable: another product may already be awaiting delivery.
if (!prepareOnly) {
  const { execFileSync } = require("child_process");
  const bytes = fs.statSync(targetScriptPath).size;
  const res = execFileSync('curl', ['--fail', '--silent', '--show-error', '--max-time', '20',
    '-H', 'Title: Clube UOL Premium', '-H', 'Priority: high', '-H', 'Tags: arrow_up',
    '-d', `Atualização disponível para Clube UOL Premium (${bytes} bytes). Execute Update Clube UOL Premium.`,
    'https://ntfy.sh/leo-macsend-8130'], {encoding:'utf8',timeout:25000});
  const receipt = JSON.parse(res);
  if (receipt.event !== 'message' || !receipt.id) throw Error('ntfy acceptance unconfirmed; do not retry blindly');
  console.log('[NTFY OTA] Aceite confirmado:', JSON.stringify({id:receipt.id,time:receipt.time,event:receipt.event}));
}

// DISTRIBUTION_BODY_END
};
