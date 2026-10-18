// Code-only updater. Network failures never require runtime code downloads.
// FileManager.move replaces an existing destination, but Scriptable does not
// promise crash-atomicity, durable fsync or a cross-process compare-and-swap.
const HUNTER_UPDATE_PROTOCOL = 1;
const HUNTER_UPDATE_MAX_BYTES = 2097152;
const HUNTER_UPDATE_MANIFEST_MAX_BYTES = 8192;
let hunterUpdateInFlight = false;
let hunterLastUpdateResult = { status: "idle" };

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
  if (typeof code !== "string" || boundDownload && unescape(encodeURIComponent(code)).length > HUNTER_UPDATE_MAX_BYTES ||
      !code.startsWith('// Variables used by Scriptable.\n// These must be at the very top of the file.') ||
      !/\/\/ Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter\r?\n/.test(code) ||
      !/\nawait main\(\);\r?\nScript.complete\(\);\s*$/.test(code) ||
      !/^async function main\(\) \{/m.test(code) || !/^function buildDashboardHTML\(/m.test(code) ||
      !/^function hunterCollectionPausedForUpdate\(action\) \{/m.test(code) ||
      !/^const HunterCollector = \(\(\) => \{/m.test(code)) return null;
  const matches = [...code.matchAll(/^const SCRIPT_VERSION = "([^"]+)";$/gm)];
  if (matches.length !== 1 || !hunterVersionParts(matches[0][1])) return null;
  try {
    // Parse the complete artifact in an async body, as required by its top-level
    // await. Neither the constructor result nor the wrapped function is called:
    // candidate code must never run during verification (including recovery).
    new Function('return async function () {\n' + code + '\n};');
  } catch (_) { return null; }
  return matches[0][1];
}

function hunterUpdateUtf8Bytes(value) {
  const raw = unescape(encodeURIComponent(value));
  const bytes = new Array(raw.length);
  for (let i = 0; i < raw.length; i++) bytes[i] = raw.charCodeAt(i);
  return bytes;
}

// SHA-256 over exact bytes; no WebView, remote library or platform crypto API.
function hunterUpdateSha256(bytes) {
  const constants = [
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
  ];
  const hash = [0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19];
  const length = bytes.length, padded = Math.ceil((length + 9) / 64) * 64;
  const words = new Array(64);
  const rotate = (value, count) => value >>> count | value << (32 - count);
  const high = Math.floor(length / 0x20000000), low = length * 8 >>> 0;
  const byteAt = index => index < length ? bytes[index] : index === length ? 128
    : index >= padded - 8 ? ((index < padded - 4 ? high : low) >>> ((padded - 1 - index) % 4 * 8)) & 255 : 0;
  for (let offset = 0; offset < padded; offset += 64) {
    for (let i = 0; i < 16; i++) {
      const index = offset + i * 4;
      words[i] = byteAt(index) << 24 | byteAt(index + 1) << 16 | byteAt(index + 2) << 8 | byteAt(index + 3);
    }
    for (let i = 16; i < 64; i++) {
      const x = words[i - 15], y = words[i - 2];
      words[i] = (words[i - 16] + (rotate(x,7) ^ rotate(x,18) ^ x >>> 3) + words[i - 7] + (rotate(y,17) ^ rotate(y,19) ^ y >>> 10)) | 0;
    }
    let [a,b,c,d,e,f,g,h] = hash;
    for (let i = 0; i < 64; i++) {
      const t1 = (h + (rotate(e,6) ^ rotate(e,11) ^ rotate(e,25)) + (e & f ^ ~e & g) + constants[i] + words[i]) | 0;
      const t2 = ((rotate(a,2) ^ rotate(a,13) ^ rotate(a,22)) + (a & b ^ a & c ^ b & c)) | 0;
      h=g; g=f; f=e; e=(d+t1)|0; d=c; c=b; b=a; a=(t1+t2)|0;
    }
    [a,b,c,d,e,f,g,h].forEach((value, i) => { hash[i] = (hash[i] + value) | 0; });
  }
  return hash.map(value => (value >>> 0).toString(16).padStart(8, '0')).join('');
}

function hunterUpdateHashCode(code) { return hunterUpdateSha256(hunterUpdateUtf8Bytes(code)); }

function hunterUpdateChannel() {
  try {
    const fm = FileManager.local(), file = fm.joinPath(fm.documentsDirectory(), 'uol_hunter_update_channel.json');
    if (fm.fileExists(file)) {
      const channel = JSON.parse(fm.readString(file)).channel;
      if (channel === 'beta') return 'beta';
    }
  } catch (_) { /* Missing or damaged preference keeps the stable channel. */ }
  return 'stable';
}

function hunterUpdateManifestURL(channel) {
  return 'https://raw.githubusercontent.com/leosaquetto/uol-bot/main/stories-hunter/releases/' + channel + '.json';
}

// Explicit Shortcut opt-in; normal openings never change the device channel.
function configureHunterUpdateChannel(value) {
  if (value === undefined) return;
  if (value !== 'stable' && value !== 'beta') throw new Error('invalid_update_channel');
  const fm = FileManager.local(), file = fm.joinPath(fm.documentsDirectory(), 'uol_hunter_update_channel.json');
  const data = JSON.stringify({channel:value});
  fm.writeString(file, data);
  if (fm.readString(file) !== data) throw new Error('update_channel_unverified');
}

function hunterValidateUpdateManifest(value, channel) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return { status: 'invalid_manifest' };
  if (value.schema !== 1 || value.updaterProtocol !== HUNTER_UPDATE_PROTOCOL) return { status: 'incompatible' };
  if (!['stable', 'beta'].includes(channel) || value.channel !== channel || !hunterVersionParts(value.version) ||
      typeof value.artifact !== 'string' || !/^https:\/\/raw\.githubusercontent\.com\/leosaquetto\/uol-bot\/[a-f0-9]{40}\/(?:stories-hunter\/dist\/Hunter\.js|scriptable_uol_stories_hunter\.js)$/.test(value.artifact) ||
      !Number.isSafeInteger(value.size) || value.size < 1 || value.size > HUNTER_UPDATE_MAX_BYTES ||
      typeof value.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(value.sha256) ||
      typeof value.publishedAt !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value.publishedAt) ||
      !Number.isFinite(Date.parse(value.publishedAt)) ||
      new Date(value.publishedAt).toISOString() !== (value.publishedAt.length === 20 ? value.publishedAt.replace('Z', '.000Z') : value.publishedAt)) return { status: 'invalid_manifest' };
  return { status: 'valid', manifest: { schema: 1, updaterProtocol: HUNTER_UPDATE_PROTOCOL,
    version: value.version, channel, artifact: value.artifact, size: value.size, sha256: value.sha256, publishedAt: value.publishedAt } };
}

async function hunterFetchUpdateBytes(url, maxBytes, manifestRequest = false) {
  let data, request;
  try {
    request = new Request(url); request.timeoutInterval = 20; request.onRedirect = () => null;
    request.headers = { Accept: manifestRequest ? 'application/json, text/plain' : 'text/plain, application/javascript', 'Cache-Control': 'no-cache' };
    data = await request.load();
  } catch (_) { return { status: 'network_error' }; }
  const response = request.response || {};
  const header = name => Object.entries(response.headers || {}).find(([key]) => key.toLowerCase() === name)?.[1] || '';
  const type = header('content-type') || response.mimeType || '';
  const contentType = manifestRequest ? /^(?:application\/json|text\/plain)(?:;|$)/i : /^(?:text\/(?:plain|javascript)|application\/(?:javascript|x-javascript))(?:;|$)/i;
  const invalid = manifestRequest ? 'invalid_manifest' : 'invalid_artifact';
  if (response.statusCode >= 500 || response.statusCode === 429) return { status: 'network_error' };
  if (response.statusCode !== 200 || response.url !== url || !contentType.test(type)) return { status: invalid, reason: 'response_invalid' };
  const declaredLength = Number(header('content-length'));
  if (Number.isFinite(declaredLength) && declaredLength > maxBytes) return { status: invalid, reason: 'size_limit' };
  try {
    // Scriptable has no documented streamed cancellation; this is a post-download bound.
    const bytes = data.getBytes();
    if (!Array.isArray(bytes) || bytes.length > maxBytes || bytes.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)) return { status: invalid, reason: 'size_limit' };
    const code = data.toRawString();
    if (typeof code !== 'string') return { status: invalid, reason: 'invalid_utf8' };
    const encoded = hunterUpdateUtf8Bytes(code);
    if (encoded.length !== bytes.length || encoded.some((byte, index) => byte !== bytes[index])) return { status: invalid, reason: 'invalid_utf8' };
    return { status: 'downloaded', bytes, code };
  } catch (_) { return { status: invalid, reason: 'invalid_utf8' }; }
}

async function checkHunterUpdate() {
  const channel = hunterUpdateChannel();
  const response = await hunterFetchUpdateBytes(hunterUpdateManifestURL(channel), HUNTER_UPDATE_MANIFEST_MAX_BYTES, true);
  if (response.status !== 'downloaded') return { ...response, channel };
  let value;
  try { value = JSON.parse(response.code); } catch (_) { return { status: 'invalid_manifest', channel }; }
  const checked = hunterValidateUpdateManifest(value, channel);
  if (checked.status !== 'valid') return { ...checked, channel };
  if (!hunterVersionParts(SCRIPT_VERSION)) return { status: 'incompatible', channel };
  return { status: isNewerVersion(checked.manifest.version, SCRIPT_VERSION) ? 'available' : 'up_to_date',
    reason: checked.manifest.version === SCRIPT_VERSION ? 'same_version' : isNewerVersion(SCRIPT_VERSION, checked.manifest.version) ? 'older_version' : undefined,
    channel, manifest: checked.manifest };
}

async function hunterDownloadUpdate(manifest) {
  const checked = hunterValidateUpdateManifest(manifest, manifest && manifest.channel);
  if (checked.status !== 'valid') return checked;
  const response = await hunterFetchUpdateBytes(manifest.artifact, HUNTER_UPDATE_MAX_BYTES);
  if (response.status !== 'downloaded') return response;
  if (response.bytes.length !== manifest.size) return { status: 'invalid_artifact', reason: 'size_mismatch' };
  if (hunterUpdateSha256(response.bytes) !== manifest.sha256) return { status: 'invalid_artifact', reason: 'hash_mismatch' };
  if (hunterUpdateVersion(response.code) !== manifest.version) return { status: 'invalid_artifact', reason: 'version_or_envelope' };
  return { status: 'verified', code: response.code, manifest };
}

function hunterUpdatePaths() {
  const fm = getFileManager(), local = FileManager.local();
  const name = typeof Script !== 'undefined' && Script.name && Script.name() ? Script.name() : 'Clube UOL - Stories Hunter';
  if (!name || /[\/\\\u0000-\u001f]/.test(name)) throw new Error('script_name_invalid');
  const scriptPath = fm.joinPath(fm.documentsDirectory(), name + '.js');
  const key = hunterUpdateHashCode(scriptPath).slice(0, 16);
  return { fm, local, name, scriptPath, key, journalPath: local.joinPath(local.documentsDirectory(), 'uol_hunter_ota_' + key + '.json') };
}

function hunterReadUpdateTarget(paths) {
  if (!paths.fm.fileExists(paths.scriptPath)) return { exists: false, code: '', hash: '', version: null };
  const code = paths.fm.readString(paths.scriptPath);
  if (typeof code !== 'string') throw new Error('installed_script_unreadable');
  return { exists: true, code, hash: hunterUpdateHashCode(code), version: hunterUpdateVersion(code, false) };
}

function hunterWriteUpdateJournal(paths, journal) {
  const encoded = JSON.stringify(journal);
  paths.local.writeString(paths.journalPath, encoded);
  if (paths.local.readString(paths.journalPath) !== encoded) throw new Error('journal_unverified');
}

function hunterOwnUpdateJournal(paths, journal) {
  try { return JSON.parse(paths.local.readString(paths.journalPath)).operationId === journal.operationId; }
  catch (_) { return false; }
}

function hunterFinishUpdateJournal(paths, journal, phase) {
  if (!hunterOwnUpdateJournal(paths, journal)) return false;
  try { hunterWriteUpdateJournal(paths, { ...journal, phase }); return true; } catch (_) { return false; }
}

function hunterRestoreUpdate(paths, journal) {
  if (!hunterOwnUpdateJournal(paths, journal)) return { status: 'concurrent_change' };
  let current;
  try { current = hunterReadUpdateTarget(paths); } catch (_) { return { status: 'recovery_required' }; }
  if (current.hash === journal.newHash && current.version === journal.newVersion) {
    hunterFinishUpdateJournal(paths, journal, 'installed'); return { status: 'installed' };
  }
  if (current.hash === journal.oldHash && current.version === journal.oldVersion) {
    hunterFinishUpdateJournal(paths, journal, 'rolled_back'); return { status: 'preserved' };
  }
  // An unrelated valid installation may belong to another execution. Never undo it.
  if (current.version) { hunterFinishUpdateJournal(paths, journal, 'superseded'); return { status: 'concurrent_change' }; }
  try {
    const backup = paths.local.readString(journal.backupPath);
    if (typeof backup !== 'string' || hunterUpdateHashCode(backup) !== journal.oldHash || hunterUpdateVersion(backup, false) !== journal.oldVersion) return { status: 'recovery_required' };
    const restorePath = paths.fm.joinPath(paths.fm.documentsDirectory(), '.' + paths.name + '.restore-' + journal.operationId + '.tmp');
    paths.fm.writeString(restorePath, backup);
    if (paths.fm.readString(restorePath) !== backup || !hunterOwnUpdateJournal(paths, journal)) return { status: 'recovery_required' };
    const latest = hunterReadUpdateTarget(paths);
    if (latest.hash !== current.hash || latest.exists !== current.exists) return { status: 'concurrent_change' };
    if (typeof paths.fm.move === 'function') paths.fm.move(restorePath, paths.scriptPath);
    else paths.fm.writeString(paths.scriptPath, backup);
    if (hunterReadUpdateTarget(paths).hash !== journal.oldHash) return { status: 'recovery_required' };
    hunterFinishUpdateJournal(paths, journal, 'rolled_back');
    return { status: 'restored' };
  } catch (_) { return { status: 'recovery_required' }; }
}

async function recoverHunterUpdate() {
  let paths;
  try {
    paths = hunterUpdatePaths();
    if (!paths.local.fileExists(paths.journalPath)) return { status: 'no_recovery' };
    const journal = JSON.parse(paths.local.readString(paths.journalPath));
    const valid = journal && journal.schema === 1 && /^[a-f0-9-]{8,80}$/.test(journal.operationId) &&
      journal.scriptPath === paths.scriptPath && /^[a-f0-9]{64}$/.test(journal.oldHash) && /^[a-f0-9]{64}$/.test(journal.newHash) &&
      hunterVersionParts(journal.oldVersion) && hunterVersionParts(journal.newVersion) &&
      journal.backupPath === paths.local.joinPath(paths.local.documentsDirectory(), 'uol_hunter_ota_backup-' + paths.key + '-' + journal.operationId + '.js');
    if (!valid) return { status: 'recovery_required' };
    if (journal.phase !== 'replacing') return { status: 'no_recovery' };
    if (paths.fm.fileExists(paths.scriptPath) && paths.fm.isFileDownloaded && !paths.fm.isFileDownloaded(paths.scriptPath)) await paths.fm.downloadFileFromiCloud(paths.scriptPath);
    return hunterRestoreUpdate(paths, journal);
  } catch (_) { return { status: 'recovery_required' }; }
}

function hunterInstallUpdate(paths, previous, downloaded) {
  const manifest = downloaded.manifest;
  const operationId = typeof UUID !== 'undefined' ? UUID.string().toLowerCase() : Date.now().toString(16) + '-' + Math.random().toString(16).slice(2);
  const backupPath = paths.local.joinPath(paths.local.documentsDirectory(), 'uol_hunter_ota_backup-' + paths.key + '-' + operationId + '.js');
  const stagingPath = paths.fm.joinPath(paths.fm.documentsDirectory(), '.' + paths.name + '.ota-' + operationId + '.tmp');
  const journal = { schema: 1, operationId, phase: 'prepared', scriptPath: paths.scriptPath, backupPath,
    oldVersion: previous.version, oldHash: previous.hash, newVersion: manifest.version, newHash: manifest.sha256 };
  let replacing = false;
  try {
    // All preparation leaves the installed file untouched. Backups have unique
    // identities so a competing execution cannot overwrite this recovery copy.
    paths.local.writeString(backupPath, previous.code);
    if (paths.local.readString(backupPath) !== previous.code) return { status: 'installation_failed', reason: 'backup_unverified' };
    paths.fm.writeString(stagingPath, downloaded.code);
    if (paths.fm.readString(stagingPath) !== downloaded.code) return { status: 'installation_failed', reason: 'staging_unverified' };
    hunterWriteUpdateJournal(paths, journal);
    if (hunterReadUpdateTarget(paths).hash !== previous.hash || !hunterOwnUpdateJournal(paths, journal)) return { status: 'concurrent_change' };
    journal.phase = 'replacing'; hunterWriteUpdateJournal(paths, journal);
    if (hunterReadUpdateTarget(paths).hash !== previous.hash || !hunterOwnUpdateJournal(paths, journal)) return { status: 'concurrent_change' };
    replacing = true;
    // Same-directory move is the smallest replacement window exposed by the API.
    // No remove(current) call; neither this check nor move is a cross-process CAS.
    // A separate process can still replace the target after the final read and
    // before this move; the API cannot condition replacement on the old hash.
    if (typeof paths.fm.move === 'function') paths.fm.move(stagingPath, paths.scriptPath);
    else paths.fm.writeString(paths.scriptPath, downloaded.code);
    const installed = hunterReadUpdateTarget(paths);
    if (installed.hash !== manifest.sha256 || installed.version !== manifest.version) throw new Error('installed_script_unverified');
    hunterFinishUpdateJournal(paths, journal, 'installed');
    return { status: 'installed', version: manifest.version };
  } catch (_) {
    if (!replacing) return { status: 'installation_failed', reason: 'preparation_failed' };
    return hunterRestoreUpdate(paths, journal);
  }
}

async function performSelfUpdate(isManual = false, unattended = false) {
  async function notice(title, message) {
    if (!isManual) return;
    try { const alert = new Alert(); alert.title = title; alert.message = message; alert.addAction('OK'); await alert.presentAlert(); } catch (_) {}
  }
  if (hunterUpdateInFlight) { hunterLastUpdateResult = { status: 'busy' }; return false; }
  hunterUpdateInFlight = true;
  try {
    const recovery = await recoverHunterUpdate();
    if (recovery.status === 'recovery_required' || recovery.status === 'concurrent_change') {
      hunterLastUpdateResult = recovery;
      await notice('Atualização não confirmada', 'Existe uma atualização anterior sem conclusão confirmada. A cópia de recuperação foi preservada.');
      return false;
    }
    const checked = await checkHunterUpdate(); hunterLastUpdateResult = checked;
    if (checked.status !== 'available') {
      await notice(checked.status === 'up_to_date' ? 'Versão instalada preservada' : 'Atualização não confirmada',
        checked.status === 'up_to_date' ? 'Nenhuma versão mais recente está disponível neste canal.'
        : checked.status === 'incompatible' ? 'A publicação exige um atualizador incompatível. A versão instalada foi preservada.'
        : checked.status === 'network_error' ? 'Não foi possível consultar a publicação agora. A versão instalada foi preservada.'
        : 'Os metadados oficiais não puderam ser validados. A versão instalada foi preservada.');
      return false;
    }
    const paths = hunterUpdatePaths();
    if (paths.fm.fileExists(paths.scriptPath) && paths.fm.isFileDownloaded && !paths.fm.isFileDownloaded(paths.scriptPath)) await paths.fm.downloadFileFromiCloud(paths.scriptPath);
    const previous = hunterReadUpdateTarget(paths);
    if (previous.version !== SCRIPT_VERSION) { hunterLastUpdateResult = { status: 'concurrent_change' }; return false; }
    const downloaded = await hunterDownloadUpdate(checked.manifest); hunterLastUpdateResult = downloaded.status === 'verified' ? { status: 'available', manifest: checked.manifest } : downloaded;
    if (downloaded.status !== 'verified') {
      await notice('Atualização não confirmada', 'O download não passou pela verificação de tamanho, integridade e versão. A versão instalada foi preservada.');
      return false;
    }
    if (!unattended) {
      const confirm = new Alert(); confirm.title = 'Atualização disponível';
      confirm.message = 'Instalar a versão ' + checked.manifest.version + '? Sessões, vouchers e histórico serão preservados.';
      confirm.addAction('Instalar atualização'); confirm.addCancelAction('Cancelar');
      if (await confirm.presentAlert() < 0) { hunterLastUpdateResult = { status: 'cancelled' }; return false; }
    }
    const installed = hunterInstallUpdate(paths, previous, downloaded); hunterLastUpdateResult = installed;
    if (installed.status !== 'installed') {
      await notice('Atualização interrompida', installed.status === 'restored' || installed.status === 'preserved'
        ? 'A instalação falhou e a versão anterior está preservada. Seus dados não foram alterados.'
        : installed.status === 'recovery_required' ? 'Não foi possível confirmar a restauração. O backup local e o registro de recuperação foram preservados.'
        : 'A versão instalada não foi substituída por esta atualização. A cópia de recuperação foi preservada.');
      return false;
    }
    await notice('Script atualizado', 'Versão ' + checked.manifest.version + ' instalada. Toque em OK para abrir; esta primeira abertura não inicia coleta.');
    if (unattended) return true; // Next user opening keeps the existing first-version collection gate.
    try { Safari.open('scriptable:///run?scriptName=' + encodeURIComponent(paths.name)); }
    catch (_) { await notice('Atualização salva', 'Abra o Stories Hunter novamente para usar a nova versão.'); }
    return true;
  } catch (_) {
    hunterLastUpdateResult = { status: 'installation_failed' };
    await notice('Atualização não confirmada', 'Não foi possível concluir a atualização. Nenhum sucesso foi confirmado; seus dados foram preservados.');
    return false;
  } finally { hunterUpdateInFlight = false; }
}

// Called only after the dashboard has loaded. It never downloads executable
// code, starts collection, installs a version, or delays UI presentation.
async function discoverHunterUpdate(view) {
  const result = await checkHunterUpdate();
  const safe = JSON.stringify({ status: result.status, channel: result.channel, version: result.manifest && result.manifest.version }).replace(/</g, '\\u003c');
  try {
    await view.evaluateJavaScript('(() => { const result = ' + safe + '; '
      + 'if (typeof handleNativeUpdateDiscovery === "function") { handleNativeUpdateDiscovery(result); } '
      + 'else if (result.status === "available") { '
      + 'const banner = document.getElementById("otaUpdateBanner"); '
      + 'const label = document.getElementById("otaVersionText"); '
      + 'if (label) label.textContent = "Versão " + result.version + " disponível"; '
      + 'if (banner) banner.style.display = "flex"; } })();');
  } catch (_) { /* A closed WebView cannot affect discovery or installation. */ }
  return result;
}
