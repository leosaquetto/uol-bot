// Code-only OTA for saquettoOS and Clube UOL Premium. No module side effects.
const SaquettoUpdater = (() => {
  const BASE = 'https://raw.githubusercontent.com/leosaquetto/uol-bot/';
  const SOURCES = Object.freeze({
    saquettoos: Object.freeze({ scriptName: 'saquettoOS', manifestBase: BASE + 'main/saquettoos/releases/', artifactPath: 'saquettoos/dist/saquettoOS.js', envelope: 'saquettoos', maxBytes: 8 * 1024 * 1024 }),
    'clube-uol': Object.freeze({ scriptName: 'Clube UOL Premium', manifestBase: BASE + 'main/stories-hunter/releases/', artifactPath: 'stories-hunter/dist/Hunter.js', envelope: 'uol', maxBytes: 2 * 1024 * 1024 })
  });
  const versionParts = value => typeof value === 'string' && /^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$/.test(value) && value.split('.').map(Number).every(Number.isSafeInteger) ? value.split('.').map(Number) : null;
  const newer = (a, b) => { const x = versionParts(a), y = versionParts(b); for (let i = 0; i < 3; i++) { if (x[i] !== y[i]) return x[i] > y[i]; } return false; };
  const utf8 = text => Array.from(unescape(encodeURIComponent(text)), c => c.charCodeAt(0));
  // SHA-256 adapted from stories-hunter/native/updater.js (same repository).
  function sha256(bytes) {
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
  const hashCode = code => sha256(utf8(code));
  function source(app) {
    if (!app || !Object.prototype.hasOwnProperty.call(SOURCES, app.id)) return { status: 'not_configured' };
    const known = SOURCES[app.id];
    if (['scriptName', 'manifestBase', 'artifactPath', 'envelope'].some(key => app[key] !== known[key])) return { status: 'invalid_source' };
    return { ...known, id: app.id, status: 'configured' };
  }
  function codeVersion(code, spec, bounded = false) {
    if (typeof code !== 'string') return null;
    try {
      if (bounded && utf8(code).length > spec.maxBytes) return null;
      let matches;
      if (spec.envelope === 'saquettoos') {
        if (!/^\/\/ saquettoOS Scriptable launcher\r?$/m.test(code) || !/^async function saquettoMain\(\)\s*\{/m.test(code) ||
            !/\nawait saquettoMain\(\);\r?\nScript.complete\(\);\s*$/.test(code)) return null;
        matches = [...code.matchAll(/^const SAQUETTO_VERSION = ['"]([^'"]+)['"];\r?$/gm)];
      } else {
        // Mirrors the UOL envelope. Private configuration stays only in the local backup.
        if (!code.startsWith('// Variables used by Scriptable.\n// These must be at the very top of the file.') ||
            !/\/\/ Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter\r?\n/.test(code) ||
            !/\nawait main\(\);\r?\nScript.complete\(\);\s*$/.test(code) ||
            !/^async function main\(\) \{/m.test(code) || !/^function buildDashboardHTML\(/m.test(code) ||
            !/^function hunterCollectionPausedForUpdate\(action\) \{/m.test(code) || !/^const HunterCollector = \(\(\) => \{/m.test(code)) return null;
        matches = [...code.matchAll(/^const SCRIPT_VERSION = "([^"]+)";\r?$/gm)];
      }
      if (matches.length !== 1 || !versionParts(matches[0][1])) return null;
      // Parse, never execute the candidate, including during recovery.
      const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
      new AsyncFunction(code);
      return matches[0][1];
    } catch (_) { return null; }
  }
  function manifest(value, spec, channel) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return { status: 'invalid_manifest' };
    if (value.schema !== 1 || value.updaterProtocol !== 1) return { status: 'incompatible' };
    const suffix = typeof value.artifact === 'string' && value.artifact.startsWith(BASE) ? value.artifact.slice(BASE.length) : '';
    const commit = suffix.slice(0, 40);
    if (value.channel !== channel || !versionParts(value.version) || !/^[a-f0-9]{40}$/.test(commit) || suffix !== commit + '/' + spec.artifactPath ||
        !Number.isSafeInteger(value.size) || value.size < 1 || value.size > spec.maxBytes || !/^[a-f0-9]{64}$/.test(value.sha256 || '') ||
        typeof value.publishedAt !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value.publishedAt) ||
        !Number.isFinite(Date.parse(value.publishedAt)) || new Date(value.publishedAt).toISOString() !== (value.publishedAt.length === 20 ? value.publishedAt.replace('Z', '.000Z') : value.publishedAt)) return { status: 'invalid_manifest' };
    return { status: 'valid', manifest: { schema: 1, updaterProtocol: 1, version: value.version, channel, artifact: value.artifact, size: value.size, sha256: value.sha256, publishedAt: value.publishedAt } };
  }
  function create({ fm, local, request, uuid, scriptName }) {
    const busy = new Set();
    function paths(spec) {
      // Never derive another app's destination from the running Script.name().
      if (spec.id === 'saquettoos' && scriptName && scriptName.normalize('NFC') !== spec.scriptName) throw new Error('script_name_mismatch');
      const names = fm.listContents(fm.documentsDirectory()).filter(name => name.normalize('NFC') === spec.scriptName + '.js');
      if (names.length > 1) throw new Error('ambiguous_installation');
      const scriptPath = fm.joinPath(fm.documentsDirectory(), names[0] || spec.scriptName + '.js');
      if (fm.fileExists(scriptPath) && fm.isDirectory && fm.isDirectory(scriptPath)) throw new Error('invalid_target');
      const key = hashCode(spec.id + ':' + scriptPath).slice(0, 16);
      return { scriptPath, key, journalPath: local.joinPath(local.documentsDirectory(), 'saquettoos_ota_' + key + '.json') };
    }
    async function downloadLocal(path) {
      if (fm.fileExists(path) && fm.isFileDownloaded && !fm.isFileDownloaded(path)) await fm.downloadFileFromiCloud(path);
    }
    function read(spec, p) {
      // Re-resolve on every read: detect newly introduced NFC duplicates too.
      if (paths(spec).scriptPath !== p.scriptPath) throw new Error('concurrent_change');
      if (!fm.fileExists(p.scriptPath)) return { exists: false, code: '', hash: '', version: null };
      const code = fm.readString(p.scriptPath);
      if (typeof code !== 'string') throw new Error('installed_script_unreadable');
      return { exists: true, code, hash: hashCode(code), version: codeVersion(code, spec) };
    }
    function failure(error, fallback = 'unreadable') {
      return { status: ['ambiguous_installation', 'script_name_mismatch', 'invalid_target', 'concurrent_change'].includes(error.message) ? error.message : fallback };
    }
    async function inspect(app) {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      try {
        const p = paths(spec); await downloadLocal(p.scriptPath); const current = read(spec, p);
        return { status: !current.exists ? 'not_installed' : current.version ? 'installed' : 'unrecognized', version: current.version, scriptName: spec.scriptName };
      } catch (error) { return failure(error); }
    }
    async function fetchBytes(url, maxBytes, isManifest) {
      const invalid = isManifest ? 'invalid_manifest' : 'invalid_artifact';
      let req, data;
      try {
        req = request(url); req.timeoutInterval = 20; req.onRedirect = () => null;
        req.headers = { Accept: isManifest ? 'application/json, text/plain' : 'text/plain, application/javascript', 'Cache-Control': 'no-cache' };
        data = await req.load();
      } catch (_) { return { status: 'network_error' }; }
      const response = req.response || {}, header = name => Object.entries(response.headers || {}).find(([key]) => key.toLowerCase() === name)?.[1] || '';
      const type = header('content-type') || response.mimeType || '';
      const accepted = isManifest ? /^(?:application\/json|text\/plain)(?:;|$)/i : /^(?:text\/(?:plain|javascript)|application\/(?:javascript|x-javascript))(?:;|$)/i;
      if (response.statusCode >= 500 || response.statusCode === 429) return { status: 'network_error' };
      if (response.statusCode !== 200 || response.url !== url || !accepted.test(type)) return { status: invalid, reason: 'response_invalid' };
      if (Number(header('content-length')) > maxBytes) return { status: invalid, reason: 'size_limit' };
      try {
        // Scriptable exposes no streamed cancellation: the byte cap is post-download.
        const bytes = data.getBytes(), code = data.toRawString();
        if (!Array.isArray(bytes) || bytes.length > maxBytes || bytes.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)) return { status: invalid, reason: 'size_limit' };
        if (typeof code !== 'string') return { status: invalid, reason: 'invalid_utf8' };
        const encoded = utf8(code);
        if (encoded.length !== bytes.length || encoded.some((byte, index) => byte !== bytes[index])) return { status: invalid, reason: 'invalid_utf8' };
        return { status: 'downloaded', code, bytes };
      } catch (_) { return { status: invalid, reason: 'invalid_utf8' }; }
    }
    async function check(app, channel = 'stable') {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      if (!['stable', 'beta'].includes(channel)) return { status: 'invalid_channel' };
      const current = await inspect(app); if (current.status !== 'installed') return current;
      const response = await fetchBytes(spec.manifestBase + channel + '.json', 8192, true);
      if (response.status !== 'downloaded') return { ...response, version: current.version, channel };
      let value; try { value = JSON.parse(response.code); } catch (_) { return { status: 'invalid_manifest', version: current.version, channel }; }
      const checked = manifest(value, spec, channel);
      if (checked.status !== 'valid') return { ...checked, version: current.version, channel };
      return { status: newer(checked.manifest.version, current.version) ? 'available' : 'up_to_date', version: current.version, latestVersion: checked.manifest.version, manifest: checked.manifest, channel };
    }
    const backupPath = (p, operationId) => local.joinPath(local.documentsDirectory(), 'saquettoos_ota_backup-' + p.key + '-' + operationId + '.js');
    function journalWrite(p, journal) { const encoded = JSON.stringify(journal); local.writeString(p.journalPath, encoded); if (local.readString(p.journalPath) !== encoded) throw new Error('journal_unverified'); }
    function own(p, journal) { try { return JSON.parse(local.readString(p.journalPath)).operationId === journal.operationId; } catch (_) { return false; } }
    function finish(p, journal, phase) { if (!own(p, journal)) return false; try { journalWrite(p, { ...journal, phase }); return true; } catch (_) { return false; } }
    function validJournal(j, p, spec) {
      return j && j.schema === 1 && j.appId === spec.id && typeof j.operationId === 'string' && /^[a-f0-9-]{8,80}$/.test(j.operationId) &&
        j.scriptPath === p.scriptPath && j.backupPath === backupPath(p, j.operationId) && /^[a-f0-9]{64}$/.test(j.oldHash) && /^[a-f0-9]{64}$/.test(j.newHash) &&
        versionParts(j.oldVersion) && versionParts(j.newVersion) && ['prepared', 'replacing', 'installed', 'rolled_back', 'superseded'].includes(j.phase);
    }
    function restore(spec, p, journal) {
      if (!own(p, journal)) return { status: 'concurrent_change' };
      try {
        const current = read(spec, p);
        if (current.hash === journal.newHash && current.version === journal.newVersion) {
          if (!finish(p, journal, 'installed')) return { status: 'recovery_required' };
          return { status: 'installed', version: current.version };
        }
        if (current.hash === journal.oldHash && current.version === journal.oldVersion) {
          if (!finish(p, journal, 'rolled_back')) return { status: 'recovery_required' };
          return { status: 'rolled_back', version: current.version };
        }
        // Never overwrite an unrelated file, including one with an unknown envelope.
        if (current.exists) return { status: current.version ? 'concurrent_change' : 'recovery_required' };
        const backup = local.readString(journal.backupPath);
        if (typeof backup !== 'string' || hashCode(backup) !== journal.oldHash || codeVersion(backup, spec) !== journal.oldVersion) return { status: 'recovery_required' };
        const staging = fm.joinPath(fm.documentsDirectory(), '.' + spec.scriptName + '.restore-' + journal.operationId + '.tmp');
        fm.writeString(staging, backup);
        if (fm.readString(staging) !== backup || !own(p, journal)) return { status: 'recovery_required' };
        if (read(spec, p).exists) return { status: 'concurrent_change' };
        fm.move(staging, p.scriptPath);
        const restored = read(spec, p);
        if (restored.hash !== journal.oldHash || restored.version !== journal.oldVersion || !finish(p, journal, 'rolled_back')) return { status: 'recovery_required' };
        return { status: 'rolled_back', version: restored.version };
      } catch (error) { return failure(error, 'recovery_required'); }
    }
    async function recoverInternal(app) {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      try {
        const p = paths(spec); if (!local.fileExists(p.journalPath)) return { status: 'no_recovery' };
        const journal = JSON.parse(local.readString(p.journalPath));
        if (!validJournal(journal, p, spec)) return { status: 'recovery_required' };
        if (!['prepared', 'replacing'].includes(journal.phase)) return { status: 'no_recovery' };
        await downloadLocal(p.scriptPath);
        return restore(spec, p, journal);
      } catch (error) { return failure(error, 'recovery_required'); }
    }
    async function recover(app) {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      if (busy.has(spec.id)) return { status: 'busy' }; busy.add(spec.id);
      try { return await recoverInternal(app); } finally { busy.delete(spec.id); }
    }
    async function install(app, channel = 'stable') {
      const spec = source(app); if (spec.status !== 'configured') return spec;
      if (!['stable', 'beta'].includes(channel)) return { status: 'invalid_channel' };
      if (busy.has(spec.id)) return { status: 'busy' }; busy.add(spec.id);
      try {
        const recovery = await recoverInternal(app);
        if (!['no_recovery', 'rolled_back', 'installed'].includes(recovery.status)) return recovery;
        const checked = await check(app, channel); if (checked.status !== 'available') return checked;
        const p = paths(spec), previous = read(spec, p);
        if (!previous.version || previous.version !== checked.version) return { status: 'concurrent_change' };
        const downloaded = await fetchBytes(checked.manifest.artifact, spec.maxBytes, false);
        if (downloaded.status !== 'downloaded') return downloaded;
        if (downloaded.bytes.length !== checked.manifest.size) return { status: 'invalid_artifact', reason: 'size_mismatch' };
        if (sha256(downloaded.bytes) !== checked.manifest.sha256) return { status: 'invalid_artifact', reason: 'hash_mismatch' };
        if (codeVersion(downloaded.code, spec, true) !== checked.manifest.version) return { status: 'invalid_artifact', reason: 'version_or_envelope' };
        const operationId = String(uuid()).toLowerCase();
        if (!/^[a-f0-9-]{8,80}$/.test(operationId)) return { status: 'installation_failed', reason: 'invalid_operation_id' };
        const backup = backupPath(p, operationId), staging = fm.joinPath(fm.documentsDirectory(), '.' + spec.scriptName + '.ota-' + operationId + '.tmp');
        if (local.fileExists(backup) || fm.fileExists(staging)) return { status: 'concurrent_change' };
        const journal = { schema: 1, appId: spec.id, operationId, phase: 'prepared', scriptPath: p.scriptPath, backupPath: backup, oldVersion: previous.version, oldHash: previous.hash, newVersion: checked.manifest.version, newHash: checked.manifest.sha256 };
        let replacing = false;
        try {
          local.writeString(backup, previous.code);
          if (local.readString(backup) !== previous.code) return { status: 'installation_failed', reason: 'backup_unverified' };
          fm.writeString(staging, downloaded.code);
          if (fm.readString(staging) !== downloaded.code) return { status: 'installation_failed', reason: 'staging_unverified' };
          // A different operation may have become active while the download awaited.
          if (local.fileExists(p.journalPath)) {
            const prior = JSON.parse(local.readString(p.journalPath));
            if (!validJournal(prior, p, spec) || ['prepared', 'replacing'].includes(prior.phase)) return { status: 'concurrent_change' };
          }
          if (read(spec, p).hash !== previous.hash) return { status: 'concurrent_change' };
          journalWrite(p, journal);
          if (read(spec, p).hash !== previous.hash || !own(p, journal)) return { status: 'concurrent_change' };
          journal.phase = 'replacing'; journalWrite(p, journal);
          if (read(spec, p).hash !== previous.hash || !own(p, journal)) return { status: 'concurrent_change' };
          replacing = true;
          // Same-directory move replaces the target. Scriptable documents no CAS or
          // atomicity guarantee; another process can still race after this last read.
          fm.move(staging, p.scriptPath);
          const installed = read(spec, p);
          if (installed.hash !== journal.newHash || installed.version !== journal.newVersion) throw new Error('readback_unverified');
          if (!finish(p, journal, 'installed')) return { status: 'recovery_required' };
          return { status: 'installed', version: installed.version, latestVersion: installed.version };
        } catch (error) { return replacing ? restore(spec, p, journal) : failure(error, 'installation_failed'); }
      } catch (error) { return failure(error, 'installation_failed'); }
      finally { busy.delete(spec.id); }
    }
    return Object.freeze({ check, install, recover, inspect });
  }
  return Object.freeze({ create, fingerprint: hashCode });
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoUpdater;
