// Explicit local installation only. Normal releases notify the native updater.
const crypto = require('node:crypto');
module.exports = function installCode({fs, path, target, code, backupDirectory}) {
  const hash = value => crypto.createHash('sha256').update(value).digest('hex');
  const directory = path.dirname(target);
  const legacy = path.join(directory, 'Clube UOL - Stories Hunter.js');
  const current = fs.existsSync(target) ? fs.readFileSync(target, 'utf8') : null;
  const old = fs.existsSync(legacy) ? fs.readFileSync(legacy, 'utf8') : null;
  if (old !== null && current !== null && old !== current)
    throw Error('Both product names exist with different content; preserve both for reconciliation');
  const previous = current ?? old;
  if (previous !== null) {
    if (!previous.startsWith('// Variables used by Scriptable.') || !previous.includes('async function main()')) throw Error('Unknown installed script');
    fs.mkdirSync(backupDirectory, {recursive:true, mode:0o700});
    const backup = path.join(backupDirectory, 'previous-' + hash(previous) + '.js');
    fs.writeFileSync(backup, previous, {mode:0o600});
    if (fs.readFileSync(backup, 'utf8') !== previous) throw Error('Backup failed');
  }
  const staging = path.join(directory, '.clube-uol-premium-' + process.pid + '.tmp');
  fs.writeFileSync(staging, code, {mode:0o600});
  if (fs.readFileSync(staging, 'utf8') !== code) throw Error('Staging failed');
  const unchanged = (file, expected) => expected === null ? !fs.existsSync(file) : fs.readFileSync(file, 'utf8') === expected;
  if (!unchanged(target,current) || !unchanged(legacy,old)) throw Error('Installation changed concurrently');
  fs.renameSync(staging,target);
  if (fs.readFileSync(target,'utf8') !== code) throw Error('Installed bytes not verified');
  if (old !== null) {
    if (!unchanged(legacy,old)) throw Error('Legacy changed concurrently; preserved');
    fs.unlinkSync(legacy); // Backed up, replaced and verified above.
  }
  return {installed:true, migrated:old !== null};
};
