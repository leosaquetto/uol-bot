const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const {execFileSync} = require('node:child_process');
const vm = require('node:vm');
const ROOT = path.resolve(__dirname, '..');
const ARTIFACT = 'stories-hunter/dist/Hunter.js';
const PREFIX = 'https://raw.githubusercontent.com/leosaquetto/uol-bot/';
const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');

function inspect(bytes) {
  const code = bytes.toString('utf8');
  if (!Buffer.from(code).equals(bytes) || bytes.length > 2097152 ||
      !code.startsWith('// Variables used by Scriptable.\n// These must be at the very top of the file.') ||
      !code.endsWith('await main();\nScript.complete();\n')) throw Error('Invalid Hunter artifact');
  const versions = [...code.matchAll(/^const SCRIPT_VERSION = "((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))";$/gm)];
  if (versions.length !== 1 || !versions[0][1].split('.').every(n => Number.isSafeInteger(Number(n)))) throw Error('Invalid artifact version');
  for (const name of ['BUNDLED_COOKIES_LEO', 'BUNDLED_COOKIES_GAORIEL', 'BUNDLED_REDEMPTIONS']) {
    const declarations = [...code.matchAll(new RegExp('^const ' + name + ' = (.*);$', 'gm'))];
    if (declarations.length !== 1 || declarations[0][1] !== '[]') throw Error('Private artifact rejected');
  }
  for (const pattern of [/^async function main\(\) \{/m, /^function buildDashboardHTML\(/m,
    /^function hunterCollectionPausedForUpdate\(action\) \{/m, /^const HunterCollector = \(\(\) => \{/m])
    if (!pattern.test(code)) throw Error('Incomplete artifact');
  new vm.Script('(async function(){\n' + code + '\n})'); // compile only, never run
  return {version: versions[0][1], size: bytes.length, sha256: hash(bytes)};
}

function createManifest({bytes, commit, channel, publishedAt = new Date().toISOString()}) {
  if (!['stable','beta'].includes(channel) || !/^[a-f0-9]{40}$/.test(commit)) throw Error('Invalid release channel/commit');
  if (new Date(publishedAt).toISOString() !== publishedAt) throw Error('Invalid publication timestamp');
  return {schema:1, updaterProtocol:1, product:'clube-uol-premium', scriptName:'Clube UOL Premium', ...inspect(bytes), channel,
    artifact:PREFIX + commit + '/' + ARTIFACT, publishedAt};
}

function readCommitted(commit) {
  if (!/^[a-f0-9]{40}$/.test(commit)) throw Error('Invalid commit');
  return execFileSync('git', ['show', commit + ':' + ARTIFACT], {cwd:ROOT, maxBuffer:4*1024*1024});
}

function verifyManifest(manifest, bytes) {
  if (!manifest || manifest.schema !== 1 || manifest.updaterProtocol !== 1 || !['stable','beta'].includes(manifest.channel)) throw Error('Invalid manifest');
  const match = String(manifest.artifact).match(/^https:\/\/raw\.githubusercontent\.com\/leosaquetto\/uol-bot\/([a-f0-9]{40})\/stories-hunter\/dist\/Hunter\.js$/);
  if (!match) throw Error('Artifact URL is not immutable');
  const actual = inspect(bytes);
  if (actual.version !== manifest.version || actual.size !== manifest.size || actual.sha256 !== manifest.sha256) throw Error('Release integrity mismatch');
  return match[1];
}

function atomicWrite(file, contents) {
  fs.mkdirSync(path.dirname(file), {recursive:true});
  const temporary = file + '.' + process.pid + '.tmp';
  try { fs.writeFileSync(temporary, contents); fs.renameSync(temporary, file); }
  finally { if (fs.existsSync(temporary)) fs.unlinkSync(temporary); }
}

function prepare({commit, channel='beta', publishedAt, root=ROOT, read=readCommitted}) {
  const bytes = read(commit);
  if (!bytes.equals(fs.readFileSync(path.join(root, ARTIFACT)))) throw Error('Working artifact differs from committed artifact');
  const manifest = createManifest({bytes, commit, channel, publishedAt});
  atomicWrite(path.join(root,'stories-hunter/releases',channel+'.json'), JSON.stringify(manifest,null,2)+'\n');
  if (channel === 'stable') atomicWrite(path.join(root,'scriptable_uol_stories_hunter.js'), bytes);
  return manifest;
}

function promote({root=ROOT, read=readCommitted, publishedAt=new Date().toISOString()}={}) {
  const beta = JSON.parse(fs.readFileSync(path.join(root,'stories-hunter/releases/beta.json'),'utf8'));
  if (beta.channel !== 'beta') throw Error('Expected beta manifest');
  const match=String(beta.artifact).match(/^https:\/\/raw\.githubusercontent\.com\/leosaquetto\/uol-bot\/([a-f0-9]{40})\/stories-hunter\/dist\/Hunter\.js$/);
  if (!match) throw Error('Invalid beta URL');
  const bytes=read(match[1]); verifyManifest(beta,bytes);
  const stable=createManifest({bytes,commit:match[1],channel:'stable',publishedAt});
  // Both writes become visible together in the subsequent publication commit.
  // No rebuild: exactly the already-verified beta artifact is promoted.
  atomicWrite(path.join(root,'scriptable_uol_stories_hunter.js'),bytes);
  atomicWrite(path.join(root,'stories-hunter/releases/stable.json'),JSON.stringify(stable,null,2)+'\n');
  return stable;
}

if (require.main === module) {
  const [command,...args]=process.argv.slice(2), options={};
  if (args.length % 2) throw Error('Expected --key value pairs');
  for(let i=0;i<args.length;i+=2) {
    if (!['--commit','--channel','--published-at'].includes(args[i]) || options[args[i]] !== undefined) throw Error('Unknown/repeated option');
    options[args[i]]=args[i+1];
  }
  if (command === 'promote' && (options['--commit'] || options['--channel'])) throw Error('Promotion uses verified beta metadata');
  const result=command==='prepare' ? prepare({commit:options['--commit'],channel:options['--channel'],publishedAt:options['--published-at']})
    : command==='promote' ? promote({publishedAt:options['--published-at']}) : (()=>{throw Error('Use prepare or promote');})();
  console.log(JSON.stringify(result,null,2));
}
module.exports={inspect,createManifest,verifyManifest,prepare,promote};
