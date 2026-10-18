const {test}=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const {assembleHunter}=require('./assemble.cjs');
const {inspect,createManifest,prepare,promote}=require('./release.cjs');
const bytes=Buffer.from(assembleHunter()),commit='a'.repeat(40),publishedAt='2026-10-18T12:00:00.000Z';

test('public build is reproducible and uses only repository inputs',()=>{
  const read=fs.readFileSync;
  try {
    fs.readFileSync=function(file,...rest){assert.ok(path.resolve(file).startsWith(path.resolve(__dirname,'..')+path.sep),'External build input: '+file);return read.call(this,file,...rest);};
    assert.equal(assembleHunter(),bytes.toString()); assert.equal(assembleHunter(),bytes.toString());
  } finally {fs.readFileSync=read;}
  assert.deepEqual(fs.readFileSync(path.join(__dirname,'dist/Hunter.js')),bytes,'Committed artifact matches sources');
});

test('release rejects private, truncated, malformed, uncompiled or ambiguous artifacts',()=>{
  assert.equal(inspect(bytes).size,bytes.length);
  for(const code of [bytes.toString().slice(0,-30),bytes.toString().replace('const BUNDLED_COOKIES_LEO = [];','const BUNDLED_COOKIES_LEO = ["synthetic"];'),
    bytes.toString().replace('await main();','invalid syntax $$$\nawait main();')]) assert.throws(()=>inspect(Buffer.from(code)));
  for(const patch of [{channel:'other'},{commit:'main'},{publishedAt:'yesterday'}]) assert.throws(()=>createManifest({bytes,commit,channel:'beta',publishedAt,...patch}));
});

test('beta leaves legacy stable untouched and promotion uses exactly the beta bytes without build',()=>{
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'hunter-release-'));
  try {
    fs.mkdirSync(path.join(root,'stories-hunter/dist'),{recursive:true});fs.writeFileSync(path.join(root,'stories-hunter/dist/Hunter.js'),bytes);
    const legacy=path.join(root,'scriptable_uol_stories_hunter.js');fs.writeFileSync(legacy,'old-stable');
    const read=sha=>{assert.equal(sha,commit);return bytes;};
    const beta=prepare({root,read,commit,publishedAt});assert.equal(fs.readFileSync(legacy,'utf8'),'old-stable');
    // Working tree can change: promotion uses the immutable committed candidate.
    fs.writeFileSync(path.join(root,'stories-hunter/dist/Hunter.js'),'different-working-copy');
    const stable=promote({root,read,publishedAt});assert.equal(stable.sha256,beta.sha256);assert.equal(stable.artifact,beta.artifact);
    assert.deepEqual(fs.readFileSync(legacy),bytes);
    assert.throws(()=>prepare({root,read,commit,publishedAt}),/differs/);
    assert.throws(()=>promote({root,read:()=>Buffer.from('bad'),publishedAt}));
  } finally {fs.rmSync(root,{recursive:true,force:true});}
});
