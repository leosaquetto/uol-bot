const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'native/updater.js'),'utf8');
const legacy='Clube UOL - Stories Hunter',canonical='Clube UOL Premium';
const code='// Variables used by Scriptable.\n// These must be at the very top of the file.\n// Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter\nconst SCRIPT_VERSION = "5.8.1";\nconst HunterCollector = (() => {return {};})();\nfunction hunterCollectionPausedForUpdate(action) {}\nfunction buildDashboardHTML() {}\nasync function main() {}\nawait main();\nScript.complete();\n';
function harness({name=legacy,target,move='normal'}={}) {
  const old='/cloud/'+legacy+'.js',next='/cloud/'+canonical+'.js';
  const files=new Map([[old,code],['/cloud/private.json','sensitive-fixture']]);if(target!==undefined)files.set(next,target);
  const operations=[];
  const manager=root=>({documentsDirectory:()=>root,joinPath:path.join,fileExists:p=>files.has(p),readString:p=>files.get(p),
    writeString(p,v){operations.push(['write',p]);files.set(p,v);},remove(p){operations.push(['remove',p]);files.delete(p);},
    move(a,b){operations.push(['move',a,b]);if(move==='fail')throw Error('failed');files.set(b,files.get(a));files.delete(a);if(move==='lost')throw Error('lost response');}});
  const context=vm.createContext({SCRIPT_VERSION:'5.8.1',Script:{name:()=>name},getFileManager:()=>manager('/cloud'),FileManager:{local:()=>manager('/local')}});
  vm.runInContext(source,context);return {context,files,old,next,operations};
}
test('exact old product migrates with verified backup and same-run canonical lookup, without data writes',async()=>{
  const h=harness();assert.equal((await h.context.migrateHunterProductName()).status,'migrated');
  assert.equal(h.files.has(h.old),false);assert.equal(h.files.get(h.next),code);assert.equal(h.context.hunterUpdatePaths().scriptPath,h.next);
  assert.equal(h.files.get('/cloud/private.json'),'sensitive-fixture');assert.ok([...h.files].some(([p,v])=>p.startsWith('/local/uol_hunter_name_backup-')&&v===code));
  assert.equal((await h.context.migrateHunterProductName()).status,'not_needed');
});
test('lost rename response reconciles exact target without repeat move',async()=>{const h=harness({move:'lost'});assert.equal((await h.context.migrateHunterProductName()).status,'migrated');assert.equal(h.operations.filter(o=>o[0]==='move').length,1);});
test('failed move keeps old installation and private data',async()=>{const h=harness({move:'fail'});assert.equal((await h.context.migrateHunterProductName()).status,'recovery_required');assert.equal(h.files.get(h.old),code);assert.equal(h.files.has(h.next),false);});
test('different canonical target is never overwritten or removed',async()=>{const h=harness({target:'unrelated'});assert.equal((await h.context.migrateHunterProductName()).status,'name_conflict');assert.equal(h.files.get(h.old),code);assert.equal(h.files.get(h.next),'unrelated');});
test('exact duplicate old copy is removed only after backup and target verification',async()=>{const h=harness({target:code});assert.equal((await h.context.migrateHunterProductName()).status,'migrated');assert.equal(h.files.has(h.old),false);assert.equal(h.operations.filter(o=>o[0]==='move').length,0);});
test('custom names and canonical installations are not renamed',async()=>{for(const name of ['Hunter custom',canonical]){const h=harness({name});assert.equal((await h.context.migrateHunterProductName()).status,'not_needed');assert.equal(h.operations.length,0);}});
test('explicit Mac install backs up and removes only the old script; conflict preserves both',()=>{
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'premium-install-')),install=require('./install-code.cjs');
  try {
    const target=path.join(root,canonical+'.js'),old=path.join(root,legacy+'.js'),backupDirectory=path.join(root,'backup');
    fs.writeFileSync(old,code);fs.writeFileSync(path.join(root,'private.json'),'fixture');
    install({fs,path,target,code:code+'// new',backupDirectory});assert.equal(fs.existsSync(old),false);assert.equal(fs.readFileSync(target,'utf8'),code+'// new');assert.equal(fs.readFileSync(path.join(root,'private.json'),'utf8'),'fixture');
    fs.writeFileSync(old,code);assert.throws(()=>install({fs,path,target,code,backupDirectory}),/different content/);assert.equal(fs.readFileSync(old,'utf8'),code);
  }finally{fs.rmSync(root,{recursive:true,force:true});}
});
test('5.8.0 updater accepts new product artifact using retained compatibility marker',()=>{
  // Exact historical bytes: 34c1d1f12965dee64fcb8390000a876615c6bae4:stories-hunter/native/updater.js.
  // Keep this fixture immutable so compatibility also runs in shallow checkouts.
  const old=fs.readFileSync(path.join(__dirname,'fixtures/updater-5.8.0.js'));
  assert.equal(require('node:crypto').createHash('sha256').update(old).digest('hex'),'3817a4010b785991bdb063f9a3309720c51b411f684031679196d509dde5fb59');
  const context=vm.createContext({});vm.runInContext(old.toString('utf8'),context);
  assert.equal(context.hunterUpdateVersion(require('./assemble.cjs').assembleHunter()),'5.8.1');
});
