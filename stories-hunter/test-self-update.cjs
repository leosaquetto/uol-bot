const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = require("./test-support/build-source.cjs")();
const code = fs.readFileSync(path.join(__dirname, 'native/updater.js'), 'utf8');
const crypto = require('node:crypto');
function artifact(version) { return '// Variables used by Scriptable.\n// These must be at the very top of the file.\n// Scriptable: Clube UOL - Hub Central de Ingressos & Stories Hunter\nconst SCRIPT_VERSION = "'+version+'";\nconst HunterCollector = (() => { return {}; })();\nfunction hunterCollectionPausedForUpdate(action) {}\nasync function main() {}\nfunction buildDashboardHTML() {}\nawait main();\nScript.complete();\n'; }
function harness() {
  const files = new Map([['/cloud/Hunter.js',artifact('5.7.6')],['/cloud/vouchers.json','private-data']]);
  const writes=[], calls=[], alerts=[], opened=[];
  let body=artifact('5.7.7'), status=200, failWrite=false;
  const manager = root => ({ documentsDirectory:()=>root,joinPath:path.join,fileExists:p=>files.has(p),readString:p=>files.get(p),
    writeString(p,v) { writes.push(p); files.set(p,v); },
    move(from,to) { if(failWrite && to==='/cloud/Hunter.js' && files.get(from)===body){files.set(to,'truncated');throw Error('write lost');} files.set(to,files.get(from));files.delete(from); } });
  const context=vm.createContext({SCRIPT_VERSION:'5.7.6', UPDATE_RAW_URL:'https://raw.githubusercontent.com/leosaquetto/uol-bot/main/scriptable_uol_stories_hunter.js',
    getFileManager:()=>manager('/cloud'),FileManager:{local:()=>manager('/local')},Script:{name:()=> 'Hunter'},
    Request:class { constructor(url){this.url=url;} async load(){calls.push(this);this.response={statusCode:status,url:this.url,headers:{'Content-Type':'text/plain;charset=utf-8'}}; const value=this.url.endsWith('.json') ? JSON.stringify({schema:1,updaterProtocol:1,channel:'stable',version:(body.match(/const SCRIPT_VERSION = "([^"]+)"/)||[])[1]||'5.7.7',artifact:'https://raw.githubusercontent.com/leosaquetto/uol-bot/'+ 'a'.repeat(40)+'/stories-hunter/dist/Hunter.js',size:Buffer.byteLength(body),sha256:crypto.createHash('sha256').update(body).digest('hex'),publishedAt:'2026-10-18T12:00:00.000Z'}) : body; const data=Buffer.from(value);return {getBytes:()=>Array.from(data),toRawString:()=>data.toString('utf8')};} },
    Alert:class { addAction(){} addCancelAction(){} async presentAlert(){alerts.push(this.title);return 0;} },Safari:{open:url=>opened.push(url)} });
  vm.runInContext(code,context);
  return {context,files,writes,calls,alerts,opened,setBody:v=>body=v,setStatus:v=>status=v,failWrite:()=>failWrite=true};
}
test('self update checks metadata then exact artifact, verified backup and code-only replacement',async()=>{
 const h=harness();assert.equal(await h.context.performSelfUpdate(true),true);
 assert.equal(h.calls.length,2);for(const request of h.calls){assert.equal(request.onRedirect(),null);assert.equal(request.timeoutInterval,20);}
 assert.equal(h.files.get('/cloud/Hunter.js'),artifact('5.7.7'));assert.equal([...h.files].find(([p])=>p.startsWith('/local/uol_hunter_ota_backup-'))[1],artifact('5.7.6'));
 assert.equal(h.files.get('/cloud/vouchers.json'),'private-data');assert.ok(!h.writes.some(p=>/cookie|voucher|settings|database/.test(p)));
 assert.equal(h.opened.length,1);assert.match(h.opened[0],/^scriptable:\/\/\/run\?scriptName=Hunter$/);
});
test('malformed, HTTP failure, same version and downgrade preserve installed code',async()=>{
 for(const [body,status] of [[artifact('5.7.7'),500],['<html>Clube UOL - Hub Central</html>',200],[artifact('5.7.6'),200],[artifact('5.7.5'),200],[artifact('5.7.7-beta'),200]]){
  const h=harness();h.setBody(body);h.setStatus(status);assert.equal(await h.context.performSelfUpdate(true),false);assert.equal(h.writes.length,0);assert.equal(h.opened.length,0);
 }
});
test('lost write restores the previous script before reporting failure',async()=>{
 const h=harness();h.failWrite();assert.equal(await h.context.performSelfUpdate(true),false);
 assert.equal(h.files.get('/cloud/Hunter.js'),artifact('5.7.6'));assert.equal(h.opened.length,0);assert.ok(h.alerts.includes('Atualização interrompida'));
});
test('update UI delegates one check without downloading script in the WebView',async()=>{
 const html=require("./assemble-ui.cjs").assembleUI();
 const section=html.slice(html.indexOf('    async function checkForScriptUpdateManual()'),html.indexOf('    // LINHA DO TEMPO DE ATIVIDADES'));
 const nodes={btnCheckUpdate:{},updateStatusMsg:{style:{}}}, location={href:'hunter'};
 const c=vm.createContext({INITIAL:{nativeBrowser:true},window:{location},document:{getElementById:id=>nodes[id]},fetch:()=>assert.fail('duplicate download')});
 vm.runInContext(section,c);await c.checkForScriptUpdateManual();assert.equal(location.href,'hunter-update://check');assert.equal(nodes.btnCheckUpdate.disabled,true);
 c.handleNativeUpdateResult(false);assert.equal(nodes.btnCheckUpdate.disabled,false);
});
