const {test}=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm');
const source=require('./test-support/build-source.cjs')();
const main=source.slice(source.indexOf('async function main()'),source.indexOf('\nfunction resolveHunterAssets('));
function harness(action='open',paused=false){
  const calls=[],output=[];let close;
  const context=vm.createContext({console:{log(){}},hunterAction:()=>action,hunterCollectionPausedForUpdate:()=>paused,
    installHunterBrowser(){},buildHunterStartupHTML:()=> 'loading',buildDashboardHTML:()=> 'dashboard',
    Timer:{schedule:(_ms,_repeat,fn)=>fn()},createHunterAccess:()=>({status:async()=>({status:'approved'})}),
    runNativeHunterCollector:async()=>{calls.push('collect');return {status:'complete'};},
    preparePrivateFiles:async()=>{calls.push('private');},persistPrivateBootstrap(){},loadSettings:()=>({}),
    loadHistory:()=>[],loadCredentials:()=>({}),loadSessionCookies:()=>[],loadRedemptionsDatabase:()=>[],
    discoverHunterUpdate:()=>{calls.push('discover');return new Promise(()=>{});},
    performSelfUpdate:async(...args)=>{calls.push(['update',...args]);},hunterUpdateChannel:()=> 'stable',
    hunterLastUpdateResult:{status:'up_to_date'},
    Script:{complete(){},setShortcutOutput:v=>output.push(v)},
    WebView:class{async loadHTML(html){calls.push(html);}present(){calls.push('present');return new Promise(resolve=>{close=resolve;});}}
  });
  vm.runInContext(main,context);
  return {context,calls,output,close:()=>close()};
}
test('metadata discovery starts only after dashboard and never holds UI close or collection',async()=>{
  const h=harness(),pending=h.context.main();await new Promise(resolve=>setImmediate(resolve));
  assert.deepEqual(h.calls,['loading','present','collect','private','dashboard','discover']);h.close();
  await Promise.race([pending,new Promise((_,reject)=>{const timer=setTimeout(()=>reject(Error('discovery blocked close')),1000);timer.unref();})]);
});
test('first updated opening, headless collection and updater do not start discovery',async()=>{
  const first=harness('open',true),pending=first.context.main();await new Promise(resolve=>setImmediate(resolve));first.close();await pending;
  assert.ok(!first.calls.includes('collect'));assert.ok(!first.calls.includes('discover'));
  for(const action of ['collect','notification']) {const h=harness(action);await h.context.main();assert.deepEqual(h.calls,['collect']);}
  for(const action of ['update','update-auto']) {const h=harness(action);await h.context.main();assert.deepEqual(h.calls,[['update',action==='update',action==='update-auto']]);assert.equal(JSON.parse(h.output[0]).status,'up_to_date');}
});
