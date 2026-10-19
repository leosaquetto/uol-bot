const {test}=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm');
const source=require('./test-support/build-source.cjs')();
const main=source.slice(source.indexOf('async function main()'),source.indexOf('\nfunction resolveHunterAssets('));
function harness(action='open',paused=false){
  const calls=[],output=[],state={relaunch:false,complete:0};let close;
  const context=vm.createContext({console:{log(){}},hunterAction:()=>action,hunterCollectionPausedForUpdate:()=>{calls.push('marker');return paused;},
    autoUpdateHunterOnOpen:async()=>{calls.push('auto');return state.relaunch;},
    showHunterStartupUpdateNotice:()=>{calls.push('notice');return new Promise(()=>{});},
    installHunterBrowser(){},buildHunterStartupHTML:()=> 'loading',buildDashboardHTML:()=> 'dashboard',
    Timer:{schedule:(_ms,_repeat,fn)=>fn()},createHunterAccess:()=>({status:async()=>({status:'approved'})}),
    runNativeHunterCollector:async()=>{calls.push('collect');return {status:'complete'};},
    preparePrivateFiles:async()=>{calls.push('private');},persistPrivateBootstrap(){},loadSettings:()=>({}),
    loadHistory:()=>[],loadCredentials:()=>({}),loadSessionCookies:()=>[],loadRedemptionsDatabase:()=>[],
    discoverHunterUpdate:()=>{calls.push('discover');return new Promise(()=>{});},
    performSelfUpdate:async(...args)=>{calls.push(['update',...args]);},hunterUpdateChannel:()=> 'stable',
    hunterLastUpdateResult:{status:'up_to_date'},HUNTER_UPDATER_BUILD:'5.8.4',
    Script:{complete(){state.complete++;},setShortcutOutput:v=>output.push(v)},
    WebView:class{async loadHTML(html){calls.push(html);}present(){calls.push('present');return new Promise(resolve=>{close=resolve;});}}
  });
  vm.runInContext(main,context);
  return {context,calls,output,state,close:()=>close()};
}
test('automatic update precedes startup and its post-dashboard notice never holds UI close or collection',async()=>{
  const h=harness(),pending=h.context.main();await new Promise(resolve=>setImmediate(resolve));
  assert.deepEqual(h.calls,['auto','marker','loading','present','private','dashboard','notice','collect']);h.close();
  await Promise.race([pending,new Promise((_,reject)=>{const timer=setTimeout(()=>reject(Error('notice blocked close')),1000);timer.unref();})]);
});
test('automatic relaunch returns before WebView, private files, collection and version marker',async()=>{
  const runtime=harness();runtime.state.relaunch=true;
  await runtime.context.main();
  assert.deepEqual(runtime.calls,['auto']);assert.equal(runtime.state.complete,1);assert.deepEqual(runtime.output,[]);
});
test('automatic update settles before any startup side effect instead of racing a background installation',async()=>{
  const runtime=harness();let finish;
  runtime.context.autoUpdateHunterOnOpen=()=>{runtime.calls.push('auto');return new Promise(resolve=>{finish=resolve;});};
  const pending=runtime.context.main();await new Promise(resolve=>setImmediate(resolve));
  assert.deepEqual(runtime.calls,['auto']);assert.equal(runtime.state.complete,0);
  finish(false);await new Promise(resolve=>setImmediate(resolve));
  assert.deepEqual(runtime.calls,['auto','marker','loading','present','private','dashboard','notice','collect']);
  runtime.close();await pending;
});
test('first updated opening preserves the collection pause while permitting its confirmed startup notice',async()=>{
  const first=harness('open',true),pending=first.context.main();await new Promise(resolve=>setImmediate(resolve));first.close();await pending;
  assert.deepEqual(first.calls,['auto','marker','loading','present','private','dashboard','notice']);
});
test('headless collection and explicit updater actions retain their existing contracts without automatic opening updates',async()=>{
  for(const action of ['collect','notification']) {const h=harness(action);await h.context.main();assert.deepEqual(h.calls,['marker','collect']);}
  for(const action of ['update','update-auto']) {const h=harness(action);await h.context.main();assert.deepEqual(h.calls,[['update',action==='update',action==='update-auto']]);assert.equal(JSON.parse(h.output[0]).status,'up_to_date');}
});
test('missing optional startup update hooks leave the existing dashboard and collection lifecycle usable',async()=>{
  const runtime=harness();delete runtime.context.autoUpdateHunterOnOpen;delete runtime.context.showHunterStartupUpdateNotice;
  const pending=runtime.context.main();await new Promise(resolve=>setImmediate(resolve));
  assert.deepEqual(runtime.calls,['marker','loading','present','private','dashboard','collect']);runtime.close();await pending;
});
