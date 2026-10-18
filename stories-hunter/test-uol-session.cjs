const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'template.html'),'utf8');
const accountCode=html.slice(html.indexOf('    let nativeRedemptionPending'),html.indexOf('    function openMyRedemptionsModal()'));
function setup(){
 const elements=new Map();
 const get=id=>{if(!elements.has(id)){const dot={};elements.set(id,{style:{},innerHTML:'',textContent:'',querySelector:()=>dot});}return elements.get(id);};
 const c=vm.createContext({INITIAL:{nativeAccounts:true},settings:{currentUser:'leosaquetto'},window:{location:{href:''}},
  document:{getElementById:get},updateCredsStatusUI(){},currentTab:'home',showToast(){},updateTimelineBadgeDot(){},
  saveSettingsToStorage(){},localStorage:{setItem(){}},redemptionIdentity:()=>'',renderRedemptionsScreen(){}});
 vm.runInContext(accountCode,c);return{c,get};
}
test('unverified session never appears disconnected; a single read updates the already open modal',async()=>{
 const {c,get}=setup();c.updateSessionStatusUI();
 assert.match(get('authModalStatusPill').innerHTML,/Não verificada/);
 assert.equal(get('btnVerifyUolSession').disabled,false);
 await c.fetchRealRedemptions();assert.equal(c.window.location.href,'hunter-account://history?account=leosaquetto');
 assert.match(get('authModalStatusPill').innerHTML,/Verificando/);assert.equal(get('btnVerifyUolSession').disabled,true);
 c.handleNativeAccountResult('history','leosaquetto',{account:'leosaquetto',status:'authenticated',records:[]});
 assert.match(get('authModalStatusPill').innerHTML,/Conectado/);assert.equal(get('btnVerifyUolSession').disabled,false);
 assert.equal(get('redemptionsSessionStatusLabel').textContent,'Conectado');
 assert.equal(get('authModalSessionDetail').textContent,'Conta confirmada pelo histórico oficial.');
});
test('local parser and network failure are distinct from an explicit authentication failure',()=>{
 const {c,get}=setup();
 for(const reason of ['history_parser_unavailable','network_error','identity_mismatch','account_access_required']){
  c.handleNativeAccountResult('history','leosaquetto',{account:'leosaquetto',status:'unavailable',reason});
  assert.match(get('authModalStatusPill').innerHTML,/Verificação indisponível/);
  assert.doesNotMatch(get('authModalStatusPill').innerHTML,/expirada|Desconectada/);
 }
 c.handleNativeAccountResult('history','leosaquetto',{account:'leosaquetto',status:'auth_required',reason:'auth_required'});
 assert.match(get('authModalStatusPill').innerHTML,/Sessão expirada/);
});
test('late response from the other profile never sets the selected profile connected',()=>{
 const{c,get}=setup();
 c.handleNativeAccountResult('history','gaoriel',{account:'gaoriel',status:'authenticated',records:[]});
 c.updateSessionStatusUI();assert.match(get('authModalStatusPill').innerHTML,/Não verificada/);
});
test('redemptions card renders an amber unknown state and updates the same indicator after native results',async()=>{
 const{c,get}=setup();
 Object.assign(c,{getAvatarUrl:()=>'',getAllRedemptionsList:()=>[],currentRedemptionsFilter:'all',ICONS:{ticketCheck:''}});
 const start=html.indexOf('    function renderRedemptionsScreen()');
 vm.runInContext(html.slice(start,html.indexOf('\n    }',start)+'\n    }'.length),c);
 c.renderRedemptionsScreen();
 assert.match(get('viewRedemptions').innerHTML,/id="redemptionsSessionStatusInd">\s*<span class="session-dot dot-amber"/);
 const dot=get('redemptionsSessionStatusInd').querySelector('.session-dot');
 await c.fetchRealRedemptions();assert.equal(dot.className,'session-dot dot-amber');
 c.handleNativeAccountResult('history','leosaquetto',{account:'leosaquetto',status:'authenticated',records:[]});
 assert.equal(dot.className,'session-dot dot-green');
 c.handleNativeAccountResult('history','leosaquetto',{account:'leosaquetto',status:'unavailable',reason:'network_error'});
 assert.equal(dot.className,'session-dot dot-amber');
 c.renderRedemptionsScreen();
 assert.match(get('viewRedemptions').innerHTML,/id="redemptionsSessionStatusInd">\s*<span class="session-dot dot-amber"/);
});
