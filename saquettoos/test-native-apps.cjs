const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const nativeApps = require('./adapters/native-apps.js');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

// Synthetic installed scripts: no iCloud reads, account requests or real health data.
const calendarSource = `
const KEY_AGENDA='test-agenda', KEY_BDAYS='test-birthdays';
let showAgenda=true, showBirthdays=true;
async function getUnifiedTimeline(){return fixtures.events.filter(e=>e.type==='birthday'?showBirthdays:showAgenda);}
async function presentTable(){throw new Error('standalone_must_not_execute');}
if (config.runsInApp && !args.queryParameters.toggle) {
  await presentTable("all");
}
Script.complete();
`;
const pillSource = `
let _dataDirty=false;
const DEFAULT_MAX_ESTOQUE=90;
function getICloudPath(){return '/synthetic/pills.json';}
const fm={fileExists:()=>!fixtures.missingFile,isFileStoredIniCloud:()=>false,readString:()=>fixtures.disk};
async function loadData(){return fixtures.data;}
async function saveData(){if(_dataDirty){fixtures.saves++;if(!fixtures.failWrite)fixtures.disk=JSON.stringify(fixtures.data);_dataDirty=false;}}
function markDirty(){_dataDirty=true;}
function todayISO(){return '2026-10-18';}
function getActiveMed(data){return {name:data.medicamentoAtivo,data:data.medicamentos[data.medicamentoAtivo]};}
function getDoseState(){return 'normal';}function getStockState(){return 'normal';}
async function showAlert(title,message){const a=new Alert();a.title=title;a.message=message;a.addAction('OK');await a.presentAlert();}
async function askText(title,message,value='',placeholder=''){const a=new Alert();a.title=title;a.message=message;a.addTextField(placeholder,value);a.addAction('Salvar');a.addCancelAction('Cancelar');const r=await a.presentAlert();return r===-1?null:a.textFieldValue(0);}
async function askNumber(title,message,value=0){const v=await askText(title,message,value,'Número');return v===null?null:Math.max(0,Math.floor(Number(v)));}
async function askTime(title,message,value){return askText(title,message,value,'hh:mm');}
async function markTodayTaken(data){const m=getActiveMed(data).data;if(m.historico.includes(todayISO()))return showAlert('Já registrado','Nada foi alterado');const a=new Alert();a.title='Registrar dose';a.message='Reduz 1 unidade';a.addAction('Registrar');a.addCancelAction('Cancelar');if(await a.presentAlert()===-1)return;m.historico.push(todayISO());m.estoque=Math.max(0,m.estoque-1);markDirty();await showAlert('Registrado','Estoque atualizado');}
async function addBottle(data){const qty=await askNumber('Novo pote','Quantidade',0);if(qty===null)return;getActiveMed(data).data.estoque+=qty;markDirty();}
async function editStock(data){const qty=await askNumber('Estoque','Quantidade atual',getActiveMed(data).data.estoque);if(qty===null)return;getActiveMed(data).data.estoque=qty;markDirty();}
async function addHistoryDay(data){const date=await askText('Adicionar dia','Data');if(date===null)return;getActiveMed(data).data.historico.push(date);markDirty();}
async function removeHistoryDay(data){const a=new Alert();a.title='Confirmar remoção';a.addAction('Remover');a.addCancelAction('Cancelar');if(await a.presentAlert()===-1)return;getActiveMed(data).data.historico.pop();markDirty();}
async function clearHistory(data){const a=new Alert();a.title='Apagar histórico';a.addAction('Apagar');a.addCancelAction('Cancelar');if(await a.presentAlert()===-1)return;getActiveMed(data).data.historico=[];markDirty();}
async function pauseMenu(data){const a=new Alert();a.addAction('Pausar');a.addCancelAction('Cancelar');if(await a.presentAlert()===-1)return;getActiveMed(data).data.pausado=!getActiveMed(data).data.pausado;markDirty();}
async function addMedication(data){const name=await askText('Adicionar medicamento','Nome');if(name===null)return;data.medicamentos[name]={estoque:0,horaIdeal:'09:00',maxEstoque:90,alertaMinimo:30,historico:[],pausado:false};markDirty();}
async function switchActiveMedication(data){const a=new Alert();const names=Object.keys(data.medicamentos);names.forEach(n=>a.addAction(n));a.addCancelAction('Cancelar');const r=await a.presentSheet();if(r===-1)return;data.medicamentoAtivo=names[r];markDirty();}
async function removeMedication(data){if(Object.keys(data.medicamentos).length<2)return;const a=new Alert();a.addAction('Remover');a.addCancelAction('Cancelar');if(await a.presentAlert()===-1)return;delete data.medicamentos[data.medicamentoAtivo];data.medicamentoAtivo=Object.keys(data.medicamentos)[0];markDirty();}
async function limparCacheImagens(){fixtures.cacheClears++;}
async function showMainMenu(){throw new Error('standalone_must_not_execute');}
try {
  const data = await loadData()
  await showMainMenu(data)
  Script.complete()
}
`;
const lastfmSource = `
const USER='synthetic-user',API_KEY='synthetic-private-key';
async function fetchJSON(url){try{return await fixtures.fetch(url);}catch(e){console.log("❌ Falha ao buscar dados da API: " + e.message);return null;}}
function loadCachedState(){return fixtures.cache;}
async function detectPlaybackSource(){return 'Device';}
function buildRecentTracksHTML(cachedState){
  const initialDataJSON = JSON.stringify({
    cached: cachedState || null,
    api_key: API_KEY,
    username: USER
  });
  return \`<!DOCTYPE html><html lang="pt-BR"><head><title>Last.fm</title></head><body><div id="btnRefresh"></div><script>
const CONFIG=\$\{initialDataJSON\};let currentUser=CONFIG.cached.user,allTracks=CONFIG.cached.tracks,playbackDevice=null;
function renderHeader(){}function renderHero(){}function renderTrackList(){}
    async function refreshData(isManual = false) {
      const privateUrl='https://api.invalid?api_key='+CONFIG.api_key;
    }
    function manualRefresh() { refreshData(true); }
</script></body></html>\`;
}
async function createSmall(){throw new Error('standalone_must_not_execute');}
async function presentRecentTracksWebView(){throw new Error('standalone_must_not_execute');}
if (config.runsInWidget) {
  let widget = await createSmall();
  Script.setWidget(widget);
} else {
  await presentRecentTracksWebView();
}
Script.complete();
`;
function setup(patch = {}) {
  const fixtures = { data: { medicamentoAtivo: 'Exemplo', medicamentos: { Exemplo: { estoque: 10, maxEstoque: 90, alertaMinimo: 30, horaIdeal: '09:00', pausado: false, historico: ['2026-10-17'] } } }, saves: 0, cacheClears: 0,
    events: [{ type: 'agenda', title: 'Evento <teste>', date: new Date('2026-10-19T12:00:00Z'), calName: 'Calendário', isAllDay: false }, { type: 'birthday', title: 'Aniversário', date: new Date('2026-10-20T12:00:00Z'), calName: 'Aniversários', isAllDay: true }],
    cache: { user: { name: 'Example', playcount: '7' }, tracks: [] },
    fetch: async url => url.includes('user.getrecenttracks') ? { recenttracks: { track: [{ name: 'Song', artist: { '#text': 'Artist' }, image: [], date: { uts: '1792324800' } }] } } : { user: { name: 'Example', playcount: '8' } }, ...patch };
  if (!Object.prototype.hasOwnProperty.call(fixtures, 'disk')) fixtures.disk = JSON.stringify(fixtures.data);
  const calls = [], keys = new Map(), sources = { 'super-calendario': calendarSource, pilltracker: pillSource, 'lastfm-small': lastfmSource };
  const loader = { async load(id, exportsString, transform, overrides = {}) {
    calls.push(id); const transformed = transform(sources[id]);
    const globals = { fixtures, config: { runsInApp: true, runsInWidget: false }, args: { queryParameters: {} }, Script: { complete() { throw new Error('standalone_must_not_execute'); } }, Keychain: { set(k, v) { keys.set(k, v); } }, ...overrides };
    return new AsyncFunction(...Object.keys(globals), transformed + '\nreturn (' + exportsString + ');')(...Object.values(globals));
  } };
  return { fixtures, sources, calls, keys, loader, api: nativeApps.create({ loader, theme: 'light' }) };
}
const submit = (panel, dialog, choice = 0, fields = []) => panel.handle('dialog.submit', { id: dialog.id, choice, fields });
function parseScripts(html) { for (const match of html.matchAll(/<script>([\s\S]*?)<\/script>/g)) new vm.Script(match[1]); }

test('calendar loads local core without executing standalone, renders theme, preserves toggles and event data', async () => {
  const h = setup(), panel = await h.api.open('super-calendario'); parseScripts(panel.html);
  assert.match(panel.html, /data-theme="light"/); assert.match(panel.html, /calendar\.toggle/);
  let result = await panel.handle('calendar.refresh'); assert.equal(result.state.events.length, 2); assert.equal(result.state.events[0].title, 'Evento <teste>');
  result = await panel.handle('calendar.toggle', { kind: 'agenda' }); assert.equal(result.state.agenda, false); assert.equal(result.state.events.length, 1); assert.equal(h.keys.get('test-agenda'), 'false');
  result = await panel.handle('calendar.toggle', { kind: 'birthdays' }); assert.equal(result.state.agenda, true); assert.equal(result.state.birthdays, true);
  assert.equal((await panel.handle('calendar.toggle', { kind: '__proto__' })).status, 'error'); panel.dispose(); assert.equal((await panel.handle('calendar.refresh')).reason, 'disposed');
});
test('unknown adapters and drifted entrypoints are rejected without running scripts', async () => {
  const h = setup(); await assert.rejects(h.api.open('unknown'), /unsupported_native_app/);
  h.sources.pilltracker = pillSource.replace('await showMainMenu(data)', 'await anotherEntrypoint(data)');
  await assert.rejects(h.api.open('pilltracker'), /adapter_entrypoint_changed/);
  h.sources['super-calendario'] += '\nif (config.runsInApp && !args.queryParameters.toggle) {';
  await assert.rejects(h.api.open('super-calendario'), /adapter_entrypoint_changed/);
});
test('PillTracker uses web dialogs, validates responses and preserves dose confirmation and stock semantics', async () => {
  const h = setup(), panel = await h.api.open('pilltracker'); parseScripts(panel.html);
  let result = await panel.handle('pill.command', { command: 'dose' }); assert.equal(result.dialog.title, 'Registrar dose'); assert.equal(h.fixtures.saves, 0); assert.equal(h.fixtures.data.medicamentos.Exemplo.estoque, 10);
  assert.equal((await submit(panel, { id: 999 }, 0)).reason, 'invalid_dialog');
  assert.equal((await panel.handle('pill.command', { command: 'stock' })).reason, 'busy');
  result = await submit(panel, result.dialog, 0); assert.equal(result.state.active.stock, 9); assert.equal(result.state.active.taken, true); assert.equal(result.dialog.title, 'Registrado');
  result = await submit(panel, result.dialog, 0); assert.equal(result.dialog, null); assert.equal(h.fixtures.saves, 1);
  result = await panel.handle('pill.command', { command: 'dose' }); assert.equal(result.dialog.title, 'Já registrado'); await submit(panel, result.dialog, 0); assert.equal(h.fixtures.data.medicamentos.Exemplo.estoque, 9); assert.equal(h.fixtures.saves, 1);
});
test('PillTracker cancellation preserves history; stale dialogs cannot authorize a later action', async () => {
  const h = setup(), panel = await h.api.open('pilltracker');
  let result = await panel.handle('pill.command', { command: 'history-clear' }), oldDialog = result.dialog;
  result = await submit(panel, result.dialog, -1); assert.deepEqual(result.state.active.history, ['2026-10-17']); assert.equal(h.fixtures.saves, 0);
  result = await panel.handle('pill.command', { command: 'history-clear' }); assert.equal((await submit(panel, oldDialog, 0)).reason, 'invalid_dialog');
  result = await submit(panel, result.dialog, 0); assert.deepEqual(result.state.active.history, []); assert.equal(h.fixtures.saves, 1);
});
test('PillTracker inline forms call original add, stock and medication settings functions without changing identity', async () => {
  const h = setup(), panel = await h.api.open('pilltracker');
  let result = await panel.handle('pill.command', { command: 'stock' }); result = await submit(panel, result.dialog, 0, ['22']); assert.equal(result.state.active.stock, 22);
  result = await panel.handle('pill.command', { command: 'edit' }); result = await submit(panel, result.dialog, 0, ['10:30']); result = await submit(panel, result.dialog, 0, ['15']); result = await submit(panel, result.dialog, 0, ['30']);
  result = await submit(panel, result.dialog, 0); assert.equal(result.state.active.name, 'Exemplo'); assert.equal(result.state.active.time, '10:30'); assert.equal(result.state.active.minimum, 15); assert.equal(result.state.active.maximum, 30);
  result = await panel.handle('pill.command', { command: 'add' }); result = await submit(panel, result.dialog, 0, ['Segundo']); assert.equal(result.state.count, 2); assert.equal(result.state.active.name, 'Exemplo');
});
test('PillTracker disposal cancels a pending confirmation and completes already-confirmed persistence', async () => {
  const h = setup(), panel = await h.api.open('pilltracker'); let result = await panel.handle('pill.command', { command: 'dose' });
  await panel.dispose(); assert.equal(h.fixtures.data.medicamentos.Exemplo.estoque, 10); assert.equal(h.fixtures.saves, 0);
  const h2 = setup(), panel2 = await h2.api.open('pilltracker'); result = await panel2.handle('pill.command', { command: 'dose' }); await submit(panel2, result.dialog, 0); await panel2.dispose();
  assert.equal(h2.fixtures.data.medicamentos.Exemplo.estoque, 9); assert.equal(h2.fixtures.saves, 1);
});
test('PillTracker rejects unknown actions and malformed field inputs before mutation', async () => {
  const h = setup(), panel = await h.api.open('pilltracker');
  for (const command of ['__proto__', 'constructor', 'invalid']) assert.equal((await panel.handle('pill.command', { command })).reason, 'invalid_action');
  const result = await panel.handle('pill.command', { command: 'stock' });
  assert.equal((await submit(panel, result.dialog, 0, [42])).reason, 'invalid_dialog'); assert.equal((await submit(panel, result.dialog, 0, ['x'.repeat(301)])).reason, 'invalid_dialog');
  await panel.dispose(); assert.equal(h.fixtures.data.medicamentos.Exemplo.estoque, 10);
});
test('PillTracker rejects missing or corrupt persisted data without initialization or overwrite', async () => {
  for (const patch of [{ missingFile: true }, { disk: '{' }, { disk: '{"medicamentos":{}}' }, { disk: '{"medicamentos":{"A":null}}' }]) {
    const h = setup(patch); await assert.rejects(h.api.open('pilltracker')); assert.equal(h.fixtures.saves, 0);
  }
});
test('PillTracker preserves concurrent external edits and refuses unverified write success', async () => {
  const h = setup(), panel = await h.api.open('pilltracker');
  let result = await panel.handle('pill.command', { command: 'stock' }); h.fixtures.disk = '{"external":"change"}';
  result = await submit(panel, result.dialog, 0, ['20']); assert.equal(result.status, 'error'); assert.equal(h.fixtures.disk, '{"external":"change"}'); assert.equal(h.fixtures.saves, 0);
  assert.equal((await panel.handle('pill.command', { command: 'stock' })).reason, 'session_requires_reload');
  const h2 = setup({ failWrite: true }), panel2 = await h2.api.open('pilltracker'); result = await panel2.handle('pill.command', { command: 'stock' }); result = await submit(panel2, result.dialog, 0, ['20']); assert.equal(result.status, 'error');
  assert.equal(JSON.parse(h2.fixtures.disk).medicamentos.Exemplo.estoque, 10);
});
test('Last.fm reuses builder with native refresh, never emits API key, and safely serializes cached data', async () => {
  const h = setup({ cache: { user: { name: '</script><script>bad()</script>', playcount: '2' }, tracks: [{ name: '<img src=x onerror=bad()>', artist: { '#text': 'Artist' }, url: 'javascript:bad()', image: [{ size: 'large', '#text': 'https://cdn.invalid/x" onerror="bad()' }] }] } });
  const panel = await h.api.open('lastfm-small'); parseScripts(panel.html);
  assert.doesNotMatch(panel.html, /synthetic-private-key|CONFIG\.api_key|privateUrl|api_key:/); assert.doesNotMatch(panel.html, /<script>bad\(\)<\/script>|javascript:bad/); assert.match(panel.html, /lastfm\.refresh/); assert.match(panel.html, /saquettoTrackDetail/);
  const result = await panel.handle('lastfm.refresh'); assert.equal(result.status, 'ok'); assert.equal(result.state.tracks[0].name, 'Song'); assert.equal(result.state.user.playcount, '8');
  assert.doesNotMatch(JSON.stringify(result), /synthetic-private-key|api_key/);
});
test('Last.fm request failure is explicit and leaves existing cache untouched', async () => {
  const h = setup({ fetch: async () => { throw new Error('private URL must not escape'); } });
  const panel = await h.api.open('lastfm-small'); assert.deepEqual(await panel.handle('lastfm.refresh'), { status: 'error', reason: 'lastfm_unavailable' }); assert.equal(h.fixtures.cache.user.playcount, '7');
  panel.dispose(); assert.equal((await panel.handle('lastfm.refresh')).reason, 'disposed');
});
test('Last.fm private-config or refresh marker drift fails closed', async () => {
  for (const target of ['    api_key: API_KEY,\n', '    async function refreshData(isManual = false) {']) {
    const h = setup(); h.sources['lastfm-small'] = lastfmSource.replace(target, 'changed');
    await assert.rejects(h.api.open('lastfm-small'), /adapter_(source|refresh)_changed/);
  }
});
