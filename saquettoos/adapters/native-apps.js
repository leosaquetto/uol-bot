// Native-app adapters load only the user's installed, hash-approved source.
// No private script, setting, medication data or API key is bundled here.
const SaquettoNativeApps = (() => {
  const ids = Object.freeze({ calendar: 'super-calendario', pill: 'pilltracker', lastfm: 'lastfm-small' });
  const escape = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const json = value => JSON.stringify(value).replace(/</g, '\\u003c').replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
  function once(source, text, replacement) {
    const at = source.indexOf(text);
    if (at < 0 || at !== source.lastIndexOf(text)) throw new Error('adapter_source_changed');
    return source.slice(0, at) + replacement + source.slice(at + text.length);
  }
  function withoutTail(source, anchor, expected) {
    const at = source.indexOf(anchor);
    if (at < 0 || at !== source.lastIndexOf(anchor) || !expected.test(source.slice(at))) throw new Error('adapter_entrypoint_changed');
    return source.slice(0, at);
  }
  function calendarTransform(source) {
    // The host supplies empty queryParameters, so the standalone URL-toggle block is inert.
    return withoutTail(source, '\nif (config.runsInApp && !args.queryParameters.toggle) {', /await presentTable\("all"\);[\s\S]*Script\.complete\(\);\s*$/);
  }
  function pillTransform(source) {
    return withoutTail(source, '\ntry {\n  const data = await loadData()', /await showMainMenu\(data\)[\s\S]*Script\.complete\(\)\s*\}\s*$/);
  }
  function lastfmTransform(source) {
    source = withoutTail(source, '\nif (config.runsInWidget) {\n  let widget = await createSmall();', /await presentRecentTracksWebView\(\);\s*\}\s*Script\.complete\(\);\s*$/);
    source = once(source, '    api_key: API_KEY,\n', '');
    source = once(source, 'console.log("❌ Falha ao buscar dados da API: " + e.message);', '/* Do not log native request URLs or private API configuration. */');
    const start = '    async function refreshData(isManual = false) {', end = '    function manualRefresh() {';
    const a = source.indexOf(start), b = source.indexOf(end);
    if (a < 0 || a !== source.lastIndexOf(start) || b <= a || b !== source.lastIndexOf(end)) throw new Error('adapter_refresh_changed');
    const replacement = `    async function refreshData(isManual = false) {
      const btn = document.getElementById("btnRefresh");
      if (btn) btn.classList.add("spinning");
      try {
        const response = await window.SaquettoApp.request("lastfm.refresh", {});
        if (!response || response.status !== "ok") throw new Error("refresh_unavailable");
        const data = response.state;
        currentUser = data.user; allTracks = data.tracks; playbackDevice = data.playbackDevice;
        renderHeader(currentUser);
        renderHero(allTracks.find(t => t["@attr"] && t["@attr"].nowplaying === "true"), playbackDevice);
        renderTrackList();
        const note = document.getElementById("saquettoLastfmStatus"); if (note) note.textContent = "Atualizado agora";
      } catch (_) {
        const note = document.getElementById("saquettoLastfmStatus"); if (note) note.textContent = "Não foi possível atualizar. Os dados anteriores foram mantidos.";
      } finally { if (btn) btn.classList.remove("spinning"); }
    }

`;
    source = source.slice(0, a) + replacement + source.slice(b);
    // JSON inside a script element must not be able to terminate the element.
    source = once(source, '    username: USER\n  });', "    username: USER\n  }).replace(/</g, '\\\\u003c').replace(/\\u2028/g, '\\\\u2028').replace(/\\u2029/g, '\\\\u2029');");
    return source;
  }
  const calendarExports = `({
    async snapshot() { return { agenda: showAgenda, birthdays: showBirthdays, events: await getUnifiedTimeline(100, true) }; },
    async toggle(kind) {
      if (kind === 'agenda') showAgenda = !showAgenda;
      else if (kind === 'birthdays') showBirthdays = !showBirthdays;
      else throw new Error('invalid_toggle');
      if (!showAgenda && !showBirthdays) { showAgenda = true; showBirthdays = true; }
      Keychain.set(KEY_AGENDA, String(showAgenda)); Keychain.set(KEY_BDAYS, String(showBirthdays));
      return this.snapshot();
    }
  })`;
  const pillExports = `(() => {
    let baseline;
    async function readDisk() {
      const path = getICloudPath();
      if (!fm.fileExists(path)) throw new Error('pill_data_unavailable');
      if (fm.isFileStoredIniCloud(path)) await fm.downloadFileFromiCloud(path);
      return fm.readString(path);
    }
    async function assertCurrent() {
      if (await readDisk() !== baseline) throw new Error('pill_data_changed');
    }
    return {
    async loadData() {
      baseline = await readDisk();
      const existing = JSON.parse(baseline);
      if (!existing || !existing.medicamentos || typeof existing.medicamentos !== 'object' || Array.isArray(existing.medicamentos) || !Object.keys(existing.medicamentos).length || Object.values(existing.medicamentos).some(m => !m || typeof m !== 'object' || Array.isArray(m))) throw new Error('pill_data_unrecognized');
      return loadData();
    },
    assertCurrent,
    async saveData(data) {
      if (!_dataDirty) return;
      await assertCurrent(); await saveData(data);
      const saved = await readDisk();
      if (JSON.stringify(JSON.parse(saved)) !== JSON.stringify(data)) throw new Error('pill_save_unverified');
      baseline = saved;
    },
    getActiveMed, todayISO, getDoseState, getStockState,
    commands: { dose: markTodayTaken, bottle: addBottle, stock: editStock, 'history-add': addHistoryDay,
      'history-remove': removeHistoryDay, 'history-clear': clearHistory, pause: pauseMenu,
      add: addMedication, select: switchActiveMedication, remove: removeMedication, 'clear-icons': limparCacheImagens,
      async edit(data) {
        const active = getActiveMed(data), med = active.data;
        const time = await askTime('Horário ideal', 'Horário registrado para ' + active.name, med.horaIdeal);
        if (time === null) return;
        const minimum = await askNumber('Alerta de estoque', 'Quantidade mínima para o alerta visual.', med.alertaMinimo);
        if (minimum === null) return;
        const maximum = await askNumber('Estoque máximo', 'Máximo exibido no indicador de estoque.', med.maxEstoque);
        if (maximum === null) return;
        med.horaIdeal = time; med.alertaMinimo = minimum; med.maxEstoque = Math.max(maximum, Number(med.estoque) || 0);
        markDirty(); await showAlert('Salvo', 'Preferências do medicamento atualizadas.');
      }
    }
  }; })()`;
  const lastfmExports = `({
    cached: loadCachedState, build: buildRecentTracksHTML,
    async refresh() {
      const recentUrl = 'https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=' + encodeURIComponent(USER) + '&api_key=' + API_KEY + '&format=json&limit=30&extended=1';
      const userUrl = 'https://ws.audioscrobbler.com/2.0/?method=user.getinfo&user=' + encodeURIComponent(USER) + '&api_key=' + API_KEY + '&format=json';
      const [recent, profile, playbackDevice] = await Promise.all([fetchJSON(recentUrl), fetchJSON(userUrl), detectPlaybackSource()]);
      if (!recent || !recent.recenttracks || !Array.isArray(recent.recenttracks.track) || !profile || !profile.user) throw new Error('lastfm_unavailable');
      return { tracks: recent.recenttracks.track, user: profile.user, playbackDevice };
    }
  })`;
  const styles = `
    :root{color-scheme:dark;--bg:#101118;--card:rgba(255,255,255,.055);--ink:#f5f5fa;--muted:#9396a9;--line:rgba(255,255,255,.09);--accent:#9c91ff;--tint:rgba(141,121,255,.14)}
    :root[data-theme="light"]{color-scheme:light;--bg:#f7f7fb;--card:#fff;--ink:#20212d;--muted:#777a8c;--line:#e8e8f0;--accent:#6854dc;--tint:#eeebff}
    :root[data-theme="glass"]{--bg:transparent;--card:rgba(255,255,255,.08);--line:rgba(255,255,255,.14)}
    @media(prefers-color-scheme:light){:root[data-theme="system"]{color-scheme:light;--bg:#f7f7fb;--card:#fff;--ink:#20212d;--muted:#777a8c;--line:#e8e8f0;--accent:#6854dc;--tint:#eeebff}}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px -apple-system,BlinkMacSystemFont,sans-serif;padding:22px 20px calc(30px + env(safe-area-inset-bottom));-webkit-tap-highlight-color:transparent}button,input{font:inherit}button{cursor:pointer;color:inherit;border:0}button:disabled{opacity:.45;cursor:wait}button:focus-visible,input:focus-visible{outline:2px solid var(--accent);outline-offset:3px}header{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:24px}.eyebrow{font-size:10px;letter-spacing:2px;color:var(--accent);font-weight:700;text-transform:uppercase}h1{font-size:29px;letter-spacing:-1px;margin:6px 0 0}h2{font-size:17px;margin:0}h3{font-size:14px;margin:0}p{line-height:1.5}.muted,.sub{color:var(--muted);font-size:12px}.sub{margin-top:5px;line-height:1.4}.card{border:1px solid var(--line);border-radius:24px;background:var(--card);padding:20px;margin-bottom:14px;backdrop-filter:blur(18px)}.hero{background:linear-gradient(145deg,var(--tint),var(--card));position:relative;overflow:hidden}.hero h2{font-size:26px;letter-spacing:-.8px;margin:8px 0}.row{display:flex;justify-content:space-between;align-items:center;gap:12px}.metric{font-size:48px;font-weight:700;letter-spacing:-2px;font-variant-numeric:tabular-nums}.caps{font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase}.round{width:42px;height:42px;border-radius:50%;background:var(--card);border:1px solid var(--line);font-size:22px}.primary,.secondary{border-radius:15px;padding:13px 16px;min-height:46px;font-size:13px;font-weight:600}.primary{background:var(--accent);color:#fff}.secondary{background:var(--card);border:1px solid var(--line)}.wide{width:100%;margin-top:16px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.action{padding:17px;text-align:left;min-height:81px;border-radius:19px;background:var(--card);border:1px solid var(--line)}.action strong{display:block;font-size:13px;margin-top:9px}.action span{font-size:19px}.label{margin:25px 0 12px}.pill{display:inline-block;border-radius:20px;padding:6px 10px;font-size:10px;font-weight:600;background:var(--tint);color:var(--accent)}.event{width:100%;display:flex;gap:14px;text-align:left;background:none;padding:15px 0;border-bottom:1px solid var(--line)}.event:last-child{border:0}.day{width:46px;flex:none;text-align:center;background:var(--tint);color:var(--accent);border-radius:13px;padding:9px 4px}.day strong{font-size:21px;display:block;line-height:1.1}.day small{font-size:9px;text-transform:uppercase}.event .content{flex:1;align-self:center}.event h3{line-height:1.45}.filters{display:flex;gap:7px;margin-bottom:16px;overflow:auto}.filter{border-radius:22px;padding:9px 13px;white-space:nowrap;background:var(--card);color:var(--muted);font-size:12px}.filter.active{background:var(--tint);color:var(--accent)}.switches{display:flex;gap:8px;margin:15px 0}.switches button{flex:1;padding:10px;border-radius:12px;background:var(--card);font-size:11px;color:var(--muted)}.switches button[aria-pressed="true"]{color:var(--accent);background:var(--tint)}.history{display:flex;gap:7px;flex-wrap:wrap}.history span{font-size:11px;border-radius:9px;background:var(--card);border:1px solid var(--line);padding:8px}.notice{font-size:12px;color:var(--muted);margin:12px 0;min-height:16px}.empty{padding:28px;text-align:center;color:var(--muted);font-size:13px}.modal{position:fixed;inset:0;background:rgba(0,0,0,.4);backdrop-filter:blur(8px);display:flex;align-items:flex-end;z-index:20;padding:14px}.modal[hidden]{display:none}.sheet{background:var(--bg);color:var(--ink);border:1px solid var(--line);border-radius:28px;width:100%;max-height:88vh;overflow:auto;padding:25px 20px}.sheet h2{font-size:21px}.sheet p{white-space:pre-line;color:var(--muted);font-size:13px}.sheet input{display:block;width:100%;padding:15px;background:var(--card);border:1px solid var(--line);color:var(--ink);border-radius:13px;margin:10px 0;font-size:16px}.sheet .choices{display:grid;gap:9px;margin-top:20px}.danger{color:#ee6d89}.section-spacer{height:10px}@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}`;
  function page(title, accent, content, script, theme) {
    const chosen = ['dark', 'light', 'glass', 'system'].includes(theme) ? theme : 'system';
    return `<!doctype html><html lang="pt-BR" data-theme="${chosen}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>${escape(title)}</title><style>${styles}:root{--accent:${accent}}</style></head><body>${content}<div id="notice" class="notice" role="status" aria-live="polite"></div><div class="modal" id="modal" hidden><section class="sheet" role="dialog" aria-modal="true" aria-labelledby="dialogTitle" tabindex="-1"><h2 id="dialogTitle"></h2><p id="dialogMessage"></p><form id="dialogForm"><div id="dialogFields"></div><div id="dialogChoices" class="choices"></div></form></section></div><script>${script}</script></body></html>`;
  }
  const clientBase = `
    const escape = value => String(value == null ? '' : value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const node = id => document.getElementById(id);
    let working = false;
    async function call(action,payload={}) {
      if (working) return; working=true;
      document.querySelectorAll('button').forEach(b=>b.disabled=true);
      try { const response = await window.SaquettoApp.request(action,payload); if (!response || response.status === 'error') throw new Error('failed'); apply(response); }
      catch (_) { node('notice').textContent='Não foi possível concluir. Verifique antes de tentar novamente.'; }
      finally { working=false; document.querySelectorAll('button').forEach(b=>b.disabled=false); }
    }
    function detail(title,message) {
      node('dialogTitle').textContent=title;node('dialogMessage').textContent=message;node('dialogFields').replaceChildren();
      node('dialogChoices').innerHTML='<button type="button" class="primary" id="closeDetail">Fechar</button>';
      node('modal').hidden=false;node('closeDetail').onclick=()=>{node('modal').hidden=true;};
      document.querySelector('.sheet').focus();
    }
  `;
  function calendarHTML(state, theme) {
    return page('Super Calendário', '#d69d46', `<header><div><div class="eyebrow">Seu tempo, reunido</div><h1>Calendário</h1></div><button class="round" id="refresh" aria-label="Atualizar calendário">↻</button></header><section class="card hero"><div class="caps" id="today"></div><h2 id="nextTitle">Sua próxima história</h2><div class="sub" id="nextDate"></div><div class="switches"><button id="agendaToggle">Eventos</button><button id="birthdayToggle">Aniversários</button></div></section><div class="filters" role="group" aria-label="Filtrar calendário"><button class="filter active" data-filter="all">Todos</button><button class="filter" data-filter="agenda">Eventos</button><button class="filter" data-filter="birthday">Aniversários</button></div><main id="events"></main>`, clientBase + `
      let state=${json(state)}, filter='all';
      function apply(response){if(response.state)state=response.state;render();}
      function render(){
        node('today').textContent=new Date().toLocaleDateString('pt-BR',{weekday:'long',day:'numeric',month:'long'});
        const first=state.events[0];node('nextTitle').textContent=first?first.title:'Um espaço para você';
        node('nextDate').textContent=first?new Date(first.date).toLocaleDateString('pt-BR',{day:'numeric',month:'long'})+' · '+first.calendar:'Nenhum compromisso encontrado nos próximos 12 meses.';
        node('agendaToggle').setAttribute('aria-pressed',String(state.agenda));node('birthdayToggle').setAttribute('aria-pressed',String(state.birthdays));
        const events=state.events.filter(e=>filter==='all'||e.type===filter);let lastMonth='';
        node('events').innerHTML=events.map(e=>{const d=new Date(e.date),month=d.toLocaleDateString('pt-BR',{month:'long',year:'numeric'});const label=month!==lastMonth?'<div class="caps label">'+escape(month)+'</div>':'';lastMonth=month;return label+'<button class="event" data-event="'+e.id+'"><span class="day"><strong>'+d.getDate()+'</strong><small>'+escape(d.toLocaleDateString('pt-BR',{month:'short'}).replace('.',''))+'</small></span><span class="content"><h3>'+escape(e.title)+'</h3><div class="sub">'+escape(e.calendar)+(e.allDay?' · Dia inteiro':' · '+escape(d.toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'})))+'</div></span><span class="sub">'+(e.type==='birthday'?'🎈':'↗')+'</span></button>';}).join('')||'<div class="card empty">Nada por aqui neste filtro.</div>';
        document.querySelectorAll('[data-event]').forEach(b=>b.onclick=()=>{const e=state.events.find(e=>e.id===Number(b.dataset.event));detail(e.title,new Date(e.date).toLocaleString('pt-BR')+'\\n'+e.calendar+(e.allDay?'\\nDia inteiro':''));});
      }
      document.querySelectorAll('[data-filter]').forEach(b=>b.onclick=()=>{filter=b.dataset.filter;document.querySelectorAll('[data-filter]').forEach(x=>x.classList.toggle('active',x===b));render();});
      node('refresh').onclick=()=>call('calendar.refresh');node('agendaToggle').onclick=()=>call('calendar.toggle',{kind:'agenda'});node('birthdayToggle').onclick=()=>call('calendar.toggle',{kind:'birthdays'});render();
    `, theme);
  }
  const pillActions = [['dose', '✓', 'Registrar dose'], ['bottle', '＋', 'Novo pote'], ['stock', '▤', 'Editar estoque'], ['edit', '◷', 'Editar medicamento'], ['add', '✦', 'Adicionar medicamento'], ['select', '⇄', 'Medicamento do widget'], ['pause', 'Ⅱ', 'Pausar ou retomar'], ['history-add', '▦', 'Adicionar dia'], ['history-remove', '−', 'Remover dia'], ['history-clear', '⌫', 'Limpar histórico'], ['remove', '×', 'Remover medicamento'], ['clear-icons', '↻', 'Limpar cache de ícones']];
  function pillHTML(state, theme) {
    const buttons = pillActions.map(([id, icon, title]) => `<button class="action" data-action="${id}"><span aria-hidden="true">${icon}</span><strong>${title}</strong></button>`).join('');
    return page('PillTracker', '#8b80df', `<header><div><div class="eyebrow">Um cuidado de cada vez</div><h1>PillTracker</h1></div><span class="pill" id="medCount"></span></header><section class="card hero"><div class="row"><span class="caps">Medicamento do widget</span><span class="pill" id="doseStatus"></span></div><h2 id="medName"></h2><div class="row"><div><div class="metric" id="stock"></div><div class="caps">unidades em estoque</div></div><div><h2 id="doseTime"></h2><div class="sub">horário registrado</div></div></div><div class="sub" id="stockStatus"></div></section><div class="grid">${buttons}</div><div class="row label"><h2>Histórico registrado</h2><span class="sub" id="historyCount"></span></div><div class="history" id="history"></div>`, clientBase + `
      let state=${json(state)}, dialog=null;
      function apply(response){if(response.state)state=response.state;dialog=response.dialog||null;render();renderDialog();}
      function render(){const med=state.active;node('medName').textContent=med.name;node('stock').textContent=med.stock;node('doseTime').textContent=med.time;node('medCount').textContent=state.count+' medicamento'+(state.count===1?'':'s');node('doseStatus').textContent=med.paused?'Pausado':med.taken?'Registrada hoje':'A registrar';node('stockStatus').textContent='Alerta visual: '+med.minimum+' unidades · Máximo: '+med.maximum;node('historyCount').textContent=med.history.length+' registro'+(med.history.length===1?'':'s');node('history').innerHTML=med.history.slice().reverse().map(date=>'<span>'+escape(date.split('-').reverse().join('/'))+'</span>').join('')||'<p class="muted">Nenhuma dose registrada.</p>';}
      function renderDialog(){
        node('modal').hidden=!dialog;if(!dialog)return;
        node('dialogTitle').textContent=dialog.title;node('dialogMessage').textContent=dialog.message;
        node('dialogFields').innerHTML=dialog.fields.map((f,i)=>'<label class="sub">'+escape(f.placeholder||'Valor')+'<input data-field="'+i+'" value="'+escape(f.value)+'" placeholder="'+escape(f.placeholder)+'" autocomplete="off" autocapitalize="off" maxlength="300"></label>').join('');
        node('dialogChoices').innerHTML=dialog.actions.map((a,i)=>'<button type="button" class="'+(i===0?'primary':'secondary')+'" data-choice="'+i+'">'+escape(a)+'</button>').join('')+(dialog.cancel?'<button type="button" class="secondary" data-choice="-1">'+escape(dialog.cancel)+'</button>':'');
        document.querySelectorAll('[data-choice]').forEach(b=>b.onclick=()=>submit(Number(b.dataset.choice)));
        const input=node('dialogFields').querySelector('input');if(input)input.focus();else document.querySelector('.sheet').focus();
      }
      function submit(choice){call('dialog.submit',{id:dialog.id,choice,fields:[...document.querySelectorAll('[data-field]')].map(e=>e.value)});}
      node('dialogForm').onsubmit=e=>{e.preventDefault();if(dialog&&dialog.actions.length)submit(0);};
      document.querySelectorAll('[data-action]').forEach(b=>b.onclick=()=>call('pill.command',{command:b.dataset.action}));
      document.addEventListener('keydown',e=>{if(e.key==='Escape'&&dialog&&dialog.cancel){e.preventDefault();submit(-1);}});render();
    `, theme);
  }
  function calendarState(value) {
    return { agenda: value.agenda === true, birthdays: value.birthdays === true, events: (value.events || []).slice(0, 100).map((event, id) => ({ id, type: event.type === 'birthday' ? 'birthday' : 'agenda', title: String(event.title || ''), date: new Date(event.date).toISOString(), calendar: String(event.calName || ''), allDay: !!event.isAllDay })) };
  }
  function safeURL(value) {
    // Scriptable's JavaScriptCore does not promise the browser URL global.
    return typeof value === 'string' && /^https:\/\/[a-z0-9.-]+(?::[0-9]+)?(?:[/?#][^\s"'<>\\]*)?$/i.test(value) ? value : '';
  }
  function lastfmState(value) {
    const text = v => escape(String(v == null ? '' : v).slice(0, 1000));
    const rawUser = value && value.user || {};
    const image = list => Array.isArray(list) ? list.slice(0, 6).map(i => ({ size: ['small', 'medium', 'large', 'extralarge', 'mega'].includes(i.size) ? i.size : 'small', '#text': safeURL(i['#text']) })) : [];
    return { user: { name: text(rawUser.name), realname: text(rawUser.realname), playcount: String(Number(rawUser.playcount) || 0), image: image(rawUser.image), url: safeURL(rawUser.url) },
      tracks: (value && Array.isArray(value.tracks) ? value.tracks : []).slice(0, 30).map(t => ({ name: text(t.name), artist: { '#text': text(t.artist && (t.artist['#text'] || t.artist.name)) }, album: { '#text': text(t.album && t.album['#text']) }, url: safeURL(t.url), image: image(t.image), date: t.date ? { uts: String(Math.max(0, Number(t.date.uts) || 0)) } : undefined, '@attr': { nowplaying: t['@attr'] && t['@attr'].nowplaying === 'true' ? 'true' : 'false' } })),
      playbackDevice: text(value && value.playbackDevice) || null };
  }
  function enrichLastfm(html, theme) {
    const chosen = ['dark', 'light', 'glass', 'system'].includes(theme) ? theme : 'system';
    const css = `<style>:root[data-saquetto-theme="light"]{color-scheme:light;--bg:#f7f7fb;--card-bg:#fff;--card-border:#e8e8f0;--card-hover:#f0eff6;--text-main:#20212d;--text-muted:#777a8c;--text-sub:#777a8c}:root[data-saquetto-theme="glass"]{--bg:transparent;--card-bg:rgba(255,255,255,.08)}@media(prefers-color-scheme:light){:root[data-saquetto-theme="system"]{color-scheme:light;--bg:#f7f7fb;--card-bg:#fff;--card-border:#e8e8f0;--text-main:#20212d;--text-muted:#777a8c;--text-sub:#777a8c}}#saquettoLastfmStatus{font:11px -apple-system,sans-serif;color:var(--text-muted);padding:10px 22px}#saquettoTrackDetail{position:fixed;inset:0;z-index:100;background:var(--bg);color:var(--text-main);padding:30px 24px;overflow:auto}#saquettoTrackDetail[hidden]{display:none}#saquettoTrackDetail button{padding:12px 18px;background:var(--card-bg);border:1px solid var(--card-border);border-radius:20px;color:var(--text-main);font:14px -apple-system,sans-serif}#saquettoTrackDetail h2{font:700 30px -apple-system,sans-serif;margin:36px 0 16px}#saquettoTrackDetail p{font:15px/1.7 -apple-system,sans-serif;color:var(--text-muted)}@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}</style>`;
    html = html.replace('<html lang="pt-BR">', `<html lang="pt-BR" data-saquetto-theme="${chosen}">`).replace('</head>', css + '</head>');
    return html.replace('</body>', `<div id="saquettoLastfmStatus" role="status" aria-live="polite">Dados locais; atualizando…</div><section id="saquettoTrackDetail" hidden><button id="saquettoCloseTrack">‹ Voltar</button><h2 id="saquettoTrackTitle"></h2><p id="saquettoTrackInfo"></p></section><script>
      document.addEventListener('click',function(event){const link=event.target.closest('a');if(!link)return;event.preventDefault();event.stopPropagation();const track=allTracks.find(t=>t.url===link.href);const title=document.getElementById('saquettoTrackTitle'),info=document.getElementById('saquettoTrackInfo');
        const decode=value=>{const t=document.createElement('textarea');t.innerHTML=value||'';return t.value;};
        title.textContent=track?decode(track.name):decode(currentUser.realname||currentUser.name);
        info.textContent=track?[decode(track.artist&&track.artist['#text']),decode(track.album&&track.album['#text']),track['@attr']&&track['@attr'].nowplaying==='true'?'Tocando agora':track.date?new Date(Number(track.date.uts)*1000).toLocaleString('pt-BR'):''].filter(Boolean).join(' · '):String(currentUser.playcount||0)+' scrobbles';
        document.getElementById('saquettoTrackDetail').hidden=false;},true);
      document.getElementById('saquettoCloseTrack').onclick=()=>{document.getElementById('saquettoTrackDetail').hidden=true;};
    </script></body>`);
  }
  function create({ loader, render, theme = 'system' }) {
    const selectedTheme = typeof theme === 'function' ? theme() : theme;
    async function openCalendar() {
      const core = await loader.load(ids.calendar, calendarExports, calendarTransform);
      let disposed = false, busy = false;
      const state = calendarState(await core.snapshot());
      return { html: calendarHTML(state, selectedTheme), async handle(action, payload = {}) {
        if (disposed || busy) return { status: 'error', reason: disposed ? 'disposed' : 'busy' };
        busy = true;
        try {
          if (action === 'calendar.refresh') return { status: 'ok', state: calendarState(await core.snapshot()) };
          if (action === 'calendar.toggle' && ['agenda', 'birthdays'].includes(payload.kind)) return { status: 'ok', state: calendarState(await core.toggle(payload.kind)) };
          return { status: 'error', reason: 'invalid_action' };
        } catch (_) { return { status: 'error', reason: 'calendar_unavailable' }; } finally { busy = false; }
      }, dispose() { disposed = true; } };
    }
    async function openPill() {
      let disposed = false, operation = null, pending = null, waiter = null, serial = 0, lastError = false;
      const notify = () => { if (waiter) { const wake = waiter; waiter = null; wake(); } };
      const next = () => new Promise(resolve => { waiter = resolve; });
      class WebAlert {
        constructor() { this.title = ''; this.message = ''; this.fields = []; this.actions = []; this.cancel = ''; }
        addTextField(placeholder = '', value = '') { this.fields.push({ placeholder: String(placeholder), value: String(value) }); }
        addAction(label) { this.actions.push(String(label)); }
        addDestructiveAction(label) { this.addAction(label); }
        addCancelAction(label) { this.cancel = String(label); }
        textFieldValue(index) { return this.fields[index] ? this.fields[index].value : ''; }
        presentAlert() { return this.presentSheet(); }
        presentSheet() {
          if (disposed) return Promise.resolve(-1);
          return new Promise(resolve => { pending = { id: ++serial, alert: this, resolve }; notify(); });
        }
      }
      const core = await loader.load(ids.pill, pillExports, pillTransform, { Alert: WebAlert });
      const data = await core.loadData();
      function snapshot() {
        const active = core.getActiveMed(data), med = active.data;
        return { count: Object.keys(data.medicamentos).length, active: { name: active.name, stock: Number(med.estoque) || 0, maximum: Number(med.maxEstoque) || 0, minimum: Number(med.alertaMinimo) || 0, time: String(med.horaIdeal || ''), paused: !!med.pausado, taken: Array.isArray(med.historico) && med.historico.includes(core.todayISO()), history: Array.isArray(med.historico) ? med.historico.map(String) : [] } };
      }
      function response() {
        const dialog = pending ? { id: pending.id, title: pending.alert.title, message: pending.alert.message, fields: pending.alert.fields.map(f => ({ ...f })), actions: pending.alert.actions.slice(), cancel: pending.alert.cancel } : null;
        return { status: lastError ? 'error' : 'ok', state: snapshot(), dialog, ...(lastError ? { reason: 'pill_operation_failed' } : {}) };
      }
      return { html: pillHTML(snapshot(), selectedTheme), async handle(action, payload = {}) {
        if (disposed) return { status: 'error', reason: 'disposed' };
        if (action === 'pill.command') {
          if (lastError) return { status: 'error', reason: 'session_requires_reload' };
          if (operation || !Object.prototype.hasOwnProperty.call(core.commands, payload.command)) return { status: 'error', reason: operation ? 'busy' : 'invalid_action' };
          lastError = false; const waiting = next();
          operation = Promise.resolve().then(async () => { try { await core.assertCurrent(); await core.commands[payload.command](data); await core.saveData(data); } catch (_) { lastError = true; } finally { operation = null; notify(); } });
          await waiting; return response();
        }
        if (action === 'dialog.submit') {
          if (!pending || payload.id !== pending.id || !Number.isInteger(payload.choice) || payload.choice < -1 || payload.choice >= pending.alert.actions.length || payload.choice === -1 && !pending.alert.cancel || !Array.isArray(payload.fields) || payload.fields.length !== pending.alert.fields.length || payload.fields.some(v => typeof v !== 'string' || v.length > 300)) return { status: 'error', reason: 'invalid_dialog' };
          const dialog = pending, waiting = next(); pending = null;
          payload.fields.forEach((value, i) => { dialog.alert.fields[i].value = value; }); dialog.resolve(payload.choice);
          await waiting; return response();
        }
        return { status: 'error', reason: 'invalid_action' };
      }, async dispose() {
        disposed = true;
        if (pending) { const dialog = pending; pending = null; dialog.resolve(-1); }
        if (operation) await operation;
      } };
    }
    async function openLastfm() {
      const core = await loader.load(ids.lastfm, lastfmExports, lastfmTransform);
      let disposed = false, busy = false;
      const state = lastfmState(core.cached());
      const html = enrichLastfm(core.build(state), selectedTheme);
      return { html, async handle(action) {
        if (disposed || busy || action !== 'lastfm.refresh') return { status: 'error', reason: disposed ? 'disposed' : busy ? 'busy' : 'invalid_action' };
        busy = true; try { return { status: 'ok', state: lastfmState(await core.refresh()) }; } catch (_) { return { status: 'error', reason: 'lastfm_unavailable' }; } finally { busy = false; }
      }, dispose() { disposed = true; } };
    }
    return Object.freeze({ async open(appId) {
      if (appId === ids.calendar) return openCalendar();
      if (appId === ids.pill) return openPill();
      if (appId === ids.lastfm) return openLastfm();
      throw new Error('unsupported_native_app');
    } });
  }
  return Object.freeze({ create });
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoNativeApps;
