// Menus live in the saquettoOS shell. Extraction and downloads remain in the
// user's hash-approved local script; no private source or configuration is bundled.
const SaquettoMediaApp = (() => {
  const APP_ID = 'media-downloader';
  const ENTRYPOINT = '\n// ── DISPARO AUTOMÁTICO SE EM AMBIENTE SCRIPTABLE ──\n';
  const TAIL = /^\n\/\/ ── DISPARO AUTOMÁTICO SE EM AMBIENTE SCRIPTABLE ──\nif \(typeof Script !== "undefined"\) \{\n  \(async function \(\) \{\n    try \{\n      await main\(\);\n    \} catch \(err\) \{[\s\S]*?\n  \}\)\(\);\n\}\n\n\/\/ ── EXPORTAÇÃO PARA TESTES UNITÁRIOS EM NODE\.JS ──\nif \(typeof module !== "undefined" && module\.exports\) \{\n  module\.exports = \{[\s\S]*?\n    main: main\n  \};\n\}\s*$/;
  function transform(source) {
    const at = source.indexOf(ENTRYPOINT);
    if (at < 0 || at !== source.lastIndexOf(ENTRYPOINT) || !TAIL.test(source.slice(at))) throw new Error('media_entrypoint_changed');
    return source.slice(0, at);
  }
  const text = (value, max = 1200) => String(value == null ? '' : value).replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '').slice(0, max);
  function validUrl(value) {
    if (typeof value !== 'string' || value.length > 4096) return '';
    const clean = value.trim();
    if (/[\u0000-\u0020\\]/.test(clean)) return '';
    const match = /^https?:\/\/([^/?#]+)(?:[/?#].*)?$/i.exec(clean);
    return match && /^(?:[a-z0-9.-]+|\[[a-f0-9:]+\])(?::\d{1,5})?$/i.test(match[1]) ? clean : '';
  }
  const safeTheme = value => ['dark', 'light', 'glass', 'system'].includes(value) ? value : 'system';
  const styles = `:root{color-scheme:dark;--bg:#101118;--card:rgba(255,255,255,.055);--ink:#f4f5fa;--muted:#9599aa;--line:rgba(255,255,255,.1);--accent:#48cbd0;--tint:rgba(72,203,208,.12)}:root[data-theme=light]{color-scheme:light;--bg:#f7f8fb;--card:#fff;--ink:#202533;--muted:#697183;--line:#e5e9ef;--accent:#07858d;--tint:#e5f5f5}:root[data-theme=glass]{--bg:transparent;--card:rgba(255,255,255,.085);--line:rgba(255,255,255,.14)}@media(prefers-color-scheme:light){:root[data-theme=system]{color-scheme:light;--bg:#f7f8fb;--card:#fff;--ink:#202533;--muted:#697183;--line:#e5e9ef;--accent:#07858d;--tint:#e5f5f5}}*{box-sizing:border-box}body{margin:0;padding:24px 20px calc(30px + env(safe-area-inset-bottom));background:var(--bg);color:var(--ink);font:15px -apple-system,BlinkMacSystemFont,sans-serif;-webkit-tap-highlight-color:transparent}button,input{font:inherit}button{cursor:pointer;color:inherit;border:0;min-height:46px}button:disabled{opacity:.45;cursor:wait}button:focus-visible,input:focus-visible{outline:2px solid var(--accent);outline-offset:3px}.eyebrow{color:var(--accent);font-size:10px;letter-spacing:2px;text-transform:uppercase;font-weight:700}h1{font-size:30px;letter-spacing:-1px;margin:8px 0 10px}h2{font-size:20px;margin:0 0 8px}p{font-size:13px;line-height:1.55;color:var(--muted)}.card{border:1px solid var(--line);background:var(--card);border-radius:24px;padding:21px;margin:20px 0;backdrop-filter:blur(18px)}.hero{background:linear-gradient(145deg,var(--tint),var(--card))}.hero-mark{font-size:32px;color:var(--accent);margin-bottom:20px}.hero strong{display:block;font-size:21px;letter-spacing:-.5px}.platforms{display:flex;gap:7px;flex-wrap:wrap;margin:17px 0 0}.platforms span{font-size:10px;border:1px solid var(--line);padding:6px 9px;border-radius:15px;color:var(--muted)}label{display:block;font-size:12px;font-weight:600;margin-bottom:10px}input{width:100%;border:1px solid var(--line);background:var(--bg);color:var(--ink);border-radius:15px;padding:15px;font-size:16px;margin-bottom:12px}.primary{width:100%;background:var(--accent);color:#062a2c;border-radius:15px;padding:14px;font-weight:700;font-size:13px}.secondary{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px 15px;font-size:12px}.state{display:flex;gap:12px;align-items:center;min-height:50px;font-size:13px;line-height:1.5;color:var(--muted)}.spinner{height:18px;width:18px;flex:none;border:2px solid var(--line);border-top-color:var(--accent);border-radius:50%;animation:spin .9s linear infinite}.spinner[hidden]{display:none}.rows{display:grid;gap:10px}.row{display:flex;gap:15px;align-items:center;width:100%;text-align:left;border:1px solid var(--line);background:var(--card);border-radius:19px;padding:16px}.row.header{background:var(--tint);border-color:transparent}.cell-text{min-width:0;flex:1}.cell-text strong{display:block;font-size:13px;line-height:1.45;overflow-wrap:anywhere}.cell-text small{display:block;color:var(--muted);font-size:11px;line-height:1.45;margin-top:5px;overflow-wrap:anywhere}.thumb{width:58px;height:65px;object-fit:contain;border-radius:12px;background:var(--tint)}.row.header .thumb{width:42px;height:42px}.cell-button{padding:8px;border-radius:12px;background:var(--tint);color:var(--accent);font-size:12px}.modal{position:fixed;inset:0;background:rgba(0,0,0,.4);backdrop-filter:blur(8px);padding:14px;display:flex;align-items:flex-end;z-index:20}.modal[hidden]{display:none}.sheet{background:var(--bg);border:1px solid var(--line);border-radius:27px;padding:24px 20px;width:100%;max-height:88vh;overflow:auto}.sheet p{white-space:pre-line}.choices{display:grid;gap:9px}.danger{color:#eb7490}.footer{margin:22px 0;color:var(--muted);font-size:11px;line-height:1.5}@keyframes spin{to{transform:rotate(360deg)}}@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}`;
  function html(theme) {
    return `<!doctype html><html lang="pt-BR" data-theme="${safeTheme(theme)}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="referrer" content="no-referrer"><title>Media Downloader</title><style>${styles}</style></head><body><header><div class="eyebrow">Sua biblioteca, do seu jeito</div><h1>Media Downloader</h1></header><section class="card hero"><div class="hero-mark" aria-hidden="true">↙</div><strong>Guarde o que vale rever.</strong><p>Fotos, vídeos, perfis e cards de posts. Cole um link para escolher o que salvar.</p><div class="platforms"><span>Instagram</span><span>X</span><span>Threads</span><span>TikTok</span><span>YouTube</span></div></section><form id="urlForm" class="card"><label for="mediaUrl">Link do post ou perfil</label><input id="mediaUrl" type="url" inputmode="url" placeholder="https://…" autocomplete="off" autocapitalize="off" spellcheck="false" required><button class="primary" id="start">Encontrar mídia</button></form><div class="state" aria-live="polite" role="status"><span class="spinner" id="spinner" hidden></span><span id="status">Pronto para receber um link.</span></div><main id="rows" class="rows"></main><button class="secondary" id="cancel" hidden>Cancelar seleção</button><p class="footer">Fotos e compartilhamento usam os controles do iOS quando você escolher salvar.</p><div class="modal" id="modal" hidden><section class="sheet" role="dialog" aria-modal="true" aria-labelledby="dialogTitle" tabindex="-1"><h2 id="dialogTitle"></h2><p id="dialogMessage"></p><form id="dialogForm"><div id="fields"></div><div id="choices" class="choices"></div></form></section></div><script>
      const byId=id=>document.getElementById(id);let state={phase:'idle'},requesting=false,timer=null,dialogId=null;
      const label=(tag,value,cls)=>{const n=document.createElement(tag);n.textContent=value||'';if(cls)n.className=cls;return n;};
      function schedule(){clearTimeout(timer);if(state.phase==='working')timer=setTimeout(()=>call('media.state'),800);}
      async function call(action,payload={}){if(requesting)return;requesting=true;byId('start').disabled=true;try{const result=await window.SaquettoApp.request(action,payload);if(!result||result.status==='error'){byId('status').textContent='Não foi possível concluir. Verifique o link e tente novamente.';}else{state=result.state;draw();}}catch(_){byId('status').textContent='Não foi possível continuar agora.';}finally{requesting=false;byId('start').disabled=['working','table','dialog'].includes(state.phase);schedule();}}
      function draw(){byId('status').textContent=state.message;byId('spinner').hidden=state.phase!=='working';byId('mediaUrl').disabled=['working','table','dialog'].includes(state.phase);byId('rows').replaceChildren();const table=state.table;byId('cancel').hidden=!table;
        if(table)table.rows.forEach((row,index)=>{const container=document.createElement(row.selectable?'button':'div');container.className='row'+(row.header?' header':'');if(row.selectable){container.type='button';container.onclick=()=>call('media.row',{id:table.id,row:index});}row.cells.forEach((cell,cellIndex)=>{if(cell.kind==='image'){if(cell.image){const img=document.createElement('img');img.className='thumb';img.src=cell.image;img.alt='';container.append(img);}return;}const box=document.createElement('span');box.className='cell-text';if(cell.kind==='button'){const b=label('button',cell.title,'cell-button');b.type='button';b.onclick=e=>{e.stopPropagation();call('media.cell',{id:table.id,row:index,cell:cellIndex});};box.append(b);}else{box.append(label('strong',cell.title));if(cell.subtitle)box.append(label('small',cell.subtitle));}container.append(box);});byId('rows').append(container);});
        const dialog=state.dialog;byId('modal').hidden=!dialog;if(!dialog){dialogId=null;byId('fields').replaceChildren();byId('choices').replaceChildren();return;}if(dialogId===dialog.id)return;dialogId=dialog.id;byId('dialogTitle').textContent=dialog.title;byId('dialogMessage').textContent=dialog.message;byId('fields').replaceChildren();dialog.fields.forEach((field,index)=>{const input=document.createElement('input');input.type=field.secure?'password':'text';input.placeholder=field.placeholder;input.value=field.value||'';input.autocomplete='off';input.dataset.field=String(index);input.setAttribute('aria-label',field.placeholder||'Campo '+(index+1));byId('fields').append(input);});byId('choices').replaceChildren();dialog.actions.forEach((action,index)=>{const button=label('button',action.title,action.destructive?'secondary danger':'primary');button.type='button';button.onclick=()=>submit(index);byId('choices').append(button);});if(dialog.cancel){const button=label('button',dialog.cancel,'secondary');button.type='button';button.onclick=()=>submit(-1);byId('choices').append(button);}document.querySelector('.sheet').focus();}
      function submit(choice){const fields=Array.from(byId('fields').querySelectorAll('input')).map(input=>input.value);call('media.dialog',{id:dialogId,choice,fields});}
      byId('urlForm').onsubmit=e=>{e.preventDefault();call('media.start',{url:byId('mediaUrl').value});};byId('dialogForm').onsubmit=e=>{e.preventDefault();submit(0);};byId('cancel').onclick=()=>call('media.cancel',{id:state.table.id});document.addEventListener('keydown',e=>{if(e.key==='Escape'&&state.dialog&&state.dialog.cancel)submit(-1);});window.addEventListener('pagehide',()=>clearTimeout(timer));draw();
    </script></body></html>`;
  }

  function create({ loader, render, theme = 'system' }) {
    void render;
    return Object.freeze({ async open() {
      let disposed = false, operation = null, pending = null, serial = 0, callbackBusy = false;
      let phase = 'idle', message = 'Pronto para receber um link.', imageBudget = 1500000;
      const effects = { photos: 0, videos: 0, shared: false, previewed: false };
      const guard = () => { if (disposed) throw new Error('media_disposed'); };
      function imageData(image) {
        if (!image || typeof Data === 'undefined' || typeof Data.fromPNG !== 'function') return '';
        try { const encoded = Data.fromPNG(image).toBase64String();
          if (typeof encoded !== 'string' || encoded.length > 400000 || encoded.length > imageBudget || !/^[A-Za-z0-9+/=]+$/.test(encoded)) return '';
          imageBudget -= encoded.length; return 'data:image/png;base64,' + encoded;
        } catch (_) { return ''; }
      }
      class WebCell {
        constructor(kind, title = '', subtitle = '', image = '') { this.kind = kind; this.title = title; this.subtitle = subtitle; this.image = image; this.dismissOnTap = false; }
        static text(title, subtitle) { return new WebCell('text', title, subtitle); }
        static button(title) { return new WebCell('button', title); }
        static image(image) { return new WebCell('image', '', '', imageData(image)); }
        static imageAtURL(url) { const cell = new WebCell('image'); cell.nativeUrl = String(url); return cell; }
        leftAligned() {} centerAligned() {} rightAligned() {}
      }
      class WebRow {
        constructor() { this.cells = []; this.isHeader = false; this.dismissOnSelect = true; }
        addCell(cell) { this.cells.push(cell); return cell; }
        addText(title, subtitle = '') { return this.addCell(WebCell.text(title, subtitle)); }
        addButton(title) { return this.addCell(WebCell.button(title)); }
        addImage(image) { return this.addCell(WebCell.image(image)); }
        addImageAtURL(url) { return this.addCell(WebCell.imageAtURL(url)); }
      }
      class WebTable {
        constructor() { this.rows = []; }
        addRow(row) { this.rows.push(row); }
        removeRow(row) { this.rows = this.rows.filter(item => item !== row); }
        removeAllRows() { this.rows = []; }
        reload() { guard(); }
        async present() {
          guard();
          if (pending) throw new Error('media_dialog_busy');
          // URLs stay native; the shell receives only raster bytes, never signed URLs.
          if (typeof Request !== 'undefined') await Promise.all(this.rows.flatMap(row => row.cells).filter(cell => cell.nativeUrl).map(async cell => {
            try { const url = validUrl(cell.nativeUrl); if (!/^https:\/\//i.test(url)) return;
              const request = new Request(url); request.timeoutInterval = 8;
              const image = await request.loadImage(); if (!disposed) cell.image = imageData(image);
            } catch (_) {} finally { delete cell.nativeUrl; }
          }));
          guard(); phase = 'table'; message = 'Escolha o que deseja salvar.';
          return new Promise(resolve => { pending = { kind: 'table', id: ++serial, table: this, resolve }; });
        }
      }
      class WebAlert {
        constructor() { this.title = ''; this.message = ''; this.fields = []; this.actions = []; this.cancel = ''; }
        addTextField(placeholder = '', value = '') { const field = { placeholder: text(placeholder), value: String(value), secure: false }; this.fields.push(field); return field; }
        addSecureTextField(placeholder = '', value = '') { const field = this.addTextField(placeholder, value); field.secure = true; return field; }
        addAction(title) { this.actions.push({ title: text(title), destructive: false }); }
        addDestructiveAction(title) { this.actions.push({ title: text(title), destructive: true }); }
        addCancelAction(title) { this.cancel = text(title); }
        textFieldValue(index) { return this.fields[index]?.value || ''; }
        presentAlert() { return this.presentSheet(); }
        present() { return this.presentSheet(); }
        presentSheet() {
          if (disposed) return Promise.resolve(-1);
          if (pending) return Promise.reject(new Error('media_dialog_busy'));
          phase = 'dialog'; message = 'Responda para continuar.';
          return new Promise(resolve => { pending = { kind: 'dialog', id: ++serial, alert: this, resolve }; });
        }
      }
      function snapshot() {
        const state = { phase, message, table: null, dialog: null };
        if (pending?.kind === 'table') state.table = { id: pending.id, rows: pending.table.rows.map(row => ({ header: !!row.isHeader, selectable: typeof row.onSelect === 'function', cells: row.cells.map(cell => ({ kind: cell.kind, title: text(cell.title), subtitle: text(cell.subtitle), image: cell.image || '', tappable: typeof cell.onTap === 'function' })) })) };
        if (pending?.kind === 'dialog') state.dialog = { id: pending.id, title: text(pending.alert.title), message: text(pending.alert.message), actions: pending.alert.actions.map(action => ({ ...action })), cancel: pending.alert.cancel, fields: pending.alert.fields.map(field => ({ placeholder: field.placeholder, secure: field.secure, value: field.secure ? '' : text(field.value, 4096) })) };
        return state;
      }
      const response = () => ({ status: 'ok', state: snapshot() });
      function guardedObject(native, after) {
        return new Proxy(native, { get(target, key) {
          const value = target[key];
          if (typeof value !== 'function') return value;
          return async (...args) => { guard(); const result = await value.apply(target, args); guard(); if (after) after(key, result); return result; };
        } });
      }
      function overrides(url) {
        const result = { args: { shortcutParameter: url, queryParameters: {}, urls: [], plainTexts: [], images: [], fileURLs: [], notification: null },
          UITable: WebTable, UITableRow: WebRow, UITableCell: WebCell, Alert: WebAlert,
          console: { log() {}, warn() {}, error() {}, debug() {} } };
        if (typeof Photos !== 'undefined') result.Photos = guardedObject(Photos, key => { if (key === 'saveVideo') effects.videos++; else if (key === 'save' || key === 'saveImage') effects.photos++; });
        if (typeof ShareSheet !== 'undefined') result.ShareSheet = guardedObject(ShareSheet, key => { if (key === 'present') effects.shared = true; });
        if (typeof QuickLook !== 'undefined') result.QuickLook = guardedObject(QuickLook, key => { if (key === 'present') effects.previewed = true; });
        if (typeof Notification !== 'undefined') result.Notification = new Proxy(Notification, { construct(Target, args) {
          guard(); const notification = new Target(...args);
          return new Proxy(notification, { get(target, key) {
            if (key === 'schedule') return async () => {
              guard();
              // The standalone script has optimistic success messages. Only announce
              // native operations observed here, without claiming a shared video was saved.
              if (!effects.photos && !effects.videos && !effects.shared && !effects.previewed) return;
              target.body = outcomeMessage();
              return await target.schedule();
            };
            const value = target[key]; return typeof value === 'function' ? value.bind(target) : value;
          } });
        } });
        if (typeof Request !== 'undefined') result.Request = new Proxy(Request, { construct(Target, args) {
          guard(); const request = new Target(...args);
          return new Proxy(request, { get(target, key) { const value = target[key]; if (typeof value !== 'function') return value;
            return (...values) => { guard(); const output = value.apply(target, values); return output && typeof output.then === 'function' ? output.then(resolved => { guard(); return resolved; }) : output; };
          } });
        } });
        return result;
      }
      async function start(url) {
        const core = await loader.load(APP_ID, '({ main, extractPostTarget })', transform, overrides(url));
        guard();
        const result = await core.main();
        guard();
        if (result?.cancelled) { phase = 'cancelled'; message = 'Seleção cancelada.'; }
        else if (effects.photos || effects.videos || effects.shared || effects.previewed) { phase = 'done'; message = outcomeMessage(); }
        else { phase = 'error'; message = 'Nenhum salvamento confirmado. Verifique o link e tente novamente.'; }
      }
      function outcomeMessage() {
        if (effects.photos || effects.videos) return [effects.photos ? effects.photos + ' foto(s)' : '', effects.videos ? effects.videos + ' vídeo(s)' : ''].filter(Boolean).join(' e ') + ' salvo(s) na galeria.';
        return effects.shared ? 'Menu de compartilhamento encerrado.' : 'Visualização encerrada.';
      }
      function release(value) {
        const active = pending; pending = null; phase = 'working'; message = 'Processando a mídia…'; active.resolve(value);
      }
      return {
        html: html(typeof theme === 'function' ? theme() : theme),
        async handle(action, payload = {}) {
          if (disposed) return { status: 'error', reason: 'disposed' };
          if (action === 'media.state') return response();
          if (action === 'media.start') {
            if (operation || pending || callbackBusy) return { status: 'error', reason: 'busy' };
            const url = validUrl(payload.url);
            if (!url) return { status: 'error', reason: 'invalid_url' };
            effects.photos = effects.videos = 0; effects.shared = effects.previewed = false;
            imageBudget = 1500000;
            phase = 'working'; message = 'Buscando o post e preparando suas opções…';
            operation = Promise.resolve().then(() => start(url)).catch(() => { if (!disposed) { phase = 'error'; message = 'Não foi possível obter ou salvar essa mídia. Tente novamente.'; } }).finally(() => { operation = null; });
            return response();
          }
          if (action === 'media.cancel' && pending?.kind === 'table' && payload.id === pending.id && !callbackBusy) { release(); return response(); }
          if (action === 'media.dialog') {
            if (pending?.kind !== 'dialog' || payload.id !== pending.id || !Number.isInteger(payload.choice) || payload.choice < -1 || payload.choice >= pending.alert.actions.length || payload.choice === -1 && !pending.alert.cancel || !Array.isArray(payload.fields) || payload.fields.length !== pending.alert.fields.length || payload.fields.some(value => typeof value !== 'string' || value.length > 4096)) return { status: 'error', reason: 'invalid_dialog' };
            payload.fields.forEach((value, i) => { if (!pending.alert.fields[i].secure || value) pending.alert.fields[i].value = value; });
            release(payload.choice); return response();
          }
          if (action === 'media.row' || action === 'media.cell') {
            if (pending?.kind !== 'table' || payload.id !== pending.id || callbackBusy || !Number.isInteger(payload.row)) return { status: 'error', reason: 'invalid_selection' };
            const active = pending, row = active.table.rows[payload.row];
            const cell = action === 'media.cell' && Number.isInteger(payload.cell) ? row?.cells[payload.cell] : null;
            const callback = action === 'media.row' ? row?.onSelect : cell?.onTap;
            if (typeof callback !== 'function') return { status: 'error', reason: 'invalid_selection' };
            callbackBusy = true;
            try {
              await callback();
              if (pending === active && (action === 'media.row' ? row.dismissOnSelect : cell.dismissOnTap)) release();
              return response();
            } catch (_) { return { status: 'error', reason: 'selection_failed' }; } finally { callbackBusy = false; }
          }
          return { status: 'error', reason: 'invalid_action' };
        },
        dispose() {
          disposed = true;
          if (pending) { const active = pending; pending = null; active.resolve(active.kind === 'dialog' ? -1 : undefined); }
          phase = 'disposed'; message = '';
        },
      };
    } });
  }
  return Object.freeze({ create });
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoMediaApp;
