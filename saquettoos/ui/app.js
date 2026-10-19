(function () {
  'use strict';

  const $ = (selector) => document.querySelector(selector);
  const escapeHTML = (value) => String(value == null ? '' : value).replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
  const paths = {
    bell: '<path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4"/>',
    search: '<circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 4.5 4.5"/>',
    close: '<path d="m6 6 12 12M18 6 6 18"/>',
    chevron: '<path d="m9 5 7 7-7 7"/>',
    back: '<path d="m15 5-7 7 7 7"/>',
    external: '<path d="M14 3h7v7m-1-6-9 9M10 4H4v16h16v-6"/>',
    sparkles: '<path d="m12 3 2.5 6.5L21 12l-6.5 2.5L12 21l-2.5-6.5L3 12l6.5-2.5L12 3ZM20 2v4m-2-2h4"/>',
    settings: '<path d="m9 3-1 3-3 1 1 3-2 2 2 2-1 3 3 1 1 3h6l1-3 3-1-1-3 2-2-2-2 1-3-3-1-1-3Z"/><circle cx="12" cy="12" r="3"/>',
    download: '<path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5"/>',
    refresh: '<path d="M20 7v5h-5M4 17v-5h5M6 7a7 7 0 0 1 11.5-2L20 8M4 16l2.5 3A7 7 0 0 0 18 17"/>',
    check: '<path d="m5 12 4 4L19 6"/>',
    lock: '<rect x="5" y="10" width="14" height="11" rx="3"/><path d="M8 10V7a4 4 0 0 1 8 0v3M12 14v3"/>',
    users: '<circle cx="9" cy="8" r="3"/><path d="M3 20v-3a6 6 0 0 1 12 0v3M16 5a3 3 0 0 1 0 6m2 3a5 5 0 0 1 3 4v2"/>',
    user: '<circle cx="12" cy="8" r="4"/><path d="M4 21v-2a8 8 0 0 1 16 0v2"/>',
    plus: '<path d="M12 5v14M5 12h14"/>',
    heart: '<path d="M12 21 3.5 12.5A5.7 5.7 0 0 1 12 5a5.7 5.7 0 0 1 8.5 7.5Z"/>',
    glass: '<path d="m12 2 9 5v10l-9 5-9-5V7l9-5ZM3 7l9 5 9-5M12 12v10"/>',
    solid: '<rect x="4" y="4" width="16" height="16" rx="4"/>',
    ticket: '<path d="M3 6h18v4a2 2 0 0 0 0 4v4H3v-4a2 2 0 0 0 0-4V6ZM15 6v2m0 3v2m0 3v2"/>',
    transit: '<rect x="5" y="3" width="14" height="16" rx="4"/><path d="M5 11h14M8 6h8M8 22l2-3m6 3-2-3M8 15h1m6 0h1"/>',
    calendar: '<rect x="3" y="5" width="18" height="16" rx="3"/><path d="M7 3v4m10-4v4M3 10h18M7 14h2m4 0h2m-8 3h2"/>',
    media: '<path d="M5 5h14v14H5V5Z"/><path d="m10 8 6 4-6 4V8Z" fill="currentColor" stroke="none"/><path d="M2 8v8m20-8v8"/>',
    pill: '<path d="M4 12 12 4a5.7 5.7 0 0 1 8 8l-8 8a5.7 5.7 0 0 1-8-8Zm4-4 8 8"/><path d="m5.5 13 2-2"/>',
    music: '<path d="M9 18V5l12-2v13M9 8l12-2"/><ellipse cx="6" cy="18" rx="3" ry="3"/><ellipse cx="18" cy="16" rx="3" ry="3"/>'
  };
  const icon = (name) => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' + (paths[name] || paths.solid) + '</svg>';
  const appIcons = { 'clube-uol': 'ticket', 'bilhete-unico': 'transit', 'super-calendario': 'calendar', 'media-downloader': 'media', pilltracker: 'pill', 'lastfm-small': 'music' };
  const appColors = { 'clube-uol': 'linear-gradient(140deg,#ae31ef,#4d18c3)', 'bilhete-unico': 'radial-gradient(ellipse at 24% 0%,#73dce4 0%,transparent 48%),linear-gradient(145deg,#30b7d7 5%,#287cca 57%,#344ca8)', 'super-calendario': 'radial-gradient(ellipse at 22% 0%,#ebc263 0%,transparent 48%),linear-gradient(145deg,#eb9449 5%,#e45e55 57%,#b73a70)', 'media-downloader': 'radial-gradient(ellipse at 22% 0%,#ac8de2 0%,transparent 48%),linear-gradient(145deg,#8867dd 5%,#625bd5 57%,#3d3fa6)', pilltracker: 'radial-gradient(ellipse at 22% 0%,#79dabe 0%,transparent 48%),linear-gradient(145deg,#30ba94 5%,#229a7f 57%,#28777f)', 'lastfm-small': 'radial-gradient(ellipse at 22% 0%,#e791b9 0%,transparent 48%),linear-gradient(145deg,#e45b91 5%,#cb3d76 57%,#933b71)' };
  const demo = {
    version: 'Prévia', profile: { name: 'Leo', role: 'admin', status: 'approved' },
    apps: [
      { id: 'clube-uol', name: 'Clube UOL Premium', scriptName: 'Clube UOL Premium' },
      { id: 'bilhete-unico', name: 'Bilhete Único', scriptName: 'Bilhete Único' },
      { id: 'super-calendario', name: 'Super Calendário', scriptName: 'Super Calendário' },
      { id: 'media-downloader', name: 'media-downloader', scriptName: 'media-downloader' },
      { id: 'pilltracker', name: 'PillTracker', scriptName: 'PillTracker' },
      { id: 'lastfm-small', name: 'Last.fm Small', scriptName: 'Last.fm Small' }
    ].map((app) => Object.assign(app, { installed: true, allowed: true, mode: 'integrated' })),
    preferences: { appearance: 'system', material: 'glass', showAppNames: false, favorites: ['clube-uol', 'bilhete-unico', 'super-calendario', 'pilltracker'], notifications: { enabled: false, updates: true, access: true } },
    notifications: [], assets: {}, accessConfigured: false, update: {}
  };
  let state;
  let preview = window.__SAQUETTO_PREVIEW__ === true;
  try {
    const raw = $('#saquetto-initial').textContent;
    if (raw.indexOf('__SAQUETTO_INITIAL_JSON__') !== -1) { preview = true; state = demo; }
    else { state = JSON.parse(raw); }
  } catch (_) { preview = true; state = demo; }
  state.preferences = Object.assign({}, demo.preferences, state.preferences || {});
  state.profile = state.profile || { name: '', role: 'user', status: 'loading' };
  state.apps = Array.isArray(state.apps) ? state.apps : [];
  state.notifications = Array.isArray(state.notifications) ? state.notifications : [];
  state.assets = state.assets || {};
  let panel = null;
  let panelAppId = null;
  let activeAppId = null;
  let activeInstanceId = null;
  let activeInstanceBase = null;
  let currentFrame = null;
  let currentFrameReady = false;
  let frameOutbox = [];
  let frameSequence = 0;
  let appClosing = false;
  const frameEvaluations = new Map();
  let entranceTilesRendered = false;
  let query = '';
  let users = null;
  let usersError = '';
  let busy = false;
  let toastTimer;
  let previousFocus;
  let requestSequence = 0;
  const pending = new Map();
  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
  const errorMessages = {
    access_unavailable: 'Não foi possível verificar seu acesso. Tente novamente quando estiver conectado.',
    access_denied: 'Seu acesso a este app não está autorizado.', approval_required: 'Seu acesso aguarda aprovação do administrador.',
    app_not_installed: 'Este script ainda não está instalado neste dispositivo.', script_name_conflict: 'Já existe outro script com esse nome. A instalação foi interrompida para preservar seu arquivo.',
    name_invalid: 'Informe um nome válido para solicitar acesso.', notifications_disabled: 'As notificações estão desativadas. Ative-as nos ajustes.',
    initialization_failed: 'Não foi possível iniciar o saquettoOS. Feche e abra o script novamente.', channel_invalid: 'O canal de atualização não é válido.',
    preview_only: 'Esta é uma prévia visual. Abra o saquettoOS no Scriptable para usar esta ação.',
    native_timeout: 'O Scriptable não respondeu. Tente novamente.', update_not_configured: 'Este app ainda não possui um canal de atualização configurado.',
    code_mismatch: 'O código não confere. Confirme o código diretamente com a pessoa.', invalid_code: 'O código não confere. Confirme o código diretamente com a pessoa.', confirmation_mismatch: 'O código não confere. Confirme o código diretamente com a pessoa.',
    admin_required: 'Esta ação exige acesso de administrador.', enrollment_capacity: 'O serviço atingiu o limite de solicitações. Tente novamente mais tarde.',
    app_busy: 'O app ainda está concluindo uma ação. Aguarde antes de voltar ao início.', app_closing: 'O app está sendo fechado. Aguarde a conclusão.',
    adapter_source_changed: 'Esta versão do script ainda não foi verificada pela central. Use Executar fora da central ou atualize o saquettoOS.',
    adapter_entrypoint_changed: 'Esta versão do script precisa de uma atualização de compatibilidade.',
    app_storage_unreadable: 'Não foi possível ler os ajustes desta tela. O arquivo foi preservado.',
    app_storage_unverified: 'Não foi possível confirmar a gravação dos ajustes. Feche e reabra o app.',
    close_app_before_update: 'Volte ao início antes de atualizar seus apps.',
    pill_data_unavailable: 'Configure o PillTracker no script original antes de abri-lo aqui.',
    pill_data_unrecognized: 'Os dados do PillTracker precisam de revisão. Nenhuma alteração foi feita.',
    not_configured: 'Este app ainda não possui um canal de atualização configurado.',
    download_failed: 'Não foi possível baixar a atualização. A versão instalada foi preservada.',
    invalid_manifest: 'A atualização recebida não é válida. A instalação foi interrompida.',
    integrity_failed: 'A verificação da atualização falhou. A instalação foi interrompida.',
    rolled_back: 'A atualização não foi concluída. A versão anterior foi restaurada.',
    recovery_required: 'A instalação precisa de recuperação. Nenhuma nova atualização foi aplicada.',
    concurrent_change: 'O script mudou durante a atualização. A operação foi interrompida para preservar a alteração.'
  };

  window.__SAQUETTO_QUEUE = window.__SAQUETTO_QUEUE || [];
  window.saquettoNative = function (action, payload) {
    if (preview) return Promise.reject(new Error('preview_only'));
    return new Promise((resolve, reject) => {
      const id = 'os-' + (++requestSequence) + '-' + Date.now();
      // Native app sessions end when the user closes the app, not after 90 seconds.
      const nativeSession = action === 'openApp' && getApp(payload && payload.id)?.mode === 'native';
      const timer = nativeSession ? null : setTimeout(() => { pending.delete(id); reject(new Error('native_timeout')); }, 90000);
      pending.set(id, { resolve, reject, timer });
      window.__SAQUETTO_QUEUE.push({ id, action, payload: payload || {} });
      if (typeof window.__SAQUETTO_WAKE === 'function') window.__SAQUETTO_WAKE();
    });
  };
  window.saquettoResolve = function (id, result) {
    const request = pending.get(id);
    if (!request) return;
    pending.delete(id); clearTimeout(request.timer);
    if (result && result.ok) request.resolve(result.data);
    else {
      const failure = result && result.error;
      const error = new Error(typeof failure === 'string' ? failure : (failure && (failure.message || failure.code)) || 'Não foi possível concluir esta ação.');
      error.code = failure && failure.code;
      request.reject(error);
    }
  };
  window.saquettoSetState = function (patch) {
    if (!patch || typeof patch !== 'object') return;
    if (patch.state) patch = patch.state;
    state = Object.assign({}, state, patch, { preferences: Object.assign({}, state.preferences, patch.preferences || {}), profile: Object.assign({}, state.profile, patch.profile || {}), assets: Object.assign({}, state.assets, patch.assets || {}) });
    if (!Array.isArray(state.apps)) state.apps = [];
    if (!Array.isArray(state.notifications)) state.notifications = [];
    render();
  };
  function applyResult(result) { if (result && result.state) window.saquettoSetState(result.state); }
  function safeImage(url) { return typeof url === 'string' && /^(data:image\/(png|jpeg|webp|gif);base64,|https:\/\/)/i.test(url) && !(preview && /^https:/i.test(url)) ? url : ''; }
  function appImage(app) { return safeImage(app.icon) || (app.id === 'clube-uol' ? safeImage(state.assets.uolIcon) : ''); }
  function appGraphic(app) { const url = appImage(app); return url ? '<img src="' + escapeHTML(url) + '" alt="">' : icon(appIcons[app.id] || 'solid'); }
  function appColor(app) { return appColors[app.id] || 'linear-gradient(145deg,#697cce,#6d4bac)'; }
  function initials(name) { return String(name || 'S').trim().split(/\s+/).slice(0, 2).map((word) => word.charAt(0)).join('').toUpperCase(); }
  function getApp(id) { return state.apps.find((app) => app.id === id); }
  function approved() { return state.profile.status === 'approved'; }
  function admin() { return approved() && state.profile.role === 'admin'; }
  function allowed(app) { return approved() && app.allowed === true; }
  function statusText(status) { return ({ approved: 'Acesso aprovado', pending: 'Aguardando aprovação', revoked: 'Acesso revogado', loading: 'Verificando acesso', unavailable: 'Acesso indisponível', expired: 'Solicitação expirada', unregistered: 'Solicite seu acesso' })[status] || 'Acesso não confirmado'; }
  function statusLabel(status) { return '<span class="status-label" data-status="' + escapeHTML(status) + '">' + statusText(status) + '</span>'; }
  function notify(message, isError) {
    const toast = $('#toast'); clearTimeout(toastTimer); toast.textContent = message; toast.dataset.error = String(Boolean(isError)); toast.hidden = false;
    toastTimer = setTimeout(() => { toast.hidden = true; }, isError ? 6500 : 4200);
  }
  function errorText(error) { return errorMessages[error.code] || errorMessages[error.message] || error.message || 'Não foi possível concluir esta ação.'; }
  function setBusy(value) {
    busy = value;
    $('#sheet').setAttribute('aria-busy', String(value));
    $('#sheet-content').querySelectorAll('button,input').forEach((element) => {
      if (value) { element.dataset.wasDisabled = String(element.disabled); element.disabled = true; }
      else if (element.dataset.wasDisabled !== undefined) { element.disabled = element.dataset.wasDisabled === 'true'; delete element.dataset.wasDisabled; }
    });
  }
  async function nativeAction(action, payload, callback) {
    if (busy) return;
    setBusy(true);
    try { const result = await window.saquettoNative(action, payload); applyResult(result); if (callback) callback(result || {}); return true; }
    catch (error) { notify(errorText(error), true); return false; }
    finally { setBusy(false); }
  }
  function applyAppearance() {
    const appearance = state.preferences.appearance || 'system';
    const resolved = appearance === 'system' ? (mediaQuery.matches ? 'dark' : 'light') : appearance;
    document.documentElement.dataset.appearance = resolved === 'light' ? 'light' : 'dark';
    document.documentElement.dataset.material = state.preferences.material === 'solid' ? 'solid' : 'glass';
    document.documentElement.dataset.showAppNames = String(state.preferences.showAppNames === true);
    const logo = safeImage(resolved === 'light' ? state.assets.logoLight : state.assets.logoDark);
    $('#brand-mark').innerHTML = logo ? '<img src="' + escapeHTML(logo) + '" alt="">' : icon('sparkles');
    document.querySelector('meta[name="theme-color"]').content = resolved === 'light' ? '#f0f2f8' : '#0d1020';
  }
  if (mediaQuery.addEventListener) mediaQuery.addEventListener('change', applyAppearance); else mediaQuery.addListener(applyAppearance);
  function tile(app, inDock, entranceIndex) {
    let badge = '';
    if (!allowed(app) && state.profile.status !== 'loading') badge = '<span class="app-badge locked">' + icon('lock') + '</span>';
    else if (!app.installed) badge = '<span class="app-badge">' + icon('download') + '</span>';
    else if (app.updateAvailable) badge = '<span class="app-badge">' + icon('refresh') + '</span>';
    const name = inDock ? ({ 'clube-uol': 'Clube UOL', 'super-calendario': 'Calendário', 'media-downloader': 'Downloads', 'lastfm-small': 'Last.fm' }[app.id] || app.name) : app.name;
    const hint = state.profile.status === 'loading' ? 'Verificando acesso' : !allowed(app) ? 'Acesso restrito' : (!app.installed ? 'Não instalado' : (app.mode === 'native' ? 'Interface original' : app.mode === 'integrated' ? 'Integrado' : 'Abre no Scriptable'));
    return '<button type="button" class="app-tile"' + (entranceIndex === undefined ? '' : ' data-entrance="' + entranceIndex + '" style="--entrance-index:' + entranceIndex + '"') + ' data-open-app="' + escapeHTML(app.id) + '" data-installed="' + Boolean(app.installed) + '" data-available="' + (state.profile.status === 'loading' ? 'loading' : allowed(app)) + '" aria-label="' + escapeHTML(app.name + '. ' + hint) + '"' + (inDock ? '' : ' role="listitem"') + '><span class="app-icon' + (appImage(app) ? ' app-icon-artwork' : '') + '" style="--app-color:' + appColor(app) + '">' + appGraphic(app) + badge + '</span><span class="app-name">' + escapeHTML(name) + '</span>' + (!app.installed ? '<span class="app-status">Não instalado</span>' : '') + '</button>';
  }
  function renderApps() {
    const normalized = query.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
    const filtered = state.apps.filter((app) => String(app.name).normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().includes(normalized));
    $('#app-grid').innerHTML = filtered.map((app, index) => tile(app, false, entranceTilesRendered ? undefined : index)).join('');
    $('#library-count').textContent = query ? filtered.length + ' encontrados' : state.apps.length + ' aplicativos';
    $('#search-empty').hidden = filtered.length > 0;
    const favoriteIds = Array.isArray(state.preferences.favorites) ? state.preferences.favorites : [];
    const favorites = favoriteIds.map(getApp).filter(Boolean).slice(0, 4);
    $('#dock').innerHTML = favorites.map((app, index) => tile(app, true, entranceTilesRendered ? undefined : index + 6)).join('') + Array.from({ length: 4 - favorites.length }, () => '<button type="button" class="dock-placeholder" data-panel="settings" aria-label="Adicionar favorito">' + icon('plus') + '</button>').join('');
    entranceTilesRendered = true;
  }
  function updateItems() { return state.update && Array.isArray(state.update.items) ? state.update.items : []; }
  function render() {
    applyAppearance(); renderApps();
    $('#preview-note').hidden = !preview;
    $('#profile-initials').textContent = initials(state.profile.name);
    $('#profile-presence').dataset.status = state.profile.status;
    $('#profile-button').setAttribute('aria-label', (state.profile.name || 'Seu perfil') + '. ' + statusText(state.profile.status) + '. Abrir ajustes');
    $('#greeting').textContent = state.profile.name ? 'SEU ESPAÇO, ' + state.profile.name.split(' ')[0].toUpperCase() + '.' : 'SEU ESPAÇO. SEUS APPS.';
    $('#home-caption').textContent = 'saquettoOS' + (state.version ? ' · ' + state.version : '');
    $('#notification-dot').hidden = !state.notifications.some((item) => !item.read);
    const banner = $('#access-banner'); banner.hidden = approved() || state.profile.status === 'loading';
    if (!approved() && state.profile.status !== 'loading') {
      const descriptions = { loading: 'Conferindo sua autorização com o saquettoOS.', pending: 'Sua solicitação foi recebida. Leo precisa aprovar seu acesso.', revoked: 'Este dispositivo está sem autorização. Consulte seu administrador.' };
      banner.innerHTML = '<strong>' + statusText(state.profile.status) + '</strong><p>' + (descriptions[state.profile.status] || 'Peça acesso para abrir os aplicativos do seu espaço.') + '</p>' + (state.profile.status === 'loading' ? '' : '<button type="button" class="text-button" data-panel="access">' + (state.profile.status === 'pending' ? 'Ver solicitação' : 'Gerenciar acesso') + '</button>');
    }
    const available = updateItems().filter((item) => item.available || item.updateAvailable);
    $('#update-peek-title').textContent = available.length ? available.length + (available.length === 1 ? ' atualização disponível' : ' atualizações disponíveis') : 'Seu espaço, sempre em dia';
    $('#update-peek-copy').textContent = available.length ? 'Veja o que há de novo nos seus apps' : 'Confira as versões dos seus apps';
    if (panel) renderPanel();
  }

  function groupRow(action, title, detail, glyph) { return '<button type="button" class="group-row" data-panel="' + action + '"><span class="row-icon">' + icon(glyph) + '</span><span class="row-text"><strong>' + title + '</strong><small>' + detail + '</small></span><span class="row-chevron">' + icon('chevron') + '</span></button>'; }
  function settingsPanel() {
    const prefs = state.preferences;
    return '<div class="profile-card"><div class="profile-avatar">' + escapeHTML(initials(state.profile.name)) + '</div><div><h3>' + escapeHTML(state.profile.name || 'Seu perfil') + '</h3><p>' + (admin() ? 'Administrador do saquettoOS' : 'Seu espaço no Scriptable') + '</p>' + statusLabel(state.profile.status) + '</div></div>' +
      '<div class="section-label">APARÊNCIA</div><div class="theme-options">' + [['light', 'Claro'], ['dark', 'Escuro'], ['system', 'Automático']].map(([value, title]) => '<button type="button" class="theme-option" data-preference="appearance" data-value="' + value + '" aria-pressed="' + (prefs.appearance === value) + '"><span class="theme-preview" aria-hidden="true"></span>' + title + '</button>').join('') + '</div>' +
      '<div class="panel-section"><div class="section-label">MATERIAL</div><div class="segmented">' + [['glass', 'Vidro', 'glass'], ['solid', 'Sólido', 'solid']].map(([value, title, glyph]) => '<button type="button" data-preference="material" data-value="' + value + '" aria-pressed="' + (prefs.material === value) + '"><span class="segment-icon">' + icon(glyph) + '</span>' + title + '</button>').join('') + '</div><p class="fine-print">Automático acompanha a aparência do iPhone. Vidro adiciona transparência e profundidade.</p></div>' +
      '<div class="panel-section"><div class="section-label">TELA INICIAL</div><div class="group"><label class="group-row"><span class="row-text"><strong>Mostrar nomes dos apps</strong><small>Exibir os nomes na tela inicial e no dock</small></span><input type="checkbox" class="toggle" data-preference-toggle="showAppNames" aria-label="Mostrar nomes dos apps"' + (prefs.showAppNames === true ? ' checked' : '') + '></label></div></div>' +
      '<div class="panel-section"><div class="section-label">FAVORITOS NO DOCK · ATÉ 4</div><div class="favorite-list">' + state.apps.map((app) => '<button type="button" class="favorite-chip" data-favorite="' + escapeHTML(app.id) + '" aria-pressed="' + (prefs.favorites || []).includes(app.id) + '">' + icon((prefs.favorites || []).includes(app.id) ? 'check' : 'plus') + escapeHTML(app.name) + '</button>').join('') + '</div></div>' +
      '<div class="panel-section"><div class="section-label">SEU SISTEMA</div><div class="group">' + groupRow('updates', 'Central de atualizações', 'saquettoOS e seus aplicativos', 'download') + groupRow('notifications', 'Notificações', 'Alertas, lembretes e novidades', 'bell') + groupRow('access', 'Meu acesso', statusText(state.profile.status), 'user') + (admin() ? groupRow('users', 'Gerenciar usuários', 'Aprovações e acesso por aplicativo', 'users') : '') + '</div></div><p class="version-footer">saquettoOS ' + escapeHTML(state.version || '') + '</p>';
  }
  function updatesPanel() {
    const items = updateItems();
    const available = items.filter((item) => item.available || item.updateAvailable);
    const checked = state.update && (state.update.checkedAt || state.update.lastCheckedAt || items.length > 0);
    return '<div class="status-card"><span>' + icon(available.length ? 'download' : 'sparkles') + '</span><h3>' + (available.length ? available.length + (available.length === 1 ? ' atualização disponível' : ' atualizações disponíveis') : checked ? 'Verificação concluída' : 'Novidades, em um só lugar') + '</h3><p>' + (available.length ? 'Atualize cada app sem sair da sua central.' : checked ? 'Confira abaixo o estado de cada canal de atualização.' : 'Busque a versão mais recente do saquettoOS e dos apps com atualização configurada.') + '</p></div><button type="button" class="primary-button full-button" data-action="checkUpdates">' + icon('refresh') + 'Verificar atualizações</button>' +
      '<div class="panel-section"><div class="section-label">APLICATIVOS E SISTEMA</div><div class="group">' + (items.length ? items : [{ id: 'saquettoos', name: 'saquettoOS', version: state.version }, ...state.apps]).map((item) => {
        const app = getApp(item.id) || item;
        const isAvailable = item.available || item.updateAvailable;
        const configured = item.status !== 'not_configured' && item.configured !== false && (item.id === 'saquettoos' || item.id === 'clube-uol' || item.channel);
        const detail = item.error ? errorMessages[item.error] || 'Não foi possível verificar' : !configured ? 'Canal de atualização não configurado' : isAvailable ? String(item.version || 'Instalável') + ' → ' + String(item.latestVersion || item.version || 'Nova versão') : item.status === 'up_to_date' || item.status === 'current' ? 'Versão atualizada' : 'Verifique a disponibilidade';
        return '<div class="group-row update-row"><span class="mini-icon" style="--app-color:' + appColor(app) + '">' + (item.id === 'saquettoos' ? icon('sparkles') : appGraphic(app)) + '</span><span class="row-text"><strong>' + escapeHTML(item.name || app.name || 'saquettoOS') + '</strong><small>' + escapeHTML(detail) + '</small></span>' + (isAvailable ? '<button type="button" class="secondary-button" data-install="' + escapeHTML(item.id) + '">Atualizar</button>' : '<span class="version-tag">' + escapeHTML(item.version || '—') + '</span>') + '</div>';
      }).join('') + '</div><p class="fine-print">Instalações e atualizações preservam os dados dos apps. Scripts sem canal configurado continuam abrindo normalmente.</p></div>';
  }
  function notificationsPanel() {
    const prefs = state.preferences.notifications || {};
    return '<p class="panel-intro">Avisos locais do Scriptable, com atalhos para voltar ao seu espaço. A entrega depende da permissão de notificações do iPhone.</p><div class="group">' + [['enabled', 'Permitir notificações', 'Ativar os avisos do saquettoOS'], ['updates', 'Atualizações', 'Avisar sobre versões disponíveis ao verificar'], ['access', 'Acesso e aprovações', 'Avisar quando uma mudança for detectada']].map(([key, title, detail]) => '<label class="group-row"><span class="row-text"><strong>' + title + '</strong><small>' + detail + '</small></span><input type="checkbox" class="toggle" data-notification-preference="' + key + '" aria-label="' + title + '"' + (prefs[key] ? ' checked' : '') + '></label>').join('') + '</div><div class="panel-actions"><button type="button" class="secondary-button" data-action="testNotification">' + icon('bell') + 'Testar notificação</button></div><p class="fine-print">O saquettoOS verifica novidades quando é executado. Não mantém um processo contínuo em segundo plano.</p><div class="panel-section"><div class="section-label">SUA CENTRAL</div>' + (state.notifications.length ? '<div class="group">' + state.notifications.map((item) => '<article class="notice-item"><h3>' + escapeHTML(item.title || 'saquettoOS') + '</h3><p>' + escapeHTML(item.body || '') + '</p>' + (item.date ? '<time>' + escapeHTML(formatDate(item.date)) + '</time>' : '') + (item.appId && getApp(item.appId) ? '<button type="button" class="text-button" data-open-app="' + escapeHTML(item.appId) + '">Abrir app</button>' : '') + '</article>').join('') + '</div>' : '<div class="status-card"><span>' + icon('bell') + '</span><h3>Tudo tranquilo por aqui</h3><p>Os avisos recebidos aparecem nesta central.</p></div>') + '</div>';
  }
  function accessPanel() {
    const status = state.profile.status;
    const code = state.profile.code || state.profile.approvalCode;
    if (approved()) return '<div class="status-card"><span>' + icon('check') + '</span><h3>Bem-vindo, ' + escapeHTML(state.profile.name || 'você') + '</h3><p>' + (admin() ? 'Você administra usuários e permissões do saquettoOS.' : 'Seu acesso foi aprovado pelo administrador.') + '</p></div><div class="group">' + state.apps.map((app) => '<div class="group-row"><span class="row-icon">' + icon(allowed(app) ? 'check' : 'lock') + '</span><span class="row-text"><strong>' + escapeHTML(app.name) + '</strong><small>' + (allowed(app) ? 'Acesso autorizado' : 'Acesso não autorizado') + '</small></span></div>').join('') + '</div>';
    return '<div class="status-card"><span>' + icon(status === 'loading' ? 'refresh' : 'lock') + '</span><h3>' + statusText(status) + '</h3><p>' + (status === 'pending' ? 'Compartilhe seu código com Leo para que ele confirme sua solicitação.' : status === 'revoked' ? 'Seu acesso foi revogado pelo administrador.' : 'Leo aprova os usuários e define quais apps cada pessoa pode abrir.') + '</p>' + (code ? '<p style="margin-top:15px;font-size:20px;letter-spacing:4px;color:var(--text)">' + escapeHTML(code) + '</p>' : '') + '</div>' + (!state.accessConfigured ? '<p class="inline-error">O serviço de acesso ainda não está configurado. Nenhuma solicitação será aprovada localmente.</p>' : '') +
      (status !== 'pending' && status !== 'loading' ? '<label class="input-label" for="access-name">Seu nome</label><input class="text-input" id="access-name" autocomplete="name" maxlength="60" placeholder="Como você quer ser chamado?" value="' + escapeHTML(state.profile.name || '') + '"><div class="panel-actions"><button type="button" class="primary-button" data-action="requestAccess"' + (!state.accessConfigured ? ' disabled' : '') + '>Solicitar acesso</button></div>' : '') + '<div class="panel-actions"><button type="button" class="secondary-button" data-action="refreshState">' + icon('refresh') + 'Verificar acesso</button></div>';
  }
  function usersPanel() {
    if (!admin()) return '<p class="inline-error">A gestão de usuários exige acesso de administrador.</p>';
    let content = '<p class="panel-intro">Confirme o código diretamente com a pessoa antes de aprovar. As permissões são verificadas pelo serviço de acesso.</p><button type="button" class="secondary-button full-button" data-action="refreshUsers">' + icon('refresh') + 'Recarregar solicitações</button>';
    if (usersError) return content + '<div class="panel-section inline-error">' + escapeHTML(usersError) + '</div>';
    if (users === null) return content + '<div class="loading-row"><span class="spinner"></span>Buscando usuários…</div>';
    if (!users.length) return content + '<div class="panel-section status-card"><span>' + icon('users') + '</span><h3>Nenhuma solicitação</h3><p>Novas pessoas aparecerão aqui quando pedirem acesso.</p></div>';
    return content + users.map((user, index) => '<article class="user-card" data-user-index="' + index + '"><div class="user-heading"><strong>' + escapeHTML(user.name || 'Usuário') + '</strong>' + statusLabel(user.status) + '</div>' + (user.role === 'admin' ? '<p class="fine-print">Administrador · permissões gerenciadas no serviço.</p>' : '<label class="input-label" for="user-code-' + index + '">Código confirmado com a pessoa</label><input class="text-input" id="user-code-' + index + '" inputmode="text" autocomplete="off" placeholder="Informe o código recebido" maxlength="80"><div class="user-apps">' + state.apps.map((app) => '<label class="user-app-option"><input type="checkbox" value="' + escapeHTML(app.id) + '"' + ((user.appIds || []).includes(app.id) ? ' checked' : '') + '>' + escapeHTML(app.name) + '</label>').join('') + '</div><div class="panel-actions"><button type="button" class="primary-button" data-decide-user="' + index + '" data-status="approved">' + (user.status === 'approved' ? 'Salvar acesso' : 'Aprovar') + '</button><button type="button" class="secondary-button danger-button" data-decide-user="' + index + '" data-status="revoked">Revogar</button></div>') + '</article>').join('');
  }
  function appPanel() {
    const app = getApp(panelAppId);
    if (!app) return '<p class="inline-error">Aplicativo não encontrado.</p>';
    return '<div class="status-card"><span>' + icon(appIcons[app.id] || 'solid') + '</span><h3>' + escapeHTML(app.name) + '</h3><p>' + (!allowed(app) ? 'Seu perfil ainda não tem permissão para este app.' : !app.installed ? 'Instale o script no Scriptable para abrir este aplicativo.' : app.mode === 'native' ? 'Abre a interface original do app. Feche a tela do app para voltar à central.' : app.mode === 'integrated' ? 'Este app está preparado para abrir dentro do saquettoOS.' : 'Este app abre sua própria interface no Scriptable.') + '</p></div><div class="group"><div class="group-row"><span class="row-text"><strong>Nome no Scriptable</strong><small>' + escapeHTML(app.scriptName || app.name) + '</small></span></div><div class="group-row"><span class="row-text"><strong>Estado</strong><small>' + (!app.installed ? 'Não instalado' : app.mode === 'native' ? 'Interface original' : app.mode === 'integrated' ? 'Integrado' : 'App externo') + '</small></span><span class="version-tag">' + escapeHTML(app.version || '') + '</span></div></div><div class="panel-actions">' + (!allowed(app) ? '<button type="button" class="secondary-button" data-panel="access">Ver meu acesso</button>' : !app.installed ? '<button type="button" class="primary-button" data-panel="updates">Ver instalação e atualizações</button>' : '<button type="button" class="primary-button" data-run-app="' + escapeHTML(app.id) + '">Abrir aplicativo</button><button type="button" class="secondary-button" data-action="standalone" data-app-id="' + escapeHTML(app.id) + '">Executar fora da central</button>') + '</div>';
  }
  function formatDate(date) { const parsed = new Date(date); return Number.isNaN(parsed.getTime()) ? String(date) : parsed.toLocaleString('pt-BR', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' }); }
  function renderPanel() {
    const titles = { settings: 'Ajustes', updates: 'Atualizações', notifications: 'Notificações', access: 'Meu acesso', users: 'Usuários', app: 'Seu aplicativo' };
    $('#sheet-title').textContent = titles[panel] || 'saquettoOS';
    $('#sheet-eyebrow').textContent = panel === 'users' ? 'ADMINISTRAÇÃO' : 'SAQUETTOOS';
    const renderers = { settings: settingsPanel, updates: updatesPanel, notifications: notificationsPanel, access: accessPanel, users: usersPanel, app: appPanel };
    $('#sheet-content').innerHTML = (renderers[panel] || settingsPanel)();
  }
  function openPanel(next, appId) {
    if (!panel) previousFocus = document.activeElement;
    panel = next; panelAppId = appId || null;
    renderPanel(); $('#sheet').hidden = false; $('#sheet-backdrop').hidden = false;
    $('#shell').setAttribute('aria-hidden', 'true'); $('#shell').inert = true;
    document.body.style.overflow = 'hidden'; $('#sheet-content').scrollTop = 0; $('#close-sheet').focus({ preventScroll: true });
    if (next === 'users' && users === null) loadUsers();
  }
  function closePanel() {
    panel = null; $('#sheet').hidden = true; $('#sheet-backdrop').hidden = true; $('#shell').removeAttribute('aria-hidden'); $('#shell').inert = false; document.body.style.overflow = '';
    if (previousFocus && previousFocus.isConnected) previousFocus.focus({ preventScroll: true });
  }
  async function loadUsers() {
    if (!admin()) return;
    usersError = '';
    if (preview) { users = []; usersError = 'Prévia visual: nenhum usuário real foi consultado.'; if (panel === 'users') renderPanel(); return; }
    try { const result = await window.saquettoNative('listUsers', {}); users = Array.isArray(result) ? result : (result.users || []); }
    catch (error) { users = []; usersError = errorText(error); }
    if (panel === 'users') renderPanel();
  }
  async function savePreferences(patch) {
    if (preview) { state.preferences = Object.assign({}, state.preferences, patch); render(); return; }
    const saved = await nativeAction('setPreferences', patch, (result) => {
      if (!result.state) { state.preferences = Object.assign({}, state.preferences, result.preferences || patch); render(); }
    });
    if (saved === false && panel) renderPanel();
  }
  async function runApp(id) {
    const app = getApp(id);
    if (!app) return;
    if (!allowed(app) || !app.installed) { openPanel('app', id); return; }
    const opened = await nativeAction('openApp', { id }, () => { if (panel) closePanel(); });
    if (opened === false && !activeAppId && !preview) openPanel('app', id);
  }
  function validBridgeId(value) { return (typeof value === 'string' && value.length > 0 && value.length <= 160) || (typeof value === 'number' && Number.isSafeInteger(value)); }
  function newInstanceId(base) { return String(base || 'saquetto-frame') + ':' + (++frameSequence); }
  function frameReply(frame, instanceId, message) {
    if (frame !== currentFrame || frame.saquettoInvalidated || instanceId !== activeInstanceId || !frame.contentWindow) return false;
    if (!currentFrameReady) { frameOutbox.push(message); return true; }
    frame.contentWindow.postMessage(Object.assign({ saquetto: 1, instanceId }, message), '*');
    return true;
  }
  function replaceFrame(html, instanceId) {
    const frame = document.createElement('iframe');
    frame.title = $('#app-stage-name').textContent;
    frame.setAttribute('sandbox', 'allow-scripts allow-forms');
    frame.setAttribute('referrerpolicy', 'no-referrer');
    activeInstanceId = instanceId;
    currentFrame = frame;
    currentFrameReady = false; frameOutbox = [];
    frameEvaluations.clear();
    frame.addEventListener('load', () => {
      if (frame !== currentFrame || instanceId !== activeInstanceId) return;
      if (currentFrameReady) {
        frame.saquettoInvalidated = true; frameOutbox = []; frameEvaluations.clear();
        window.saquettoHome(); return;
      }
      currentFrameReady = true;
      const messages = frameOutbox; frameOutbox = [];
      messages.forEach((message) => frameReply(frame, instanceId, message));
    });
    frame.srcdoc = String(html || '');
    $('#app-frame-container').replaceChildren(frame);
    return instanceId;
  }
  window.saquettoShowApp = function (app) {
    const registered = getApp(app && app.id);
    if (!registered || registered.mode !== 'integrated' || !allowed(registered) || !registered.installed) return false;
    if (panel) closePanel();
    if (currentFrame && activeAppId === app.id && (!app.instanceId || app.instanceId === activeInstanceId)) {
      $('#home-screen').hidden = true; $('#app-stage').hidden = false;
      return activeInstanceId;
    }
    activeAppId = app.id; appClosing = false;
    activeInstanceBase = validBridgeId(app.instanceId) ? app.instanceId : 'saquetto-frame';
    $('#app-stage-name').textContent = app.name || registered.name;
    $('#home-screen').hidden = true; $('#app-stage').hidden = false;
    return replaceFrame(app.html, validBridgeId(app.instanceId) ? app.instanceId : newInstanceId());
  };
  window.saquettoSetAppHTML = function (value) {
    if (!activeAppId || !currentFrame || appClosing || !value || typeof value.html !== 'string') return false;
    return replaceFrame(value.html, newInstanceId(activeInstanceBase));
  };
  window.saquettoFrameMessage = function (message) {
    if (!currentFrame || currentFrame.saquettoInvalidated || appClosing || !message || message.kind !== 'eval' || !validBridgeId(message.id) || typeof message.code !== 'string') return false;
    if (message.instanceId !== undefined && message.instanceId !== activeInstanceId) return false;
    frameEvaluations.set(message.id, activeInstanceId);
    return frameReply(currentFrame, activeInstanceId, { kind: 'eval', id: message.id, code: message.code, useCallback: message.useCallback === true });
  };
  window.saquettoHome = async function () {
    if (!activeAppId || !currentFrame) return true;
    if (appClosing) return false;
    appClosing = true;
    const appId = activeAppId, instanceId = activeInstanceId, frame = currentFrame;
    $('#app-stage-header')?.setAttribute('aria-busy', 'true');
    try {
      const result = await window.saquettoNative('closeApp', { appId, instanceId });
      if (currentFrame !== frame || activeInstanceId !== instanceId) return false;
      applyResult(result);
      activeAppId = null; activeInstanceId = null; activeInstanceBase = null; currentFrame = null; currentFrameReady = false; frameOutbox = []; frameEvaluations.clear();
      $('#app-stage').hidden = true; $('#app-frame-container').replaceChildren(); $('#home-screen').hidden = false;
      return true;
    } catch (error) { notify(errorText(error), true); return false; }
    finally { appClosing = false; $('#app-stage-header')?.removeAttribute('aria-busy'); }
  };
  window.addEventListener('message', async (event) => {
    const frame = currentFrame, instanceId = activeInstanceId, appId = activeAppId;
    if (!frame || frame.saquettoInvalidated || !appId || !frame.contentWindow || event.source !== frame.contentWindow) return;
    const message = event.data;
    if (!message || typeof message !== 'object' || Array.isArray(message) || message.saquetto !== 1 || !validBridgeId(message.id)) return;
    if (message.instanceId !== instanceId) return;
    if (message.kind === 'request') {
      if (appClosing) { frameReply(frame, instanceId, { kind: 'response', id: message.id, ok: false, error: 'app_closing' }); return; }
      if (typeof message.action !== 'string' || !message.action || message.action.length > 100 || (message.payload !== undefined && (!message.payload || typeof message.payload !== 'object' || Array.isArray(message.payload)))) {
        frameReply(frame, instanceId, { kind: 'response', id: message.id, ok: false, error: 'payload_invalid' }); return;
      }
      try {
        const result = await window.saquettoNative('appAction', { appId, instanceId, request: { id: message.id, action: message.action, payload: message.payload || {} } });
        frameReply(frame, instanceId, { kind: 'response', id: message.id, ok: true, data: result });
      } catch (error) { frameReply(frame, instanceId, { kind: 'response', id: message.id, ok: false, error: error.code || error.message }); }
    } else if (message.kind === 'evalResult') {
      if (frameEvaluations.get(message.id) !== instanceId) return;
      frameEvaluations.delete(message.id);
      try {
        await window.saquettoNative('appEvalResult', { appId, instanceId, id: message.id, value: message.value, error: typeof message.error === 'string' ? message.error : null });
      } catch (error) {
        if (frame === currentFrame && instanceId === activeInstanceId) notify(errorText(error), true);
      }
    }
  });

  document.querySelectorAll('[data-icon]').forEach((element) => { element.innerHTML = icon(element.dataset.icon); });
  $('#app-search').addEventListener('input', (event) => { query = event.target.value; renderApps(); });
  $('#close-sheet').addEventListener('click', closePanel);
  $('#sheet-backdrop').addEventListener('click', closePanel);
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && panel) { event.preventDefault(); closePanel(); }
    if (event.key === 'Tab' && panel) {
      const items = Array.from($('#sheet').querySelectorAll('button:not(:disabled),input:not(:disabled),[tabindex="0"]'));
      const first = items[0], last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    }
  });
  document.addEventListener('change', async (event) => {
    if (event.target.dataset.preferenceToggle === 'showAppNames') {
      await savePreferences({ showAppNames: event.target.checked === true }); return;
    }
    const key = event.target.dataset.notificationPreference;
    if (!key) return;
    const next = Object.assign({}, state.preferences.notifications || {}, { [key]: event.target.checked });
    if (preview) { event.target.checked = !event.target.checked; notify(errorMessages.preview_only, true); return; }
    await nativeAction('setNotificationPreferences', next, (result) => {
      if (!result.state) { state.preferences.notifications = result.preferences || next; renderPanel(); }
    });
    if (panel === 'notifications') renderPanel();
  });
  document.addEventListener('click', async (event) => {
    const button = event.target.closest('button');
    if (!button || button.disabled) return;
    const data = button.dataset;
    if (data.panel) { openPanel(data.panel); return; }
    if (data.openApp || data.runApp) { await runApp(data.openApp || data.runApp); return; }
    if (data.preference) { await savePreferences({ [data.preference]: data.value }); return; }
    if (data.favorite) {
      let favorites = [...(state.preferences.favorites || [])];
      if (favorites.includes(data.favorite)) favorites = favorites.filter((id) => id !== data.favorite);
      else if (favorites.length < 4) favorites.push(data.favorite);
      else { notify('O dock tem 4 espaços. Remova um favorito para adicionar outro.'); return; }
      await savePreferences({ favorites }); return;
    }
    if (data.install) {
      await nativeAction('installUpdate', { id: data.install }, (result) => {
        if (result.status === 'installed') {
          const item = updateItems().find((entry) => entry.id === data.install);
          if (item) Object.assign(item, { available: false, updateAvailable: false, status: 'up_to_date', version: result.version || item.latestVersion });
          render(); notify('Atualização instalada. Abra novamente para usar a nova versão.');
        } else notify(errorMessages[result.status] || 'Atualização não concluída (' + String(result.status || 'estado não confirmado') + ').', true);
      }); return;
    }
    if (data.decideUser !== undefined) {
      const user = users && users[Number(data.decideUser)];
      const card = button.closest('.user-card'); if (!user || !card || !admin()) return;
      const code = card.querySelector('.text-input').value.trim().toUpperCase();
      if (!/^[A-F0-9]{8}$/.test(code)) { notify('Informe os 8 caracteres do código confirmado com a pessoa.', true); return; }
      const appIds = Array.from(card.querySelectorAll('input[type="checkbox"]:checked')).map((input) => input.value);
      await nativeAction('decideUser', { id: user.id, status: data.status, appIds, code }, () => { users = null; loadUsers(); }); return;
    }
    switch (data.action) {
      case 'home': if (panel) closePanel(); if (activeAppId) await window.saquettoHome(); else if (!preview) await nativeAction('returnHome', {}); break;
      case 'standalone': {
        const id = data.appId || activeAppId || panelAppId;
        if (!id) break;
        if (activeAppId && !await window.saquettoHome()) break;
        await nativeAction('launchStandalone', { id }); break;
      }
      case 'clearSearch': query = ''; $('#app-search').value = ''; renderApps(); $('#app-search').focus(); break;
      case 'checkUpdates': await nativeAction('checkUpdates', {}, (result) => { if (Array.isArray(result.items)) { state.update = result; render(); } }); break;
      case 'testNotification': await nativeAction('testNotification', {}, () => { notify('Notificação solicitada ao Scriptable.'); }); break;
      case 'requestAccess': {
        const name = ($('#access-name') && $('#access-name').value || '').trim();
        if (name.length < 2) { notify('Informe seu nome para solicitar acesso.', true); return; }
        await nativeAction('requestAccess', { name }); break;
      }
      case 'refreshState': await nativeAction('getState', {}, (result) => { if (!result.state && result.profile) window.saquettoSetState(result); }); break;
      case 'refreshUsers': users = null; renderPanel(); await loadUsers(); break;
    }
  });
  // Track the visible WebView when the keyboard opens, without disabling pinch zoom.
  function fitViewport() {
    const viewport = window.visualViewport;
    if (!viewport || viewport.scale > 1.05 || !Number.isFinite(viewport.height) || viewport.height <= 0) return;
    document.documentElement.style.setProperty('--viewport-height', Math.round(viewport.height) + 'px');
    document.documentElement.dataset.compactViewport = String(viewport.height < 480);
  }
  if (window.visualViewport) {
    window.visualViewport.addEventListener('resize', fitViewport);
    fitViewport();
  }
  $('#shell').dataset.enter = 'true';
  $('.home-bottom').addEventListener('animationend', (event) => {
    if (event.target === $('.home-bottom') && event.animationName === 'home-layer-in') $('#shell').dataset.enter = 'false';
  });
  render();
  window.__SAQUETTO_RENDERED = true;
}());
