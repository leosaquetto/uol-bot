// Variables used by Scriptable.
// These must be at the very top of the file. Do not edit.
// icon-color: purple; icon-glyph: layer-group;
// saquettoOS Scriptable launcher
const SAQUETTO_VERSION = '1.0.12';
const SAQUETTO_API = 'https://saquettoos-access.leosaquetto.workers.dev';
const SAQUETTO_CATALOG = __SAQUETTO_CATALOG__;
const SAQUETTO_HTML = __SAQUETTO_HTML__;
const SAQUETTO_ASSETS = __SAQUETTO_ASSETS__;
const SAQUETTO_COMPATIBILITY = __SAQUETTO_COMPATIBILITY__;
const SAQUETTO_SELF = { id: 'saquettoos', name: 'saquettoOS', scriptName: 'saquettoOS',
  manifestBase: 'https://raw.githubusercontent.com/leosaquetto/uol-bot/main/saquettoos/releases/',
  artifactPath: 'saquettoos/dist/saquettoOS.js', envelope: 'saquettoos' };

__SAQUETTO_UPDATER__
__SAQUETTO_ADAPTERS__
__SAQUETTO_RUNTIME__

const SaquettoHost = (() => {
  const key = 'saquettoos.identity.v1';
  const stateFile = 'saquettoos.settings.v1.json';
  const bootstrapFile = 'saquettoos-owner-bootstrap.json';
  const defaults = { appearance: 'system', material: 'glass', channel: 'stable', showAppNames: false,
    favorites: ['clube-uol', 'bilhete-unico', 'super-calendario', 'media-downloader'],
    notifications: { enabled: false, updates: true, access: true } };
  function preferences(value = {}) {
    return { appearance: ['system', 'light', 'dark'].includes(value.appearance) ? value.appearance : defaults.appearance,
      material: ['glass', 'solid'].includes(value.material) ? value.material : defaults.material,
      showAppNames: typeof value.showAppNames === 'boolean' ? value.showAppNames : defaults.showAppNames,
      channel: ['stable', 'beta'].includes(value.channel) ? value.channel : defaults.channel,
      favorites: Array.isArray(value.favorites) ? [...new Set(value.favorites.filter(id => SAQUETTO_CATALOG.some(app => app.id === id)))].slice(0, 4) : defaults.favorites.slice(),
      notifications: Object.fromEntries(['enabled', 'updates', 'access'].map(name => [name,
        typeof value.notifications?.[name] === 'boolean' ? value.notifications[name] : defaults.notifications[name]])) };
  }
  function identityValid(value) {
    return value && /^[a-f0-9-]{36}$/i.test(value.deviceId) && /^[a-f0-9]{64}$/.test(value.token);
  }
  function appFor(id) { return id === 'saquettoos' ? SAQUETTO_SELF : SAQUETTO_CATALOG.find(app => app.id === id); }
  function scriptFile(fm, app) {
    const matches = fm.listContents(fm.documentsDirectory()).filter(name =>
      name.normalize('NFC') === (app.scriptName + '.js').normalize('NFC'));
    if (matches.length > 1) throw new Error('script_name_conflict');
    return matches.length ? { name: matches[0].slice(0, -3), path: fm.joinPath(fm.documentsDirectory(), matches[0]) } : null;
  }
  function safeJSON(value) { return JSON.stringify(value).replace(/</g, '\\u003c').replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029'); }
  function create(deps) {
    const { fm, local, keychain, request, uuid, launch, notify, now = Date.now, scriptName = 'saquettoOS' } = deps;
    const disk = local.joinPath(local.documentsDirectory(), stateFile);
    let saved = {}, identity = null, update = { items: [] }, runtime = null;
    if (local.fileExists(disk)) {
      try { saved = JSON.parse(local.readString(disk)); } catch (_) { throw new Error('settings_unreadable'); }
    }
    let prefs = preferences(saved.preferences);
    let notifications = Array.isArray(saved.notifications) ? saved.notifications.slice(-50) : [];
    let lastAccess = saved.lastAccess || '', noticeKeys = Array.isArray(saved.noticeKeys) ? saved.noticeKeys.slice(-100) : [];
    let profile = { name: 'Seu espaço', role: 'member', status: 'not_registered', appIds: [] };
    const updater = SaquettoUpdater.create({ fm, local, request, uuid, scriptName });
    function persist() {
      const data = JSON.stringify({ preferences: prefs, notifications, lastAccess, noticeKeys });
      local.writeString(disk, data);
      if (local.readString(disk) !== data) throw new Error('settings_unverified');
    }
    function loadIdentity() {
      if (!keychain.contains(key)) return null;
      let value;
      try { value = JSON.parse(keychain.get(key)); } catch (_) { throw new Error('identity_unreadable'); }
      if (!identityValid(value)) throw new Error('identity_invalid');
      return value;
    }
    async function consumeBootstrap() {
      identity = loadIdentity();
      const file = fm.joinPath(fm.documentsDirectory(), bootstrapFile);
      if (!fm.fileExists(file)) return;
      if (fm.isFileDownloaded && !fm.isFileDownloaded(file)) await fm.downloadFileFromiCloud(file);
      const value = JSON.parse(fm.readString(file));
      if (!identityValid(value) || value.api !== SAQUETTO_API) throw new Error('bootstrap_invalid');
      if (identity && (identity.deviceId !== value.deviceId || identity.token !== value.token)) throw new Error('identity_conflict');
      keychain.set(key, JSON.stringify({ deviceId: value.deviceId, token: value.token }));
      identity = loadIdentity();
      if (!identity || identity.token !== value.token) throw new Error('identity_unverified');
      fm.remove(file); // This one-time credential has been verified in Keychain.
    }
    async function api(path, method = 'GET', body) {
      const req = request(SAQUETTO_API + path);
      req.method = method; req.timeoutInterval = 15; req.onRedirect = () => null;
      req.headers = { Accept: 'application/json', ...(identity ? { Authorization: 'Bearer ' + identity.token } : {}) };
      if (body !== undefined) { req.headers['Content-Type'] = 'application/json'; req.body = JSON.stringify(body); }
      let data;
      try { data = await req.loadJSON(); } catch (_) { throw new Error('access_unavailable'); }
      const status = req.response?.statusCode;
      if (status < 200 || status >= 300 || !Number.isInteger(status)) {
        const code = typeof data?.error === 'string' && /^[a-z_]{1,60}$/.test(data.error) ? data.error : 'access_unavailable';
        throw new Error(code);
      }
      return data;
    }
    async function addNotice(id, title, body, appId, category) {
      if (noticeKeys.includes(id)) return;
      const item = { id, title, body, appId: appFor(appId)?.id || '', date: new Date(now()).toISOString(), read: false };
      notifications = [...notifications, item].slice(-50); noticeKeys = [...noticeKeys, id].slice(-100);
      persist();
      if (prefs.notifications.enabled && prefs.notifications[category]) {
        try { await notify(item); } catch (_) { /* Inbox remains available if iOS denies delivery. */ }
      }
    }
    async function refreshProfile() {
      if (!identity) return profile;
      let next;
      try {
        next = (await api('/v1/me')).profile;
        if (!next || !['pending', 'approved', 'revoked', 'expired'].includes(next.status) ||
            !['admin', 'member'].includes(next.role)) throw new Error('access_invalid');
        next = { id: next.id, name: String(next.name || '').slice(0, 60), role: next.role, status: next.status,
          code: String(next.code || ''), appIds: Array.isArray(next.appIds) ? next.appIds.filter(id => appFor(id) && id !== 'saquettoos') : [] };
      } catch (_) { profile = { ...profile, role: 'member', status: 'unavailable', appIds: [] }; return profile; }
      profile = next;
      if (lastAccess && lastAccess !== next.status) {
        await addNotice('access-' + next.status + '-' + now(), 'Acesso ao saquettoOS',
          next.status === 'approved' ? 'Seu acesso foi aprovado. Abra para conferir seus apps.' : 'Seu acesso mudou. Consulte seu perfil.', '', 'access');
      }
      lastAccess = next.status; persist();
      return profile;
    }
    async function state(refresh = false) {
      if (refresh) await refreshProfile();
      const apps = [];
      for (const app of SAQUETTO_CATALOG) {
        let found = null, issue = '';
        try { found = scriptFile(fm, app); } catch (_) { issue = 'script_name_conflict'; }
        apps.push({ id: app.id, name: app.name, scriptName: app.scriptName, icon: app.icon, color: app.color,
          mode: app.mode, installed: !!found, issue, allowed: profile.status === 'approved' && profile.appIds.includes(app.id),
          version: update.items.find(item => item.id === app.id)?.version || null,
          updateAvailable: update.items.find(item => item.id === app.id)?.available || false });
      }
      return { version: SAQUETTO_VERSION, profile, apps, preferences: prefs, notifications,
        accessConfigured: true, assets: SAQUETTO_ASSETS, update };
    }
    async function authorize(app) {
      if (!app || app.id === 'saquettoos') throw new Error('app_invalid');
      if (!identity) throw new Error('approval_required');
      const result = await api('/v1/access/' + app.id);
      if (result.allowed !== true) throw new Error('approval_required');
    }
    async function verifyCurrentUOL(app, file) {
      const reviewed = SAQUETTO_COMPATIBILITY['clube-uol'];
      if (!reviewed || !Array.isArray(reviewed.sha256)) throw new Error('adapter_source_changed');
      if (fm.isFileDownloaded && !fm.isFileDownloaded(file.path)) await fm.downloadFileFromiCloud(file.path);
      const installedHash = SaquettoUpdater.fingerprint(fm.readString(file.path));
      if (reviewed.sha256.includes(installedHash)) return;
      // UOL can update itself after this launcher was built. Its updater's check
      // validates the fixed official manifest URL, immutable artifact path,
      // installed envelope and version. Only admit those exact installed bytes.
      const checked = await updater.check(app, prefs.channel);
      if (checked.status !== 'up_to_date' || !checked.manifest || checked.version !== checked.manifest.version ||
          checked.manifest.sha256 !== installedHash) throw new Error('adapter_source_changed');
      reviewed.sha256.push(installedHash);
    }
    async function openApp(id, standalone = false) {
      const app = appFor(id);
      await authorize(app);
      const file = scriptFile(fm, app);
      if (!file) throw new Error('app_not_installed');
      if (!standalone) {
        if (!runtime) throw new Error('runtime_unavailable');
        if (id === 'clube-uol') await verifyCurrentUOL(app, file);
        return runtime.open(id);
      }
      if (runtime?.activeId) await runtime.close();
      await launch('scriptable:///run?scriptName=' + encodeURIComponent(file.name));
      return { status: 'dispatched', appId: id, mode: 'external' };
    }
    async function checkUpdates() {
      const items = [];
      for (const app of [SAQUETTO_SELF, ...SAQUETTO_CATALOG]) {
        const result = await updater.check(app, prefs.channel);
        items.push({ id: app.id, name: app.name, status: result.status, version: result.version || null,
          latestVersion: result.latestVersion || result.manifest?.version || null, available: result.status === 'available' });
        if (result.status === 'available') await addNotice('update-' + app.id + '-' + result.manifest.version,
          'Atualização disponível', app.name + ' · ' + result.manifest.version, app.id, 'updates');
      }
      update = { items, checkedAt: new Date(now()).toISOString() };
      return update;
    }
    async function installUpdate(id) {
      const app = appFor(id);
      if (!app) throw new Error('app_invalid');
      if (runtime?.activeId) throw new Error('close_app_before_update');
      if (id !== 'saquettoos') await authorize(app);
      const result = await updater.install(app, prefs.channel);
      if (result.status === 'installed') {
        await addNotice('installed-' + id + '-' + result.version, 'Atualização concluída',
          app.name + ' foi verificado. Abra novamente para usar a versão ' + result.version + '.', id, 'updates');
      }
      // A dispatch, attempted write or ambiguous result is never called installed.
      return result;
    }
    async function dispatch(action, payload = {}) {
      if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error('payload_invalid');
      if (action === 'getState') return { state: await state(true) };
      if (action === 'openApp' || action === 'launchStandalone') return openApp(payload.id || payload.appId, action === 'launchStandalone');
      if (['appAction', 'appEvalResult', 'closeApp'].includes(action)) {
        if (!runtime) throw new Error('runtime_unavailable');
        return runtime.dispatch(action, payload);
      }
      if (action === 'returnHome') return { state: await state(true) };
      if (action === 'setPreferences') {
        if (payload.channel !== undefined && !['stable', 'beta'].includes(payload.channel)) throw new Error('channel_invalid');
        prefs = preferences({ ...prefs, ...payload }); persist(); return { state: await state() };
      }
      if (action === 'setNotificationPreferences') {
        prefs = preferences({ ...prefs, notifications: { ...prefs.notifications, ...payload } }); persist();
        return { state: await state() };
      }
      if (action === 'testNotification') {
        if (!prefs.notifications.enabled) throw new Error('notifications_disabled');
        await notify({ id: 'test-' + uuid(), title: 'saquettoOS', body: 'Notificações ativadas neste aparelho.', appId: '' });
        return { status: 'scheduled' };
      }
      if (action === 'requestAccess') {
        const name = typeof payload.name === 'string' ? payload.name.trim() : '';
        if (!name || name.length > 60 || /[\u0000-\u001f\u007f]/.test(name)) throw new Error('name_invalid');
        if (!identity) {
          const next = { deviceId: uuid().toLowerCase(), token: (uuid() + uuid()).replace(/-/g, '').toLowerCase() };
          if (!identityValid(next)) throw new Error('identity_invalid');
          keychain.set(key, JSON.stringify(next)); identity = loadIdentity();
          if (!identity || identity.token !== next.token) throw new Error('identity_unverified');
        }
        await api('/v1/enroll', 'POST', { name, ...identity });
        return { state: await state(true) };
      }
      if (action === 'listUsers') return api('/v1/users');
      if (action === 'decideUser') {
        const result = await api('/v1/decision', 'POST', { id: payload.id, status: payload.status, appIds: payload.appIds, code: payload.code });
        return { ...result, ...(await api('/v1/users')) };
      }
      if (action === 'checkUpdates') return checkUpdates();
      if (action === 'installUpdate') return installUpdate(payload.id || payload.appId || 'saquettoos');
      throw new Error('action_invalid');
    }
    async function start() {
      await consumeBootstrap();
      const recoveries = [];
      for (const app of [SAQUETTO_SELF, SAQUETTO_CATALOG[0]]) recoveries.push({ id: app.id, ...(await updater.recover(app)) });
      const unresolved = recoveries.filter(item => ['recovery_required', 'concurrent_change'].includes(item.status));
      if (unresolved.length) update = { items: unresolved.map(item => ({ ...item, name: appFor(item.id).name })) };
      return state(true);
    }
    return { start, state, dispatch, preferences, updater, attachRuntime(value) { runtime = value; } };
  }
  return { create, preferences, safeJSON, scriptFile, identityValid };
})();

async function saquettoMain() {
  if (Script.name() !== 'saquettoOS') {
    const alert = new Alert(); alert.title = 'Nome do script';
    alert.message = 'Salve este script com o nome saquettoOS para usar sua instalação e suas atualizações.';
    alert.addAction('OK'); await alert.presentAlert(); return;
  }
  const local = FileManager.local();
  // module.filename is documented by Scriptable; a local run must not update an iCloud namesake.
  const filename = typeof module !== 'undefined' ? module.filename : '';
  const fm = filename && local.isFileStoredIniCloud(filename) ? FileManager.iCloud() : local;
  const parameters = typeof args.shortcutParameter === 'object' && args.shortcutParameter ? args.shortcutParameter : {};
  const query = args.queryParameters || {};
  const action = String(query.action || parameters.action || 'open');
  const allowedApp = SAQUETTO_CATALOG.find(app => app.id === (query.appId || parameters.appId || args.notification?.userInfo?.appId));
  if (config.runsInNotification) {
    await QuickLook.present('saquettoOS\n' + (allowedApp?.name || 'Sua central de apps') + '\nAbra a notificação para continuar.');
    return;
  }
  async function notify(item) {
    const notification = new Notification();
    notification.identifier = 'saquettoos.' + item.id;
    notification.threadIdentifier = 'saquettoos.' + (item.appId || 'system');
    notification.title = item.title; notification.body = item.body; notification.sound = null;
    notification.scriptName = 'saquettoOS'; notification.userInfo = { appId: item.appId || '' };
    notification.openURL = 'scriptable:///run?scriptName=saquettoOS' + (item.appId && item.appId !== 'saquettoos' ? '&appId=' + encodeURIComponent(item.appId) : '');
    notification.addAction('Abrir saquettoOS', notification.openURL);
    await notification.schedule();
  }
  let host;
  try {
    host = SaquettoHost.create({ fm, local, keychain: Keychain, request: url => new Request(url),
      uuid: () => UUID.string(), launch: url => Safari.open(url), notify, scriptName: Script.name() });
  } catch (_) {
    const alert = new Alert(); alert.title = 'Configuração indisponível';
    alert.message = 'Não foi possível ler os ajustes do saquettoOS. O arquivo foi preservado para recuperação.';
    alert.addAction('OK'); await alert.presentAlert(); return;
  }
  if (action === 'update' || action === 'update-auto') {
    await host.start();
    if (query.channel || parameters.channel) await host.dispatch('setPreferences', { channel: query.channel || parameters.channel });
    const result = await host.dispatch('installUpdate', { id: query.appId || parameters.appId || 'saquettoos' });
    Script.setShortcutOutput(JSON.stringify(result)); return;
  }
  const view = new WebView();
  let dismissed = false;
  let evaluationTail = Promise.resolve();
  let cancelEvaluation = null;
  const evaluate = code => {
    const next = evaluationTail.then(() => {
      if (dismissed) throw new Error('app_closed');
      return new Promise((resolve, reject) => {
        let settled = false;
        const finish = (error, value) => {
          if (settled) return;
          settled = true;
          if (cancelEvaluation === cancel) cancelEvaluation = null;
          if (error) reject(error); else resolve(value);
        };
        // Scriptable has no WebView evaluation cancellation API. Release only our wait
        // on dismissal; ignore late native results and refuse all queued evaluations.
        const cancel = () => finish(new Error('app_closed'));
        cancelEvaluation = cancel;
        try { Promise.resolve(view.evaluateJavaScript(code)).then(value => finish(null, value), error => finish(error)); }
        catch (error) { finish(error); }
      });
    });
    evaluationTail = next.catch(() => {});
    return next;
  };
  let cancelIdle = null;
  const pause = ms => dismissed ? Promise.resolve() : new Promise(resolve => {
    const timer = Timer.schedule(ms, false, () => { cancelIdle = null; resolve(); });
    cancelIdle = () => { timer.invalidate(); cancelIdle = null; resolve(); };
  });
  // Only fixed stage names and layout measurements; never identity, app data or errors containing URLs.
  const diagnostic = { version: SAQUETTO_VERSION, stages: [] };
  function trace(stage, layout) {
    diagnostic.stages.push({ stage, at: new Date().toISOString(), ...(layout ? { layout } : {}) });
    try { fm.writeString(fm.joinPath(fm.documentsDirectory(), 'saquettoos.startup.v1.json'), JSON.stringify(diagnostic)); } catch (_) {}
  }
  const failureHTML = () => '<!doctype html><html lang="pt-BR"><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>saquettoOS</title></head>' +
    '<body style="margin:0;background:#101225;color:#f6f7ff;font:17px -apple-system,sans-serif;padding:80px 28px"><h1 style="font-size:30px">saquettoOS</h1>' +
    '<p>A interface não terminou de carregar. Feche esta janela para ver o diagnóstico.' +
    '</p><p style="color:#afb7d0;font-size:13px">Versão ' + SAQUETTO_VERSION + '</p></body></html>';
  const baseURL = SAQUETTO_API + '/app/';
  let currentPreferences = (await host.state()).preferences;
  const runtime = SaquettoRuntime.create({ fm, local, catalog: SAQUETTO_CATALOG, compatibility: SAQUETTO_COMPATIBILITY,
    hash: SaquettoUpdater.fingerprint, script: Script, shell: evaluate,
    uuid: () => UUID.string(), timer: (ms, callback) => Timer.schedule(ms, false, callback),
    request: url => new Request(url), nativeWebView: WebView, openURL: url => Safari.open(url),
    theme: () => {
      const appearance = currentPreferences.appearance === 'system' ? (Device.isUsingDarkAppearance() ? 'dark' : 'light') : currentPreferences.appearance;
      return currentPreferences.material === 'glass' && appearance === 'dark' ? 'glass' : appearance;
    } });
  host.attachRuntime(runtime);
  const initial = await host.state();
  initial.profile = { ...initial.profile, status: 'loading' };
  trace('starting');
  try {
    // The first document already contains the home screen. No splash or second load.
    // Keep native presentation before any evaluateJavaScript call.
    await view.loadHTML(SAQUETTO_HTML.replace('__SAQUETTO_INITIAL_JSON__', () => SaquettoHost.safeJSON(initial)), baseURL);
    trace('startup_loaded');
  } catch (_) {
    trace('startup_load_failed');
    const alert = new Alert(); alert.title = 'Não foi possível abrir o saquettoOS';
    alert.message = 'A interface não terminou de carregar. Feche e abra o script novamente. Versão ' + SAQUETTO_VERSION + '.';
    alert.addAction('OK'); await alert.presentAlert(); return;
  }
  const presentation = view.present(true).finally(() => {
    dismissed = true;
    if (cancelIdle) cancelIdle();
    if (cancelEvaluation) cancelEvaluation();
    trace('dismissed');
  });
  trace('presentation_requested');
  const tasks = new Set();
  let initialized = false, startupFailure = false, synchronized = Promise.resolve();
  const ready = host.start().then(state => {
    initialized = true; return state;
  }).catch(() => {
    trace('access_initialization_failed');
    return { ...initial, profile: { name: 'Seu espaço', role: 'member', status: 'unavailable', appIds: [] } };
  });
  try {
    // present() resolves on dismissal, not on appearance. Give the native transition a turn.
    await pause(800);
    if (!dismissed) {
      view.shouldAllowRequest = req => runtime.shouldAllowRequest(req);
      if (!dismissed) {
        const layout = await evaluate(`(() => {
          const shell = document.getElementById('shell');
          const rect = shell && shell.getBoundingClientRect();
          return { ready: !!(window.__SAQUETTO_RENDERED && typeof window.saquettoNative === 'function' && Array.isArray(window.__SAQUETTO_QUEUE)),
            width: Math.round(rect ? rect.width : 0), height: Math.round(rect ? rect.height : 0) };
        })()`);
        if (!layout?.ready || layout.width <= 0 || layout.height <= 0) throw new Error('interface_unavailable');
        trace('dashboard_ready', { width: layout.width, height: layout.height });
        synchronized = ready.then(async state => {
          if (dismissed) return;
          currentPreferences = state.preferences;
          // Assets are already loaded. Only the small verified access/state patch crosses the bridge.
          const { assets, ...patch } = state;
          await evaluate('window.saquettoSetState(' + SaquettoHost.safeJSON(patch) + ');');
          if (!dismissed && initialized && allowedApp && action === 'open') {
            const opening = host.dispatch('openApp', { id: allowedApp.id }).catch(() => {});
            tasks.add(opening); opening.finally(() => tasks.delete(opening));
          }
        }).catch(() => { if (!dismissed) trace('access_state_failed'); });
        tasks.add(synchronized); synchronized.finally(() => tasks.delete(synchronized));
      }
    }
  } catch (_) {
    if (!dismissed) {
      startupFailure = true; trace('dashboard_failed');
      // No app has opened on this failure path. Permit only the host's trusted replacement.
      try {
        view.shouldAllowRequest = () => true;
        await view.loadHTML(failureHTML(), baseURL);
      } catch (_) {}
      view.shouldAllowRequest = req => runtime.shouldAllowRequest(req);
    }
  }
  async function process(item) {
    let result;
    try {
      await synchronized;
      if (!initialized) throw new Error('initialization_failed');
      if (dismissed) throw new Error('app_closed');
      const data = await host.dispatch(item.action, item.payload);
      if (data?.state?.preferences) currentPreferences = data.state.preferences;
      result = { ok: true, data };
    } catch (error) { result = { ok: false, error: /^[a-z_]{1,60}$/.test(error?.message || '') ? error.message : 'operation_failed' }; }
    if (!dismissed) {
      try { await evaluate('window.saquettoResolve(' + JSON.stringify(item.id) + ',' + SaquettoHost.safeJSON(result) + ');'); } catch (_) {}
    }
  }
  while (!dismissed && !startupFailure) {
    // The child owns native presentation/evaluation until its own UI closes.
    // Do not poll the covered central while a child WebView callback is pending.
    if (runtime.nativeActive) { await runtime.waitForNative(); continue; }
    let items;
    try {
      // Return the queue directly; release our wait if the native view closes mid-evaluation.
      items = await evaluate('window.__SAQUETTO_QUEUE.splice(0, 20)');
    } catch (_) { if (!dismissed) trace('bridge_failed'); break; }
    if (dismissed || !Array.isArray(items)) break;
    for (const item of items) {
      if (!item || typeof item.id !== 'string' || typeof item.action !== 'string') continue;
      // Callback results must drain while an app action is awaiting its WebView evaluation.
      const task = process(item); tasks.add(task); task.finally(() => tasks.delete(task));
    }
    if (!items.length) await pause(100);
  }
  await ready;
  try { await runtime.shutdown(); } catch (_) {}
  await Promise.allSettled([...tasks]);
  await presentation;
  trace('complete');
  if (startupFailure) {
    const alert = new Alert(); alert.title = 'Diagnóstico saquettoOS';
    alert.message = 'Versão ' + SAQUETTO_VERSION + ': dashboard_failed. O registro de abertura foi salvo na pasta Scriptable como saquettoos.startup.v1.json.';
    alert.addAction('OK'); await alert.presentAlert();
  }
}

try { await saquettoMain(); }
finally { Script.complete(); }
