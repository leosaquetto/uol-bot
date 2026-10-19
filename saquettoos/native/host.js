// Variables used by Scriptable.
// These must be at the very top of the file. Do not edit.
// icon-color: purple; icon-glyph: layer-group;
// saquettoOS Scriptable launcher
const SAQUETTO_VERSION = '1.0.1';
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
  const defaults = { appearance: 'system', material: 'glass', channel: 'stable',
    favorites: ['clube-uol', 'bilhete-unico', 'super-calendario', 'media-downloader'],
    notifications: { enabled: false, updates: true, access: true } };
  function preferences(value = {}) {
    return { appearance: ['system', 'light', 'dark'].includes(value.appearance) ? value.appearance : defaults.appearance,
      material: ['glass', 'solid'].includes(value.material) ? value.material : defaults.material,
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
    async function openApp(id, standalone = false) {
      const app = appFor(id);
      await authorize(app);
      const file = scriptFile(fm, app);
      if (!file) throw new Error('app_not_installed');
      if (!standalone) {
        if (!runtime) throw new Error('runtime_unavailable');
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
  const evaluate = code => dismissed ? Promise.reject(new Error('app_closed')) : view.evaluateJavaScript(code);
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
  try {
    // Scriptable does not specify the internal request URL used by loadHTML.
    // Install the app navigation filter only after this trusted, self-contained document loads.
    await view.loadHTML(SAQUETTO_HTML.replace('__SAQUETTO_INITIAL_JSON__', () => SaquettoHost.safeJSON(initial)));
    view.shouldAllowRequest = req => runtime.shouldAllowRequest(req);
    const loaded = await evaluate("!!(document.getElementById('shell') && typeof window.saquettoSetState === 'function' && typeof window.saquettoNative === 'function' && Array.isArray(window.__SAQUETTO_QUEUE))");
    if (!loaded) throw new Error('interface_unavailable');
  } catch (_) {
    const alert = new Alert(); alert.title = 'Não foi possível abrir o saquettoOS';
    alert.message = 'A interface não terminou de carregar. Feche e abra o script novamente. Versão ' + SAQUETTO_VERSION + '.';
    alert.addAction('OK'); await alert.presentAlert(); return;
  }
  let cancelIdle = null;
  const presentation = view.present(true).finally(() => {
    dismissed = true;
    if (cancelIdle) cancelIdle();
  });
  const tasks = new Set();
  // Show the offline shell before the access request. Native actions wait for verified initialization.
  const ready = host.start().then(async state => {
    if (!dismissed) await evaluate('window.saquettoSetState(' + SaquettoHost.safeJSON(state) + ');');
    if (!dismissed && allowedApp && action === 'open') {
      // Let the ordinary shell display a friendly failure; never launch another instance.
      const opening = host.dispatch('openApp', { id: allowedApp.id }).catch(() => {});
      tasks.add(opening); opening.finally(() => tasks.delete(opening));
    }
    return true;
  }).catch(async () => {
    if (!dismissed) {
      try { await evaluate('window.saquettoSetState({profile:{name:"Seu espaço",role:"member",status:"unavailable",appIds:[]}});'); } catch (_) {}
    }
    return false;
  });
  async function process(item) {
    let result;
    try {
      if (!await ready) throw new Error('initialization_failed');
      if (dismissed) throw new Error('app_closed');
      const data = await host.dispatch(item.action, item.payload);
      if (data?.state?.preferences) currentPreferences = data.state.preferences;
      result = { ok: true, data };
    } catch (error) { result = { ok: false, error: /^[a-z_]{1,60}$/.test(error?.message || '') ? error.message : 'operation_failed' }; }
    if (!dismissed) {
      try { await evaluate('window.saquettoResolve(' + JSON.stringify(item.id) + ',' + SaquettoHost.safeJSON(result) + ');'); } catch (_) {}
    }
  }
  while (!dismissed) {
    let items;
    try {
      // Every evaluation completes immediately. No native completion callback survives Close.
      items = await evaluate('window.__SAQUETTO_QUEUE.splice(0, 20)');
    } catch (_) { break; }
    if (dismissed || !Array.isArray(items)) break;
    for (const item of items) {
      if (!item || typeof item.id !== 'string' || typeof item.action !== 'string') continue;
      // Callback results must drain while an app action is awaiting its WebView evaluation.
      const task = process(item); tasks.add(task); task.finally(() => tasks.delete(task));
    }
    if (!items.length) await new Promise(resolve => {
      const timer = Timer.schedule(100, false, () => { cancelIdle = null; resolve(); });
      cancelIdle = () => { timer.invalidate(); cancelIdle = null; resolve(); };
    });
  }
  await ready;
  try { await runtime.shutdown(); } catch (_) {}
  await Promise.allSettled([...tasks]);
  await presentation;
}

await saquettoMain();
Script.complete();
