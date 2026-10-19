// Reuse the installed app's HTML and native business logic inside saquettoOS.
// Source code, credentials and account state remain on the user's device.
const SaquettoWebviewApps = (() => {
  const IDS = new Set(['clube-uol', 'bilhete-unico']);
  const bilheteModes = new Set(['painel', 'metro', 'extrato', 'tarifas', 'viagem', 'recarga', 'corrigir', 'desfazer', 'limite', 'auth', 'atualizar', 'checkFares', 'saveRouteCard', 'openMap', 'manageFavorites', 'favoriteShortcuts', 'routeStats', 'structuredHealth', 'icloudAssistant', 'widgetPreview', 'auditCapture', 'exportReports', 'cleanupCaptures', 'structuredSync', 'manageLogin', 'reconcile', 'close', 'destino']);
  function uolTransform(source) {
    const tail = /\nawait main\(\);\r?\nScript\.complete\(\);\s*$/;
    if (!tail.test(source) || !/^async function main\(\)\s*\{/m.test(source)) throw new Error('adapter_entrypoint_changed');
    const header = 'function buildDashboardHTML(historyData, settings, credentials, sessionCookies, realRedemptions, collectionPausedForUpdate = false, collectionResult = null) {';
    const start = source.indexOf(header);
    if (start < 0 || start !== source.lastIndexOf(header)) throw new Error('adapter_dashboard_changed');
    let builder = source.slice(start).replace(tail, '\n');
    const replaceOnce = (from, to) => {
      if (builder.split(from).length !== 2) throw new Error('adapter_dashboard_changed');
      builder = builder.replace(from, to);
    };
    // Only the presentation DTO changes. Native account functions retain their
    // original arguments, credential files and isolated authenticated sessions.
    replaceOnce(header, header + `
  const uiAccounts = {};
  for (const account of ["leosaquetto", "gaoriel"]) {
    const entry = credentials && credentials.accounts && credentials.accounts[account] || {};
    const login = typeof entry.login === "string" ? entry.login : "";
    const masked = login.length > 5 ? login.slice(0, 3) + "••••" + login.slice(-2) : login ? "••••••" : "";
    uiAccounts[account] = { login: masked.replace(/[^a-zA-Z0-9@._•-]/g, "•"), hasCredentials: Boolean(login && entry.senha) };
  }
  const uiSettings = {};
  for (const key of ["currentUser", "currentTab", "priorizarWords", "despriorizarWords", "groupedView", "myRedemptions", "realRedemptions", "navigationBarTheme"]) {
    if (Object.prototype.hasOwnProperty.call(settings, key)) uiSettings[key] = settings[key];
  }`);
    replaceOnce('    settings: settings,', '    settings: uiSettings,');
    replaceOnce('    credentials: credentials,', '    credentials: { accounts: uiAccounts, source: "Login nativo" },');
    replaceOnce('    sessionCookies: (sessionCookies && sessionCookies.length > 0) ? sessionCookies : cookiesForActiveUser,', '    sessionCookies: [],');
    replaceOnce('    sessionCookiesLeo: settings.userSessions.leosaquetto,', '    sessionCookiesLeo: [],');
    replaceOnce('    sessionCookiesGaoriel: settings.userSessions.gaoriel,', '    sessionCookiesGaoriel: [],');
    replaceOnce('  return resolveHunterAssets(rawHtml).replace("__INITIAL_DATA_JSON__", () => jsonStr);', `  const uiHtml = resolveHunterAssets(rawHtml).replace("__INITIAL_DATA_JSON__", () => jsonStr)
    .replaceAll("Boolean(targetCreds.login && targetCreds.senha)", "Boolean(targetCreds.hasCredentials)")
    .replaceAll("Boolean(userCreds.login && userCreds.senha)", "Boolean(userCreds.hasCredentials)");
  // Legacy credential helpers now enter the existing native reconnect flow.
  // No raw login, password or cookie is available to these iframe functions.
  const nativeLogin = '<script>openAuthAssistantModal = reconnectUolSession; copyAuthLogin = reconnectUolSession; copyAuthPassword = reconnectUolSession; openUolLogoutAndLogin = reconnectUolSession; openUolDirectLogin = reconnectUolSession; toggleAuthPassVisibility = reconnectUolSession;</script>';
  if (!uiHtml.includes("</body>")) throw new Error("adapter_dashboard_changed");
  return uiHtml.replace("</body>", nativeLogin + "</body>");`);
    return source.slice(0, start) + builder;
  }
  function bilheteTransform(source) {
    const anchor = '\ntry{\n  cleanupRetiredInfrastructureKeys();';
    const tail = '\ntry{\n  cleanupRetiredInfrastructureKeys();\n  if(config.runsInWidget)Script.setWidget(await widget());\n  else await panel(args.queryParameters?.mode||\'painel\');\n}catch(e){\n  if(config.runsInWidget){recordAutomaticError(e);const w=new ListWidget();w.addText(\'Abra Bilhete Único no Scriptable\');w.url=open(\'painel\');Script.setWidget(w);}\n  else await errorDialog(e);\n}\nScript.complete();';
    const at = source.indexOf(anchor);
    if (at < 0 || at !== source.lastIndexOf(anchor) || source.slice(at).trimEnd() !== tail || !/^async function panel\(mode\)\s*\{/m.test(source)) throw new Error('adapter_entrypoint_changed');
    return source.slice(0, at);
  }
  function bilheteRoute(raw) {
    if (typeof raw !== 'string' || raw.length > 8192 || !/^scriptable:\/\/\/run\?/.test(raw) || /[\x00-\x20]/.test(raw) || raw.includes('#')) return null;
    const values = Object.create(null);
    try {
      for (const part of raw.slice('scriptable:///run?'.length).split('&')) {
        const at = part.indexOf('='); if (at < 1) return null;
        const key = decodeURIComponent(part.slice(0, at)), value = decodeURIComponent(part.slice(at + 1));
        if (!['scriptName', 'mode', 'origem', 'destino', 'favorito', 'tab'].includes(key) || Object.prototype.hasOwnProperty.call(values, key) || value.length > 1000 || /[\x00-\x1f]/.test(value)) return null;
        values[key] = value;
      }
      if (!values.scriptName || values.scriptName.normalize('NFC') !== 'Bilhete Único' || !bilheteModes.has(values.mode || 'painel')) return null;
      if (values.tab && !['home', 'metro', 'history', 'settings'].includes(values.tab)) return null;
      const mode = values.mode || 'painel'; delete values.scriptName; values.mode = mode;
      return { mode, params: values };
    } catch (_) { return null; }
  }
  function themedHTML(html, theme) {
    if (typeof html !== 'string') throw new Error('invalid_app_html');
    const value = typeof theme === 'function' ? theme() : theme;
    const light = '--bg:#f7f7fb;--card-bg:#fff;--card-elevated:#f0f0f7;--card-border:#e0e2ec;--text-primary:#212331;--text-main:#212331;--text-muted:#606779;--text-dim:#70778a;--code-bg:#eceef5;color-scheme:light';
    const css = value === 'light' ? ':root{' + light + '}' : value === 'glass' ? ':root{--bg:transparent;--card-bg:rgba(22,24,37,.65);--card-elevated:rgba(30,33,48,.75);--card-border:rgba(255,255,255,.14)}' : value === 'system' ? '@media(prefers-color-scheme:light){:root{' + light + '}}' : '';
    const style = '<style data-saquetto-app-theme>' + css + '@media(prefers-reduced-motion:reduce){*{scroll-behavior:auto!important;animation-duration:.01ms!important;transition-duration:.01ms!important}}</style>';
    return html.includes('</head>') ? html.replace('</head>', style + '</head>') : style + html;
  }
  function create({ loader, WebView: NativeWebView, render, evalFrame, openURL, theme = 'system', closeRequested = () => {} }) {
    if (!loader || typeof loader.load !== 'function' || typeof render !== 'function' || typeof evalFrame !== 'function') throw new Error('webview_adapter_host_required');
    async function open(appId) {
      if (!IDS.has(appId)) throw new Error('unsupported_webview_app');
      let disposed = false, disposal = null, closeScheduled = false, current = null, revision = 0, renderQueue = Promise.resolve(), lastHTML = null, busy = false;
      const nativeViews = new Set(), embeddedViews = new Set(), tasks = new Set(), listeners = new Set();
      const args = { queryParameters: {}, shortcutParameter: null, widgetParameter: null, urls: [], plainTexts: [], images: [], fileURLs: [] };
      const emit = () => { for (const listener of [...listeners]) listener(revision); };
      function waitForPresentation(after = revision) {
        let cancel;
        const promise = new Promise(resolve => { const listener = next => { if (next > after || disposed) { listeners.delete(listener); resolve({ shown: next > after, disposed }); } }; listeners.add(listener); cancel = () => listeners.delete(listener); });
        return { promise, cancel };
      }
      function track(promise) { tasks.add(promise); promise.then(() => tasks.delete(promise), () => tasks.delete(promise)); return promise; }
      class EmbeddedWebView {
        constructor() { this.native = null; this.nativeOnly = false; this.html = null; this.baseURL = undefined; this.active = false; this.closed = false; this.navigation = null; this.presentation = null; this.resolvePresentation = null; }
        set shouldAllowRequest(handler) { this.navigation = handler; if (this.native) this.native.shouldAllowRequest = handler; }
        get shouldAllowRequest() { return this.navigation; }
        set hunterActionTask(task) { this.actionTask = task && typeof task.then === 'function' ? track(Promise.resolve(task)) : task; }
        get hunterActionTask() { return this.actionTask; }
        async nativeView(loadPending = true) {
          if (!this.native) {
            if (typeof NativeWebView !== 'function') throw new Error('native_webview_unavailable');
            this.native = new NativeWebView(); nativeViews.add(this.native);
            if (this.navigation) this.native.shouldAllowRequest = this.navigation;
            if (loadPending && this.html !== null) await this.native.loadHTML(this.html, this.baseURL);
          }
          return this.native;
        }
        async loadURL(url) {
          if (disposed || this.closed) throw new Error('app_closed');
          this.nativeOnly = true; const native = await this.nativeView(false); return track(Promise.resolve(native.loadURL(url)));
        }
        async loadRequest(request) {
          if (disposed || this.closed) throw new Error('app_closed');
          this.nativeOnly = true; const native = await this.nativeView(false); return track(Promise.resolve(native.loadRequest(request)));
        }
        async loadHTML(html, baseURL) {
          if (disposed || this.closed) return;
          this.html = html; this.baseURL = baseURL;
          if (this.nativeOnly || this.native && !this.active) { const native = await this.nativeView(false); return native.loadHTML(html, baseURL); }
          if (this.active) return this.renderHTML();
        }
        async renderHTML() {
          const html = themedHTML(this.html, theme), view = this;
          renderQueue = renderQueue.catch(() => {}).then(async () => {
            if (disposed || view.closed) return;
            await render(html, view.baseURL);
            if (disposed || view.closed) return;
            lastHTML = html; revision++; emit();
          });
          return renderQueue;
        }
        async evaluateJavaScript(code, useCallback = false) {
          if (disposed || this.closed) throw new Error('app_closed');
          if (this.active && current === this) { await renderQueue; if (disposed || this.closed || current !== this) throw new Error('stale_app_view'); return evalFrame(code, useCallback); }
          const native = await this.nativeView(); return track(Promise.resolve(native.evaluateJavaScript(code, useCallback)));
        }
        async getHTML() {
          if (this.active && current === this) return evalFrame('document.documentElement.outerHTML', false);
          const native = await this.nativeView(); return native.getHTML();
        }
        async present(fullscreen = true) {
          if (disposed || this.closed) return;
          if (this.nativeOnly || this.native) { const native = await this.nativeView(); return track(Promise.resolve(native.present(fullscreen))); }
          if (this.html === null) throw new Error('app_html_missing');
          if (this.presentation) return this.presentation;
          if (current && current !== this) current.dismiss();
          current = this; this.active = true; embeddedViews.add(this);
          this.presentation = new Promise(resolve => { this.resolvePresentation = resolve; });
          await this.renderHTML();
          return this.presentation;
        }
        dismiss() {
          this.closed = true; this.active = false;
          if (this.resolvePresentation) this.resolvePresentation();
          embeddedViews.delete(this);
        }
      }
      const overrides = { WebView: EmbeddedWebView, args };
      // UOL's offer navigation starts detached Safari promises. Keep those in
      // the same drain as core actions without closing the user's native sheet.
      if (typeof Safari !== 'undefined') {
        overrides.Safari = {};
        for (const method of ['open', 'openInApp']) overrides.Safari[method] = (...values) => {
          if (disposed) return Promise.reject(new Error('app_closed'));
          return track(Promise.resolve().then(() => Safari[method](...values)));
        };
      }
      const core = await loader.load(appId, appId === 'clube-uol' ? '({main})' : '({panel})', appId === 'clube-uol' ? uolTransform : bilheteTransform, overrides);
      async function runUntilPresented(run) {
        const waiting = waitForPresentation(), task = track(Promise.resolve().then(run));
        // No error detail is returned: native errors may contain private URLs or payloads.
        const outcome = await Promise.race([waiting.promise, task.then(() => ({ finished: true }), () => ({ failed: true }))]);
        waiting.cancel();
        return outcome;
      }
      const first = await runUntilPresented(() => appId === 'clube-uol' ? core.main() : core.panel('painel'));
      if (first.failed || first.finished && !first.shown || disposed) {
        disposed = true; for (const view of embeddedViews) view.dismiss(); emit();
        throw new Error('app_open_failed');
      }
      async function currentTab() {
        try { const tab = await evalFrame("(document.querySelector('.tab-content.active')?.id || 'tab-home').replace(/^tab-/, '')", false); return ['home', 'metro', 'history', 'settings'].includes(tab) ? tab : 'home'; } catch (_) { return 'home'; }
      }
      async function navigateBilhete(route) {
        if (disposed || busy) return { status: 'error', reason: disposed ? 'disposed' : 'busy', allow: false };
        if (route.mode === 'close') {
          // The host closes by awaiting dispose(). Do not include that callback
          // in our tracked navigation promise, which dispose itself must drain.
          if (!closeScheduled) { closeScheduled = true; Promise.resolve().then(() => closeRequested()).catch(() => {}); }
          return { status: 'ok', allow: false };
        }
        busy = true;
        try {
          const tab = await currentTab();
          args.queryParameters = { ...route.params };
          const outcome = await runUntilPresented(() => core.panel(route.mode));
          if (disposed) return { status: 'error', reason: 'disposed', allow: false };
          if (outcome.failed) return { status: 'error', reason: 'native_action_failed', allow: false };
          if (!outcome.shown) {
            args.queryParameters = { mode: 'painel', tab };
            const refreshed = await runUntilPresented(() => core.panel('painel'));
            if (!refreshed.shown || refreshed.failed) return { status: 'error', reason: 'refresh_failed', allow: false };
          }
          return { status: 'ok', allow: false };
        } finally { busy = false; }
      }
      function allowOriginal(request) {
        if (disposed || !current || current.closed) return false;
        try { return typeof current.navigation === 'function' ? current.navigation(request) !== false : true; } catch (_) { return false; }
      }
      async function navigateOriginal(request) {
        const allow = allowOriginal(request);
        if (!allow) return { status: 'ok', allow: false };
        if (!/^https?:\/\//i.test(request.url) || typeof openURL !== 'function') return { status: 'error', reason: 'external_navigation_unavailable', allow: false };
        try { await openURL(request.url); return { status: 'ok', allow: false }; }
        catch (_) { return { status: 'error', reason: 'navigation_failed', allow: false }; }
      }
      function shouldAllowRequest(request) {
        const url = request && request.url;
        if (disposed || typeof url !== 'string' || url.length > 8192) return false;
        if (appId === 'bilhete-unico' && /^scriptable:/i.test(url)) {
          const route = bilheteRoute(url); if (route) track(navigateBilhete(route).catch(() => ({ status: 'error' })));
          return false;
        }
        // Never replace the host document. Native auth/offer handlers keep their
        // own URL checks; other web navigations use the host's authorized opener.
        track(navigateOriginal(request).catch(() => ({ status: 'error' })));
        return false;
      }
      return {
        get html() { return lastHTML; },
        shouldAllowRequest,
        async handle(action, payload = {}) {
          if (disposed) return { status: 'error', reason: 'disposed' };
          if (action !== 'webview.navigate' || typeof payload.url !== 'string' || payload.url.length > 8192) return { status: 'error', reason: 'invalid_action' };
          if (appId === 'bilhete-unico' && /^scriptable:/i.test(payload.url)) {
            const route = bilheteRoute(payload.url);
            return route ? track(navigateBilhete(route)) : { status: 'error', reason: 'invalid_route', allow: false };
          }
          return track(navigateOriginal({ url: payload.url }));
        },
        dispose() {
          if (disposal) return disposal;
          disposed = true; for (const view of [...embeddedViews]) view.dismiss(); emit();
          disposal = (async () => {
            await renderQueue.catch(() => {});
            // Native login sheets retain their own Close control. Waiting here
            // prevents another app instance from writing while one is still open.
            while (tasks.size) await Promise.allSettled([...tasks]);
          })();
          return disposal;
        }
      };
    }
    return Object.freeze({ open });
  }
  return Object.freeze({ create });
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoWebviewApps;
