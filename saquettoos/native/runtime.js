// The installed scripts remain local. Adapters run only reviewed source fingerprints.
const SaquettoRuntime = (() => {
  const json = value => JSON.stringify(value).replace(/</g, '\\u003c').replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
  function createLoader({ fm, catalog, compatibility, hash, script, globals = {} }) {
    const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
    return { async load(id, exports, transform, overrides = {}) {
      const app = catalog.find(item => item.id === id);
      if (!app || !compatibility[id]) throw new Error('app_invalid');
      const names = fm.listContents(fm.documentsDirectory()).filter(name => name.normalize('NFC') === (app.scriptName + '.js').normalize('NFC'));
      if (names.length !== 1) throw new Error(names.length ? 'script_name_conflict' : 'app_not_installed');
      const filename = fm.joinPath(fm.documentsDirectory(), names[0]);
      if (fm.isFileDownloaded && !fm.isFileDownloaded(filename)) await fm.downloadFileFromiCloud(filename);
      const original = fm.readString(filename);
      if (!compatibility[id].sha256.includes(hash(original))) throw new Error('adapter_source_changed');
      const source = transform(original);
      if (typeof source !== 'string') throw new Error('adapter_source_changed');
      const bindings = { ...globals, Script: { ...script, name: () => names[0].slice(0, -3), complete() {}, setShortcutOutput() {} },
        config: { runsInApp: true, runsInWidget: false, runsInActionExtension: false, runsWithSiri: false, runsInNotification: false },
        args: { queryParameters: {}, shortcutParameter: null, urls: [], plainTexts: [], images: [], fileURLs: [] },
        module: { filename, exports: {} }, ...overrides };
      return new AsyncFunction(...Object.keys(bindings), source + '\nreturn (' + exports + ');')(...Object.values(bindings));
    } };
  }
  // This function is serialized into the sandboxed child, before app scripts.
  function childBridge(initial) {
    const waiting = new Map(); let serial = 0;
    const request = (action, payload = {}) => new Promise((resolve, reject) => {
      const id = 'child-' + (++serial);
      waiting.set(id, { resolve, reject });
      parent.postMessage({ saquetto: 1, kind: 'request', id, instanceId: initial.instanceId, action, payload }, '*');
    });
    window.SaquettoApp = Object.freeze({ request });
    window.addEventListener('message', event => {
      if (event.source !== parent || event.data?.saquetto !== 1 || event.data.instanceId !== initial.instanceId) return;
      const message = event.data;
      if (message.kind === 'response') {
        const pending = waiting.get(message.id); if (!pending) return;
        waiting.delete(message.id);
        message.ok ? pending.resolve(message.data) : pending.reject(new Error(message.error || 'operation_failed'));
      } else if (message.kind === 'eval') {
        let done = false;
        const finish = (value, error) => {
          if (done) return; done = true;
          try { value = value === undefined ? null : JSON.parse(JSON.stringify(value)); }
          catch (_) { value = null; error = 'evaluation_result_invalid'; }
          parent.postMessage({ saquetto: 1, kind: 'evalResult', id: message.id, instanceId: message.instanceId, value, error }, '*');
        };
        try {
          if (message.useCallback) new Function('completion', message.code)(value => finish(value));
          else Promise.resolve((0, eval)(message.code)).then(value => finish(value), () => finish(null, 'evaluation_failed'));
        } catch (_) { finish(null, 'evaluation_failed'); }
      }
    });
    let values = { ...initial.storage };
    const storage = { get length() { return Object.keys(values).length; }, key: index => Object.keys(values)[index] || null,
      getItem: key => Object.prototype.hasOwnProperty.call(values, key) ? values[key] : null,
      setItem(key, value) { key = String(key); value = String(value); values[key] = value; request('storage.set', { key, value }).catch(() => storageError()); },
      removeItem(key) { key = String(key); delete values[key]; request('storage.remove', { key }).catch(() => storageError()); },
      clear() { values = {}; request('storage.clear').catch(() => storageError()); } };
    function storageError() {
      let notice = document.getElementById('saquetto-storage-error');
      if (!notice) { notice = document.createElement('div'); notice.id = 'saquetto-storage-error'; notice.setAttribute('role', 'alert'); notice.style.cssText = 'position:fixed;bottom:0;left:0;right:0;z-index:2147483647;padding:16px;background:#762c42;color:white;font:14px -apple-system'; (document.body || document.documentElement).appendChild(notice); }
      notice.textContent = 'Não foi possível salvar os ajustes desta tela. Feche e reabra o app.';
    }
    Object.defineProperty(window, 'localStorage', { value: storage });
    if (initial.proxyFetch) window.fetch = async (input, options = {}) => {
      const url = new URL(typeof input === 'string' ? input : input.url, initial.baseURL).href;
      if (options.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
      const headers = {}; new Headers(options.headers || {}).forEach((value, key) => { headers[key] = value; });
      const result = await request('web.fetch', { url, method: options.method || 'GET', headers });
      if (options.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
      const bytes = Uint8Array.from(atob(result.body), c => c.charCodeAt(0));
      return new Response([204,205,304].includes(result.status) ? null : bytes, { status: result.status, headers: result.headers });
    };
    document.addEventListener('click', event => {
      const anchor = event.target.closest?.('a[href]'); if (!anchor) return;
      const raw = anchor.getAttribute('href'); if (!raw || raw[0] === '#') return;
      if (/^(https?:|scriptable:|hunter-)/i.test(raw)) {
        event.preventDefault(); request('webview.navigate', { url: new URL(raw, initial.baseURL).href }).catch(() => {});
      }
    }, true);
  }
  function inject(html, initial) {
    const bridge = '<script>(' + childBridge.toString() + ')(' + json(initial) + ');</script>';
    return /<head(?:\s[^>]*)?>/i.test(html) ? html.replace(/<head(?:\s[^>]*)?>/i, match => match + bridge) : bridge + html;
  }
  function create(deps) {
    const { fm, local, catalog, compatibility, hash, script, shell, uuid, timer, request, nativeWebView, openURL, theme, globals } = deps;
    const loader = createLoader({ fm, catalog, compatibility, hash, script, globals });
    let active = null, opening = false, evalSequence = 0, shutdownRequested = false, openingDone = Promise.resolve(), finishOpening;
    const evaluations = new Map();
    function cancelEvaluations() { for (const item of evaluations.values()) { item.timer.invalidate(); item.reject(new Error('app_closed')); } evaluations.clear(); }
    async function render(html, baseURL = 'https://clube.uol.com.br') {
      const app = active; if (!app || app.closing || shutdownRequested) throw new Error('app_closed');
      cancelEvaluations();
      app.instanceId = uuid(); // Register before child scripts can post their first request.
      const prepared = inject(html, { instanceId: app.instanceId, storage: app.storage, baseURL, proxyFetch: app.id === 'clube-uol' });
      const result = await shell('window.saquettoShowApp(' + json({ id: app.id, name: app.name, html: prepared, instanceId: app.instanceId }) + ');');
      if (!result || app !== active) throw new Error('app_render_failed');
      app.instanceId = result;
      return result;
    }
    async function evalFrame(code, useCallback = false) {
      const app = active; if (!app?.instanceId || shutdownRequested) throw new Error('app_not_ready');
      const id = 'eval-' + (++evalSequence), instanceId = app.instanceId;
      return new Promise((resolve, reject) => {
        const timeout = timer(30000, () => { evaluations.delete(id); reject(new Error('evaluation_timeout')); });
        evaluations.set(id, { resolve, reject, timer: timeout, instanceId });
        shell('window.saquettoFrameMessage(' + json({ kind: 'eval', id, code, useCallback, instanceId }) + ');').then(ok => {
          if (!ok && evaluations.has(id)) { timeout.invalidate(); evaluations.delete(id); reject(new Error('app_not_ready')); }
        }, () => { timeout.invalidate(); evaluations.delete(id); reject(new Error('evaluation_failed')); });
      });
    }
    function matches(payload) { return active && payload.appId === active.id && payload.instanceId === active.instanceId; }
    async function proxyFetch(app, payload) {
      const match = typeof payload.url === 'string' && payload.url.match(/^https:\/\/([^/?#]+)(\/[^#]*)?$/i);
      const hosts = ['clube.uol.com.br', 'uol-telegram-shadow-pilot.leosaquetto.workers.dev'];
      if (app.id !== 'clube-uol' || !match || !hosts.includes(match[1].toLowerCase()) || !['GET','HEAD'].includes(payload.method)) throw new Error('network_not_allowed');
      const req = request(payload.url); req.method = payload.method; req.timeoutInterval = 30; req.onRedirect = () => null;
      req.headers = {};
      for (const [name, value] of Object.entries(payload.headers || {})) {
        if (!['accept', 'if-none-match', 'if-modified-since'].includes(name.toLowerCase()) || typeof value !== 'string' || value.length > 1024 || /[\r\n]/.test(value)) throw new Error('network_not_allowed');
        req.headers[name] = value;
      }
      const data = await req.load();
      const response = req.response;
      if (!response || response.statusCode >= 300 && response.statusCode !== 304 && response.statusCode < 400) throw new Error('redirect_not_allowed');
      const body = data.toBase64String();
      if (body.length > 12 * 1024 * 1024) throw new Error('response_too_large');
      const headers = {};
      for (const [name, value] of Object.entries(response.headers || {})) if (['content-type','etag','last-modified'].includes(name.toLowerCase())) headers[name] = value;
      return { body, status: response.statusCode, headers };
    }
    async function open(id) {
      if (shutdownRequested) throw new Error('app_closed');
      if (opening || active) throw new Error('app_busy');
      opening = true;
      openingDone = new Promise(resolve => { finishOpening = resolve; });
      try {
        const app = catalog.find(item => item.id === id); if (!app) throw new Error('app_invalid');
        const storagePath = local.joinPath(local.documentsDirectory(), 'saquettoos.appstate.' + id + '.json');
        let storage = {};
        if (local.fileExists(storagePath)) {
          try { storage = JSON.parse(local.readString(storagePath)); } catch (_) { throw new Error('app_storage_unreadable'); }
          if (!storage || Array.isArray(storage) || Object.values(storage).some(value => typeof value !== 'string')) throw new Error('app_storage_unreadable');
        }
        const session = active = { ...app, storagePath, storage, instanceId: '', panel: null, closing: false, storageFailed: false };
        const common = { loader, theme, render, WebView: nativeWebView, evalFrame, openURL, closeRequested: () => shell('window.saquettoHome();') };
        const factory = ['clube-uol','bilhete-unico'].includes(id) ? SaquettoWebviewApps : id === 'media-downloader' ? SaquettoMediaApp : SaquettoNativeApps;
        session.panel = await factory.create(common).open(id);
        if (shutdownRequested) throw new Error('app_closed');
        if (session.panel.html && !session.instanceId) await render(session.panel.html);
        return { status: 'opened', appId: id, mode: 'integrated' };
      } catch (error) { if (active?.panel) await active.panel.dispose(); active = null; cancelEvaluations(); throw error; }
      finally { opening = false; finishOpening(); }
    }
    async function close(payload) {
      if (!active) return { status: 'closed' };
      if (payload && !matches(payload)) throw new Error('app_instance_changed');
      if (opening || active.closing) throw new Error('app_busy');
      const app = active; app.closing = true;
      cancelEvaluations();
      try { await app.panel?.dispose(); active = null; return { status: 'closed' }; }
      catch (_) { app.closing = false; throw new Error('app_close_failed'); }
    }
    async function dispatch(action, payload) {
      if (shutdownRequested) throw new Error('app_closed');
      if (!matches(payload)) throw new Error('app_instance_changed');
      if (action === 'appEvalResult') {
        const pending = evaluations.get(payload.id);
        if (!pending || pending.instanceId !== active.instanceId) throw new Error('evaluation_invalid');
        pending.timer.invalidate(); evaluations.delete(payload.id);
        payload.error ? pending.reject(new Error('evaluation_failed')) : pending.resolve(payload.value);
        return { status: 'received' };
      }
      if (action === 'closeApp') return close(payload);
      if (action !== 'appAction' || active.closing) throw new Error('action_invalid');
      const app = active, message = payload.request;
      if (!message || typeof message.action !== 'string' || !message.payload || typeof message.payload !== 'object' || Array.isArray(message.payload)) throw new Error('payload_invalid');
      if (message.action.startsWith('storage.')) {
        if (app.storageFailed) throw new Error('app_storage_unverified');
        const data = message.payload, next = { ...app.storage };
        if (message.action === 'storage.clear') Object.keys(next).forEach(key => delete next[key]);
        else {
          if (typeof data.key !== 'string' || data.key.length > 160 || ['__proto__','constructor','prototype'].includes(data.key)) throw new Error('storage_key_invalid');
          if (message.action === 'storage.remove') delete next[data.key];
          else if (message.action === 'storage.set' && typeof data.value === 'string') next[data.key] = data.value;
          else throw new Error('payload_invalid');
        }
        const text = JSON.stringify(next); if (text.length > 2 * 1024 * 1024) throw new Error('storage_full');
        try { local.writeString(app.storagePath, text); if (local.readString(app.storagePath) !== text) throw new Error('readback'); app.storage = next; }
        catch (_) { app.storageFailed = true; throw new Error('app_storage_unverified'); }
        return { status: 'saved' };
      }
      if (message.action === 'web.fetch') return proxyFetch(app, message.payload);
      return app.panel.handle(message.action, message.payload);
    }
    function shouldAllowRequest(req) {
      if (['about:blank','about:srcdoc'].includes(req.url)) return true;
      if (active?.panel?.shouldAllowRequest) return active.panel.shouldAllowRequest(req);
      return false;
    }
    async function shutdown() {
      shutdownRequested = true; cancelEvaluations();
      await openingDone;
      return close();
    }
    return { open, close, shutdown, dispatch, shouldAllowRequest, get activeId() { return active?.id || null; } };
  }
  return { create, createLoader, inject };
})();
if (typeof module !== 'undefined' && module.exports) module.exports = SaquettoRuntime;
