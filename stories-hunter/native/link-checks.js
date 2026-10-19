// Read-only campaign reachability checks. Network, parser and clock are injected
// so the same code runs in Scriptable and in offline tests.
const HunterLinkChecks = (() => {
  const ORIGIN = 'https://clube.uol.com.br';
  const MIN_GONE_GAP_MS = 5000;
  const MAX_GONE_EVIDENCE_AGE_MS = 120000;
  const MAX_BODY_LENGTH = 2000000;

  function canonicalTicketUrl(value) {
    const input = typeof value === 'string' ? value : value?.url || value?.link;
    if (typeof input !== 'string' || input.length > 2048 ||
        /[\s\\#\u0000-\u001f\u007f]/.test(input)) return '';
    const match = /^https:\/\/clube\.uol\.com\.br(\/campanhasdeingresso\/[A-Za-z0-9][A-Za-z0-9_-]{0,159})\/?(?:\?[^#]*)?$/.exec(input);
    return match ? ORIGIN + match[1] : '';
  }

  function header(headers, name) {
    return Object.entries(headers || {}).find(([key]) => key.toLowerCase() === name.toLowerCase())?.[1] || '';
  }

  function homeRedirect(location) {
    if (typeof location !== 'string' || location.length > 2048) return false;
    let path = location;
    if (/^https?:/i.test(path)) {
      const match = /^https:\/\/clube\.uol\.com\.br(?=\/|[?#]|$)/i.exec(path);
      if (!match) return false;
      path = path.slice(match[0].length) || '/';
    }
    return /^\/(?:campanhasdeingresso\/?)?(?:[?#].*)?$/.test(path);
  }

  function parserConfirmsPage(offer, url) {
    const proof = offer?.identityEvidence;
    return offer?.url === url && offer?.requiresLogin !== true &&
      typeof offer.title === 'string' && !!offer.title.trim() &&
      typeof offer.description === 'string' && !!offer.description.trim() &&
      proof?.scoped === true && proof.titleCount === 1 &&
      proof.descriptionCount === 1 && proof.canonicalMatches === true &&
      ((proof.actionCount === 1 && proof.actionMatches === true) || offer.soldOut === true);
  }

  function authOrChallenge(body) {
    return typeof body === 'string' &&
      /<input\b[^>]*type\s*=\s*["']?password\b|<title[^>]*>\s*(?:login|entrar|access denied|just a moment)|cf-chl-|challenge-platform|<(?:iframe|div)\b[^>]*(?:captcha|challenge)/i.test(body);
  }

  function create(env = {}) {
    const now = typeof env.now === 'function' ? env.now : Date.now;
    const pause = typeof env.pause === 'function' ? env.pause : ms => new Promise(resolve => {
      if (typeof Timer !== 'undefined' && typeof Timer.schedule === 'function')
        Timer.schedule(ms, false, resolve);
      else if (typeof setTimeout === 'function') setTimeout(resolve, ms);
      else resolve();
    });
    const request = env.request;
    const parseOffer = env.parseOffer;
    const iso = () => new Date(now()).toISOString();
    const unknown = evidence => ({ status: 'unknown', checkedAt: iso(), ...(evidence || {}) });

    async function probe(url, cookieHeader) {
      if (!url || typeof request !== 'function') return 'unknown';
      let req, body;
      try {
        req = request(url);
        req.method = 'GET';
        req.timeoutInterval = 4;
        req.onRedirect = () => null;
        req.headers = { Accept: 'text/html',
          ...(typeof cookieHeader === 'string' && cookieHeader.length <= 8192 &&
            !/[\u0000-\u001f\u007f]/.test(cookieHeader) ? { Cookie: cookieHeader } : {}) };
        body = await req.loadString();
      } catch (_) { return 'unknown'; }
      const response = req.response || {};
      const code = Number(response.statusCode);
      if (response.url && response.url !== url) return 'unknown';
      if (typeof body !== 'string' || body.length > MAX_BODY_LENGTH || authOrChallenge(body))
        return 'unknown';
      if (code === 404 || code === 410) return 'gone';
      if ([301, 302, 303, 307, 308].includes(code))
        return homeRedirect(header(response.headers, 'Location')) ? 'gone' : 'unknown';
      if (code !== 200 || typeof parseOffer !== 'function') return 'unknown';
      try { return parserConfirmsPage(await parseOffer(body, url), url) ? 'reachable' : 'unknown'; }
      catch (_) { return 'unknown'; }
    }

    async function check(candidate, controlCandidates = [], previousEvidence = null) {
      const url = canonicalTicketUrl(candidate);
      if (!url) return unknown();
      const cookieHeader = typeof candidate === 'object' && candidate?.cookieHeader !== undefined
        ? candidate.cookieHeader : env.cookieHeader;
      const first = await probe(url, cookieHeader);
      if (first === 'reachable') return { status: 'available', checkedAt: iso() };
      if (first !== 'gone') return unknown();

      const observedAt = now();
      const priorAt = Date.parse(previousEvidence?.firstGoneAt || '');
      const priorValid = Number(previousEvidence?.goneCount) >= 1 && Number.isFinite(priorAt) &&
        observedAt >= priorAt && observedAt - priorAt <= MAX_GONE_EVIDENCE_AGE_MS;
      const firstGoneAt = priorValid ? new Date(priorAt).toISOString() : new Date(observedAt).toISOString();
      let goneCount = priorValid && observedAt - priorAt >= MIN_GONE_GAP_MS ? 2 : 1;
      if (goneCount < 2) {
        try { await pause(Math.max(0, MIN_GONE_GAP_MS - (now() - Date.parse(firstGoneAt)))); }
        catch (_) { return unknown({ goneCount, firstGoneAt }); }
        if (now() - Date.parse(firstGoneAt) < MIN_GONE_GAP_MS) return unknown({ goneCount, firstGoneAt });
        const second = await probe(url, cookieHeader);
        if (second === 'reachable') return { status: 'available', checkedAt: iso() };
        if (second !== 'gone') return unknown({ goneCount, firstGoneAt });
        goneCount = 2;
      }

      const controls = [...new Set((Array.isArray(controlCandidates) ? controlCandidates : [])
        .map(canonicalTicketUrl).filter(controlUrl => controlUrl && controlUrl !== url))].slice(0, 3);
      for (const controlUrl of controls) {
        if (await probe(controlUrl, cookieHeader) === 'reachable') {
          const checkedAt = iso();
          return { status: 'unavailable', checkedAt, unavailableAt: checkedAt,
            goneCount, firstGoneAt };
        }
      }
      return unknown({ goneCount, firstGoneAt });
    }

    return { check };
  }

  return { create, canonicalTicketUrl };
})();
if (typeof module !== 'undefined' && module.exports) module.exports = HunterLinkChecks;
