export function acceptedPush(event) {
  return event && ['https://x.com', 'https://twitter.com'].includes(event.origin) &&
    ['pushMessaging', 'notifications'].includes(event.service) &&
    Number.isFinite(event.timestamp) && typeof event.eventName === 'string' &&
    Array.isArray(event.eventMetadata) && event.eventMetadata.every(m =>
      m && typeof m.key === 'string' && typeof m.value === 'string') &&
    JSON.stringify(event).length <= 256 * 1024;
}

export function postFromPush(event) {
  const strings = event.eventMetadata.map(m => String(m.value || ''));
  const found = new Map();
  for (const value of strings) {
    // Only canonical status links. A number or display name alone is not identity.
    const decoded = value.replaceAll('\\/', '/').replaceAll('&amp;', '&');
    for (const m of decoded.matchAll(/https:\/\/(?:www\.)?(?:x\.com|twitter\.com)\/([a-zA-Z0-9_]{1,15})\/status\/(\d{10,25})(?=[/?#\s"'\\]|$)/g)) {
      found.set(m[2], { author: m[1].toLowerCase(), id: m[2], url: `https://x.com/${m[1].toLowerCase()}/status/${m[2]}` });
    }
  }
  return found.size === 1 ? [...found.values()][0] : null;
}

export async function observePush({ endpoint, store, onEvent = () => {}, logger = console }) {
  const { chromium } = await import('playwright-core');
  const browser = await chromium.connectOverCDP(endpoint, { timeout: 10_000 });
  const context = browser.contexts()[0];
  const page = context.pages()[0] || await context.newPage();
  const cdp = await context.newCDPSession(page);
  let ready = false, stopped = false, enabling = false, retry;
  const enable = async () => {
    if (stopped || enabling) return;
    enabling = true;
    try {
      for (const service of ['pushMessaging', 'notifications']) {
        await cdp.send('BackgroundService.startObserving', { service });
        await cdp.send('BackgroundService.setRecording', { service, shouldRecord: true });
      }
      ready = true;
    } finally { enabling = false; }
  };
  cdp.on('BackgroundService.backgroundServiceEventReceived', ({ backgroundServiceEvent: event }) => {
    if (!acceptedPush(event)) return;
    const result = store.recordPush(event);
    if (result.added) {
      logger.log(JSON.stringify({ event: 'push_persisted', id: result.id.slice(0,12), service: event.service }));
      onEvent();
    }
  });
  cdp.on('BackgroundService.recordingStateChanged', ({ isRecording }) => {
    if (!isRecording && !stopped) {
      ready = false; clearTimeout(retry);
      retry = setTimeout(() => enable().catch(()=>{ready=false;}),1000);
      retry.unref();
    }
  });
  browser.on('disconnected', () => { ready = false; });
  try { await enable(); } catch (error) { await cdp.detach().catch(()=>{}); await browser.close(); throw error; }
  // This renews local recording, never requests an X timeline.
  const renewal = setInterval(() => enable().catch(() => { ready = false; }), 12 * 60 * 60 * 1000);
  renewal.unref();
  return { context, page, isReady: () => ready && browser.isConnected(),
    async stop() { stopped=true;clearTimeout(retry);clearInterval(renewal); await cdp.detach().catch(() => {}); await browser.close(); } };
}
