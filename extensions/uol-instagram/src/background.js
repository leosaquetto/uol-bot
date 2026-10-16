import { StoryMonitor, readJsonLimited } from './monitor.js';

const ALARM = 'uol-instagram-check';
const monitor = new StoryMonitor({
  storage: chrome.storage.local,
  notify: ({ username, count }) => chrome.notifications.create(`story:${username}`, {
    type: 'basic', iconUrl: chrome.runtime.getURL('icons/icon128.png'), title: `Novo Story de @${username}`,
    message: count === 1 ? 'Um novo Story foi encontrado.' : `${count} novos Stories foram encontrados.`,
  }),
});
const ready = (async () => {
  await chrome.storage.local.setAccessLevel({ accessLevel: 'TRUSTED_CONTEXTS' });
  await monitor.init();
  if (!monitor.config) {
    // Local operator package only. This file is never committed or web-accessible.
    try {
      const response = await fetch(chrome.runtime.getURL('provisioning.json'));
      if (response.ok) await monitor.configure(await readJsonLimited(response, 16384));
    } catch { /* Generic package is configured through its options page. */ }
  }
  if (!(await chrome.alarms.get(ALARM))) await chrome.alarms.create(ALARM, { delayInMinutes: 1, periodInMinutes: 5 });
})();

async function updateBadge() {
  const state = monitor.publicState();
  const unresolved = state.deliveries.filter(d => ['uncertain', 'blocked'].includes(d.status)).length;
  const bad = state.pausedReason || state.lastError || unresolved;
  await chrome.action.setBadgeBackgroundColor({ color: bad ? '#a52c38' : '#176c53' });
  await chrome.action.setBadgeText({ text: !state.enabled ? 'Ⅱ' : bad ? '!' : state.lastResults.reduce((n, p) => n + p.newCount, 0).toString().replace(/^0$/, '') });
}
async function run() { await ready; await monitor.run(); await updateBadge(); }
chrome.alarms.onAlarm.addListener(alarm => { if (alarm.name === ALARM) void run().catch(() => {}); });
chrome.runtime.onStartup.addListener(() => { void run().catch(() => {}); });
chrome.runtime.onInstalled.addListener(() => { void run().catch(() => {}); });
chrome.runtime.onMessage.addListener((message, sender, reply) => {
  // No content script, external-message endpoint or arbitrary fetch bridge.
  if (sender.id !== chrome.runtime.id || !sender.url?.startsWith(chrome.runtime.getURL(''))) return false;
  (async () => {
    await ready;
    let state;
    switch (message?.action) {
      case 'getState': state = monitor.publicState(); break;
      case 'configure': state = await monitor.configure(message.config); break;
      case 'check': state = await monitor.run(); break;
      case 'pause': state = await monitor.pause(); break;
      case 'resume': state = await monitor.resume(); break;
      default: throw new Error('action_invalid');
    }
    await updateBadge(); reply({ ok: true, state });
  })().catch(error => reply({ ok: false, error: /^[a-z_]+$/.test(error.reason || '') ? error.reason : 'operation_failed' }));
  return true;
});
chrome.notifications.onClicked.addListener(id => {
  const username = /^story:([a-z0-9_][a-z0-9_.]{0,29})$/.exec(id)?.[1];
  if (username) { void chrome.tabs.create({ url: `https://www.instagram.com/stories/${username}/` }); void chrome.notifications.clear(id); }
});
