import { parseReels, candidatesForWorker } from './stories.js';

export const WORKER_URL = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev';
export const LIMITS = Object.freeze({ requestsPerDay: 300, profiles: 3, outbox: 200, responseBytes: 4 * 1024 * 1024 });
const MINUTE = 60_000;
const intervals = [5, 10, 15, 30, 60];
const safeError = (reason, extra = {}) => Object.assign(new Error(reason), { reason, ...extra });
const day = now => new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Sao_Paulo' }).format(new Date(now));

export function validateConfig(input, previous = {}) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw safeError('config_invalid');
  if (input.workerUrl && input.workerUrl !== WORKER_URL) throw safeError('worker_url_invalid');
  const rawProfiles = input.profiles ?? previous.profiles ?? ['clubeuol'];
  if (!Array.isArray(rawProfiles)) throw safeError('profiles_invalid');
  const profiles = [...new Set(rawProfiles.map(p => String(p).trim().toLowerCase().replace(/^@/, '')))];
  if (!profiles.length || profiles.length > LIMITS.profiles || profiles.some(p => !/^[a-z0-9_][a-z0-9_.]{0,29}$/.test(p))) throw safeError('profiles_invalid');
  const intervalMinutes = Number(input.intervalMinutes ?? previous.intervalMinutes ?? 5);
  if (!intervals.includes(intervalMinutes)) throw safeError('interval_invalid');
  const token = input.token || previous.token || '';
  if (typeof token !== 'string' || token.length < 16 || token.length > 4096 || /\s/.test(token)) throw safeError('token_invalid');
  const enabled = input.enabled ?? previous.enabled ?? true, notifications = input.notifications ?? previous.notifications ?? true;
  if (typeof enabled !== 'boolean' || typeof notifications !== 'boolean') throw safeError('config_invalid');
  return { workerUrl: WORKER_URL, token, profiles, intervalMinutes, enabled, notifications };
}

export async function readJsonLimited(response, limit = LIMITS.responseBytes) {
  if (Number(response.headers.get('content-length') || 0) > limit) {
    await response.body?.cancel(); throw safeError('body_limit');
  }
  const reader = response.body?.getReader();
  if (!reader) throw safeError('invalid_response');
  const chunks = []; let size = 0;
  try {
    while (true) {
      const { value, done } = await reader.read(); if (done) break;
      size += value.byteLength;
      if (size > limit) { await reader.cancel(); throw safeError('body_limit'); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
  catch { throw safeError('invalid_response'); }
}

function retryDelay(response, now, fallback) {
  const raw = response.headers.get('retry-after');
  const ms = raw && /^\d+$/.test(raw) ? Number(raw) * 1000 : Date.parse(raw || '') - now;
  return Number.isFinite(ms) && ms > 0 ? Math.max(fallback, ms) : fallback;
}
const initialState = () => ({ schema: 1, nextPollAt: 0, sourceStatus: 'starting', pausedReason: '', lastError: '',
  workerError: '', lastCheckAt: '', budget: { day: '', used: 0 }, profileIds: {}, seen: {}, lastResults: [], outbox: {}, failures: 0 });
const targets = ['main', 'canal2', 'discord', 'beeper'];
function validReceipt(value, entry) {
  return value?.ok === true && value.storyId === entry.payload.storyId && value.link === entry.payload.link &&
    value.canonicalKey === entry.key && targets.every(t => value.targets?.[t] && typeof value.targets[t].status === 'string');
}
function recoverable(receipt) {
  return targets.some(name => {
    const t = receipt.targets[name];
    return ['pending', 'failed_safe', 'reconciling'].includes(t.status) ||
      (name === 'discord' && t.status === 'confirmed' && !t.imageConfirmed) ||
      (t.status === 'held' && t.error === 'story_image_proxy_pending');
  });
}
async function candidateKey(payload) {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(payload.link)));
  return `instagram:${payload.storyId}:${[...digest].map(x => x.toString(16).padStart(2, '0')).join('')}`;
}

export class StoryMonitor {
  constructor({ storage, fetchImpl = fetch, notify = async () => {}, now = Date.now }) {
    Object.assign(this, { storage, fetchImpl, notify, now });
    this.state = initialState(); this.config = null; this.busy = false;
  }
  async init() {
    const saved = await this.storage.get(['settings', 'monitor']);
    if (saved.monitor?.schema === 1) this.state = { ...initialState(), ...saved.monitor };
    if (saved.settings) {
      try { this.config = validateConfig(saved.settings); }
      catch { this.state.lastError = 'config_invalid'; }
    }
    for (const entry of Object.values(this.state.outbox)) {
      if (entry.status === 'sending') { entry.status = 'uncertain'; entry.error = 'interrupted_attempt'; }
    }
    await this.save(); return this.publicState();
  }
  save() { return this.storage.set({ monitor: this.state }); }
  async configure(input) {
    if (this.busy) throw safeError('operation_in_progress');
    this.config = validateConfig(input, this.config || {});
    await this.storage.set({ settings: this.config });
    return this.publicState();
  }
  async pause() { if (this.busy) throw safeError('operation_in_progress'); return this.configure({ enabled: false }); }
  async resume() {
    if (this.busy) throw safeError('operation_in_progress');
    this.state.pausedReason = ''; this.state.lastError = '';
    await this.save(); return this.configure({ enabled: true });
  }
  publicState() {
    const s = this.state, c = this.config;
    return { configured: Boolean(c), enabled: Boolean(c?.enabled), profiles: c?.profiles || ['clubeuol'],
      intervalMinutes: c?.intervalMinutes || 5, notifications: c?.notifications ?? true,
      pausedReason: s.pausedReason, sourceStatus: s.sourceStatus, lastCheckAt: s.lastCheckAt,
      nextPollAt: s.nextPollAt, lastError: s.lastError, workerError: s.workerError,
      budget: { day: s.budget.day, used: s.budget.day === day(this.now()) ? s.budget.used : 0, limit: LIMITS.requestsPerDay },
      lastResults: s.lastResults,
      deliveries: Object.values(s.outbox).slice(-20).reverse().map(e => ({ storyId: e.payload.storyId, link: e.payload.link,
        status: e.status, deliveryStatus: e.receipt?.status || '', updatedAt: e.updatedAt,
        targets: Object.fromEntries(targets.filter(t => e.receipt?.targets?.[t]).map(t => [t, {
          status: e.receipt.targets[t].status, messageId: e.receipt.targets[t].messageId || '',
          imageConfirmed: e.receipt.targets[t].imageConfirmed === true }])) })) };
  }
  async instagram(url) {
    const today = day(this.now());
    if (this.state.budget.day !== today) this.state.budget = { day: today, used: 0 };
    if (this.state.budget.used >= LIMITS.requestsPerDay) throw safeError('daily_budget');
    this.state.budget.used++; await this.save(); // Charge before request, including interruption.
    const response = await this.fetchImpl(url, { credentials: 'include', redirect: 'error', cache: 'no-store',
      headers: { Accept: 'application/json', 'X-IG-App-ID': '936619743392459', 'X-ASBD-ID': '359341' }, signal: AbortSignal.timeout(20_000) });
    if (response.status === 429) {
      const delay = retryDelay(response, this.now(), 6 * 60 * MINUTE); await response.body?.cancel();
      throw safeError('rate_limited', { delay });
    }
    if ([401, 403].includes(response.status)) { await response.body?.cancel(); throw safeError('auth_required'); }
    if (response.status !== 200) { await response.body?.cancel(); throw safeError('instagram_http_error'); }
    const value = await readJsonLimited(response);
    if (value.login_required || value.challenge_required || ['login_required', 'challenge_required', 'checkpoint_required'].includes(value.message)) throw safeError('auth_required');
    return value;
  }
  async worker(path, payload) {
    const response = await this.fetchImpl(`${WORKER_URL}${path}`, { method: payload ? 'POST' : 'GET', credentials: 'omit',
      redirect: 'error', cache: 'no-store', signal: AbortSignal.timeout(20_000),
      headers: { Accept: 'application/json', Authorization: `Bearer ${this.config.token}`, ...(payload ? { 'Content-Type': 'application/json' } : {}) },
      ...(payload ? { body: JSON.stringify(payload) } : {}) });
    const body = await readJsonLimited(response, (payload ? 64 : 256) * 1024);
    return { status: response.status, body, delay: retryDelay(response, this.now(), 30 * MINUTE) };
  }
  recordReceipt(entry, receipt) {
    const complete = receipt.status === 'delivered' && targets.every(t => receipt.targets[t].status === 'confirmed' &&
      typeof receipt.targets[t].messageId === 'string' && receipt.targets[t].messageId && receipt.targets[t].imageConfirmed === true);
    entry.receipt = receipt; entry.status = complete ? 'delivered' : 'accepted'; entry.error = '';
    const future = targets.filter(t => ['pending', 'failed_safe', 'reconciling'].includes(receipt.targets[t].status))
      .map(t => Date.parse(receipt.targets[t].nextAttemptAt)).filter(Number.isFinite);
    entry.retryAt = Math.max(this.now() + this.config.intervalMinutes * MINUTE, future.length ? Math.min(...future) : 0);
    if (!complete && !recoverable(receipt)) entry.retryAt = Math.max(entry.retryAt, this.now() + 30 * MINUTE);
    entry.updatedAt = new Date(this.now()).toISOString();
  }
  async drain() {
    let operations = 0;
    for (const entry of Object.values(this.state.outbox)) {
      if (operations >= 3 || ['delivered', 'blocked', 'expired'].includes(entry.status) || (entry.retryAt || 0) > this.now()) continue;
      const statusOnly = entry.status === 'accepted' && !recoverable(entry.receipt);
      if (entry.status === 'uncertain' || statusOnly) {
        operations++;
        try {
          const { status, body } = await this.worker(`/instagram-story-status?storyId=${entry.payload.storyId}&limit=50`);
          const receipt = status === 200 && body.ok && body.stories?.find(v => validReceipt(v, entry));
          if (receipt) { this.recordReceipt(entry, receipt); this.state.workerError = ''; }
          else { entry.retryAt = this.now() + 30 * MINUTE; this.state.workerError = status === 401 ? 'worker_auth_required' : 'receipt_unconfirmed'; }
        } catch { entry.retryAt = this.now() + 30 * MINUTE; this.state.workerError = 'receipt_unconfirmed'; }
        await this.save(); continue; // A lost POST response never triggers a blind resend.
      }
      if (Date.parse(entry.payload.expiresAt) <= this.now()) { entry.status = 'expired'; await this.save(); continue; }
      if (entry.attempts >= 20) { entry.status = 'blocked'; entry.error = 'local_attempt_limit'; await this.save(); continue; }
      entry.status = 'sending'; entry.attempts++; entry.updatedAt = new Date(this.now()).toISOString(); await this.save();
      operations++;
      try {
        const { status, body, delay } = await this.worker('/ingest-instagram-story', entry.payload);
        if (status === 200 && validReceipt(body, entry)) {
          this.recordReceipt(entry, body); this.state.workerError = '';
        } else if (status === 429 && ['instagram_story_daily_limit', 'instagram_story_capacity_reached'].includes(body.error)) {
          entry.status = 'pending'; entry.retryAt = this.now() + delay; entry.error = body.error;
        } else if ([400, 401, 403, 409, 410, 413, 415].includes(status) && body.ok === false) {
          entry.status = status === 410 ? 'expired' : 'blocked'; entry.error = 'worker_rejected'; this.state.workerError = status === 401 ? 'worker_auth_required' : 'worker_rejected';
        } else { entry.status = 'uncertain'; entry.error = 'response_unconfirmed'; entry.retryAt = this.now() + 5 * MINUTE; }
      } catch { entry.status = 'uncertain'; entry.error = 'response_unconfirmed'; entry.retryAt = this.now() + 5 * MINUTE; }
      await this.save();
    }
  }
  async run() {
    if (this.busy || !this.config?.enabled) return this.publicState();
    this.busy = true;
    try {
      for (const [key, entry] of Object.entries(this.state.outbox)) {
        if (entry.status === 'delivered' && Date.parse(entry.payload.expiresAt) < this.now() - 7 * 24 * 60 * MINUTE) delete this.state.outbox[key];
      }
      await this.save();
      await this.drain();
      if (this.state.pausedReason || this.state.nextPollAt > this.now()) return this.publicState();
      if (Object.keys(this.state.outbox).length >= LIMITS.outbox) {
        this.state.lastError = 'outbox_full'; await this.save(); return this.publicState();
      }
      this.state.nextPollAt = this.now() + this.config.intervalMinutes * MINUTE;
      this.state.lastCheckAt = new Date(this.now()).toISOString(); await this.save();
      const profiles = [];
      for (const username of this.config.profiles) {
        let cached = this.state.profileIds[username];
        if (!cached || this.now() - cached.verifiedAt > 24 * 60 * MINUTE) {
          const value = await this.instagram(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`);
          const user = value?.data?.user;
          if (user?.username?.toLowerCase() !== username || typeof user.id !== 'string' || !/^\d{1,24}$/.test(user.id)) throw safeError('profile_unconfirmed');
          cached = { id: user.id, verifiedAt: this.now() }; this.state.profileIds[username] = cached; await this.save();
        }
        profiles.push({ username, id: cached.id });
      }
      const query = new URLSearchParams(); for (const p of profiles) query.append('reel_ids', p.id);
      const value = await this.instagram(`https://www.instagram.com/api/v1/feed/reels_media/?${query}`);
      const results = parseReels(value, profiles, this.now());
      if (results.some(r => r.status === 'auth_required')) throw safeError('auth_required');
      const notices = [];
      this.state.lastResults = [];
      for (const result of results) {
        const previous = this.state.seen[result.username];
        const fresh = result.stories.filter(s => !previous?.includes(s.storyId));
        this.state.lastResults.push({ username: result.username, status: result.status, count: result.stories.length, newCount: previous ? fresh.length : 0 });
        if (!['found', 'empty'].includes(result.status)) continue;
        for (const payload of candidatesForWorker(result)) {
          const key = await candidateKey(payload);
          if (this.state.outbox[key]) continue;
          if (Object.keys(this.state.outbox).length >= LIMITS.outbox) throw safeError('outbox_full');
          this.state.outbox[key] = { key, payload, status: 'pending', attempts: 0, retryAt: 0, updatedAt: new Date(this.now()).toISOString() };
        }
        this.state.seen[result.username] = result.stories.map(s => s.storyId);
        if (previous && fresh.length && this.config.notifications) notices.push({ username: result.username, count: fresh.length });
      }
      const healthy = results.every(r => ['found', 'empty'].includes(r.status));
      this.state.sourceStatus = healthy ? (results.some(r => r.stories.length) ? 'found' : 'empty') : 'unknown';
      this.state.lastError = healthy ? '' : 'story_structure_unconfirmed';
      this.state.failures = healthy ? 0 : this.state.failures + 1;
      if (!healthy) this.state.nextPollAt = Math.max(this.state.nextPollAt, this.now() + 30 * MINUTE);
      await this.save();
      for (const notice of notices) { try { await this.notify(notice); } catch { /* Optional desktop notification; never replay. */ } }
      await this.drain();
      return this.publicState();
    } catch (error) {
      const reason = error.reason || 'network_error'; this.state.lastError = reason;
      this.state.sourceStatus = reason === 'daily_budget' ? 'budget_wait' : reason === 'auth_required' ? 'auth_required' : reason === 'rate_limited' ? 'rate_limited' : 'unknown';
      this.state.failures++;
      if (reason === 'auth_required') this.state.pausedReason = reason;
      this.state.nextPollAt = Math.max(this.state.nextPollAt, this.now() + (error.delay || Math.min(60, 5 * 2 ** Math.min(this.state.failures, 4)) * MINUTE));
      await this.save(); return this.publicState();
    } finally { this.busy = false; }
  }
}
