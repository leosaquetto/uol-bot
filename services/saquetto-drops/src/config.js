import { readFileSync } from 'node:fs';

const profile = /^[a-z0-9_]{1,15}$/;
const alias = /^[a-z0-9][a-z0-9-]{0,63}$/;
const fail = (code) => { throw new Error(code); };
const strings = (value) => Array.isArray(value) && value.every(v => typeof v === 'string');
export const normalize = value => String(value).normalize('NFKD').replace(/\p{M}/gu, '').toLowerCase();

export function validateConfig(value) {
  const c = structuredClone(value);
  if (c?.version !== 1 || !strings(c.sources) || !c.sources.length ||
      !c.sources.every(s => profile.test(s)) || new Set(c.sources).size !== c.sources.length) fail('invalid_sources');
  if (!c.destinations || Array.isArray(c.destinations) || !Object.keys(c.destinations).length) fail('invalid_destinations');
  for (const [key, d] of Object.entries(c.destinations)) {
    const suffix = { group: '@g.us', contact: '@s.whatsapp.net', channel: '@newsletter' }[d?.type];
    if (!alias.test(key) || !suffix || typeof d.jid !== 'string' || typeof d.verified !== 'boolean') fail('invalid_destination');
    if (!d.jid.endsWith(suffix) && !(d.type === 'contact' && d.jid.endsWith('@lid'))) fail('invalid_destination_jid');
    if (d.verified && !/^\d+(?:-\d+)?@(g\.us|s\.whatsapp\.net|lid|newsletter)$/.test(d.jid)) fail('invalid_verified_jid');
  }
  if (!Array.isArray(c.rules) || !c.rules.length) fail('invalid_rules');
  const ids = new Set();
  for (const r of c.rules) {
    if (!alias.test(r?.id) || ids.has(r.id)) fail('invalid_rule_id');
    ids.add(r.id);
    if (!strings(r.sources) || !r.sources.length || !r.sources.every(s => c.sources.includes(s))) fail('invalid_rule_sources');
    if (!strings(r.types) || !r.types.length || !r.types.every(t => ['post', 'quote', 'reply', 'repost'].includes(t))) fail('invalid_rule_types');
    if (!strings(r.destinations) || !r.destinations.length || !r.destinations.every(d => Object.hasOwn(c.destinations, d))) fail('invalid_rule_destinations');
    for (const field of ['any', 'all', 'none']) {
      if (!strings(r[field]) || r[field].some(s => !s.trim() || s.length > 500)) fail('invalid_rule_terms');
    }
  }
  if (typeof c.operation?.paused !== 'boolean' || typeof c.operation?.dryRun !== 'boolean' ||
      !Number.isSafeInteger(c.operation.minDelayMs) || c.operation.minDelayMs < 5000) fail('invalid_operation');
  return c;
}

export function matchRules(config, post) {
  if (!config.sources.includes(post.author) || !['post', 'quote', 'reply', 'repost'].includes(post.type)) return [];
  const text = normalize(post.text);
  const matches = new Set();
  for (const r of config.rules) {
    if (!r.sources.includes(post.author) || !r.types.includes(post.type)) continue;
    if (r.any.length && !r.any.some(t => text.includes(normalize(t)))) continue;
    if (!r.all.every(t => text.includes(normalize(t))) || r.none.some(t => text.includes(normalize(t)))) continue;
    for (const d of r.destinations) matches.add(d);
  }
  return [...matches];
}

export function loadConfig(path, previous) {
  try { return { config: validateConfig(JSON.parse(readFileSync(path, 'utf8'))), error: null }; }
  catch (e) {
    if (!previous) throw new Error('configuration_unavailable');
    return { config: previous, error: e instanceof SyntaxError ? 'invalid_json' : 'invalid_configuration' };
  }
}
