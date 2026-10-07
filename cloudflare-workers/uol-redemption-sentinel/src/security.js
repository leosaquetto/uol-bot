const encoder = new TextEncoder();
export function normalizeLogin(value) {
  if (typeof value !== 'string' || !value.trim() || value.length > 256) throw Error('invalid_account');
  const login = value.trim().toLowerCase();
  return /^[\d.\-]+$/.test(login) ? login.replace(/\D/g, '') : login;
}
export async function digest(value) {
  return [...new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(value)))].map(x => x.toString(16).padStart(2, '0')).join('');
}
export async function matchesToken(actual, expected) {
  if (!expected || !actual || actual.length > 1024) return false;
  const [a, b] = await Promise.all([digest(actual), digest(expected)]);
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}
async function key(secret) {
  const bytes = Uint8Array.from(atob(secret), c => c.charCodeAt(0));
  if (bytes.length !== 32) throw Error('invalid_session_key');
  return crypto.subtle.importKey('raw', bytes, 'AES-GCM', false, ['encrypt', 'decrypt']);
}
const encode = bytes => btoa(String.fromCharCode(...new Uint8Array(bytes)));
const decode = text => Uint8Array.from(atob(text), c => c.charCodeAt(0));
export async function seal(value, secret, accountHash) {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = await crypto.subtle.encrypt({name:'AES-GCM', iv, additionalData:encoder.encode(accountHash)}, await key(secret), encoder.encode(JSON.stringify(value)));
  return {v:1, iv:encode(iv), data:encode(ciphertext)};
}
export async function unseal(value, secret, accountHash) {
  if (value?.v !== 1) throw Error('invalid_session');
  const plain = await crypto.subtle.decrypt({name:'AES-GCM',iv:decode(value.iv),additionalData:encoder.encode(accountHash)},await key(secret),decode(value.data));
  return JSON.parse(new TextDecoder().decode(plain));
}
export function readAccounts(env) {
  const accounts = JSON.parse(env.ACCOUNTS_JSON || '{}');
  if (!accounts || Array.isArray(accounts) || typeof accounts !== 'object') throw Error('invalid_accounts');
  for (const [id, account] of Object.entries(accounts)) {
    if (!/^[a-z][a-z0-9_-]{0,39}$/.test(id) || !account || typeof account !== 'object') throw Error('invalid_account');
    normalizeLogin(account.login);
    if (typeof account.expectedName !== 'string' || !account.expectedName.trim()) throw Error('invalid_account');
  }
  return accounts;
}
