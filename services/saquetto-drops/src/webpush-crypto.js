import { createECDH } from 'node:crypto';
import { createRequire } from 'node:module';

// The upstream diagnostic mode prints key material. Refuse it before loading.
if (process.env.ECE_KEYLOG === '1') throw new Error('unsafe_key_logging');
const ece = createRequire(import.meta.url)('http_ece');
const decode = value => {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_-]+={0,2}$/.test(value)) throw new Error('invalid_base64url');
  return Buffer.from(value, 'base64url');
};
const parameter = (header, name) => {
  if (typeof header !== 'string') throw new Error('missing_encryption_header');
  const matches = [...header.matchAll(new RegExp(`(?:^|[;,]\\s*)${name}="?([A-Za-z0-9_-]+={0,2})"?(?=[;,]|$)`, 'g'))];
  if (matches.length !== 1) throw new Error('ambiguous_encryption_header');
  return matches[0][1];
};

export function decryptPush(message, registration) {
  const ciphertext = decode(message.data);
  if (ciphertext.length > 65536 || ciphertext.length < 17) throw new Error('invalid_push_size');
  const version = message.headers?.encoding;
  if (!['aes128gcm','aesgcm'].includes(version)) throw new Error('unsupported_push_encoding');
  const key = createECDH('prime256v1');
  key.setPrivateKey(decode(registration.privateKey));
  if (!key.getPublicKey().equals(decode(registration.publicKey))) throw new Error('registration_key_mismatch');
  const authSecret = decode(registration.auth);
  if (authSecret.length !== 16) throw new Error('invalid_auth_secret');
  const options = {version,privateKey:key,authSecret};
  if (version === 'aes128gcm') {
    if (ciphertext.length < 103 || ciphertext[20] !== 65 || ciphertext[21] !== 4 ||
        ciphertext.readUInt32BE(16) < 18 || ciphertext.readUInt32BE(16) > 65536) throw new Error('invalid_record_header');
  } else {
    options.salt = parameter(message.headers.encryption, 'salt');
    options.dh = parameter(message.headers.crypto_key, 'dh');
    if (decode(options.salt).length !== 16 || decode(options.dh).length !== 65) throw new Error('invalid_legacy_key');
  }
  const plaintext = ece.decrypt(ciphertext, options);
  if (plaintext.length > 65536) throw new Error('push_payload_too_large');
  return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(plaintext));
}
