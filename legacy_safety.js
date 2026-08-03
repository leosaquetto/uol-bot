'use strict';

// Safety helpers for the archived Mac fallback. The active delivery path is
// the Cloudflare Worker; these functions keep a manual recovery from writing
// credentials to terminal output or audit files.

const REDACTED = '[REDACTED]';
const SENSITIVE_KEY_RE = /(?:authorization|password|passwd|secret|token|api[_-]?key|credential|cookie)/i;

function configuredSecrets() {
  return [...new Set(
    Object.entries(process.env)
      .filter(([name, value]) => SENSITIVE_KEY_RE.test(name) && value && String(value).length >= 4)
      .map(([, value]) => String(value))
  )].sort((a, b) => b.length - a.length);
}

function redactSensitiveText(value) {
  let text = String(value ?? '');
  for (const secret of configuredSecrets()) {
    text = text.split(secret).join(REDACTED);
  }
  text = text.replace(/(https?:\/\/api\.telegram\.org\/bot)[^/\s]+/gi, `$1${REDACTED}`);
  text = text.replace(/\b(Bearer|Basic|token)\s+[^\s,;]+/gi, (_, scheme) => `${scheme} ${REDACTED}`);
  text = text.replace(/\b(?:github_pat_[A-Za-z0-9_]{16,}|gh[pousr]_[A-Za-z0-9_]{16,})\b/g, REDACTED);
  text = text.replace(/([?&](?:access[_-]?token|token|api[_-]?key|apikey|secret|password|authorization)=)[^&#\s"']+/gi, `$1${REDACTED}`);
  text = text.replace(/(https?:\/\/)[^/@\s:]+:[^/@\s]+@/gi, `$1${REDACTED}@`);
  return text;
}

function sanitizeAuditValue(value, keyHint = '') {
  if (keyHint && SENSITIVE_KEY_RE.test(String(keyHint))) return REDACTED;
  if (Array.isArray(value)) return value.map((item) => sanitizeAuditValue(item));
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, sanitizeAuditValue(item, key)])
    );
  }
  if (typeof value === 'string') return redactSensitiveText(value);
  return value;
}

function macCompletionSignal(workflowStatus) {
  const status = String(workflowStatus || 'skipped').trim().toLowerCase();
  return status === 'ok'
    ? { ok: true, marker: 'MAC_OK', exitCode: 0 }
    : { ok: false, marker: 'MAC_FAIL', exitCode: 3 };
}

module.exports = {
  REDACTED,
  macCompletionSignal,
  redactSensitiveText,
  sanitizeAuditValue,
};
