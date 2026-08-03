'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  REDACTED,
  macCompletionSignal,
  redactSensitiveText,
  sanitizeAuditValue,
} = require('../legacy_safety');

test('redacts credentials from errors and audit values', () => {
  const previous = process.env.GITHUB_TOKEN;
  const secret = 'github_pat_this_is_a_test_secret_123456';
  process.env.GITHUB_TOKEN = secret;
  try {
    const sample = [
      `Bearer ${secret}`,
      'https://api.telegram.org/bot123456:ABC_def/sendMessage',
      'https://example.test/path?token=query-secret&ok=1',
      'https://alice:password@example.test/private',
    ].join(' ');
    const text = redactSensitiveText(sample);
    const audit = sanitizeAuditValue({
      authorization: secret,
      error: sample,
      nested: { apiKey: 'abc123' },
    });

    assert.doesNotMatch(text, new RegExp(secret));
    assert.doesNotMatch(text, /123456:ABC_def|query-secret|alice:password/);
    assert.equal(audit.authorization, REDACTED);
    assert.equal(audit.nested.apiKey, REDACTED);
    assert.doesNotMatch(JSON.stringify(audit), new RegExp(secret));
  } finally {
    if (previous === undefined) delete process.env.GITHUB_TOKEN;
    else process.env.GITHUB_TOKEN = previous;
  }
});

test('MAC_OK is reserved for a confirmed workflow dispatch', () => {
  assert.deepEqual(macCompletionSignal('ok'), { ok: true, marker: 'MAC_OK', exitCode: 0 });
  for (const status of ['failed', 'skipped', '', null, undefined]) {
    assert.deepEqual(
      macCompletionSignal(status),
      { ok: false, marker: 'MAC_FAIL', exitCode: 3 },
    );
  }
});
