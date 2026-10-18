# UOL technical credential renewal

## Scope and current state

The operator requested renewal automation on 17 October 2026. Scope is only
`UOL_API_AUTHORIZATION` on `uol-telegram-shadow-pilot`; no personal UOL login,
Instagram login/session, other secret, code rollout, migration, paid tier or
ambiguous delivery retry is authorized by this maintenance task.

Known expiry: `2026-10-23T04:04:28Z` (23 October 01:04:28 Sao Paulo).
Issuance/refresh remains **unproven**. Previous bounded investigations exhausted
related commits, legacy scripts, available original archives and the anonymous
client; do not repeat them unchanged or ask the operator for the unknown procedure.
A scheduled investigation is not evidence that automatic issuance works.

## Each scheduled run

1. Read this runbook, the current publication manifest at
   `~/.config/uol-ticket-research/uol-publication-2026-10-16.json` and private
   `uol-credential-renewal-state.json` in the same directory. Check Git status.
   Do not reindex the repository. Obtain the current version from the manifest,
   not a historical SHA in a document.
2. Use configured authenticated, read-only `/livez`, `/dashboard.json` and
   `/readyz` to inspect `ticketApi.applicationAuthorizationExpiresAt`, current
   API success/error, natural primary cycle, version and budget guards. These
   endpoints inspect existing state; do not force `/run` or `/maintenance`.
3. Until a technical issuance/refresh procedure is proven, preserve the current
   secret. Investigate only a concrete new lead (changed authoritative client,
   new technical documentation or newly available original issuance record),
   at most two targeted sources per run. No guessed auth endpoints, repeated
   login attempts, endpoint fuzzing, personal login or token fabrication.
4. Record `blocked_issuer_unproven` and an evidence signature privately. Report
   a new actionable finding/failure once; unchanged blocked state stays quiet.
   If still blocked, report the 48-hour, 24-hour and actual-expiry milestones
   once each. Do not ask again for a procedure the user already said is unknown.
5. If expiry is more than seven days away and API health is current, finish with
   no issuer search or mutation. The six-hour schedule remains lightweight.

## Conditional renewal, only after issuance is proven

- Confirm the technical issuer, exact acquisition/refresh protocol and existing
  technical access. Obtain a candidate in memory; validate its later expiry and
  authenticated API response/contract. Decoding JWT `exp` alone is insufficient.
- Serialize renewal with a private exclusive lock. Re-read the deployed version
  and latest uploaded version; they must equal the manifest version. If there is
  a concurrent/unpublished version, stop without building on unknown code.
- Run the installed Wrangler with Node 22 from the Worker directory. Use
  `wrangler versions secret put UOL_API_AUTHORIZATION` with candidate via stdin,
  never command arguments or logs. This stages one-secret replacement without
  publishing. Preserve every other binding and the deployed code provenance.
- Record the staged version ID privately. Recheck deployed/latest versions, staged code/binding equivalence to the
  known previous version and original release debt baseline before publishing
  exactly the staged version
  at 100%; do not use a gradual split for this Durable Object. Use the existing
  production verification gates, preserving read estimate limit 512, cycle
  limit 4,096, 80,000 projected writes and at least 20,000 write headroom.
- On ambiguous publication/secret responses, reconcile versions and health
  read-only before another mutation. Preserve the known previous version for
  rollback; do not clear incidents or hide a failed authentication check.
- Require the new expiry in Worker diagnostics, API contract valid and no auth
  error, an authenticated API success after publication, and a natural primary
  success on the new version after publication. Separate `published`,
  `runtimeVerified` and `ready`; unrelated historical incidents remain explicit.
- Save receipts and state atomically with mode 0600, update the publication
  manifest to the actual new version while preserving code SHA provenance, and
  append sanitized evidence to the operational record. Never put a secret,
  cookie, session, credential value or private API body in Git/chat/notifications.

## Scheduling and execution boundary

The Codex thread automation runs every six hours. Local files require the Mac
awake and Codex running; this is not an Oracle/systemd renewal daemon. Production
Worker collection continues independently. The schedule must not claim renewal
until the conditional procedure above has actually passed.

References: [Cloudflare versioned secrets](https://developers.cloudflare.com/workers/configuration/secrets/#adding-secrets-to-your-project),
[scheduled tasks](https://learn.chatgpt.com/docs/automations?surface=app).

Automation ID: `renovar-credencial-t-cnica-uol`, active every six hours in the
current Codex conversation. Initial manual read-only check passed: current
version matched the manifest, Worker Ready, known expiry unchanged. Issuer
remains unproven; no candidate was issued and no production secret changed.

## Verification — 18 October 2026, 06:41 UTC

Authenticated health matched Worker version `21e1f465-c842-4806-87f6-cfbb88bd4f3f`
and source `091e3fcb77e636be8d0a67c529a46152f8dc1750`. Ready HTTP 200;
API contract valid, no authentication error. Natural primary success at
`2026-10-18T06:41:36.095Z` used 24 rows; estimate 190, within the unchanged
512 gate. Projected writes 62,528 and headroom 37,472 remain within the guards.
Expiry remains `2026-10-23T04:04:28Z`. No concrete new issuer lead or candidate;
`blocked_issuer_unproven` is unchanged. No production mutation or repeated alert.
