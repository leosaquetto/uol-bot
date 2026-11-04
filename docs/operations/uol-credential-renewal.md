# UOL technical credential renewal

## Scope and current state

The operator requested renewal automation on 17 October 2026. Scope is only
`UOL_API_AUTHORIZATION` on `uol-telegram-shadow-pilot`; no personal UOL login,
Instagram login/session, other secret, code rollout, migration, paid tier or
ambiguous delivery retry is authorized by this maintenance task.

Current credential expiry: `2027-01-26T10:05:29Z` (26 January 07:05:29 Sao Paulo).
The previous credential expired on `2026-10-23T04:04:28Z`.
Technical issuance was proven on 3 November 2026 through the native client's
application OAuth flow. The issuer returned HTTP 200; the replacement passed
the existing coupons parser and contract with 50 valid offers. See the
[observed protocol and validation procedure](uol-technical-oauth.md).
Exact technical inputs are stored privately in
`~/.config/uol-ticket-research/uol-technical-issuer.json`, mode 0600.
Earlier issuer investigations below are historical. Do not ask the operator for
the procedure or repeat archive/client searches while this validated flow works.

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
- Serialize renewal with the private exclusive lock
  `~/.config/uol-ticket-research/uol-credential-renewal.lock`. Re-read the deployed version
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

## Recovery publication — 3 November 2026

Published at 23:14 BRT (4 November 02:14 UTC), version
`9b6d2973-5a53-48b0-a966-b3e224c79dcf`, with 100% traffic. Code source remains
`2431cb17005ba47c052398e215aa72ed69c3203f`; script etag, runtime, handlers and all
61 bindings matched the previous version. Only the technical authorization secret
was replaced.

Verification at 23:21 BRT: authenticated API HTTP 200, 50 valid offers, no API error,
new expiry `2027-01-26T10:05:29Z`, and repeated natural primary successes on the
exact published version. Last successful cycle read 32 rows; estimate 230/512,
maximum cycle 1,732/4,096. Projected daily writes 61,568/80,000; headroom 38,432.

The newly discovered queue drained. Two preexisting uncertain main deliveries
remain; no actionable main work or new release debt was added. Liveness HTTP 200;
readiness remains HTTP 503. Standard runtime verification still reports
`critical_queue_pending:2`, and original-baseline regressions remain explicit.
Five existing critical incident records were still active at this observation.
The original debt baseline, uncertain delivery identities and receipts are preserved.
