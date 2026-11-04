# UOL technical OAuth renewal

Companion to [the credential renewal runbook](uol-credential-renewal.md).
This records a protocol observed in the Android client. Its public documentation
does not establish a supported UOL API contract. Report publication and readiness
separately using the receipts from each operation.

## Observed acquisition flow

The inspected Meu UOL 1.8.2 client handles API HTTP 401/403 by calling its
`UserDataHttpApi.oAuthApi` method through interceptor `C1.a`:

```http
POST https://gateway.produtos.uol.com.br/oauth/token
Authorization: <application-authorization-from-validated-client>
Content-Type: application/x-www-form-urlencoded

client=<technical-client>&username=<technical-username>&password=<technical-password>&grant_type=<observed-grant-type>
```

These are the client's technical values, not the operator's personal UOL login.
Encode each form value correctly; do not interpolate raw values into a shell command.
The observed exchange returned HTTP 200 and an access token accepted by the coupons API.
Keep the issuer response, token and all input values out of Git, chat and command logs.
Do not infer that unrelated OAuth endpoints or authentication combinations work.

## Private issuer configuration

Canonical local path: `~/.config/uol-ticket-research/uol-technical-issuer.json`.
Keep the directory private and the file owner-only, mode `0600`.
Store the exact observed issuer inputs there; never copy their values into this document.
Read values into memory and pass secrets through protected stdin or request bodies.
The client or issuer may change. Revalidate the acquisition flow and API contract
after an unexpected response; do not guess replacements or repeatedly retry failures.

The private acquisition helper is
`~/.config/uol-ticket-research/acquire-uol-authorization.mjs` (Node 22).
It checks owner-only issuer configuration, uses the exact issuer, rejects redirects,
validates a later expiry and runs the existing coupons parser before saving a
candidate with mode 0600. It never publishes a Worker version. Its default mode
performs zero network requests while expiry is more than seven days away.
`--check-config` validates configuration without issuing a token; `--force` is
reserved for a confirmed early authorization failure.

## Candidate validation and publication

1. Serialize renewal with the private exclusive lock from the renewal runbook.
   Read the actual deployed version, latest version and publication manifest.
   Stop on unknown code, concurrent publication or an unexplained staged version.
2. Acquire one candidate using the exact observed protocol. Require successful
   HTTP/JSON handling, a nonempty access token and a later, usable expiry.
   Decoding JWT metadata is diagnostic only; it is not authorization proof.
3. Test the candidate with the existing `fetchOffersFromApi` implementation.
   Require authenticated HTTP success and `validateTicketApiPayload` acceptance;
   preserve only sanitized status, counts, contract result and expiry as evidence.
4. Stage only `UOL_API_AUTHORIZATION` using installed Wrangler's
   `versions secret put`, with the candidate supplied through stdin.
   Do not replace another secret or deploy local unrelated changes.
5. Recheck deployed/latest version identities. Compare staged code, provenance
   and every unaffected binding with the known deployed version before publishing
   that exact staged version at 100%. Keep the previous version for rollback.
6. Preserve the original release-debt baseline and obtain authenticated pre/post
   snapshots. Do not clear incidents, receipts, unknown deliveries or counters.
   For a credential-only corrective publication, identify and freeze preexisting
   debt and require no new or changed debt. Keep the standard verification result
   explicit; this frozen comparison does not replace its original baseline.
   Respect every existing runtime and cost guard.
7. Require new expiry in diagnostics, no authentication error, valid API contract,
   API success after publication and natural primary success on the exact version.
   Reconcile an ambiguous publication read-only before attempting another mutation.
8. Save private receipts atomically with mode `0600`; update the actual publication
   version while preserving code SHA provenance. Record only sanitized evidence.

Report `published`, `runtimeVerified` and `ready` independently. Historical incidents
or unverified deliveries can keep readiness degraded after authentication recovers.

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
