# Instagram recovery — 19 October 2026

The Portal collector was healthy while both Instagram paths were blocked. The
phone reported `auth_required` and persistent suspension. Oracle reported
`invalid_session_file`: its protected JSON file was valid but no longer contained
`sessionid`. This does not establish which upstream response removed the cookie.

The owner signed into the existing dedicated Instagram account and explicitly
authorized reconnecting Oracle and the Scriptable client. The replacement session
matched the previous account identity. Protected backups preserve the original
session, monitor state and iCloud bootstrap; no credential is stored here.

Only the monitor was paused for atomic session replacement. Its authentication
backoff was released after local validation; rate-limit guards, polling mode,
request budgets, existing signals, outbox and receipts were preserved. The
existing pending notification produced a successful two-request collection at
**13:27:45 São Paulo / 16:27:45 UTC**. No synthetic signal was created.

Story `4011415238472326441`, published at **10:53:01 São Paulo**, links to campaign
`pRp`, “2 INGRESSOS: 31/10 Nubank Arte Lab SP”. The Worker confirmed image delivery
to all four configured destinations, one attempt each. `/catalog` contains the
same campaign with `isPortal: true` and its Story. The owner subsequently supplied
an iPhone screenshot confirming both source badges. This proves catalog display,
not the iPhone's independent Instagram authentication.

`HUNTER_INSTAGRAM_SESSION` was renewed using a secret-only Worker version:
`c84ecce2-137b-460b-b021-a84269fd81d9`. Code remains `091e3fcb77e6`.
The authenticated configuration endpoint returned the replacement session for the
same approved device. Public readiness and a natural primary cycle matched the
new version. The private publication manifest was reconciled.

## Scriptable 5.8.6

- Adds **Ajustes → Coleta neste iPhone → Reconectar Instagram** and the explicit
  `action=reconnect-instagram` entry point.
- Fresh device approval and validated server configuration precede replacement.
  Cached approval cannot authorize recovery. Credentials stay in native code.
- A different session and verified account binding are required. The collector
  shares its existing lease, suspends before writing, verifies Keychain readback,
  and restores the prior session on a failed state write.
- Request history, receipts and rate-limit deadlines remain intact. Only an
  explicit recovery action resumes the suspended source; updates do not do so.
- A successful source read forces catalog refresh. Session replacement alone is
  not labeled successful authentication.
- Compact Instagram badges use the existing colored circular styling. Large
  badges and category classification are unchanged.

Focused offline tests cover approval, identity, suspension, storage failure,
concurrency, request limits, UI routing and catalog merging. The owner confirmed
5.8.6 on the iPhone and ran the explicit recovery. It reported session replacement;
the subsequent collection diagnostic reported `backoff` ("aguardando intervalo
de segurança"). Local session replacement is confirmed by that runtime result,
but a successful iPhone Instagram read remains unproven until the guard expires.
The current UI does not expose the deadline, so no release time is asserted.

Stable 5.8.6 points to artifact commit `2608cfc99b963cac0ebca252b37d0b8ece65ae14`:
693781 bytes, SHA-256 `f09dc2ccce09b8993ddd7261f666b4f03d5abf4777b58c7de5f0ad60b1f80a7f`.
Remote manifest and artifact integrity were verified. The existing saquettoOS
updater reads the same stable channel; no saquettoOS change was necessary.
ntfy accepted one update notice titled **Scriptable - Clube UOL Premium** at
13:50:18 São Paulo (receipt `rvoL7bIdWb4f`). This acceptance is independent of the
owner's confirmation that automatic updating had already reached 5.8.6.

Private evidence: `~/.config/uol-ticket-research/instagram-reconnect-2026-10-19-evidence.json`
and the matching sanitized catalog/Story receipts. Prior FBNS registration remains
unverified; this recovery does not establish native push coverage.
