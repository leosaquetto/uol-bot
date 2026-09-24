# Saquetto Drops — isolated migration pilot

Push-only X ingestion through a lightweight WebSocket receiver and a private
Baileys delivery foundation. The stable
`../beeper-preview-gateway` remains the production sender. Its routes, Scriptable,
UOL and BuyTicket are not switched by installing this service.

## Current release boundary

The first implementation stage includes configuration/rules, direct Web Push
receipt/decryption (and an optional CDP adapter), strict post-link resolution,
post extraction, simulation, SQLite queue,
crash-safe ambiguous-send handling, SQLite Baileys credentials, sender and private
operations API. All gates are false; `dryRun` and `paused` are true by default.

**Not production accepted:** routing a real notification from the three configured
sources, reply/quote classification across real samples, combined capacity,
channel publication and downstream cutover. Own-DM visual acceptance and a
Lover Tour participant delivery receipt have passed their bounded pilot.
`observerConnected` means the receiver handshake completed, not that every X post
will produce a notification. Synthetic pushes do not prove X push delivery.

The browser-based Oracle trial failed capacity and remains stopped. The subsequent
direct Web Push trial registered successfully with X, received/decrypted real
notifications, reconnected after restart, and fetched a notified post on Oracle.
The integrated receiver runs in simulation with WhatsApp paired; its credentials
survived service restarts without another QR. Eleven requested destinations are
privately mapped and verified, but only Lover Tour is referenced by a rule.
An explicit self-DM pilot reached the user's phone. Its first preview was rejected
for low resolution, small branding and avatar spacing; a corrected revision was
sent only to self and accepted by the user. A subsequent single Lover Tour test
received a participant delivery receipt. The user removed that out-of-scope sample
and clarified the permanent rule: **all tests go only to self**. Group test support
has been removed; the explicit self pilot is currently off.
The first sending sample peaked near 148 MiB (systemd memory.peak), left 397 MiB
available and kept the existing gateway at HTTP 200. This short sample does not
establish long-term stability or sustained combined capacity.
See [PILOT_STATUS.md](PILOT_STATUS.md) for evidence and limits.

There is no scheduled timeline polling or automatic fallback. Timers renew local
WebSocket keepalive/reconnect, optional CDP recording, and persisted work only. An
unresolvable notification remains `pending_review`; it never selects the latest
post by guessing. The observed X push contains `data.type=tweet` and a relative
`data.uri`; only that URI identifies the post. Links in notification text cannot
redirect processing. Unsupported notification types and malformed encrypted data
remain pending for review. Full content is fetched only after the push.

Public x-web bootstrap records are parsed as syntax/literals with Acorn, never
executed. Author, reply/quote flags, full text, media frame and avatar come from
records linked to the exact post ID. Unsupported long-form notes/articles remain
pending rather than sending truncated text.

## Configuration

Copy `config.example.json` to `/etc/saquetto-drops/config.json` (0600, owned by the
service account). Source handles are lowercase. Destinations are private aliases
bound to verified WhatsApp identities. Never commit actual JIDs or credentials.

Rules use literal, case/accent-insensitive `any`, `all`, and `none` terms against
the author's own text. Matching rules union their destination aliases. Quotes are
allowed; replies and reposts are excluded by the initial rules. Automatic sends
deduplicate by post ID plus actual JID. The manual stable endpoint still resends
on each invocation.

`operation.paused` stops dispatch; `dryRun` still records events and evaluates
rules without creating send jobs. An invalid reload retains the last valid
configuration. The CLI pause is persisted and wins over file reloads. Activating
requires every acceptance gate, verified destinations referenced by rules, an available observer,
connected WhatsApp and `dryRun:false`. The activation timestamp prevents sending
historical posts; resuming does not reset it.

Additional saved destinations do not create routes. The private registry can keep
labels, verification timestamps and alternate contact IDs alongside each primary
JID. Every actual send rechecks membership/permission. Name lookup is not repeated
at dispatch time, and registering a destination never sends a test to it.

```sh
npm ci
npm test
npm run drops -- validate config.example.json
DROPS_CONFIG=config.example.json npm run drops -- simulate post-fixture.json
```

Simulation input: `{ "author":"taylorswift13", "type":"post", "text":"new album" }`.
Types are `post`, `quote`, `reply`, `repost`. Simulation never sends.

## Oracle deployment

Use `/opt/saquetto-drops` alongside `/opt/beeper-preview-gateway`; the sender reuses
the stable thumbnail compositor. Do not move, duplicate or change its artwork.
Node >=22.13 is required. Chrome is needed only for the abandoned CDP trial, not
the direct receiver. Docker, PostgreSQL, Redis and paid APIs are not used. Install
npm dependencies on Linux, never copy macOS `node_modules`.

Private state lives in `/var/lib/saquetto-drops` (0700), including `drops.sqlite`,
its WAL, `browser/`, and `acceptance.private.json`. Run one writer per state
directory. The service lock refuses a second process. Baileys stores credential
and Signal-key batches in SQLite transactions; it does not import Beeper sessions.
Keep SQLite backups consistent (SQLite backup API or stopped writer), and stop
Chrome before copying its profile. Backups must remain outside the repository,
private and encrypted before leaving the host. Never restore an old Signal state
over a running session.

Environment in `/etc/saquetto-drops/service.env` (0600):

```text
DROPS_TOKEN=<random 32-byte hex secret>
DROPS_CONFIG=/etc/saquetto-drops/config.json
DROPS_DATA=/var/lib/saquetto-drops
DROPS_PORT=8788
DROPS_PUSH_TRANSPORT=webpush
DROPS_PUSH_REGISTRATION=/var/lib/saquetto-drops/webpush.private.json
DROPS_WHATSAPP=0
DROPS_PILOT=0
```

The API binds to localhost only; do not add it or CDP to Caddy. CDP has full browser
access and must stay on loopback/SSH. Chrome keeps its sandbox enabled.
`DROPS_WHATSAPP=1` starts pairing; QR material is written only to the private
`pairing-qr.private.txt`, never logs. Enable only for the authorized WhatsApp pilot.
Known logout/bad-session events stop reconnect attempts without deleting state.

Web Push registration contains a dedicated Mozilla UAID/channel, endpoint and
encryption keys (0600). It is independent of the in-app browser's subscription.
Only one receiver may use it at once. The Oracle receiver needs no X password or
session cookie. Subscription creation uses X's authenticated web notification
settings endpoint and the public VAPID key observed in X's client; it is not an
official X developer API and may change.

The WebSocket connects only to `push.services.mozilla.com`. It stores the complete
encrypted notification in SQLite with synchronous FULL durability before ACK.
Decryption supports authenticated `aesgcm` and `aes128gcm`; key logging is refused.
Reconnect presents the existing UAID/channel, never silently replaces them.
Changed identities or storage failure halt reception and require review. A
four-minute protocol heartbeat keeps the push connection alive without querying X.
`pushContinuity` replaces the CDP-only `recordingRenewal` acceptance gate. There is
no Chrome recording expiry in this transport; backend subscription validity and
event delivery still need ongoing observation.

The credential import tool stores the two-line download in the macOS Keychain
(`saquetto-drops-x`, account `automation`) and restricts the source file to 0600.
It does not put the password in argv, stdout or the Oracle configuration.

## Private API

All routes require `Authorization: Bearer <DROPS_TOKEN>`. Do not pass secrets as
command-line arguments; use the private environment file.

| Method/path | Result |
| --- | --- |
| `GET /v1/status` | Sanitized component states, gates and aggregate counts |
| `GET /v1/pending` | Up to 100 pending/ambiguous jobs and unresolved events, without content/JIDs |
| `GET /v1/messages/:id` | Local receipt state; not an assertion of delivery |
| `POST /v1/simulate` | Rule matches for a normalized post, no enqueue/send |
| `POST /v1/config/reload` | Validate and reload private configuration |
| `POST /v1/pause` | Persist pause immediately |
| `POST /v1/activate` | Refuse until the acceptance gates and configuration pass |
| `POST /v1/destinations/verify` | Verify a configured alias against live WhatsApp metadata without sending or returning JIDs |
| `POST /v1/pilot/verify` | Check an allowed pilot destination when the explicit pilot gate is enabled |
| `POST /v1/pilot/send` | Enqueue one persisted notified post for the explicitly enabled pilot, never an arbitrary URL/JID |

CLI wrappers: `status`, `pending`, `pause`, `reload`, `activate`. No unrestricted
message-send endpoint is exposed in this pilot.

`DROPS_PILOT=1` allows only an explicit `self` pilot while the normal automation
stays paused. It also requires real push/restart evidence and a connected account.
All group and other-contact test targets are rejected, even if their source would
match an active rule. Legacy queued group pilot jobs fail before preparation or
dispatch. There is no environment switch to bypass this restriction. After self
validation, destinations enter only the normal, rule-filtered delivery flow.
The send body is `{ "alias":"self", "eventId":"<persisted event hash>" }`.
An optional short `revision` identifies an explicitly requested visual correction;
repeating the same post/destination/revision returns the existing job. This does
not permit automatic retry of unknown sends or change normal deduplication.

The Baileys preview embeds a 1024px JPEG (quality 92, 4:4:4) as well as the uploaded
full-size image. The optional compositor settings enlarge the existing vector
badge and circular avatar, inset both by 5.5%, and increase the avatar shadow.
The shared compositor defaults remain unchanged for existing gateway consumers.

Delivery states: `queued`, `dispatching`, `accepted` (server acknowledgement only),
`confirmed` (recipient/participant receipt), `unknown`, `failed`. A local echo or
return from `sendMessage` never becomes confirmed. Restart during dispatch yields
`unknown`. Ambiguous sends are never re-enqueued automatically. Only a known
pre-dispatch failure can retry, at 30/60 seconds and at most three attempts.
Minimum dispatch interval is five seconds, persisted across restarts. Alerts
have a lower priority number than Drops jobs when the gateway cutover is added.

## Remaining gated migration

1. Keep the Oracle browser stopped. Observe real notifications from the three
   configured sources and validate their routing and post type/full text.
2. Verify receiver continuity and notifications after integrated-service restart.
   Resolve unsupported notification contexts from actual payload evidence.
3. Measure receiver + gateway + Baileys together: no OOM, no sustained swap pressure,
   >=150 MiB available under normal load. Resource caps are safeguards, not proof.
4. Perform any further visual tests only in self DM. Validate Lover Tour delivery
   through the normal flow using eligible posts after activation. Resolve
   channels using `newsletterMetadata`, verify admin rights and publication with
   real readback. Channels currently fail closed with `channel_pilot_required`.
5. Complete the private gateway send adapter and consumer compatibility: retain
   `/v1/send-x-post`, token, payload and personal destination; introduce an honest
   Baileys confirmation state and update Scriptable/UOL/BuyTicket before switching.
   Never label Baileys success `confirmed_by_whatsapp_bridge`.
6. Activate only after recorded acceptance, then observe 72 hours. Migrate other
   consumers separately; preserve their idempotency ledgers, alert destinations,
   and dormant monitor/payment state. Do not test purchases or PIX.
7. Retire the Beeper dependency only after every consumer is migrated and seven
   stable days have elapsed. Cancellation remains pending until then. Rollback
   affects future sends, never resends accepted/unknown jobs.

The current code must not be described as a completed migration until those
runtime gates pass. Do not set acceptance booleans based solely on unit tests.
