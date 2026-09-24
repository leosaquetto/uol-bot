# Saquetto Drops — isolated migration pilot

Push-only X ingestion and a private Baileys delivery foundation. The stable
`../beeper-preview-gateway` remains the production sender. Its routes, Scriptable,
UOL and BuyTicket are not switched by installing this service.

## Current release boundary

The first implementation stage includes configuration/rules, persistent CDP push
recording, strict post-link resolution, post extraction, simulation, SQLite queue,
crash-safe ambiguous-send handling, SQLite Baileys credentials, sender and private
operations API. All gates are false; `dryRun` and `paused` are true by default.

**Not production accepted:** real X push, reply/quote
classification on real notifications, browser recovery, combined capacity,
WhatsApp pairing/delivery/previews, channel publication and downstream cutover.
`observerConnected` means CDP recording is enabled, not that X is logged in or that
notifications are arriving. Synthetic pushes do not prove X push delivery.

The X session was authenticated on Oracle during the pilot. Browser capacity did
not pass: headless worker activation stalled, and the Xvfb fallback caused heavy
swap activity and a production gateway timeout. The pilot browser is stopped;
the existing gateway recovered to HTTP 200. See [PILOT_STATUS.md](PILOT_STATUS.md).
Do not start either new service automatically until this blocker is resolved.

There is no scheduled timeline polling or automatic fallback. Timers renew local
CDP recording/reconnect the browser and drain already persisted work only. An
unresolvable notification remains `pending_review`; it never selects the latest
post by guessing. Currently only notifications carrying exactly one canonical
post URL can be processed; the authenticated notification-context resolver must
be completed against a real push payload during the pilot.

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
requires every acceptance gate, verified destinations, an available observer,
connected WhatsApp and `dryRun:false`. The activation timestamp prevents sending
historical posts; resuming does not reset it.

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
Node >=22.13 and Chrome are required; Docker, PostgreSQL, Redis and paid APIs are
not used. Install npm dependencies on Linux, never copy macOS `node_modules`.

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
DROPS_CDP=http://127.0.0.1:9225
DROPS_WHATSAPP=0
```

The API binds to localhost only; do not add it or CDP to Caddy. CDP has full browser
access and must stay on loopback/SSH. Chrome keeps its sandbox enabled.
`DROPS_WHATSAPP=1` starts pairing; QR material is written only to the private
`pairing-qr.private.txt`, never logs. Enable only for the authorized WhatsApp pilot.
Known logout/bad-session events stop reconnect attempts without deleting state.

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

CLI wrappers: `status`, `pending`, `pause`, `reload`, `activate`. No unrestricted
message-send endpoint is exposed in this pilot.

Delivery states: `queued`, `dispatching`, `accepted` (server acknowledgement only),
`confirmed` (recipient/participant receipt), `unknown`, `failed`. A local echo or
return from `sendMessage` never becomes confirmed. Restart during dispatch yields
`unknown`. Ambiguous sends are never re-enqueued automatically. Only a known
pre-dispatch failure can retry, at 30/60 seconds and at most three attempts.
Minimum dispatch interval is five seconds, persisted across restarts. Alerts
have a lower priority number than Drops jobs when the gateway cutover is added.

## Remaining gated migration

1. Resolve the capacity blocker before restarting the Oracle browser. Login and
   the three source bells were verified; prove real post push on the VM.
   Record actual payload shapes before finalizing context/type
   handling. Verify post details and the stable formatting on live samples.
2. Prove browser/observer restart and recording renewal. If headless push fails,
   test Chrome under the existing Xvfb; never silently enable polling.
3. Measure Chrome + gateway + Baileys together: no OOM, no sustained swap pressure,
   >=150 MiB available under normal load. Resource caps are safeguards, not proof.
4. Pair Baileys; validate own DM and Lover Tour receipts + actual preview. Resolve
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
