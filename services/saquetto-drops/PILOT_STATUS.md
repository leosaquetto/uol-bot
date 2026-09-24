# Pilot evidence — 2026-09-24

## Additional live route — BASS PERSUADES

- At the user's request, added private rule `bass-persuades-posts`: `miley` and
  `tudomiley`, types `post`/`quote`, no text filters, destination `bass-persuades`.
- Miley already had post notifications enabled. The dedicated X account followed
  TudoMiley; its resulting UI showed `Following @TudoMiley` and
  `Turn off post notifications`, confirming notifications enabled.
- BASS PERSUADES membership/posting rights were verified without sending. The
  previous private configuration was backed up and validated, then reloaded
  without restarting the service or changing the Lover Tour rule.
- Readback: two configured rules, live/unpaused, push and WhatsApp connected.
  No test messages or historical event replay. The existing observation heartbeat
  was updated to recognize both approved routes.

## Activated initial flow — 21:16:13 UTC / 18:16:13 São Paulo

- The user requested starting operation now. The initial flow is live and unpaused:
  only new own posts/quotes from `taylorswift13`, `taylorswiftbr`, `updateswiftbr`
  to Lover Tour. The activation timestamp excludes earlier publications. The
  other ten saved destinations have no routes. Test mode remains disabled and
  test destinations are permanently restricted to self in code.
- Before activation: no queued, dispatching or unknown jobs; Lover Tour permission
  revalidated; WhatsApp and push connected; browser stopped; 343 MiB available;
  no OOM/high/max memory events; no sustained intense swap in the final six-second
  sample; existing gateway HTTP 200. The receiver had remained connected past
  its four-minute heartbeat interval. Seven real push events and restart recovery
  had already been recorded.
- Initial capacity/continuity readiness was accepted on those bounded observations.
  This does not claim 72-hour stability or complete notification coverage. The
  first eligible automatic push from the configured sources is still pending.
- Configuration and previous acceptance state were privately backed up. Runtime
  status confirmed `mode=live`, `paused=false`, with a persisted activation time
  of `2026-09-24T21:16:13.734Z`. The file also persists unpaused live operation.
- Hourly thread monitoring of existing state is scheduled through 2026-09-27
  21:16:13 UTC. It does not poll X or send tests, and reports only meaningful
  changes, actionable failures or completion. UOL/BuyTicket cutover and Beeper
  retirement remain separate pending stages.

The sections below record the earlier pilot evidence and restrictions at the
time of each trial; the activation above supersedes the former paused status.

## Direct Web Push alternative supersedes the browser trial

- A dedicated subscription was created with Mozilla Autopush using X's public
  VAPID key. X's authenticated `notifications/settings/login.json` accepted it
  with HTTP 200 and `TweetsSetting=on`. The user's in-app subscription was retained.
- The Oracle Node receiver received real encrypted X notifications. A notification
  at 19:56:09 UTC was authenticated/decrypted as `aesgcm`, with `data.type=tweet`
  and the exact post URI. Later notifications arrived after a receiver restart.
- The sample was from an account outside the three configured sources. It proves
  X delivery/identity compatibility, not the Lover Tour routing acceptance gate.
- Oracle fetched the notified public post and recovered its full visible text,
  video frame and avatar. A link to the video's original post did not misclassify
  the containing post as a quote. No page JavaScript was executed.
- The three encrypted prototype events were imported into the main SQLite store.
  A fourth real notification subsequently arrived in the integrated service.
  All four were ignored as unconfigured sources, with no automatic send jobs.
- The integrated `saquetto-drops` service is running on localhost:8788 in dry-run,
  paused, with `DROPS_WHATSAPP=1`. The prototype receiver was stopped first so
  the subscription has a single connected receiver.
- After excluding unused Playwright loading, the integrated service measured
  78,696,448 bytes in memory.current (about 75 MiB), peak 81,346,560 bytes. Sampled
  swap-in/out was zero; gateway readiness was HTTP 200 in 0.037 seconds.
- Baileys was paired with the user's QR and remained connected across service
  restarts. Eleven requested destinations (eight groups, self and two contacts)
  were verified against current group permissions/contact registration. The
  Festivais group was distinguished from its community/announcement counterpart.
  JIDs and verification evidence live only in private Oracle files.
- An explicit real-push sample was sent only to self; WhatsApp acknowledged it
  and the user confirmed receipt with a screenshot. The first visual failed:
  the embedded 240px thumbnail blurred the logo/avatar, the logo looked too small,
  and the avatar was too close to the edge. A second explicit revision uses a
  1024px embedded JPEG, larger branding/avatar, 5.5% insets and stronger shadow.
  The user confirmed the second revision was correct on their phone. After that
  acceptance, one Lover Tour test was sent and reached `confirmed` through a
  `participant_receipt`. No other contact or group received tests.
- The user deleted the group sample because its source was outside the three
  agreed accounts, then clarified: all tests must always go only to self. Group
  pilot support has been removed in both enqueue and dispatch; regression tests
  cover even permissive gates and previously queued group pilot jobs. Future group
  delivery is exclusively the normal rule-filtered flow, never a test bypass.
- `DROPS_PILOT` is now off and the former group environment switch was removed. Normal operation stays
  paused/dry-run with one rule targeting Lover Tour only.
- During the first self-DM send, memory.current was 148,877,312 bytes and peak
  155,066,368 bytes. Available system memory was 397 MiB. The eight-second sample
  showed mostly zero swap activity with one 1,404 KiB/s swap-in burst, no swap-out;
  the existing gateway returned HTTP 200 in 0.435 seconds. This is preliminary
  evidence, not sustained-load or continuity acceptance.
- The 30-second sample around the group trial had at least 409.9 MiB available,
  service memory up to 172.9 MiB, no cgroup OOM/high/max events and gateway HTTP 200.
  System swap moved approximately 30 MiB in and 27 MiB out during that interval;
  it does not establish an absence of sustained swap pressure.
- A subsequent 45-second idle sample had at least 391.4 MiB available and service
  memory up to 130.3 MiB. Swap totaled 2,296 KiB in and 268 KiB out, with activity
  in 15 of 45 seconds; the largest second moved 1,340 KiB. There were no cgroup
  pressure/OOM events and the gateway returned HTTP 200 in 0.313 seconds. The
  short interval still does not replace sustained observation.
- `realPush`, `pushRestart`, `whatsappPilot` and `previewPilot` have evidence;
  continuity and capacity gates remain false.

The following browser trial remains historical evidence; its resource failure
does not apply to the subsequent direct receiver, which still needs load testing
with WhatsApp connected and sustained operation.

## Verified

- Dedicated X account authenticated in the user's in-app browser. Only X cookies
  were transferred through the loopback SSH tunnel to the private Oracle Chrome
  profile. The temporary transfer file was deleted; no cookies or password are
  included in this repository.
- Oracle displayed the authenticated Notifications page. Login is no longer the
  observed blocker; persistence across a complete future login cycle is unproven.
- In-app browser push settings show enabled. All three requested source accounts
  appear with post notifications enabled. This does not establish a push
  subscription or receipt on Oracle.
- Headless Chrome exposed Notification, PushManager and ServiceWorker APIs, but
  its X worker remained inactive with no push subscription. Protocol permission
  override alone did not establish persistent push permission.

## Capacity gate failed in this trial

- Oracle has approximately 954 MiB RAM and existing production services.
- Headless browser at MemoryHigh=300M repeatedly reclaimed memory. A temporary
  500M/600M limit allowed the authenticated page to render and briefly stabilized
  activity, but did not activate the X worker.
- A bounded Xvfb fallback using the same profile encountered repeated browser
  timeouts. Samples showed swap-in of approximately 7–10 MiB/s and 34–52% I/O
  wait; a localhost production gateway readiness request timed out after 5s.
- The pilot browser was stopped immediately. The existing gateway returned
  HTTP 200; available memory recovered to approximately 582 MiB.
- No OOM kill was observed in the headless memory-event sample. That does not
  make the capacity test pass: production responsiveness and swap pressure failed.
- This trial does not prove the VM can never support a smaller implementation.
  It does prohibit enabling this browser configuration alongside production.

## Left in place

- Browser profile remains private, owned by the dedicated service account.
- Chrome and a stopped, non-enabled systemd browser unit remain installed. The
  pilot Xvfb override is present for reproducibility, not automatic startup.
- At the end of the browser trial, the new service was not running. The direct
  receiver deployment above supersedes that state.
- Beeper, gateway routes, Scriptable, UOL, BuyTicket and the stable snapshot retain
  their behavior. The shared thumbnail compositor now accepts optional appearance
  settings, used only by the new Baileys sender; all legacy defaults pass their
  regression tests. The browser trial itself sent no WhatsApp messages.

## Still required

Prove sustained direct receiver operation, notifications from configured sources,
strict post identity/type extraction and sustained combined load with Baileys.
Both self preview and the historical group delivery passed. Automatic operation
is now in its first 72-hour observation period. Gateway/consumer cutover,
channel validation, 72-hour observation and seven-day retirement gate remain open.
