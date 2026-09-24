# Pilot evidence — 2026-09-24

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
- New Node service source and local tests are retained; the service itself is
  not deployed or running on Oracle. All acceptance gates remain false.
- Beeper, gateway routes, Scriptable, UOL, BuyTicket and the stable snapshot are
  unchanged. No WhatsApp test or automated messages were sent.

## Still required

Resolve capacity without compromising existing services or adding paid resources,
then prove actual Oracle push receipt, observer restart/renewal, strict post
identity/type extraction and combined load with Baileys. Pair WhatsApp, verify
destinations and real cards before any send activation. Gateway/consumer cutover,
channel validation, 72-hour observation and seven-day retirement gate remain open.
