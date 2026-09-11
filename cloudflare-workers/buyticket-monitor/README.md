# Demi Lovato BuyTicket monitor

SQLite Durable Object without a Cron Trigger. It checks the September 16 and 17, 2026 BuyTicket pages concurrently every 30 seconds using their public RSC price matrices.

The monitor is notification-only. Automatic checkout and PIX generation are retired and `/purchases/start` returns `410 purchases_retired`. It sends one WhatsApp alert per affected show when a previously unseen listing reference appears with an available price strictly below R$299. Existing qualifying listings seed the baseline silently, so deployment does not broadcast old inventory. Each listing reference is persisted after its first observation to prevent duplicate alerts.

All routes require `ADMIN_TOKEN`. `POST /initialize` replaces a changed scope silently and schedules monitoring. `POST /start` activates a new object without sending a snapshot. `GET /status` reports the two dates, R$299 threshold, freshness and pending delivery state. `GET /preview` is read-only. `POST /retire-rock-in-rio` disables the previous Rock in Rio Durable Object and deletes its alarm. Alarms stop September 18, 2026 at 03:00 UTC.

Delivery uses the authenticated `/v1/send-buyticket` gateway route. The gateway pins BuyTicket alerts to `BEEPER_BUYTICKET_CHAT_ID`, independently from the Clube UOL destination, and requires final WhatsApp bridge confirmation. Unknown outcomes reconcile with the same idempotency key and cannot create duplicate sends.

Secrets: `ADMIN_TOKEN`, `BEEPER_GATEWAY_URL` and `BEEPER_GATEWAY_TOKEN`. Never print their values. The local administrator token remains outside the repository at `~/.config/buyticket-monitor/admin-token`.

Validation: `node --test test/*.test.js`, gateway tests, `git diff --check`, Wrangler authentication and deployment dry-run.

Published endpoint: https://buyticket-rir-monitor.leosaquetto.workers.dev (authenticated).
