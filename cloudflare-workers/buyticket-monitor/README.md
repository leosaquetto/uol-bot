# Demi Lovato BuyTicket monitor

SQLite Durable Object without a Cron Trigger. It checks only the September 16, 2026 BuyTicket page every 30 seconds using its public RSC price matrix. September 15 monitoring and purchasing are disabled.

It sends one WhatsApp alert per affected show when a previously unseen listing reference appears with an available price strictly below R$299. Existing qualifying listings seed the baseline silently, so deployment does not broadcast old inventory. Each listing reference is persisted after it is queued to prevent duplicate alerts. An ambiguous delivery is retained for diagnosis but cannot block or consume later offers.

The optional purchase lane creates at most one PIX order per show for the cheapest currently available listing advertised strictly below R$100. Meia Idoso is excluded; every other category is eligible. The advertised price is the trigger; the final PIX total may exceed R$100 after service fees and coupon. The final review must still reconcile ticket, fee, coupon discount and PIX total before the order is committed. The PIX copy-and-paste code is sent to the same dedicated WhatsApp chat as the event alerts; payment remains manual.

All routes require `ADMIN_TOKEN`. `POST /initialize` replaces a changed scope silently and schedules monitoring. `POST /start` activates a new object without sending a snapshot. `POST /purchases/dry-run` validates login, listing, coupon, form and final review without creating an order. `POST /purchases/start` arms PIX generation. `GET /status` reports the active date, R$299 alert threshold, R$100 purchase trigger, freshness and pending states. `GET /preview` is read-only. `POST /retire-rock-in-rio` disables the previous Rock in Rio Durable Object and deletes its alarm. Alarms stop September 18, 2026 at 03:00 UTC.

Delivery uses the authenticated `/v1/send-buyticket` gateway route. The gateway pins BuyTicket alerts to `BEEPER_BUYTICKET_CHAT_ID`, independently from the Clube UOL destination, and requires final WhatsApp bridge confirmation. Unknown outcomes reconcile with the same idempotency key and cannot create duplicate sends.

Secrets: `ADMIN_TOKEN`, `BEEPER_GATEWAY_URL`, `BEEPER_GATEWAY_TOKEN`, `PIX_CHECKOUT_VALIDATED`, `BUYTICKET_USERNAME`, `BUYTICKET_PASSWORD`, `BUYTICKET_COUPON`, `BUYTICKET_QUENTRO_EMAIL`, `BUYTICKET_PHONE`, `BUYTICKET_CPF`, `BUYTICKET_CEP`, `BUYTICKET_ADDRESS` and `BUYTICKET_ADDRESS_NUMBER`. Never print their values. The local administrator token remains outside the repository at `~/.config/buyticket-monitor/admin-token`.

Validation: `node --test test/*.test.js`, gateway tests, `git diff --check`, Wrangler authentication and deployment dry-run.

Published endpoint: https://buyticket-rir-monitor.leosaquetto.workers.dev (authenticated).
