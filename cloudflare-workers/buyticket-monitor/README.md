# Rock in Rio BuyTicket monitor

Separate SQLite Durable Object, five-minute alarm, no Cron Trigger. Watches September 12 and 13. Stores the last valid snapshot; invalid requests do not become sold-out rows. Sends a separate message for each affected day only when the minimum across all available categories of a day falls versus the previous valid snapshot. Keeps the six reference rows per day and adds any winning category outside that selection. Highlights winners with fire and full-line bold. Quantities refer to the category, not necessarily stock at the minimum price.

The optional purchase lane scans every available category for exactly one ticket. It launches Browser Run only for a plausible candidate, follows the verified `/r?event=...&c_anuncio=...` redirect, applies the coupon, and treats the coupon-adjusted PIX total as authoritative. Bounds are inclusive: R$ 200–350 for September 12 and R$ 100–250 for September 13. It creates at most one PIX per day and sends the copy-and-paste code to the same WhatsApp group. Payment remains manual. Ambiguous purchase or delivery outcomes are terminal and never retried automatically.

Deployment starts silent. All routes require ADMIN_TOKEN. POST /initialize collects the baseline and schedules silent monitoring. GET /status reports enabled, freshness and pending state. GET /preview returns the complete message without sending. **POST /start sends the first snapshot in two sequential messages and enables future drop alerts; only call after the user's explicit first-send authorization.** Repeated starts return 409. Alarms stop September 14, 2026 at 03:00 UTC.

Secrets: ADMIN_TOKEN, BEEPER_GATEWAY_URL (dedicated /v1/send-buyticket route), BEEPER_GATEWAY_TOKEN, BUYTICKET_USERNAME, BUYTICKET_PASSWORD, BUYTICKET_COUPON, BUYTICKET_QUENTRO_EMAIL, BUYTICKET_PHONE, BUYTICKET_CPF, BUYTICKET_CEP, BUYTICKET_ADDRESS and BUYTICKET_ADDRESS_NUMBER. Never print values. The existing gateway fixes the recipient group and confirms bridge delivery. Unknown delivery is persisted and blocks further sends pending operator reconciliation; never blindly replay. A source/delivery failure appears in status.

POST `/purchases/dry-run` validates login, listing identity, coupon and final PIX price without creating an order. POST `/purchases/start` arms automatic PIX generation after the dry run passes. Both require ADMIN_TOKEN. GET `/status` exposes only sanitized purchase state.

Validation: node --test test/*.test.js; Wrangler dry-run; authenticated production dry run with an out-of-range listing; production status. The dry run must report `noOrderCreated: true`.

Published endpoint: https://buyticket-rir-monitor.leosaquetto.workers.dev (authenticated). Local administrator token is stored privately in ~/.config/buyticket-monitor/admin-token; excluded from the repository. Gateway deployment preserves a pre-BuyTicket source backup on the server.

Event scope changes replace the baseline silently and retire any previous-scope pending delivery without replay. Monitoring remains enabled.

## PIX preparation status — 2026-09-08

Automatic PIX generation is **disabled**. It was briefly armed after validating the coupon-only path, then explicitly stopped when the complete billing validation failed. `PIX_CHECKOUT_VALIDATED` is absent; `/purchases/start` rejects activation and the scheduled purchase lane exits without launching a browser. Price monitoring remains enabled.

Live Browser Run verified authenticated login (a 200 response from `/api/me` alone is insufficient), the listing redirect, coupon application, and a September 13 total of R$386.00 after coupon. Full-form dry run reached billing but failed while waiting for the final purchase control. All dry runs returned `noOrderCreated: true`; no PIX delivery was attempted by these tests.

Latest preparation deployment: `16ef37cc-a2d9-4e5a-a56b-d94f3b655f11`. The current changes are not an accepted production checkout implementation. Before removing the validation gate, verify the final purchase control, authoritative total/quantity/payment selection, complete billing fields, exact PIX payload/expiry contract, concurrent purchase exclusion, delivery queue preservation across both days, and bounded browser usage/retries. Never enable solely because the coupon-only check passes. POST `/purchases/stop` disables the lane without changing price alerts.

Local validation: 16 tests passed, covering parsing, price thresholds, category selection, existing alerts, ambiguous purchase blocking and the activation gate. Wrangler deployment succeeded. End-to-end order creation and PIX delivery remain unvalidated.

### Complete interactive simulation — 2026-09-08

The signed-in in-app browser reached the actual final review without clicking `Comprar agora`: one September 13 ticket, R$360 ticket + R$36 service fee - R$10 coupon = R$386, payment Pix. Billing requires its own `Continuar` step before final review. Masked phone/CPF/CEP inputs need typing and blur; filling alone did not reliably update their state. All mandatory fields were verified as populated in this interactive simulation.

The adapter now includes that billing transition, masked-input verification, the observed `N° do endereço` selector and a final-review parser that reconciles ticket + fees - coupon against the authoritative total and checks Pix. 17 tests passed. Deployment `0e2a7ef8-ab71-401c-a562-cbdaf439c47c` contains these changes. Its live dry run stopped at `billing_postal_code` while waiting for address autofill, returning `finalActionClicked:false` and `noOrderCreated:true`. Interactive flow is validated through review; remote unattended execution remains unvalidated and disabled. No confirmation, order generation or group PIX message was performed in this simulation.
