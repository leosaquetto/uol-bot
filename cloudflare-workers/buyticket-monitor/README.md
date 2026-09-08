# Rock in Rio BuyTicket monitor

Separate SQLite Durable Object, no Cron Trigger. It normally checks every five minutes; while automatic purchase is armed it checks every 15 seconds. It watches September 12 and 13, stores the last valid snapshot, and never turns an invalid request into sold-out rows. It sends a separate message for each affected day only when the minimum across all available categories of a day falls to a value not previously observed for that day and category. A rebound followed by the same price therefore does not repeat an alert. Quantities refer to the category, not necessarily stock at the minimum price.

The optional purchase lane launches Browser Run only for a plausible candidate, follows the verified `/r?event=...&c_anuncio=...` redirect, applies the coupon, and treats the coupon-adjusted PIX total as authoritative. It creates one PIX at a time and sends the copy-and-paste code to the same WhatsApp group; payment remains manual. Bounds are inclusive: R$ 200–350 for September 12 and R$ 100–270 for September 13, across categories except Meia Idoso. Ambiguous purchase or delivery outcomes are terminal and never retried automatically.

Deployment starts silent. All routes require ADMIN_TOKEN. POST /initialize collects the baseline and schedules silent monitoring. GET /status reports enabled, freshness and pending state. GET /preview returns the complete message without sending. **POST /start sends the first snapshot in two sequential messages and enables future drop alerts; only call after the user's explicit first-send authorization.** Repeated starts return 409. Alarms stop September 14, 2026 at 03:00 UTC.

Secrets: ADMIN_TOKEN, PIX_CHECKOUT_VALIDATED, BEEPER_GATEWAY_URL (dedicated /v1/send-buyticket route), BEEPER_GATEWAY_TOKEN, BUYTICKET_USERNAME, BUYTICKET_PASSWORD, BUYTICKET_COUPON, BUYTICKET_QUENTRO_EMAIL, BUYTICKET_PHONE, BUYTICKET_CPF, BUYTICKET_CEP, BUYTICKET_ADDRESS and BUYTICKET_ADDRESS_NUMBER. Never print values. The existing gateway fixes the recipient group and confirms bridge delivery. Unknown delivery is reconciled after five minutes with the same idempotency key: a durable accepted receipt clears it without another send, while a genuinely unknown receipt remains blocked. A source/delivery failure appears in status.

POST `/purchases/dry-run` validates login, listing identity, coupon and final PIX price without creating an order. POST `/purchases/start` arms automatic PIX generation after the dry run passes. Both require ADMIN_TOKEN. GET `/status` exposes only sanitized purchase state.

Validation: node --test test/*.test.js; Wrangler dry-run; authenticated production dry run with an out-of-range listing; production status. The dry run must report `noOrderCreated: true`.

Published endpoint: https://buyticket-rir-monitor.leosaquetto.workers.dev (authenticated). Local administrator token is stored privately in ~/.config/buyticket-monitor/admin-token; excluded from the repository. Gateway deployment preserves a pre-BuyTicket source backup on the server.

Event scope changes replace the baseline silently and retire any previous-scope pending delivery without replay. Monitoring remains enabled.

## PIX automation status — 2026-09-08

The complete billing and PIX path was validated interactively with one September 13 purchase, and the resulting PIX was paid manually. `PIX_CHECKOUT_VALIDATED=true` is now configured, and `/purchases/start` is armed for both event days. The Worker creates one PIX at a time and sends it to the existing WhatsApp group; it never pays automatically.

The live flow verifies authenticated login, listing identity, one-ticket quantity, coupon application, billing, final arithmetic and PIX selection before the final purchase action. A dry-run request never clicks the final action and always reports `noOrderCreated: true`. Browser Run rate-limit failures are returned as `browser_rate_limited` and receive a 15-minute cooldown instead of launching repeatedly.

Latest deployment: `ba0c1142-3036-47a5-a55d-9fb38f85b56f`. `POST /purchases/stop` disables the lane without changing price alerts; `POST /purchases/start` arms it again after the validation gate is present.

Local validation: 24 tests passed, covering parsing, thresholds, category selection, the Meia Idoso exclusion, repeated-minimum suppression, alert receipt reconciliation, ambiguous purchase blocking, browser rate-limit cooldown and the activation gate. Wrangler deployment succeeded. The latest live dry-run was blocked by Cloudflare Browser Run's temporary 429 quota and did not create an order.

### Complete interactive simulation — 2026-09-08

The signed-in in-app browser reached the actual final review and then generated a PIX for one September 13 ticket: R$244,48 total after the R$10 coupon, with payment completed manually. Billing requires its own `Continuar` step before final review. Masked phone/CPF/CEP inputs need typing and blur; filling alone did not reliably update their state. All mandatory fields were verified as populated.

The adapter includes that billing transition, placeholder-based login, masked-input verification, the observed `N° do endereço` selector, address-autofill retry and a final-review parser that reconciles ticket + fees - coupon against the authoritative total and checks PIX. The purchase candidate rules are R$200–350 for September 12 and R$100–270 for September 13, across categories.
