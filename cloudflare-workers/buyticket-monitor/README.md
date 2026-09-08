# Rock in Rio BuyTicket monitor

Separate SQLite Durable Object, five-minute alarm, no Cron Trigger. Watches the six configured sector/category combinations for September 12 and 13. Stores the last valid snapshot; invalid requests do not become sold-out rows. Sends one consolidated message for decreases versus the previous valid snapshot. Quantities refer to the category, not necessarily stock at the minimum price. Uses event URLs until listing-link semantics are verified.

Deployment starts silent. All routes require ADMIN_TOKEN. POST /initialize collects the baseline and schedules silent monitoring. GET /status reports enabled, freshness and pending state. GET /preview returns the complete message without sending. **POST /start sends the first snapshot and enables future drop alerts; only call after the user's explicit first-send authorization.** Repeated starts return 409. Alarms stop September 14, 2026 at 03:00 UTC.

Secrets: ADMIN_TOKEN, BEEPER_GATEWAY_URL (dedicated /v1/send-buyticket route), BEEPER_GATEWAY_TOKEN. Never print values. The existing gateway fixes the recipient group and confirms bridge delivery. Unknown delivery is persisted and blocks further sends pending operator reconciliation; never blindly replay. A source/delivery failure appears in status. The first actual group send and WhatsApp rendering remain unverified until authorized.

Validation: node --test test/*.test.js; gateway route tests; Wrangler dry-run; authenticated production status and preview. Preview is read-only. No browser or image generation required.

Published endpoint: https://buyticket-rir-monitor.leosaquetto.workers.dev (authenticated). Local administrator token is stored privately in ~/.config/buyticket-monitor/admin-token; excluded from the repository. Gateway deployment preserves a pre-BuyTicket source backup on the server.

Event scope changes replace the baseline silently and retire any previous-scope pending delivery without replay. Monitoring remains enabled.
