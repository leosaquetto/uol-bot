# Beeper preview gateway

Private sidecar for the UOL Worker. It accepts only authenticated Clube UOL offer
links, pins delivery to the configured Beeper chat, and restores the native
WhatsApp card by passing Beeper's original `links[]` preview payload through the
private local transport. The Worker only sees success after the local Beeper
index records final bridge status `SUCCESS` for that exact pending message.

Required environment:

- `GATEWAY_TOKEN`
- `BEEPER_CHAT_ID`
- `BEEPER_BUYTICKET_CHAT_ID` (destino exclusivo dos alertas BuyTicket)
- `BEEPER_ACCESS_TOKEN`
- `BEEPER_API_URL` (defaults to `http://127.0.0.1:23373`)
- `BEEPER_TRANSPORT_NONCE` and `BEEPER_ACCOUNT_ID` (enable the headless account bootstrap)
- `BEEPER_INDEX_DB_PATH` (read-only Beeper index used for final bridge confirmation)
- `DATA_PATH` (defaults to `/var/lib/beeper-preview-gateway/deliveries.sqlite`)

Optional Scriptable X post delivery:

- `TVGLOBO_TOKEN`: separate token for `POST /v1/send-x-post` and legacy `POST /v1/send-tvglobo`.
- `BEEPER_SELF_CHAT_ID`: verified personal WhatsApp chat, different from the UOL group.

The general route accepts `{link, text, preview: {summary, imageUrl}}`, but only
canonical `https://x.com/<username>/status/<id>` links (lowercase usernames,
1–15 letters, digits or underscores) and images from the image paths on
`pbs.twimg.com`. Each general-route request sends again, even for a previously
sent post; no `Idempotency-Key` header is required. A fresh receipt is stored in
the delivery ledger for each request. The legacy route still accepts only TV Globo
with its original `tvglobo:<id>:self:v1` duplicate protection. The destination is fixed on the server; request fields
cannot redirect delivery. Existing UOL and BuyTicket tokens/routes are unchanged.

`scriptable/Ultimo-Tweet-TVGlobo.js` runs in a Shortcuts background action. It
is version 13.1: the user-validated async wrapper finalizes in `finally`, clears
the Shortcut output on errors and rethrows the original failure. Successful
runs set the post URL before finalizing. The iOS correction was supplied and
confirmed by the user; local lifecycle tests do not emulate the Shortcuts host.
It accepts `@username`, `username`, or a profile URL as its text parameter; empty
input defaults to TV Globo. Raw HTML input preserves the original TV Globo mode.
In Shortcuts, connect an Ask for Input action to the script's Parameter field.
Each execution checks only that profile and sends its latest post again; it does
not schedule checks or send every post published since the last execution.
The script and gateway impose no daily message quota; external network limits
still apply, and HTTP 429 stops the execution without automatic retry. It
extracts the latest own post from public profile HTML and the full text from the
matching article on the post page. Open Graph is a fallback for short text;
an apparently truncated description is rejected if the article is unavailable.
External links are expanded from their HTML href. The body has no custom character
limit on `/v1/send-x-post` (the HTTP payload ceiling is 1 MiB; network limits still
apply). Other routes retain their existing limits.
The native card title is `Name (@username) no X`, with image and title only.
The personal route clears the preview summary so the post text is not repeated.
WhatsApp determines the final wrapping. The body follows the user's `embed.txt`
model: quoted monospace post text, `𝕏 Name (@username) no X, HH:mm`, then a quoted
monospace `https://x.com/username/status/id?s=46` link. The timestamp is the publication time
decoded from the post's Snowflake ID, displayed in America/Sao_Paulo. There is no
top spacer, extra blank line between sections, separate heading, or credit footer.
Nonempty post lines retain their text and order; empty spacer lines are removed.
Scriptable supplies
`format: "whatsapp"`; only the general personal route forwards `formatText: false`
to Beeper, avoiding extra breaks introduced by Markdown-to-HTML conversion.
Other clients and routes keep their existing formatting behavior. The preview's
matched URL is the full HTTPS URL with the requested `?s=46` share suffix.
Fetching the original X post still uses the canonical URL without that suffix. A device test showed that omitting the scheme
hides the preview even when its metadata and image are delivered.
Media posts use their own image or video frame. Otherwise Scriptable uses the
queried profile's avatar, upgrading known CDN size suffixes to 400x400. The user
confirmed and accepted the large avatar layout on iPhone. If neither image nor
avatar is available, the message is sent without a card.
For `/v1/send-x-post` only, Sharp composites the supplied
`assets/pushpushpushsaquetto.svg` over the image. The supplied artwork and transparency are
preserved; badge width is 36% of the shorter side, with its original aspect ratio.
It sits at the top right: right margin is 1.5% of image width and top margin is
1.5% of image height. Images retain their aspect ratio and are limited to 1600px
on either axis. Sources whose longest side is below 1080px are enlarged to that
canvas size before applying the vector badge, so small video frames do not make
the brand blurry. This does not recover missing detail in the original photo.
The SVG is rasterized at 216 DPI before sizing, then the composite is exported
as JPEG at quality 92. The original source is not modified. Other routes retain
their image bytes.
Banners and generic X images are ignored. Older clients still receive a missing
URL before the final credit line and the updated signature wording.
On first run it imports `TVGlobo-Beeper-config.json` (`{"token":"..."}`) from
the user's Scriptable iCloud folder into Keychain and removes that bootstrap
file. Never commit or log the real configuration. If X blocks the page, fails
to include posts, or omits the post text, no message is sent.

The service binds to `127.0.0.1:8787`. Caddy provides public HTTPS. The raw
Beeper API and private transport must remain bound to localhost.

`deploy/beeper-profile-server.service` gives Beeper Server a persistent private
transport nonce. The gateway uses the same nonce to initialize the configured
account and restore the original `links[]` preview payload during delivery. The
private transport is never proxied publicly.

Health endpoints:

- `GET /livez`: process liveness.
- `GET /readyz`: public readiness for the ledger, headless transport, and
  configured Beeper chat and read-only delivery confirmation index.
- `GET /v1/readyz`: the same no-send probe authenticated with
  `Authorization: Bearer <GATEWAY_TOKEN>`. It also returns aggregate ledger
  counts, so the Worker-to-gateway token and route can be checked safely.

Successful delivery means that Beeper replaced the pending echo with a final
event whose bridge status is `SUCCESS`. When an image was supplied, the final
event must also retain the preview image in `links[]`.
The idempotency ledger records that state as `accepted`. A timeout or an
ambiguous upstream response after the request is stored as `unknown` and is
never retried automatically, preventing duplicate messages. Only a definitive
pre-dispatch rejection releases the same idempotency key for retry.

The service writes one-line JSON events for send decisions and transport state.
They contain a generated request ID and a short hash of the idempotency key,
never authorization values, message text, offer URLs, previews, or chat IDs.

Enable lingering for the Beeper Server user (`loginctl enable-linger ubuntu`).
Without it, systemd stops the user service when the last SSH session closes.
Keep the gateway SQLite files private (`0600`).
