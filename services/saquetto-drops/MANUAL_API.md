# Manual sending from Shortcuts

Uses the existing Baileys session and serial queue, with a separate
`DROPS_MANUAL_TOKEN` (64 lowercase hex characters). The administrative token is
not accepted here. Bind remains localhost; Caddy exposes only `/v1/whatsapp/*`.
All requests use HTTPS and `Authorization: Bearer <manual token>`. Never put a
token in a URL, repository, shortcut export, logs, or process arguments.

## Contract

- `GET /v1/whatsapp/destinations`: `{destinations:[{id,name,type}]}`. Only verified
  configured contacts/groups are returned. JIDs are never returned.
- `POST /v1/whatsapp/media`: raw JPEG or PNG body and matching `Content-Type`.
  Maximum 8 MiB / 25 megapixels; one simultaneous upload. Images are oriented,
  resized within 2560 pixels, stripped of metadata, and stored privately. No
  Drops badge/avatar is added. Returns 201 `{mediaId}`.
- The same media route accepts `Content-Type: application/octet-stream` for
  MacSend documents up to 100 MiB. Set `X-Macsend-Filename` to the percent-encoded
  UTF-8 filename and `X-Macsend-Mime` to its MIME type. Upload streams to a private
  file; it preserves the original bytes, filename and MIME without image
  conversion. ZIPs, images, audio and video use WhatsApp document attachments.
  Names are reduced to their basename and control characters removed. MIME
  parameters and malformed MIME types are rejected. Returns 201 `{mediaId}`.
- `POST /v1/whatsapp/send`: JSON `{requestId,destinations,text,mediaIds}`.
  `requestId` is a UUID; destinations are aliases; mediaIds is an optional list.
  Maximum 20 recipient selections / 10 attachments, 8000 text characters or 1024
  characters as an attachment caption. Caption accompanies the first attachment only.
  Text and URLs may be sent without photos. Links are clickable; this route
  does not generate the X-specific preview card.
  Sends addressed only to `mac-send` preserve all text whitespace; other
  destination selections retain the existing trimming behavior.
- `GET /v1/whatsapp/requests/<UUID>`: persisted batch state, with each job's id,
  destination alias, state, code and confirmation. No content/JID is returned.

Send returns 202 `{requestId,jobs:[{id,destination,state,code,confirmation}]}`.
202 means queued, not delivered. `accepted` means WhatsApp server acknowledgement;
`confirmed` means a recipient receipt (a group receipt need not include everyone).
`unknown` is not automatically retried. Repeat exactly the same requestId/body
to recover after a timeout; changing the body with that ID returns 409. A new
user-authorized send generates a new requestId even for identical content.

When an outbound message to the personal WhatsApp chat first reaches `accepted`
(or directly reaches `confirmed`), the service sends one best-effort ntfy
notification to `leo-saquetto-wpp-3054`. The notification opens that chat from
either its body or the “Abrir Conversa” action: both use the current sender's
number to open the incoming conversation on the original recipient's phone.
Receipt matching still uses that unchanged personal recipient. Enqueue responses
alone do not send the notification; repeated receipt events do not duplicate it. An ntfy
failure does not change the WhatsApp job state or retry the message.

## Persistence and limits

Batch creation is atomic and rejects the whole batch if any destination or media
is invalid. The sender rechecks the destination and permission before dispatch.
No contact or JID can be supplied outside the saved allowlist. Global pause,
dry-run and the existing acceptance gates apply. Previously claimed accepted or
ambiguous messages are never reset by the manual API.

Pending manual messages expire after 30 minutes to prevent surprise delivery
after a long disconnection. Existing automatic routes are unchanged. The combined
queue accepts at most 200 queued/dispatching items for a new manual batch.
Uploads are capped at 512 MiB on disk, charged by their actual stored bytes,
to accommodate bounded 100 MiB documents without buffering them in RAM. They expire for new sends after 24 hours, and
are removed after 24 hours when no queued/dispatching job references them.
SQLite preserves batch IDs/status for deduplication and audit. Media never has a
public download endpoint. `manual-media` and its contents use private permissions.
Document metadata lives beside each file in a private JSON sidecar and expires
with the file; the existing SQLite schema is unchanged. JPEG/PNG uploads retain
their original 8 MiB processing limit. Caddy's manual namespace body limit must
allow 104857600 bytes; the server request timeout is five minutes.

## MacSend destination registration

`POST /v1/macsend/register` is a loopback-only administrative route, authenticated
with `DROPS_TOKEN`, with JSON `{name:"mac-send 💻",alias:"mac-send"}`. It must remain
outside Caddy's public `/v1/whatsapp/*` namespace. It searches participating
groups by exact name, rejects zero or multiple matches, and verifies membership
and posting permission before saving the alias. The merged configuration is
validated before writing only this destination to
`DROPS_DATA/macsend-destination.private.json` (0600) by atomic rename. The live
configuration changes after the write succeeds. Startup and config reload apply
the saved destination; an invalid record excludes it from the manual allowlist.
The base configuration, other destinations and routing rules are preserved.
The response contains only the
name, match count, alias and masked sender ending (no JID or full phone number).

Configure Scriptable using a generated private bootstrap in its iCloud Documents
directory, then store the token in Keychain. Only the nonsecret source and
unsigned shortcut generator belong in Git. Device import and actual execution
must be checked on the iPhone; local generation/signature are separate evidence.

All test messages are restricted to the user's own chat.
