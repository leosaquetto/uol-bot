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
- `POST /v1/whatsapp/send`: JSON `{requestId,destinations,text,mediaIds}`.
  `requestId` is a UUID; destinations are aliases; mediaIds is an optional list.
  Maximum 20 recipient selections / 10 photos, 8000 text characters or 1024
  characters as a photo caption. Caption accompanies the first photo only.
  Text and URLs may be sent without photos. Links are clickable; this route
  does not generate the X-specific preview card.
- `GET /v1/whatsapp/requests/<UUID>`: persisted batch state, with each job's id,
  destination alias, state, code and confirmation. No content/JID is returned.

Send returns 202 `{requestId,jobs:[{id,destination,state,code,confirmation}]}`.
202 means queued, not delivered. `accepted` means WhatsApp server acknowledgement;
`confirmed` means a recipient receipt (a group receipt need not include everyone).
`unknown` is not automatically retried. Repeat exactly the same requestId/body
to recover after a timeout; changing the body with that ID returns 409. A new
user-authorized send generates a new requestId even for identical content.

## Persistence and limits

Batch creation is atomic and rejects the whole batch if any destination or media
is invalid. The sender rechecks the destination and permission before dispatch.
No contact or JID can be supplied outside the saved allowlist. Global pause,
dry-run and the existing acceptance gates apply. Previously claimed accepted or
ambiguous messages are never reset by the manual API.

Pending manual messages expire after 30 minutes to prevent surprise delivery
after a long disconnection. Existing automatic routes are unchanged. The combined
queue accepts at most 200 queued/dispatching items for a new manual batch.
Uploads are capped at 128 MiB on disk, expire for new sends after 24 hours, and
are removed after 24 hours when no queued/dispatching job references them.
SQLite preserves batch IDs/status for deduplication and audit. Media never has a
public download endpoint. `manual-media` and its contents use private permissions.

Configure Scriptable using a generated private bootstrap in its iCloud Documents
directory, then store the token in Keychain. Only the nonsecret source and
unsigned shortcut generator belong in Git. Device import and actual execution
must be checked on the iPhone; local generation/signature are separate evidence.

All test messages are restricted to the user's own chat.
