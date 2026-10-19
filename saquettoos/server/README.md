# saquettoOS access

Isolated HTTPS native API, one SQLite Durable Object for the small `saquettoos-personal` workspace. No UOL credentials, sessions, cookies or grants are handled here. Bearer tokens authorize only the launcher and its managed apps.

Run `node --test saquettoos/server/test-access.mjs`. Deploy `saquettoos/server/wrangler.jsonc` with the existing Wrangler installation. Provision `BOOTSTRAP_TOKEN` as a Wrangler secret, never a config variable. `/v1/bootstrap` requires that secret in Authorization and creates one admin; exact credential retries are safe. Remove the bootstrap secret after owner verification.

| Route | Contract |
| --- | --- |
| `GET /health` | Public `{status:"ready",service:"saquettoos-access",version:"1.0.0"}` |
| `POST /v1/enroll` | `{name,deviceId,token}` → `{profile}`; pending member |
| `POST /v1/bootstrap` | Same body, bootstrap bearer → one approved admin |
| `GET /v1/me` | User bearer → `{profile}` |
| `GET /v1/access/:appId` | Approved bearer + app grant → `{allowed:true}` |
| `GET /v1/users` | Approved admin bearer → `{users:[profile]}` |
| `POST /v1/decision` | Admin bearer; `{id,status,appIds,code}` → `{profile}` |

Profiles contain only `id,name,role,status,appIds,code`. Status is `pending`, `approved`, `revoked` or `expired`. Pending registrations expire after seven days and lose access. An explicit enrollment with the same credentials renews only an expired pending request: new confirmation code, new seven-day window, zero grants, subject to rate and pending capacity limits. Subsequent retries are idempotent. Manual revocation cannot be renewed this way. All other registration retries retain the original name, status and grants. UUID device IDs are normalized to lowercase. Tokens are 64 lowercase hexadecimal characters; storage contains only SHA-256 token hashes. Confirmation codes are eight uppercase hexadecimal characters.

Requests are bounded to 4096 bytes. JSON POST bodies reject extra fields. Native calls use HTTPS without Origin/CORS. New enrollment attempts are bounded to ten per hashed IP per UTC clock hour and forty per UTC day; exact credential retries do not consume quotas, except expired request renewal. Storage caps are 32 pending and 128 total users. At full capacity, only the oldest expired pending registration can yield its slot; approved users and manual revocations are retained. A removed expired registration may submit a fresh pending request, subject to all limits. IP addresses are salted and hashed before persistence. Browser-origin requests, token query parameters and unknown app grants are denied.

Errors use `{error:code}` with 400 validation, 401 authentication, 403 authorization, 404 missing resource, 409 identity/confirmation/expiry conflict, 413 oversized body, 415 media type, 429 rate/capacity or 503 unavailable. There are no body/header/token logs. Traces are sampled at 1%, invocation logs are disabled. Client calls must keep secrets out of URLs.
