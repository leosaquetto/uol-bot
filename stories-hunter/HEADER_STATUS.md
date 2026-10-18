# Header status presentation — 5.8.2

The glass container retains its fixed position, safe-area padding, translucent
background, blur/saturation, lower corners, border, shadow and scroll treatment.
Only its internal information hierarchy changes.

Previously, the logo, catalog sentence and collection sentence shared a narrow
column beside the actions. The new first row contains the logo and three 44px
action targets. The second row separates the offer count from catalog freshness.
Collection follows as a compact disclosure with receipt counts and source badges.
The disclosure also has a 44px minimum touch target.

| State category | Compact presentation | Detail |
| --- | --- | --- |
| Catalog | Offer count; updated/cache/refresh status and dot | Count remains visible during refresh. Cache is not presented as current data. |
| Collection progress | Searching, another execution, or reopen after update | Preserves the reason without suggesting a source was queried. |
| Counts | Received, delivered and pending are separate | Identifies server receipts versus deliveries and items pending on this iPhone. |
| No news | Source name and check mark | Keeps accessible text and the original no-news/zero-found distinction. |
| News | Source name and positive count | Exact number of discoveries, independent of received/delivered counts. |
| Waiting | Pause/clock with waiting, brief pause, service limit or daily limit | Distinguishes backoff, cooldown, rate limiting and daily budget. |
| Access/configuration/authentication/failure | Explicit label and restrained amber treatment | Opens details automatically for a new issue; retains approval, suspension, expiry and private-setup reasons. |
| Unknown | “A confirmar” | Does not report successful collection. Unrecognized payload values are never inserted into markup. |

Normal states start collapsed. Tapping the status row reveals complete diagnoses
under “Coleta neste iPhone”. A newly changed attention condition opens the panel;
repeated identical updates respect manual collapse. Resolution returns it to the
compact state. Disabled collection clears and hides the entire collection surface.

Rows wrap without truncation or fixed container height. ResizeObserver continues to
reserve the measured height plus 12px for every tab. The older WebView fallback
also observes disclosure changes; the native details toggle requests measurement.
Reduced motion disables header transitions, dot pulses and refresh rotation.

Collector rules, access protocols, cache, storage, native bridge, updater and
redemptions are unchanged. The release version is the only native-host change.
Frozen baseline tests still compare every UI byte outside the explicit header
presentation and existing product-label changes. The frozen baseline is untouched.

## Validation

- Full synthetic suite: 246 passed. Final focused header, assembly, equivalence
  and version checks: 24 passed after the last presentation adjustment.
- Chromium with synthetic data: status layouts at 320, 355, 390, 430 and 768px;
  no horizontal overflow, 44px action targets and measured content offsets.
- Visual captures inspected for cached catalog, discoveries, authentication
  attention and scrolling over cards. No console or page errors in the final run.
- Disclosure, refresh, timeline/badge, avatar/settings, scroll and reduced motion
  exercised. At 390px the normal header measures about 153px without a safe-area
  inset; full receipt/source details measure about 264px. Height stays intrinsic.
- The integrated browser timed out; local WebKit did not finish launching.
  Chromium evidence does not establish physical iPhone/Scriptable acceptance.
  The supplied iPhone image informed the design; post-release device confirmation
  remains separate from source, browser and publication verification.
