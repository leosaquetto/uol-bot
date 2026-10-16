// Variables used by Scriptable.
// icon-color: purple; icon-glyph: bell;
// This client sends a wake signal only. Instagram and delivery stay on the server.
const WAKE_URL = "https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/instagram-wake";
const CODE_KEY = "uol-instagram.wake-code.v1";
const MAX_QUEUE = 64;
const CODE_PATTERN = /^w1\.[a-zA-Z0-9_-]{1,64}\.[0-9a-f]{64}$/;

async function main() {
  if (!Keychain.contains(CODE_KEY)) {
    if (config.runsInWidget || config.runsWithSiri || args.shortcutParameter != null) {
      return { ok: false, state: "setup_required" };
    }
    const prompt = new Alert();
    prompt.title = "Configurar UOL Instagram";
    prompt.message = "Cole seu código individual de gatilho. Não use sessão do Instagram ou token de WhatsApp.";
    prompt.addSecureTextField("w1.…", "");
    prompt.addAction("Salvar");
    prompt.addCancelAction("Cancelar");
    if (await prompt.presentAlert() !== 0) return { ok: false, state: "setup_cancelled" };
    const code = prompt.textFieldValue(0).trim();
    if (!CODE_PATTERN.test(code)) return { ok: false, state: "invalid_code" };
    Keychain.set(CODE_KEY, code);
    if (!Keychain.contains(CODE_KEY) || Keychain.get(CODE_KEY) !== code) throw new Error("setup_storage_failed");
    return { ok: true, state: "configured" };
  }
  const code = Keychain.get(CODE_KEY);
  if (!CODE_PATTERN.test(code)) return { ok: false, state: "invalid_stored_code" };
  const fm = FileManager.local();
  const directory = fm.joinPath(fm.documentsDirectory(), "UOL Instagram-pending.v2");
  if (!fm.fileExists(directory)) fm.createDirectory(directory, true);
  const filenamePattern = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.json$/;
  function filenames() { return fm.listContents(directory).filter(name => filenamePattern.test(name)).sort(); }
  function save(event) {
    const path = fm.joinPath(directory, event.eventId + ".json");
    const encoded = JSON.stringify(event);
    if (fm.fileExists(path)) {
      if (fm.readString(path) !== encoded) throw new Error("pending_identity_conflict");
    } else {
      fm.writeString(path, encoded);
      if (fm.readString(path) !== encoded) throw new Error("pending_storage_failed");
    }
    return { event, path };
  }
  function loadPending() {
    // Concurrent invocations can briefly exceed the admission limit. Drain a
    // bounded snapshot rather than refusing recovery when capacity is reached.
    return filenames().slice(0, MAX_QUEUE + 16).map(name => {
      const path = fm.joinPath(directory, name);
      if (!fm.fileExists(path)) return null; // Another execution acknowledged it.
      let event;
      try { event = JSON.parse(fm.readString(path)); } catch (_) { throw new Error("pending_storage_invalid"); }
      if (!validEvent(event) || name !== event.eventId + ".json") throw new Error("pending_storage_invalid");
      return { event, path };
    }).filter(Boolean).sort((a, b) => a.event.observedAt.localeCompare(b.event.observedAt));
  }
  // Import the earlier draft queue without deleting its private evidence.
  const legacy = fm.joinPath(fm.documentsDirectory(), "UOL Instagram-pending.v1.json");
  const imported = fm.joinPath(directory, "v1-imported");
  if (fm.fileExists(legacy) && !fm.fileExists(imported)) {
    let old;
    try { old = JSON.parse(fm.readString(legacy)); } catch (_) { return { ok: false, state: "pending_storage_invalid" }; }
    const records = Array.isArray(old) ? old : old && Array.isArray(old.events) ? old.events.concat(old.overflowHint ? [old.overflowHint] : []) : null;
    if (!records || records.length > MAX_QUEUE + 1 || records.some(event => !validEvent(event))) return { ok: false, state: "pending_storage_invalid" };
    for (const event of records) save(event);
    fm.writeString(imported, "imported");
    if (fm.readString(imported) !== "imported") throw new Error("pending_storage_failed");
  }
  const notification = { eventId: UUID.string().toLowerCase(), profile: "clubeuol", source: "scriptable", observedAt: new Date().toISOString() };
  let admitted = false;
  function admit() {
    if (!admitted && filenames().length < MAX_QUEUE) {
      const entry = save(notification); // Durable before any request using it.
      admitted = true;
      return entry;
    }
    return null;
  }
  admit();
  const queue = loadPending();
  let accepted = 0;
  function result(ok, state, extra = {}) {
    admit(); // A concurrent execution may have freed capacity meanwhile.
    return { ok, state, accepted, pending: filenames().length, coalesced: !admitted, ...extra };
  }
  // Each request owns one immutable event file. An ACK can never rewrite or
  // delete another execution's new notification.
  for (let sent = 0; sent < 8 && queue.length; sent++) {
    const entry = queue.shift();
    if (!fm.fileExists(entry.path)) continue;
    const event = entry.event;
    let response = null;
    for (let attempt = 0; attempt < 2; attempt++) {
      const request = new Request(WAKE_URL);
      request.method = "POST";
      request.timeoutInterval = 12;
      request.headers = { Authorization: "Bearer " + code, "Content-Type": "application/json" };
      request.body = JSON.stringify(event);
      request.onRedirect = () => null;
      try {
        const raw = await request.loadString();
        const status = request.response && request.response.statusCode;
        let body;
        try { body = JSON.parse(raw); } catch (_) { body = null; }
        response = { accepted: (status === 200 || status === 202) && body && body.ok === true && body.accepted === true && typeof body.eventId === "string" && body.eventId.toLowerCase() === event.eventId, status };
        break; // Definitive HTTP response is never retried in this invocation.
      } catch (_) {
        // Lost responses retry the same durable event ID, at most once.
      }
    }
    if (!response || !response.accepted) return result(false, response ? "server_rejected" : "network_uncertain", { httpStatus: response ? response.status : null });
    if (fm.fileExists(entry.path)) {
      // Never remove a changed file or a different pending identity.
      if (fm.readString(entry.path) !== JSON.stringify(event)) throw new Error("pending_identity_conflict");
      try { fm.remove(entry.path); } catch (error) { if (fm.fileExists(entry.path)) throw error; }
    }
    accepted++;
    const newest = admit();
    if (newest) queue.push(newest);
  }
  return result(true, filenames().length ? "pending" : "accepted");
}

function validEvent(event) {
  return event && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(event.eventId) && event.profile === "clubeuol" && event.source === "scriptable" && typeof event.observedAt === "string" && Number.isFinite(Date.parse(event.observedAt));
}

try {
  Script.setShortcutOutput(await main());
} catch (_) {
  Script.setShortcutOutput({ ok: false, state: "client_failure" });
} finally {
  Script.complete();
}
