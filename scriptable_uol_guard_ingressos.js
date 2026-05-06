// scriptable uol guard ingressos
// roda antes do scraping iOS para decidir se deve executar fallback local.

const DECISION_FILE = "uol_ios_fallback_decision.json"
const STALE_WINDOW_MIN = 30

// Configure este atalho para executar o SSH no Mac e retornar JSON com:
// {
//   "ok": true,
//   "mtime_epoch_ms": 1715000000000,
//   "mtime_iso": "2026-05-06T12:34:56.000Z",
//   "source": "ssh"
// }
// Se SSH falhar/inacessível, o atalho pode retornar {"ok": false, "error": "..."}
const SSH_CHECK_SHORTCUT_NAME = "UOL Guard SSH Check"

function log(msg) { console.log(`[guard ${new Date().toISOString()}] ${msg}`) }

function getIcloudDecisionPath() {
  const fm = FileManager.iCloud()
  const dir = fm.documentsDirectory()
  return { fm, path: fm.joinPath(dir, DECISION_FILE) }
}

function toIsoUtc(tsMs = Date.now()) {
  return new Date(tsMs).toISOString()
}

async function persistDecision(payload) {
  const { fm, path } = getIcloudDecisionPath()
  const serial = JSON.stringify(payload, null, 2)
  fm.writeString(path, serial)
  return path
}

async function runSshProbeViaShortcut() {
  const cb = new CallbackURL("shortcuts://run-shortcut")
  cb.addParameter("name", SSH_CHECK_SHORTCUT_NAME)
  cb.addParameter("input", "text")
  cb.addParameter("text", "uol_guard_pipeline_audit_mtime")

  // Se o atalho usar x-callback-url, o resultado costuma vir em cb.open() + cb.callbackResponse.
  const opened = await cb.open()
  if (!opened) return { ok: false, error: "shortcut_not_opened" }

  const response = cb.callbackResponse || {}
  const raw = String(response.result || response.output || "").trim()
  if (!raw) return { ok: false, error: "empty_shortcut_result" }

  try {
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === "object" ? parsed : { ok: false, error: "invalid_json_object" }
  } catch (e) {
    return { ok: false, error: `invalid_json:${String(e)}`, raw }
  }
}

function evaluateDecision(probe) {
  const nowMs = Date.now()
  const staleLimitMs = STALE_WINDOW_MIN * 60 * 1000

  if (!probe || probe.ok !== true) {
    return {
      decision: "run_fallback",
      run_fallback: true,
      reason: "mac_offline_or_inaccessible",
      now_utc: toIsoUtc(nowMs),
      stale_window_min: STALE_WINDOW_MIN,
      ssh_probe: probe || null,
    }
  }

  const mtimeMs = Number(probe.mtime_epoch_ms || 0)
  if (!Number.isFinite(mtimeMs) || mtimeMs <= 0) {
    return {
      decision: "run_fallback",
      run_fallback: true,
      reason: "missing_or_invalid_mtime",
      now_utc: toIsoUtc(nowMs),
      stale_window_min: STALE_WINDOW_MIN,
      ssh_probe: probe,
    }
  }

  const ageMs = Math.max(0, nowMs - mtimeMs)
  const isFresh = ageMs <= staleLimitMs

  if (isFresh) {
    return {
      decision: "skip_fallback",
      run_fallback: false,
      reason: "pipeline_audit_recent",
      now_utc: toIsoUtc(nowMs),
      stale_window_min: STALE_WINDOW_MIN,
      pipeline_audit_mtime_utc: toIsoUtc(mtimeMs),
      pipeline_audit_age_min: Number((ageMs / 60000).toFixed(2)),
      mac_path_checked: "/Users/leosaquetto/Documentos MAC/BotLeoUol/pipeline_audit.jsonl",
      ssh_probe: probe,
    }
  }

  return {
    decision: "run_fallback",
    run_fallback: true,
    reason: "pipeline_audit_stale",
    now_utc: toIsoUtc(nowMs),
    stale_window_min: STALE_WINDOW_MIN,
    pipeline_audit_mtime_utc: toIsoUtc(mtimeMs),
    pipeline_audit_age_min: Number((ageMs / 60000).toFixed(2)),
    mac_path_checked: "/Users/leosaquetto/Documentos MAC/BotLeoUol/pipeline_audit.jsonl",
    ssh_probe: probe,
  }
}

async function main() {
  log("iniciando guard pré-scraping iOS")

  let probe
  try {
    probe = await runSshProbeViaShortcut()
  } catch (e) {
    probe = { ok: false, error: `ssh_probe_exception:${String(e)}` }
  }

  const decision = evaluateDecision(probe)
  const persisted = {
    ...decision,
    recorded_at_utc: toIsoUtc(),
    version: 1,
  }

  const outputPath = await persistDecision(persisted)
  log(`decisão: ${persisted.decision} | arquivo: ${outputPath}`)

  console.log(JSON.stringify({
    decision: persisted.decision,
    run_fallback: persisted.run_fallback,
    recorded_at_utc: persisted.recorded_at_utc,
    decision_file: DECISION_FILE,
  }))
}

await main()
