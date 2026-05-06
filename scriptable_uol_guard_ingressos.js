// scriptable uol guard ingressos
// roda antes do scraping iOS para decidir se deve executar fallback local.

const DECISION_FILE = "uol_ios_fallback_decision.json"
const LOCK_FILE = "uol_ios_fallback_lock.json"
const AUDIT_FILE = "uol_ios_fallback_audit.jsonl"
const STALE_WINDOW_MIN = 30
const STALE_DRIFT_MARGIN_MIN = 2
const LOCK_STALE_SEC = 120

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

function getIcloudPaths() {
  const fm = FileManager.iCloud()
  const dir = fm.documentsDirectory()
  return {
    fm,
    decisionPath: fm.joinPath(dir, DECISION_FILE),
    lockPath: fm.joinPath(dir, LOCK_FILE),
    auditPath: fm.joinPath(dir, AUDIT_FILE),
  }
}

function toIsoUtc(tsMs = Date.now()) {
  return new Date(tsMs).toISOString()
}

async function persistDecision(payload) {
  const { fm, decisionPath } = getIcloudPaths()
  const serial = JSON.stringify(payload, null, 2)
  fm.writeString(decisionPath, serial)
  return decisionPath
}

function appendFallbackAudit(event, payload = {}) {
  try {
    const { fm, auditPath } = getIcloudPaths()
    const line = JSON.stringify({ event, at_utc: toIsoUtc(), ...payload })
    const prev = fm.fileExists(auditPath) ? fm.readString(auditPath) : ""
    fm.writeString(auditPath, `${prev}${prev ? "\n" : ""}${line}`)
  } catch (e) {
    log(`falha ao gravar auditoria local: ${String(e)}`)
  }
}

function acquireFallbackLock() {
  const { fm, lockPath } = getIcloudPaths()
  const nowMs = Date.now()
  try {
    if (fm.fileExists(lockPath)) {
      const raw = fm.readString(lockPath)
      const lock = raw ? JSON.parse(raw) : null
      const startedTs = Number(lock && lock.started_ts ? lock.started_ts : 0)
      const ageMs = startedTs > 0 ? nowMs - startedTs : 0
      if (startedTs > 0 && ageMs < LOCK_STALE_SEC * 1000) {
        return { ok: false, reason: "lock_active", lock_age_sec: Math.trunc(ageMs / 1000) }
      }
    }
  } catch (e) {
    log(`lock inválido, sobrescrevendo: ${String(e)}`)
  }

  fm.writeString(lockPath, JSON.stringify({ started_ts: nowMs, started_at_utc: toIsoUtc(nowMs), pid: "scriptable_guard" }, null, 2))
  return { ok: true }
}

function releaseFallbackLock() {
  const { fm, lockPath } = getIcloudPaths()
  try { if (fm.fileExists(lockPath)) fm.remove(lockPath) } catch (e) { log(`falha ao remover lock: ${String(e)}`) }
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


function parseRemoteMtimeToEpoch(probe) {
  if (!probe || typeof probe !== "object") return 0

  const epochMs = Number(probe.mtime_epoch_ms || 0)
  if (Number.isFinite(epochMs) && epochMs > 0) return epochMs

  const mtimeRaw = String(probe.mtime_utc || probe.mtime_iso || "").trim()
  if (!mtimeRaw) return 0

  const parsedMs = Date.parse(mtimeRaw)
  return Number.isFinite(parsedMs) && parsedMs > 0 ? parsedMs : 0
}

function evaluateDecision(probe) {
  const nowMs = Date.now()
  const staleLimitMs = (STALE_WINDOW_MIN + STALE_DRIFT_MARGIN_MIN) * 60 * 1000

  if (!probe || probe.ok !== true) {
    return {
      decision: "run_fallback",
      run_fallback: true,
      reason: "mac_offline_or_inaccessible",
      now_utc: toIsoUtc(nowMs),
      stale_window_min: STALE_WINDOW_MIN,
      drift_margin_min: STALE_DRIFT_MARGIN_MIN,
      ssh_probe: probe || null,
    }
  }

  const mtimeMs = parseRemoteMtimeToEpoch(probe)
  if (!Number.isFinite(mtimeMs) || mtimeMs <= 0) {
    return {
      decision: "run_fallback",
      run_fallback: true,
      reason: "missing_or_invalid_mtime",
      now_utc: toIsoUtc(nowMs),
      stale_window_min: STALE_WINDOW_MIN,
      drift_margin_min: STALE_DRIFT_MARGIN_MIN,
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
      drift_margin_min: STALE_DRIFT_MARGIN_MIN,
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
    drift_margin_min: STALE_DRIFT_MARGIN_MIN,
    pipeline_audit_mtime_utc: toIsoUtc(mtimeMs),
    pipeline_audit_age_min: Number((ageMs / 60000).toFixed(2)),
    mac_path_checked: "/Users/leosaquetto/Documentos MAC/BotLeoUol/pipeline_audit.jsonl",
    ssh_probe: probe,
  }
}

async function main() {
  log("iniciando guard pré-scraping iOS")

  const lock = acquireFallbackLock()
  if (!lock.ok) {
    const blocked = { decision: "skip_guard", run_fallback: false, reason: "fallback_lock_active", lock }
    await persistDecision({ ...blocked, recorded_at_utc: toIsoUtc(), version: 1 })
    appendFallbackAudit("guard.lock_blocked", blocked)
    console.log(JSON.stringify(blocked))
    return
  }

  try {
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
    appendFallbackAudit("guard.decision", {
      decision: persisted.decision,
      reason: persisted.reason,
      run_fallback: persisted.run_fallback,
      now_utc: persisted.now_utc || null,
      mtime_utc: persisted.pipeline_audit_mtime_utc || null,
      age_min: persisted.pipeline_audit_age_min != null ? persisted.pipeline_audit_age_min : null,
    })
    log(`decisão: ${persisted.decision} | arquivo: ${outputPath}`)

    console.log(JSON.stringify({
      decision: persisted.decision,
      run_fallback: persisted.run_fallback,
      recorded_at_utc: persisted.recorded_at_utc,
      decision_file: DECISION_FILE,
    }))
  } catch (e) {
    const errMsg = `guard_exception:${String(e)}`
    log(`erro no guard: ${errMsg}`)
    appendFallbackAudit("guard.error", { error: errMsg })
    const safeDecision = {
      decision: "run_fallback",
      run_fallback: true,
      reason: "guard_error",
      error: errMsg,
      recorded_at_utc: toIsoUtc(),
      version: 1,
    }
    await persistDecision(safeDecision)
    console.log(JSON.stringify({
      decision: safeDecision.decision,
      run_fallback: safeDecision.run_fallback,
      reason: safeDecision.reason,
      recorded_at_utc: safeDecision.recorded_at_utc,
      decision_file: DECISION_FILE,
    }))
  } finally {
    releaseFallbackLock()
  }
}

await main()
