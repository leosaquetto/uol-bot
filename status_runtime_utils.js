function buildDefaultStatusRuntime() {
  return {
    scriptable: {
      last_started_at: "",
      last_finished_at: "",
      last_success_at: "",
      status: "",
      summary: "",
      offers_seen: 0,
      new_offers: 0,
      pending_count: 0,
      last_error: "",
    },
    scraper: {
      last_started_at: "",
      last_finished_at: "",
      last_success_at: "",
      status: "",
      summary: "",
      offers_seen: 0,
      new_offers: 0,
      pending_count: 0,
      last_error: "",
    },
    consumer: {
      last_started_at: "",
      last_finished_at: "",
      last_success_at: "",
      status: "",
      summary: "",
      processed: 0,
      sent: 0,
      failed: 0,
      pending_count: 0,
      last_error: "",
    },
    global: {
      last_offer_title: "",
      last_offer_at: "",
      last_offer_id: "",
    },
  }
}

const STATUS_RUNTIME_MAX_RETRIES = 5
const STATUS_RUNTIME_BACKOFF_MS = 250

const CRITICAL_COMPONENT_KEYS = {
  scriptable: ["status", "summary", "last_finished_at", "pending_count", "last_error"],
  scraper: ["status", "summary", "last_finished_at", "pending_count", "last_error"],
  consumer: ["status", "summary", "last_finished_at", "pending_count", "last_error"],
}

function normalizeStatusRuntime(raw) {
  const defaults = buildDefaultStatusRuntime()
  const source = raw && typeof raw === "object" ? raw : {}
  const out = {}
  for (const component of Object.keys(defaults)) {
    const node = source[component]
    out[component] = { ...defaults[component], ...(node && typeof node === "object" ? node : {}) }
  }
  return out
}



function stableStringify(value) {
  try {
    return JSON.stringify(value)
  } catch (e) {
    return ""
  }
}

async function sleepMs(ms) {
  const seconds = Math.max(0.01, Number(ms || 0) / 1000)
  return await new Promise((resolve) => Timer.schedule(seconds, false, () => resolve()))
}

function isPartiallyNormalizedRuntime(raw) {
  const defaults = buildDefaultStatusRuntime()
  if (!raw || typeof raw !== "object") return true
  for (const component of Object.keys(defaults)) {
    const node = raw[component]
    if (!node || typeof node !== "object") return true
    for (const key of Object.keys(defaults[component])) {
      if (!(key in node)) return true
    }
  }
  return false
}

function warnForMissingCriticalKeys(status, component, logFn) {
  const required = Array.isArray(CRITICAL_COMPONENT_KEYS[component]) ? CRITICAL_COMPONENT_KEYS[component] : []
  const node = status && status[component] && typeof status[component] === "object" ? status[component] : {}
  const missing = required.filter((key) => !(key in node))
  if (missing.length > 0) {
    logFn(`⚠️ warning status_runtime.json: chaves críticas ausentes em '${component}': ${missing.join(", ")}`)
  }
}

async function updateStatusRuntimeComponent({
  githubGetJson,
  githubPutFile,
  component,
  componentPatch,
  commitMessage,
  logFn = console.log,
}) {
  for (let attempt = 1; attempt <= STATUS_RUNTIME_MAX_RETRIES; attempt++) {
    const resp = await githubGetJson("status_runtime.json")
    const current = resp && resp.ok && resp.data && typeof resp.data === "object" ? resp.data : {}
    const baseSha = resp && resp.ok ? String(resp.sha || "") : ""
    const normalized = normalizeStatusRuntime(current)

    if (isPartiallyNormalizedRuntime(current)) {
      const normalizeResp = await githubPutFile(
        "status_runtime.json",
        JSON.stringify(normalized, null, 2),
        `normalize status_runtime schema ${new Date().toISOString()}`
      )
      if (!normalizeResp || !normalizeResp.ok) return normalizeResp
      if (attempt < STATUS_RUNTIME_MAX_RETRIES) {
        await sleepMs(STATUS_RUNTIME_BACKOFF_MS * attempt)
        continue
      }
    }

    const beforeSaveResp = await githubGetJson("status_runtime.json")
    const beforeSaveSha = beforeSaveResp && beforeSaveResp.ok ? String(beforeSaveResp.sha || "") : ""
    if (baseSha && beforeSaveSha && baseSha !== beforeSaveSha) {
      logFn(`⚠️ status_runtime lock detectado (SHA alterado) tentativa ${attempt}/${STATUS_RUNTIME_MAX_RETRIES}`)
      await sleepMs(STATUS_RUNTIME_BACKOFF_MS * attempt)
      continue
    }

    const previous = normalized[component] && typeof normalized[component] === "object" ? normalized[component] : {}
    normalized[component] = { ...previous, ...(componentPatch || {}) }

    const saveResp = await githubPutFile("status_runtime.json", JSON.stringify(normalized, null, 2), commitMessage)
    if (!saveResp || !saveResp.ok) {
      return saveResp
    }

    const afterResp = await githubGetJson("status_runtime.json")
    const afterData = afterResp && afterResp.ok && afterResp.data && typeof afterResp.data === "object" ? afterResp.data : {}
    if (stableStringify(normalizeStatusRuntime(afterData)) !== stableStringify(afterData)) {
      await githubPutFile(
        "status_runtime.json",
        JSON.stringify(normalizeStatusRuntime(afterData), null, 2),
        `normalize status_runtime schema ${new Date().toISOString()}`
      )
    }
    warnForMissingCriticalKeys(afterData, component, logFn)
    return saveResp
  }
  return { ok: false, error: "status_runtime update esgotou tentativas por colisão de SHA" }
}

module.exports = {
  buildDefaultStatusRuntime,
  normalizeStatusRuntime,
  updateStatusRuntimeComponent,
}
