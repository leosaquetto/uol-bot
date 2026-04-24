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
  const resp = await githubGetJson("status_runtime.json")
  const current = resp && resp.ok && resp.data && typeof resp.data === "object" ? resp.data : {}
  const status = normalizeStatusRuntime(current)
  const previous = status[component] && typeof status[component] === "object" ? status[component] : {}
  status[component] = { ...previous, ...(componentPatch || {}) }
  const saveResp = await githubPutFile("status_runtime.json", JSON.stringify(status, null, 2), commitMessage)
  if (saveResp && saveResp.ok) {
    const afterResp = await githubGetJson("status_runtime.json")
    const afterData = afterResp && afterResp.ok && afterResp.data && typeof afterResp.data === "object" ? afterResp.data : {}
    warnForMissingCriticalKeys(afterData, component, logFn)
  }
  return saveResp
}

module.exports = {
  buildDefaultStatusRuntime,
  normalizeStatusRuntime,
  updateStatusRuntimeComponent,
}
