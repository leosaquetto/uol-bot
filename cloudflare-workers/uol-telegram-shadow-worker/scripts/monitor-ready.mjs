import { classifyHeadlessHealth } from "../src/headless-health.js";
import { mergeBeeperGatewayHealth } from "../src/beeper-gateway-health.js";

const marker = "<!-- uol-worker-headless-monitor -->";
const issueTitle = "[UOL Worker] Monitor headless";
const readyUrl = String(
  process.env.READY_URL || "https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/readyz",
).replace(/\/+$/, "");
const beeperReadyUrl = String(
  process.env.BEEPER_READY_URL || "https://163-176-194-58.sslip.io/readyz",
).replace(/\/+$/, "");
const apiUrl = String(process.env.GITHUB_API_URL || "https://api.github.com").replace(/\/+$/, "");
const token = String(process.env.GITHUB_TOKEN || "").trim();
const repository = String(process.env.GITHUB_REPOSITORY || "").trim();
const runUrl = String(process.env.GITHUB_RUN_URL || "").trim();

function githubHeaders() {
  return {
    Accept: "application/vnd.github+json",
    "Content-Type": "application/json",
    "X-GitHub-Api-Version": "2022-11-28",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  };
}

async function readEndpoint(url) {
  try {
    const response = await fetch(url, {
      headers: { Accept: "application/json", "Cache-Control": "no-cache" },
      signal: AbortSignal.timeout(20_000),
    });
    const text = await response.text();
    let body = null;
    try {
      body = JSON.parse(text);
    } catch {
      // The classifier treats a non-JSON body as unavailable.
    }
    return { status: response.status, body };
  } catch {
    return { status: 0, body: null };
  }
}

async function githubRequest(path, options = {}) {
  if (!token || !repository) return null;
  const response = await fetch(`${apiUrl}${path}`, {
    ...options,
    headers: { ...githubHeaders(), ...(options.headers || {}) },
    signal: AbortSignal.timeout(20_000),
  });
  if (!response.ok) {
    throw new Error(`github_http_${response.status}`);
  }
  return response.status === 204 ? null : response.json();
}

async function findIncident() {
  if (!token || !repository) return null;
  const issues = await githubRequest(
    `/repos/${repository}/issues?state=open&per_page=100`,
  );
  return (issues || []).find((issue) =>
    !issue.pull_request && String(issue.body || "").includes(marker)
  ) || null;
}

function issueBody(result) {
  return `${marker}\nstate: ${result.state}\n\n` +
    "O monitor externo detectou uma alteração no Worker ou no gateway Beeper.\n\n" +
    `Snapshot sanitizado:\n\`\`\`json\n${JSON.stringify(result.snapshot, null, 2)}\n\`\`\`\n` +
    (runUrl ? `\n[Execução](${runUrl})\n` : "");
}

async function updateIncident(result, existing) {
  if (!token || !repository) return;
  const path = `/repos/${repository}/issues`;
  if (result.state === "healthy") {
    if (!existing) return;
    await githubRequest(`${path}/${existing.number}/comments`, {
      method: "POST",
      body: JSON.stringify({
        body: `Readiness recuperado; o monitor voltou a healthy em ${new Date().toISOString()}.`,
      }),
    });
    await githubRequest(`${path}/${existing.number}`, {
      method: "PATCH",
      body: JSON.stringify({ state: "closed", state_reason: "completed" }),
    });
    return;
  }

  const currentBody = String(existing?.body || "");
  const previousState = currentBody.match(/\nstate: (healthy|degraded|outage)\n/)?.[1] || "";
  if (!existing) {
    await githubRequest(path, {
      method: "POST",
      body: JSON.stringify({ title: issueTitle, body: issueBody(result) }),
    });
  } else if (previousState !== result.state) {
    await githubRequest(`${path}/${existing.number}/comments`, {
      method: "POST",
      body: JSON.stringify({
        body: `Estado alterado para **${result.state}**.\n\n${issueBody(result)}`,
      }),
    });
  }
}

const [liveness, readiness, beeperReadiness] = await Promise.all([
  readEndpoint(`${readyUrl.replace(/\/readyz$/, "")}/livez?verify=${Date.now()}`),
  readEndpoint(`${readyUrl}?verify=${Date.now()}`),
  readEndpoint(`${beeperReadyUrl}?verify=${Date.now()}`),
]);
const result = mergeBeeperGatewayHealth(
  classifyHeadlessHealth({ liveness, readiness }),
  beeperReadiness,
);

try {
  await updateIncident(result, await findIncident());
} catch (error) {
  // Monitoring must not turn an operational incident into a noisy failed-run
  // storm. The Worker remains responsible for its direct Telegram/Discord ops alert.
  console.warn(`headless_monitor_github_update_failed:${String(error?.message || error)}`);
}

console.log(JSON.stringify({
  state: result.state,
  hardFailure: result.hardFailure,
  reasons: result.reasons,
  snapshot: result.snapshot,
}));
