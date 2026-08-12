import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const RELEASE_TAG_PATTERN = /^git-[0-9a-f]{12}$/;
const CI_WORKFLOW_NAME = "UOL Worker CI";
const CI_JOB_NAME = "Testes e bundle";

export function releaseTagForSha(sha) {
  const normalized = String(sha || "").trim().toLowerCase();
  if (!/^[0-9a-f]{40}$/.test(normalized)) {
    throw new Error("release_head_sha_invalid");
  }
  return `git-${normalized.slice(0, 12)}`;
}

export function releaseMessageForCommit(subject, sha) {
  const normalizedSubject = String(subject || "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalizedSubject) {
    throw new Error("release_commit_subject_missing");
  }
  const message = `${releaseTagForSha(sha)} ${normalizedSubject}`;
  return message.slice(0, 200);
}

export function parseWranglerOutputNdjson(contents) {
  const records = [];
  for (const line of String(contents || "").split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      records.push(JSON.parse(trimmed));
    } catch {
      // The output file should be NDJSON, but unrelated Wrangler records do not
      // decide a release. Invalid lines are ignored and the deployment record
      // remains mandatory below.
    }
  }
  return records;
}

export function versionIdFromWranglerOutput(contents) {
  const matches = parseWranglerOutputNdjson(contents).filter(
    (record) => record?.type === "deploy" && UUID_PATTERN.test(String(record.version_id || "")),
  );
  const versionIds = [...new Set(matches.map((record) => record.version_id.toLowerCase()))];
  if (versionIds.length !== 1) {
    throw new Error(`wrangler_deploy_version_id_${versionIds.length === 0 ? "missing" : "ambiguous"}`);
  }
  return versionIds[0];
}

export function versionIdFromDeployText(contents) {
  const matches = String(contents || "").match(
    /(?:Current\s+Version\s+ID|Version\s+ID)\s*:\s*([0-9a-f-]{36})/gi,
  ) || [];
  const versionIds = [...new Set(matches
    .map((entry) => entry.match(/([0-9a-f-]{36})$/i)?.[1]?.toLowerCase())
    .filter((value) => UUID_PATTERN.test(String(value || ""))))];
  if (versionIds.length !== 1) {
    throw new Error(`wrangler_deploy_text_version_id_${versionIds.length === 0 ? "missing" : "ambiguous"}`);
  }
  return versionIds[0];
}

function versionsPayload(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.result)) return value.result;
  throw new Error("wrangler_versions_json_invalid");
}

export function versionIdFromVersionsJson(contents, expectedTag) {
  if (!RELEASE_TAG_PATTERN.test(String(expectedTag || ""))) {
    throw new Error("release_tag_invalid");
  }
  let parsed;
  try {
    parsed = JSON.parse(String(contents || ""));
  } catch {
    throw new Error("wrangler_versions_json_invalid");
  }
  const matches = versionsPayload(parsed).filter(
    (entry) => entry?.annotations?.["workers/tag"] === expectedTag &&
      UUID_PATTERN.test(String(entry.id || "")),
  );
  const versionIds = [...new Set(matches.map((entry) => entry.id.toLowerCase()))];
  if (versionIds.length !== 1) {
    throw new Error(`wrangler_tag_version_id_${versionIds.length === 0 ? "missing" : "ambiguous"}`);
  }
  return versionIds[0];
}

export function assertSuccessfulCiRuns(contents, expectedSha) {
  let runs;
  try {
    runs = JSON.parse(String(contents || ""));
  } catch {
    throw new Error("github_ci_json_invalid");
  }
  if (!Array.isArray(runs)) throw new Error("github_ci_json_invalid");
  const exactRuns = runs.filter(
    (run) => run?.headSha === expectedSha && run?.workflowName === CI_WORKFLOW_NAME,
  );
  if (exactRuns.length === 0) throw new Error("github_ci_run_missing_for_head");
  if (!exactRuns.some((run) => run.status === "completed" && run.conclusion === "success")) {
    throw new Error("github_ci_not_successful_for_head");
  }
}

export function assertSuccessfulCiJobs(contents, expectedSha) {
  let run;
  try {
    run = JSON.parse(String(contents || ""));
  } catch {
    throw new Error("github_ci_jobs_json_invalid");
  }
  if (run?.headSha !== expectedSha || run?.workflowName !== CI_WORKFLOW_NAME) {
    throw new Error("github_ci_identity_mismatch");
  }
  if (run.status !== "completed" || run.conclusion !== "success") {
    throw new Error("github_ci_not_successful_for_head");
  }
  const workerJob = Array.isArray(run.jobs)
    ? run.jobs.find((job) => job?.name === CI_JOB_NAME)
    : undefined;
  if (!workerJob || workerJob.status !== "completed" || workerJob.conclusion !== "success") {
    throw new Error("github_worker_validation_not_successful");
  }
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    encoding: "utf8",
    env: options.env || process.env,
    stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    const diagnostic = options.capture
      ? String(result.stderr || result.stdout || "").trim().split(/\r?\n/).slice(-3).join(" | ")
      : "";
    throw new Error(`${options.label || command}_failed${diagnostic ? `: ${diagnostic}` : ""}`);
  }
  return {
    stdout: String(result.stdout || ""),
    stderr: String(result.stderr || ""),
  };
}

function commandExists(command, cwd) {
  const result = spawnSync(command, ["--version"], {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "ignore", "ignore"],
  });
  return !result.error && result.status === 0;
}

function git(args, repositoryRoot) {
  return run("git", args, { cwd: repositoryRoot, capture: true, label: "git" }).stdout.trim();
}

function nodeMajor() {
  return Number.parseInt(process.versions.node.split(".")[0], 10);
}

function resolveCi(repositoryRoot, workerRoot, headSha) {
  if (commandExists("gh", repositoryRoot)) {
    const runsJson = run(
      "gh",
      [
        "run", "list", "--workflow", "uol-worker-ci.yml", "--commit", headSha,
        "--event", "push", "--limit", "20",
        "--json", "databaseId,headSha,status,conclusion,workflowName,url",
      ],
      { cwd: repositoryRoot, capture: true, label: "github_ci_lookup" },
    ).stdout;
    let runs;
    try {
      runs = JSON.parse(runsJson);
    } catch {
      throw new Error("github_ci_json_invalid");
    }
    const exactRuns = Array.isArray(runs)
      ? runs.filter((entry) => entry?.headSha === headSha && entry?.workflowName === CI_WORKFLOW_NAME)
      : [];
    if (exactRuns.length === 0) {
      // A run can take a few seconds to appear after push. Missing is not a
      // GitHub outage and must never downgrade the remote gate to local CI.
      throw new Error("github_ci_run_missing_for_head");
    }
    assertSuccessfulCiRuns(runsJson, headSha);
    const successful = exactRuns.find(
      (entry) => entry.status === "completed" && entry.conclusion === "success",
    );
    const detailsJson = run(
      "gh",
      [
        "run", "view", String(successful.databaseId),
        "--json", "jobs,headSha,conclusion,status,workflowName,url",
      ],
      { cwd: repositoryRoot, capture: true, label: "github_ci_jobs_lookup" },
    ).stdout;
    assertSuccessfulCiJobs(detailsJson, headSha);
    console.log(`release_ci_gate_ok: github ${successful.url || successful.databaseId}`);
    return;
  }

  if (nodeMajor() !== 22) {
    throw new Error(`local_ci_requires_node_22_current_${process.versions.node}`);
  }
  run("npm", ["run", "check:ci"], { cwd: workerRoot, label: "local_ci" });
  console.log(`release_ci_gate_ok: local node@${process.versions.node}`);
}

function help() {
  console.log(`Uso: npm run release

Publica produção apenas depois de:
  1. validar main, worktree limpo e HEAD idêntico a origin/main;
  2. confirmar UOL Worker CI verde para o SHA exato via gh;
     se gh estiver indisponível, executar check:ci local sob Node 22;
  3. executar wrangler deploy --strict com tag git-<sha> e mensagem do commit;
  4. obter a Version ID automaticamente e executar postdeploy em modo live.

Opção segura para validar somente argumentos/parsers, sem deploy:
  npm run release -- --dry-run

O --dry-run ainda exige o source guard. Não consulta CI, Cloudflare ou produção.`);
}

async function main() {
  const args = new Set(process.argv.slice(2));
  if (args.has("--help")) {
    help();
    return;
  }
  const unsupported = [...args].filter((arg) => arg !== "--dry-run");
  if (unsupported.length > 0) throw new Error(`release_argument_unsupported:${unsupported.join(",")}`);

  const scriptDirectory = fileURLToPath(new URL(".", import.meta.url));
  const workerRoot = fileURLToPath(new URL("..", import.meta.url));
  const repositoryRoot = git(["rev-parse", "--show-toplevel"], workerRoot);

  run(process.execPath, [join(scriptDirectory, "assert-deploy-source.mjs")], {
    cwd: workerRoot,
    label: "deploy_source_guard",
  });

  const headSha = git(["rev-parse", "HEAD"], repositoryRoot).toLowerCase();
  const subject = git(["show", "-s", "--format=%s", "HEAD"], repositoryRoot);
  const releaseTag = releaseTagForSha(headSha);
  const releaseMessage = releaseMessageForCommit(subject, headSha);

  if (args.has("--dry-run")) {
    console.log(JSON.stringify({ ok: true, dryRun: true, headSha, releaseTag, releaseMessage }));
    return;
  }

  resolveCi(repositoryRoot, workerRoot, headSha);

  const temporaryDirectory = mkdtempSync(join(tmpdir(), "uol-worker-release-"));
  const outputPath = join(temporaryDirectory, "wrangler-output.ndjson");
  try {
    const deploy = run(
      join(workerRoot, "node_modules", ".bin", "wrangler"),
      ["deploy", "--strict", "--tag", releaseTag, "--message", releaseMessage],
      {
        cwd: workerRoot,
        capture: true,
        label: "wrangler_deploy",
        env: {
          ...process.env,
          FORCE_COLOR: "0",
          WRANGLER_LOG_SANITIZE: "true",
          WRANGLER_OUTPUT_FILE_PATH: outputPath,
        },
      },
    );
    process.stdout.write(deploy.stdout);
    process.stderr.write(deploy.stderr);

    let versionId;
    try {
      versionId = versionIdFromWranglerOutput(readFileSync(outputPath, "utf8"));
    } catch (outputError) {
      try {
        versionId = versionIdFromDeployText(`${deploy.stdout}\n${deploy.stderr}`);
      } catch {
        const versionsJson = run(
          join(workerRoot, "node_modules", ".bin", "wrangler"),
          ["versions", "list", "--json"],
          { cwd: workerRoot, capture: true, label: "wrangler_versions_list" },
        ).stdout;
        versionId = versionIdFromVersionsJson(versionsJson, releaseTag);
      }
      console.warn(`release_version_primary_output_unavailable: ${outputError.message}`);
    }

    console.log(`release_deploy_ok: ${releaseTag} version=${versionId}`);
    run("npm", ["run", "postdeploy:check"], {
      cwd: workerRoot,
      label: "postdeploy_check",
      env: {
        ...process.env,
        EXPECTED_VERSION_ID: versionId,
        EXPECTED_DELIVERY_MODE: "live",
      },
    });
    console.log(`release_ok: ${releaseTag} version=${versionId} mode=live`);
  } finally {
    rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}

const invokedAsScript = process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (invokedAsScript) {
  main().catch((error) => {
    console.error(`release_failed: ${error?.message || String(error)}`);
    process.exit(1);
  });
}
