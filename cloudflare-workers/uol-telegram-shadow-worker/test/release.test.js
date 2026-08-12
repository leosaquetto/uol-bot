import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  assertSuccessfulCiJobs,
  assertSuccessfulCiRuns,
  parseWranglerOutputNdjson,
  releaseMessageForCommit,
  releaseTagForSha,
  versionIdFromDeployText,
  versionIdFromVersionsJson,
  versionIdFromWranglerOutput,
} from "../scripts/release.mjs";

const SHA = "15a5cb88be02d431167cd2d970f22f67bdf2068b";
const VERSION_ID = "60a1673a-c37a-4d9f-92a9-987c849c0a5b";

test("deploy delega integralmente para o release", () => {
  const packageJson = JSON.parse(
    readFileSync(new URL("../package.json", import.meta.url), "utf8"),
  );

  assert.equal(packageJson.scripts.deploy, "npm run release --");
  assert.equal(Object.hasOwn(packageJson.scripts, "predeploy"), false);
});

test("gera tag e mensagem determinísticas do commit", () => {
  assert.equal(releaseTagForSha(SHA), "git-15a5cb88be02");
  assert.equal(
    releaseMessageForCommit("fix(uol):  exemplo\n com espaços", SHA),
    "git-15a5cb88be02 fix(uol): exemplo com espaços",
  );
  assert.throws(() => releaseTagForSha("curto"), /release_head_sha_invalid/);
  assert.throws(() => releaseMessageForCommit("", SHA), /release_commit_subject_missing/);
});

test("extrai exatamente uma versão do registro NDJSON do deploy", () => {
  const output = [
    JSON.stringify({ type: "wrangler-session", version: 1 }),
    "linha ignorada",
    JSON.stringify({ type: "deploy", version: 1, version_id: VERSION_ID }),
  ].join("\n");
  assert.equal(parseWranglerOutputNdjson(output).length, 2);
  assert.equal(versionIdFromWranglerOutput(output), VERSION_ID);
  assert.throws(
    () => versionIdFromWranglerOutput('{"type":"wrangler-session"}'),
    /wrangler_deploy_version_id_missing/,
  );
});

test("fallback textual aceita somente o campo explícito do Wrangler", () => {
  assert.equal(versionIdFromDeployText(`Current Version ID: ${VERSION_ID}`), VERSION_ID);
  assert.throws(
    () => versionIdFromDeployText(`deployment ${VERSION_ID}`),
    /wrangler_deploy_text_version_id_missing/,
  );
});

test("fallback da listagem exige tag única do SHA", () => {
  const payload = JSON.stringify([
    { id: "a9cda8ba-f302-4052-9a4a-6463097e50b1", annotations: {} },
    { id: VERSION_ID, annotations: { "workers/tag": "git-15a5cb88be02" } },
  ]);
  assert.equal(versionIdFromVersionsJson(payload, "git-15a5cb88be02"), VERSION_ID);
  assert.throws(
    () => versionIdFromVersionsJson(payload, "git-000000000000"),
    /wrangler_tag_version_id_missing/,
  );
});

test("gate remoto exige workflow verde para o SHA exato", () => {
  const healthy = JSON.stringify([{
    databaseId: 42,
    headSha: SHA,
    status: "completed",
    conclusion: "success",
    workflowName: "UOL Worker CI",
  }]);
  assert.doesNotThrow(() => assertSuccessfulCiRuns(healthy, SHA));
  assert.throws(
    () => assertSuccessfulCiRuns(healthy, "0000000000000000000000000000000000000000"),
    /github_ci_run_missing_for_head/,
  );
});

test("gate remoto confirma o job do Worker, não apenas o workflow agregado", () => {
  const healthy = JSON.stringify({
    headSha: SHA,
    status: "completed",
    conclusion: "success",
    workflowName: "UOL Worker CI",
    jobs: [{ name: "Testes e bundle", status: "completed", conclusion: "success" }],
  });
  assert.doesNotThrow(() => assertSuccessfulCiJobs(healthy, SHA));

  const missingWorkerJob = JSON.stringify({
    ...JSON.parse(healthy),
    jobs: [{ name: "Testes do fallback Python", status: "completed", conclusion: "success" }],
  });
  assert.throws(
    () => assertSuccessfulCiJobs(missingWorkerJob, SHA),
    /github_worker_validation_not_successful/,
  );
});
