import { execFileSync } from "node:child_process";

function git(args, cwd) {
  return execFileSync("git", args, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function fail(reason) {
  console.error(`deploy_source_guard_failed: ${reason}`);
  process.exit(1);
}

let repositoryRoot;
try {
  repositoryRoot = git(["rev-parse", "--show-toplevel"], process.cwd());
} catch {
  fail("checkout Git indisponível");
}

let branch;
try {
  branch = git(["symbolic-ref", "--quiet", "--short", "HEAD"], repositoryRoot);
} catch {
  fail("HEAD destacado; deploy exige a branch main");
}

if (branch !== "main") {
  fail(`branch ${branch || "desconhecida"}; deploy exige main`);
}

let worktreeStatus;
try {
  worktreeStatus = git(
    ["status", "--porcelain=v1", "--untracked-files=normal"],
    repositoryRoot,
  );
} catch {
  fail("não foi possível verificar o worktree");
}

if (worktreeStatus) {
  fail("worktree com alterações; commit ou remova-as antes do deploy");
}

try {
  git(
    ["fetch", "--quiet", "origin", "refs/heads/main:refs/remotes/origin/main"],
    repositoryRoot,
  );
} catch {
  fail("não foi possível atualizar origin/main");
}

let head;
let originMain;
try {
  head = git(["rev-parse", "HEAD"], repositoryRoot);
  originMain = git(["rev-parse", "refs/remotes/origin/main"], repositoryRoot);
} catch {
  fail("não foi possível comparar HEAD com origin/main");
}

if (head !== originMain) {
  fail("HEAD difere de origin/main; sincronize main antes do deploy");
}

console.log(`deploy_source_guard_ok: main@${head.slice(0, 12)}`);
