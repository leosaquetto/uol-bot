import { readFileSync, mkdirSync, writeFileSync, mkdtempSync, rmSync, existsSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { spawnSync } from "node:child_process";

// No secret is passed in argv, printed, or stored in the repository.
const configDir = join(homedir(), ".config/gymrats-smartfit-monitor");
mkdirSync(configDir, { recursive: true, mode: 0o700 });
const adminFile = join(configDir, "admin-token");
if (!existsSync(adminFile)) writeFileSync(adminFile, randomBytes(32).toString("hex"), { mode: 0o600 });
const credentials = JSON.parse(readFileSync(join(homedir(), ".gymrats_creds.json"), "utf8"));
if (String(credentials.user_id) !== "336445" || !credentials.token) throw new Error("GymRats credential account mismatch");
const scratch = mkdtempSync(join(tmpdir(), "gymrats-seed-"));
function wrangler(args, input) {
  const result = spawnSync("./node_modules/.bin/wrangler", args, { input, encoding: "utf8" });
  if (result.status !== 0) throw new Error(`Wrangler ${args.slice(0, 3).join(" ")} failed; secret output withheld`);
}
try {
  const tokenFile = join(scratch, "jwt");
  writeFileSync(tokenFile, credentials.token, { mode: 0o600 });
  wrangler(["kv", "key", "put", "GYMRATS_JWT", "--binding", "SESSION", "--remote", "--path", tokenFile]);
  console.log("GymRats token seeded in dedicated KV.");
  wrangler(["secret", "put", "ADMIN_TOKEN"], readFileSync(adminFile, "utf8"));
  console.log("Admin secret configured; private local credential retained outside repository.");
} finally { rmSync(scratch, { recursive: true, force: true }); }
