import { DurableObject } from "cloudflare:workers";
import { timingSafeEqual } from "node:crypto";
import { ACCOUNT_ID, TOKEN_KEY, readWorkouts, candidates, checkDestination, sendNotification, confirmNotification } from "./api.js";

function errorCode(error) {
  const message = String(error?.message || "");
  return /^(gymrats_|beeper_|invalid_json$|response_too_large$)/.test(message)
    ? message.replace(/[^a-z0-9_]/gi, "").slice(0, 80) : "external_or_storage_error";
}

export class GymratsMonitor extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.running = false;
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS deliveries (
      entry_id TEXT PRIMARY KEY, state TEXT NOT NULL,
      occurred_at INTEGER NOT NULL, attempted_at INTEGER NOT NULL,
      message_id TEXT, error_code TEXT
    )`);
  }

  async status() {
    const last = await this.ctx.storage.get("last_run");
    const states = this.ctx.storage.sql.exec("SELECT state, COUNT(*) AS count FROM deliveries GROUP BY state").toArray();
    return {
      deliveryEnabled: this.env.DELIVERY_ENABLED === "true",
      gatewayConfigured: !!(this.env.BEEPER_API_URL && this.env.BEEPER_API_TOKEN),
      lastRun: last || null, deliveries: states,
    };
  }

  async run() {
    if (this.running) return { state: "busy" };
    this.running = true;
    const now = Date.now();
    let result;
    let phase = "storage";
    try {
      // Limits manual triggers and duplicate Cron deliveries to one poll/5 minutes.
      const lastPoll = await this.ctx.storage.get("last_poll");
      if (lastPoll && Math.floor(now / 300_000) === Math.floor(lastPoll / 300_000)) return { state: "throttled" };
      await this.ctx.storage.put("last_poll", now);
      let startedAt = await this.ctx.storage.get("started_at");
      if (!startedAt) {
        startedAt = now;
        await this.ctx.storage.put("started_at", startedAt);
      }
      phase = "session";
      const token = await this.env.SESSION.get(TOKEN_KEY);
      if (!token) throw new Error("gymrats_token_missing");
      phase = "gymrats";
      const { workouts, tokenNext } = await readWorkouts(token, now);
      phase = "refresh";
      if (tokenNext && tokenNext !== token) await this.env.SESSION.put(TOKEN_KEY, tokenNext);
      phase = "detection";
      const entries = candidates(workouts, now, startedAt);
      const deliveryEnabled = this.env.DELIVERY_ENABLED === "true";
      const configured = !!(this.env.BEEPER_API_URL && this.env.BEEPER_API_TOKEN);
      result = {
        state: !deliveryEnabled ? "monitoring_delivery_disabled" : !configured ? "blocked_gateway" : "ok",
        checkedAt: new Date(now).toISOString(), matches: entries.length, accepted: 0, uncertain: 0, pending: 0,
      };
      if (deliveryEnabled && configured) {
        phase = "delivery";
        const pending = entries.filter(entry => !this.ctx.storage.sql.exec(
          "SELECT state FROM deliveries WHERE entry_id = ?", entry.id,
        ).toArray().length);
        if (pending.length) await checkDestination(this.env);
        // Normal use is one check-in; bounded sends keep the cycle comfortably short.
        for (const entry of pending.slice(0, 2)) {
          this.ctx.storage.sql.exec(
            "INSERT OR IGNORE INTO deliveries(entry_id, state, occurred_at, attempted_at) VALUES (?, 'attempted', ?, ?)",
            entry.id, entry.occurred, now,
          );
          // Flush the reservation before external side effects. Never release it automatically.
          await this.ctx.storage.sync();
          try {
            const messageId = await sendNotification(this.env, entry.id);
            this.ctx.storage.sql.exec("UPDATE deliveries SET state = 'pending', message_id = ? WHERE entry_id = ?", messageId, entry.id);
            await this.ctx.storage.sync();
          } catch (error) {
            this.ctx.storage.sql.exec("UPDATE deliveries SET state = 'uncertain', error_code = ? WHERE entry_id = ?", errorCode(error), entry.id);
            result.uncertain++;
            result.state = "delivery_requires_review";
          }
        }
        // Receipt reconciliation is GET-only and cannot create another message.
        const receipts = this.ctx.storage.sql.exec(
          "SELECT entry_id, message_id, attempted_at FROM deliveries WHERE state = 'pending' ORDER BY attempted_at LIMIT 2",
        ).toArray();
        for (const receipt of receipts) {
          if (now - receipt.attempted_at > 86_400_000) {
            this.ctx.storage.sql.exec("UPDATE deliveries SET state = 'uncertain', error_code = 'beeper_confirmation_expired' WHERE entry_id = ?", receipt.entry_id);
            result.uncertain++;
            result.state = "delivery_requires_review";
            continue;
          }
          try {
            if (await confirmNotification(this.env, receipt.message_id)) {
              this.ctx.storage.sql.exec("UPDATE deliveries SET state = 'accepted', error_code = NULL WHERE entry_id = ?", receipt.entry_id);
              result.accepted++;
            } else { result.pending++; }
          } catch (error) {
            this.ctx.storage.sql.exec("UPDATE deliveries SET error_code = ? WHERE entry_id = ?", errorCode(error), receipt.entry_id);
            result.pending++;
          }
        }
        if (result.pending && result.state === "ok") result.state = "delivery_pending";
      }
      // KV is only a compatibility checkpoint. The authoritative ledger is never pruned.
      phase = "checkpoint";
      const latest = this.ctx.storage.sql.exec(
        "SELECT entry_id FROM deliveries WHERE state = 'accepted' ORDER BY occurred_at DESC, entry_id DESC LIMIT 1",
      ).toArray()[0];
      if (latest && await this.env.SESSION.get("LAST_NOTIFIED_WORKOUT_ENTRY_ID") !== latest.entry_id) {
        await this.env.SESSION.put("LAST_NOTIFIED_WORKOUT_ENTRY_ID", latest.entry_id);
      }
    } catch (error) {
      result = { state: "error", checkedAt: new Date(now).toISOString(), phase, code: errorCode(error) };
    } finally {
      try {
        if (result) {
          await this.ctx.storage.put("last_run", result);
          console.log(JSON.stringify({ event: "gymrats_cycle", ...result }));
        }
      } finally { this.running = false; }
    }
    return result;
  }
}

function authorized(request, env) {
  if (!env.ADMIN_TOKEN) return false;
  const actual = new TextEncoder().encode(request.headers.get("Authorization") || "");
  const expected = new TextEncoder().encode(`Bearer ${env.ADMIN_TOKEN}`);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    const headers = { "Cache-Control": "no-store" };
    if (request.method === "GET" && path === "/health") {
      return Response.json({ service: "gymrats-smartfit-monitor", deployed: true }, { headers });
    }
    if (!authorized(request, env)) return Response.json({ error: "unauthorized" }, { status: 401, headers });
    const monitor = env.MONITOR.getByName(ACCOUNT_ID);
    if (request.method === "GET" && path === "/status") return Response.json(await monitor.status(), { headers });
    if (request.method === "POST" && path === "/run") return Response.json(await monitor.run(), { headers });
    return Response.json({ error: "not_found" }, { status: 404, headers });
  },
  async scheduled(_controller, env) {
    await env.MONITOR.getByName(ACCOUNT_ID).run();
  },
};
