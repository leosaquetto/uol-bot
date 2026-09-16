import worker, { GymratsMonitor } from "../src/worker.js";
import { ACCOUNT_ID } from "../src/api.js";

export class TestMonitor extends GymratsMonitor {
  async prepare() {
    await this.ctx.storage.put({ last_poll: 0, started_at: Date.now() - 86_400_000 });
  }
}

export default {
  ...worker,
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/test/prepare") {
      await env.MONITOR.getByName(ACCOUNT_ID).prepare();
      return new Response("ok");
    }
    return worker.fetch(request, env);
  },
};
