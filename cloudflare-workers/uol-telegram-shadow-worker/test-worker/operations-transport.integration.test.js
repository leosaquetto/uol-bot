import { env } from "cloudflare:workers";
import { expect, it } from "vitest";
import { sendOperationalNotification } from "../src/operations-notify.js";

it("validates the real ntfy request in workerd before intercepted egress", async () => {
  let requests = 0;
  expect(() => new Request("https://example.invalid", { redirect: "error" }))
    .toThrow(/Invalid redirect value/);
  expect(await sendOperationalNotification({ kind: "ticket-api", severity: "critical", id: "e:opened" }, env,
    async (url, init) => {
      const request = new Request(url, init);
      expect(request.url).toBe("https://ntfy.example.test/");
      expect(request.method).toBe("POST");
      expect(request.redirect).toBe("manual");
      expect(request.signal.aborted).toBe(false);
      expect(request.headers.get("Authorization")).toBe("Bearer vitest-ntfy-token-not-a-secret");
      const payload = await request.json();
      expect(payload).toMatchObject({ topic: "saquettoos", title: "UOL bot - aviso tecnico", priority: 5 });
      const envelope = JSON.parse(payload.message);
      expect(envelope).toMatchObject({ id: "uol-ops-e:opened", source: "saquettoos", route: "saquettoOS://home" });
      expect(envelope.body).toContain("API de ofertas");
      requests += 1;
      return Response.json({ id: "x", event: "message", topic: "saquettoos" });
    }))
    .toEqual({ status: "accepted", reason: "http_accepted", httpStatus: 200 });
  expect(requests).toBe(1);
});
