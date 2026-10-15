import { expect, it } from "vitest";
import { sendOperationalNotification } from "../src/operations-notify.js";

it("validates the real ntfy request in workerd before intercepted egress", async () => {
  let requests = 0;
  expect(() => new Request("https://example.invalid", { redirect: "error" }))
    .toThrow(/Invalid redirect value/);
  expect(await sendOperationalNotification({ kind: "ticket-api", severity: "critical" }, async (url, init) => {
    const request = new Request(url, init);
    expect(request.method).toBe("POST");
    expect(request.redirect).toBe("manual");
    expect(request.signal.aborted).toBe(false);
    expect(request.headers.get("Title")).toBe("UOL bot - aviso tecnico");
    expect(request.headers.get("Priority")).toBe("5");
    expect(await request.text()).toContain("API de ofertas");
    requests += 1;
    return new Response(null, { status: 202 });
  }))
    .toEqual({ status: "accepted", reason: "http_accepted", httpStatus: 202 });
  expect(requests).toBe(1);
});
