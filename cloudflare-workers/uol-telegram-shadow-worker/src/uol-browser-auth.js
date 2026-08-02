import puppeteer from "@cloudflare/puppeteer";

const AUTH_URL = "https://clube.uol.com.br/auth/uol/login";
const CLUB_URL = "https://clube.uol.com.br/?order=new";

export function browserAuthConfiguration(env) {
  return {
    configured: Boolean(
      env.UOL_BROWSER &&
      String(env.UOL_LOGIN_USERNAME || "").trim() &&
      String(env.UOL_LOGIN_PASSWORD || ""),
    ),
  };
}

function rawAuthorizationToken(value) {
  return String(value || "").trim().replace(/^bearer\s+/i, "");
}

export function authorizationExpiresAt(value) {
  const parts = rawAuthorizationToken(value).split(".");
  if (parts.length !== 3) return "";
  try {
    const normalized = parts[1].replaceAll("-", "+").replaceAll("_", "/");
    const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
    const payload = JSON.parse(atob(padded));
    const expiresAt = Number(payload?.exp || 0) * 1_000;
    return Number.isFinite(expiresAt) && expiresAt > 0
      ? new Date(expiresAt).toISOString()
      : "";
  } catch {
    return "";
  }
}

export function shouldAttemptAuthorizationRefresh({
  authorization = "",
  apiError = "",
  lastAttemptAt = "",
  now = new Date(),
  refreshBeforeMinutes = 60,
  cooldownMinutes = 360,
} = {}) {
  const lastAttempt = Date.parse(lastAttemptAt);
  if (Number.isFinite(lastAttempt) &&
      now.getTime() - lastAttempt < cooldownMinutes * 60_000) return false;
  if (/(?:uol_api_http_401|uol_api_http_403|unauthor|forbidden)/i.test(apiError)) return true;
  const expiresAt = Date.parse(authorizationExpiresAt(authorization));
  return Number.isFinite(expiresAt) &&
    expiresAt - now.getTime() <= refreshBeforeMinutes * 60_000;
}

async function fillInFrames(page, selectors, value) {
  for (const frame of page.frames()) {
    for (const selector of selectors) {
      const input = await frame.$(selector);
      if (!input) continue;
      await input.click({ clickCount: 3 });
      await input.type(value, { delay: 25 });
      return true;
    }
    const filled = await frame.evaluate((candidateSelectors, nextValue) => {
      const roots = [document];
      for (let index = 0; index < roots.length; index += 1) {
        for (const element of roots[index].querySelectorAll("*")) {
          if (element.shadowRoot) roots.push(element.shadowRoot);
        }
      }
      for (const root of roots) {
        for (const selector of candidateSelectors) {
          const input = root.querySelector(selector);
          if (!input) continue;
          const descriptor = Object.getOwnPropertyDescriptor(
            HTMLInputElement.prototype,
            "value",
          );
          descriptor?.set?.call(input, nextValue);
          input.dispatchEvent(new Event("input", { bubbles: true, composed: true }));
          input.dispatchEvent(new Event("change", { bubbles: true, composed: true }));
          input.focus();
          return true;
        }
      }
      return false;
    }, selectors, value);
    if (filled) return true;
  }
  return false;
}

async function clickSubmit(page) {
  for (const frame of page.frames()) {
    const direct = await frame.$('button[type="submit"], input[type="submit"]');
    if (direct) {
      const disabled = await direct.evaluate((element) =>
        Boolean(element.disabled || element.getAttribute("aria-disabled") === "true"));
      if (!disabled) {
        await direct.click();
        return true;
      }
    }
    const clicked = await frame.evaluate(() => {
      const labels = /^(entrar|continuar|acessar|login|concordar|autorizar)$/i;
      const roots = [document];
      for (let index = 0; index < roots.length; index += 1) {
        for (const item of roots[index].querySelectorAll("*")) {
          if (item.shadowRoot) roots.push(item.shadowRoot);
        }
      }
      const elements = roots.flatMap((root) => [
        ...root.querySelectorAll('button[type="submit"], input[type="submit"], button, a'),
      ]);
      const element = elements.find((item) =>
        !item.disabled && item.getAttribute?.("aria-disabled") !== "true" && (
          item.matches?.('button[type="submit"], input[type="submit"]') ||
          labels.test(String(item.textContent || item.value || "").trim())
        ));
      element?.click();
      return Boolean(element);
    });
    if (clicked) return true;
  }
  return false;
}

async function waitAndSubmit(page, timeout = 20_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await clickSubmit(page)) return true;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  for (const frame of page.frames()) {
    const activeInput = await frame.$('input:focus');
    if (!activeInput) continue;
    await activeInput.press("Enter");
    return true;
  }
  return false;
}

async function waitAndFillInFrames(page, selectors, value, timeout = 30_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await fillInFrames(page, selectors, value)) return true;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  return false;
}

async function waitBriefly(page, timeout = 8_000) {
  await Promise.race([
    page.waitForNavigation({ waitUntil: "domcontentloaded", timeout }).catch(() => null),
    new Promise((resolve) => setTimeout(resolve, 1_500)),
  ]);
}

async function safePageShape(page) {
  const frames = [];
  for (const frame of page.frames()) {
    let location = "";
    try {
      const url = new URL(frame.url());
      location = `${url.hostname}${url.pathname}`;
    } catch {
      location = "invalid-url";
    }
    const shape = await frame.evaluate(() => {
      const text = String(document.body?.innerText || "").toLowerCase();
      return {
        flags: {
          captcha: text.includes("captcha") || text.includes("não sou um robô"),
          invalid: text.includes("inválid") || text.includes("não encontramos"),
          error: text.includes("erro"),
          continue: text.includes("continuar"),
          password: text.includes("senha"),
        },
        buttons: [...document.querySelectorAll("button")].slice(0, 8).map((button) => ({
          type: String(button.type || ""),
          disabled: Boolean(button.disabled),
          text: String(button.textContent || "").trim().slice(0, 40),
        })),
        inputs: [...document.querySelectorAll("input")].slice(0, 8).map((input) => ({
          type: String(input.type || ""),
          name: String(input.name || "").slice(0, 40),
          id: String(input.id || "").slice(0, 40),
        })),
      };
    });
    frames.push({ location, ...shape });
  }
  return JSON.stringify(frames).slice(0, 500);
}

export async function capturePersonalAuthorization(env) {
  if (!browserAuthConfiguration(env).configured) {
    throw new Error("uol_browser_auth_not_configured");
  }

  const limits = await puppeteer.limits(env.UOL_BROWSER);
  const remaining = Number(limits?.remaining);
  if (Number.isFinite(remaining) && remaining > 0 && remaining < 30_000) {
    throw new Error("uol_browser_daily_quota_low");
  }

  const browser = await puppeteer.launch(env.UOL_BROWSER);
  let capturedAuthorization = "";
  let authorizationCodeSeen = false;
  const resourceFailures = [];
  const networkTrace = [];
  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(15_000);
    // The native data-center desktop identity is rejected by Conta UOL. Keep
    // the engine and UA coherent so the PagSeguro anti-fraud bootstrap runs.
    await page.setViewport({ width: 412, height: 915, deviceScaleFactor: 1 });
    await page.setUserAgent(
      "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36",
    );
    page.on("response", (response) => {
      try {
        const request = response.request();
        if (["document", "xhr", "fetch"].includes(request.resourceType()) && networkTrace.length < 16) {
          const url = new URL(response.url());
          networkTrace.push(`${response.status()}:${request.resourceType()}:${url.hostname}${url.pathname}`);
        }
      } catch {
        // Diagnostics are best effort and never include query strings or bodies.
      }
      if (response.status() < 400 || resourceFailures.length >= 8) return;
      try {
        const url = new URL(response.url());
        resourceFailures.push(`${response.status()}:${url.hostname}${url.pathname}`);
      } catch {
        resourceFailures.push(String(response.status()));
      }
    });
    page.on("requestfailed", (request) => {
      if (resourceFailures.length >= 8) return;
      try {
        const url = new URL(request.url());
        resourceFailures.push(`failed:${url.hostname}${url.pathname}`);
      } catch {
        resourceFailures.push("failed:invalid-url");
      }
    });
    page.on("request", (request) => {
      const headers = request.headers();
      const candidate = String(headers["x-authorization"] || "").trim();
      if (
        candidate &&
        request.url().startsWith("https://gateway.produtos.uol.com.br/")
      ) {
        capturedAuthorization = candidate;
      }
    });

    await page.goto(AUTH_URL, { waitUntil: "domcontentloaded", timeout: 25_000 });
    await new Promise((resolve) => setTimeout(resolve, 3_000));

    const usernameFilled = await fillInFrames(page, [
      'input[type="email"]',
      'input[name="username"]',
      'input[name="login"]',
      'input[name="user"]',
      'input[type="text"]',
    ], String(env.UOL_LOGIN_USERNAME).trim());
    if (!usernameFilled) {
      throw new Error(
        `uol_browser_username_field_missing:shape=${await safePageShape(page)}:` +
        `failures=${resourceFailures.join(",")}`,
      );
    }
    if (!await waitAndSubmit(page)) {
      throw new Error(`uol_browser_username_submit_missing:shape=${await safePageShape(page)}`);
    }
    await waitBriefly(page);

    const passwordFilled = await waitAndFillInFrames(page, [
      'input[type="password"]',
      'input[name="password"]',
      'input[name="passwd"]',
    ], String(env.UOL_LOGIN_PASSWORD));
    if (!passwordFilled) {
      throw new Error(
        `uol_browser_password_field_missing:trace=${networkTrace.join(",")}:` +
        `failures=${resourceFailures.join(",")}:` +
        `shape=${await safePageShape(page)}`,
      );
    }
    if (!await waitAndSubmit(page)) {
      throw new Error(`uol_browser_password_submit_missing:shape=${await safePageShape(page)}`);
    }
    await waitBriefly(page, 12_000);

    const currentUrl = page.url();
    try {
      const callback = new URL(currentUrl);
      authorizationCodeSeen = Boolean(callback.searchParams.get("code"));
    } catch {
      authorizationCodeSeen = false;
    }

    await page.goto(CLUB_URL, { waitUntil: "domcontentloaded", timeout: 25_000 });
    await new Promise((resolve) => setTimeout(resolve, 3_000));
    return {
      authorization: capturedAuthorization,
      authorizationCodeSeen,
      outcome: capturedAuthorization
        ? "authorization_captured"
        : authorizationCodeSeen
          ? "authorization_code_only"
          : "authorization_not_observed",
    };
  } finally {
    await browser.close();
  }
}
