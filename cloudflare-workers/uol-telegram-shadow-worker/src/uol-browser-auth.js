import puppeteer from "@cloudflare/puppeteer";

const AUTH_URL = "https://api.uol.com.br/oauth/auth";
const CLUB_URL = "https://clube.uol.com.br/?order=new";
const OAUTH_CLIENT_ID = "aefe43150d4f4af39c6a2553fcfa817e";
const REDIRECT_URI = "http://localhost/";

export function browserAuthConfiguration(env) {
  return {
    configured: Boolean(
      env.UOL_BROWSER &&
      String(env.UOL_LOGIN_USERNAME || "").trim() &&
      String(env.UOL_LOGIN_PASSWORD || ""),
    ),
  };
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
      await direct.click();
      return true;
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
        item.matches?.('button[type="submit"], input[type="submit"]') ||
        labels.test(String(item.textContent || item.value || "").trim()));
      element?.click();
      return Boolean(element);
    });
    if (clicked) return true;
  }
  return false;
}

async function waitAndFillInFrames(page, selectors, value, timeout = 12_000) {
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
  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(15_000);
    await page.setViewport({ width: 390, height: 844, deviceScaleFactor: 1 });
    await page.setUserAgent(
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) " +
      "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
    );
    page.on("response", (response) => {
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

    const state = crypto.randomUUID().replaceAll("-", "");
    const authUrl = new URL(AUTH_URL);
    authUrl.searchParams.set("response_type", "code");
    authUrl.searchParams.set("redirect_uri", REDIRECT_URI);
    authUrl.searchParams.set("state", state);
    authUrl.searchParams.set("client_id", OAUTH_CLIENT_ID);
    authUrl.searchParams.set("login_params", "t=default");
    await page.goto(authUrl.href, { waitUntil: "domcontentloaded", timeout: 25_000 });
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
        `uol_browser_username_field_missing:failures=${resourceFailures.join(",")}:` +
        await safePageShape(page),
      );
    }
    await clickSubmit(page);
    await waitBriefly(page);

    const passwordFilled = await waitAndFillInFrames(page, [
      'input[type="password"]',
      'input[name="password"]',
      'input[name="passwd"]',
    ], String(env.UOL_LOGIN_PASSWORD));
    if (!passwordFilled) {
      throw new Error(
        `uol_browser_password_field_missing:failures=${resourceFailures.join(",")}:` +
        await safePageShape(page),
      );
    }
    await clickSubmit(page);
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
