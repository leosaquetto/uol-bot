import puppeteer from "@cloudflare/puppeteer";

const MOBILE_USER_AGENT =
  "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36";

export async function captureOfferDetailInBrowser(env, link) {
  if (!env.UOL_BROWSER) throw new Error("uol_browser_detail_not_configured");

  const limits = await puppeteer.limits(env.UOL_BROWSER);
  if (Number(limits?.allowedBrowserAcquisitions || 0) < 1) {
    throw new Error("uol_browser_session_unavailable");
  }

  const browser = await puppeteer.launch(env.UOL_BROWSER);
  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(15_000);
    await page.setViewport({ width: 412, height: 915, deviceScaleFactor: 1 });
    await page.setUserAgent(MOBILE_USER_AGENT);
    await page.goto(link, { waitUntil: "domcontentloaded", timeout: 25_000 });
    await page.waitForSelector(".info-beneficio, meta[name='description']", {
      timeout: 5_000,
    }).catch(() => null);

    return page.evaluate(() => {
      const text = (selector) => String(document.querySelector(selector)?.innerText || "").trim();
      const meta = (selector) => String(document.querySelector(selector)?.content || "").trim();
      const images = [
        meta('meta[property="og:image:secure_url"]'),
        meta('meta[property="og:image"]'),
        meta('meta[name="twitter:image"]'),
        String(document.querySelector('[data-src*="/beneficios/"]')?.getAttribute("data-src") || ""),
      ].filter(Boolean);
      return {
        finalUrl: location.href,
        title: text("h2") || text("h1") || meta('meta[property="og:site_name"]'),
        description: text(".info-beneficio") || meta('meta[name="description"]') ||
          meta('meta[property="og:description"]'),
        bodyText: String(document.body?.innerText || "").slice(0, 14_000),
        imageUrl: images[0] || "",
      };
    });
  } finally {
    await browser.close();
  }
}
