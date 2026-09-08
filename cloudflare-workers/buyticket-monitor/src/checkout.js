import { launch } from '@cloudflare/playwright';
import { EVENTS, finalPriceAllowed } from './core.js';
import { parseFinalReview, extractPixTotal, extractPixCode } from './checkout-logic.js';

export { parseBrl, extractPixTotal, extractPixCode } from './checkout-logic.js';

const BASE = 'https://buyticketbrasil.com';
const NAVIGATION_TIMEOUT = 12_000;

async function pageText(page) {
  return page.locator('body').innerText({ timeout: 5_000 });
}

async function readAccountName(page) {
  const response = await page.goto(`${BASE}/api/me`, { waitUntil: 'domcontentloaded', timeout: NAVIGATION_TIMEOUT });
  if (response?.status() !== 200) return null;
  const account = await response.json().catch(() => null);
  return typeof account?.name === 'string' && account.name.trim() ? account.name.trim() : null;
}

async function ensureLogin(page, env) {
  let name = await readAccountName(page).catch(() => null);
  if (name) return name;
  await page.goto(`${BASE}/entrar`, { waitUntil: 'domcontentloaded', timeout: NAVIGATION_TIMEOUT });
  await page.getByRole('textbox', { name: 'Seu email' }).fill(env.BUYTICKET_USERNAME);
  await page.getByRole('textbox', { name: 'Sua senha' }).fill(env.BUYTICKET_PASSWORD);
  await page.getByText('Entrar na conta', { exact: true }).click();
  await page.waitForURL(url => !url.pathname.startsWith('/entrar'), { timeout: NAVIGATION_TIMEOUT });
  name = await readAccountName(page).catch(() => null);
  if (!name) throw new Error('login_failed');
  return name;
}

async function dismissSafetyNotice(page) {
  const buttons = page.getByRole('button', { name: 'Continuar' });
  const notice = page.getByText(/plataforma de revenda/i).last();
  if (await buttons.count() > 1 || await notice.isVisible().catch(() => false)) {
    await buttons.last().click({ force: true });
    await page.waitForTimeout(250);
  }
}

async function fillByNames(page, names, value) {
  for (const name of names) {
    const field = page.getByRole('textbox', { name }).first();
    if (await field.waitFor({ state: 'visible', timeout: 4_000 }).then(() => true).catch(() => false)) {
      await field.fill(value);
      return;
    }
  }
  throw new Error('checkout_field_missing');
}

async function fillMasked(page, placeholder, value) {
  const digits = value.replace(/\D/g, '');
  const field = page.getByPlaceholder(placeholder, { exact: true });
  await field.fill('');
  await field.pressSequentially(digits);
  await field.press('Tab');
  const actual = await field.inputValue();
  if (actual.replace(/\D/g, '') !== digits) throw new Error('billing_value_not_accepted');
}

async function clickFirstButton(page, label) {
  const clicked = await page.evaluate(text => {
    const button = [...document.querySelectorAll('button')].find(item => {
      const rect = item.getBoundingClientRect();
      const style = getComputedStyle(item);
      return item.textContent?.trim() === text && !item.disabled && rect.width > 0 && rect.height > 0 &&
        style.visibility !== 'hidden' && style.display !== 'none';
    });
    button?.click();
    return Boolean(button);
  }, label);
  if (!clicked) throw new Error('checkout_button_missing');
}

async function findPixCodeInPage(page) {
  const candidates = await page.locator('input, textarea, [data-pix-code], code, pre').evaluateAll(nodes =>
    nodes.flatMap(node => [node.value, node.textContent, node.getAttribute('data-pix-code')]).filter(Boolean));
  let code = extractPixCode(candidates);
  if (code) return code;
  const copyButton = page.getByRole('button', { name: /copiar.*pix|pix.*copiar|copia e cola/i }).first();
  if (await copyButton.isVisible().catch(() => false)) {
    await copyButton.click();
    code = extractPixCode(await page.evaluate(() => navigator.clipboard.readText()).catch(() => ''));
  }
  return code || extractPixCode(await pageText(page));
}

export async function runCheckout(env, candidate, { dryRun = false, beforeCommit = async () => {} } = {}) {
  const browser = await launch(env.BROWSER);
  let committed = false;
  let stage = 'browser_started';
  const done = result => ({ ...result, stage });
  try {
    stage = 'context';
    const context = await browser.newContext({ locale: 'pt-BR', timezoneId: 'America/Sao_Paulo' });
    const page = await context.newPage();
    page.setDefaultTimeout(8_000);
    stage = 'login';
    const buyerName = await ensureLogin(page, env);
    const direct = `${BASE}/r?event=rockinrio2026&c_anuncio=${encodeURIComponent(candidate.idRef)}`;
    stage = 'listing';
    await page.goto(direct, { waitUntil: 'domcontentloaded', timeout: NAVIGATION_TIMEOUT });
    const expectedDate = EVENTS[candidate.dayIndex].day;
    await page.getByRole('button', { name: /Comprar agora por/ }).waitFor({ state: 'visible', timeout: NAVIGATION_TIMEOUT });
    let text = await pageText(page);
    if (!text.includes(candidate.sector) || !text.includes(candidate.category)) {
      return done({ status: 'listing_mismatch' });
    }
    if (!/\(1x ingresso\)/i.test(text)) return done({ status: 'quantity_mismatch' });
    stage = 'checkout';
    await page.getByRole('button', { name: /Comprar agora por/ }).click();
    await page.getByRole('textbox', { name: 'Código do Cupom' }).waitFor({ state: 'visible', timeout: NAVIGATION_TIMEOUT });
    await dismissSafetyNotice(page);
    const pix = page.getByRole('radio', { name: 'PIX' });
    await pix.waitFor({ state: 'visible', timeout: NAVIGATION_TIMEOUT });
    await page.waitForFunction(() => {
      const radio = document.querySelector('input[type="radio"]');
      return radio && !radio.disabled && /PIX[\s\S]{0,180}?R\$\s*[\d.]+,\d{2}/i.test(document.body?.innerText || '');
    }, undefined, { timeout: 5_000 }).catch(() => {});
    text = await pageText(page);
    if (!text.includes(expectedDate)) return done({ status: 'listing_mismatch' });
    const listedTotal = extractPixTotal(text);
    if (!Number.isSafeInteger(listedTotal)) return done({ status: 'price_unavailable' });
    stage = 'coupon_fill';
    await page.getByRole('textbox', { name: 'Código do Cupom' }).fill(env.BUYTICKET_COUPON);
    stage = 'coupon_apply';
    const applyCoupon = page.getByRole('button', { name: 'Aplicar cupom' });
    await applyCoupon.waitFor({ state: 'visible', timeout: 5_000 });
    if (!await applyCoupon.isEnabled()) return done({ status: 'coupon_unavailable' });
    const couponClicked = await page.evaluate(() => {
      const button = [...document.querySelectorAll('button')]
        .find(item => item.textContent?.trim() === 'Aplicar cupom');
      button?.click();
      return Boolean(button);
    });
    if (!couponClicked) return done({ status: 'coupon_unavailable' });
    stage = 'coupon_wait';
    const couponChanged = await page.waitForFunction(previous => {
      const text = document.body?.innerText || '';
      const start = text.search(/\bPIX\b/i);
      if (start < 0) return false;
      const section = text.slice(start, start + 300).split(/Cart[aã]o de cr[eé]dito/i, 1)[0];
      return [...section.matchAll(/R\$\s*([\d.]+,\d{2})/gi)].some(match =>
        Math.round(Number(match[1].replaceAll('.', '').replace(',', '.')) * 100) < previous);
    }, listedTotal, { timeout: 4_000 }).then(() => true).catch(() => false);
    if (!couponChanged) {
      await page.getByRole('textbox', { name: 'Código do Cupom' }).press('Enter').catch(() => {});
      await page.waitForTimeout(1_500);
    }
    stage = 'coupon_read';
    text = await pageText(page);
    const finalPrice = extractPixTotal(text);
    if (!Number.isSafeInteger(finalPrice) || finalPrice >= listedTotal) {
      const couponSignal = /cupom[^\n]{0,80}(inv[aá]lid|expir|n[aã]o.*aplic|erro)/i.test(text) ? 'rejected' :
        /cupom[^\n]{0,80}(aplic|sucesso)|desconto/i.test(text) ? 'reported_applied' : 'no_feedback';
      return done({ status: 'coupon_not_applied', finalPrice, listedTotal, couponSignal });
    }
    const withinBounds = finalPriceAllowed(candidate.dayIndex, finalPrice);
    if (!withinBounds && !dryRun) return done({ status: 'outside_range', finalPrice });
    if (!await pix.isEnabled().catch(() => false)) return done({ status: 'pix_unavailable', finalPrice });

    stage = 'payment_select';
    const pixSelected = await page.evaluate(() => {
      const radios = [...document.querySelectorAll('input[type="radio"]')];
      const input = radios.find(item => item.closest('label, div')?.textContent?.includes('PIX')) || radios[0];
      if (!input || input.disabled) return false;
      input.click();
      if (!input.checked) {
        input.checked = true;
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
      }
      return input.checked;
    });
    if (!pixSelected) return done({ status: 'pix_unavailable', finalPrice });
    stage = 'payment_continue';
    await clickFirstButton(page, 'Continuar');
    stage = 'quentro_email';
    await fillByNames(page, [/e-?mail.*Quentro/i, /e-?mail/i], env.BUYTICKET_QUENTRO_EMAIL);
    stage = 'quentro_continue';
    await clickFirstButton(page, 'Continuar');
    stage = 'billing_name';
    await fillByNames(page, [/nome completo/i, /^nome$/i], buyerName);
    stage = 'billing_phone';
    await fillMasked(page, 'Telefone celular', env.BUYTICKET_PHONE);
    stage = 'billing_tax_id';
    await fillMasked(page, 'CPF/CNPJ', env.BUYTICKET_CPF);
    stage = 'billing_postal_code';
    await fillMasked(page, 'CEP (Código postal)', env.BUYTICKET_CEP);
    await page.waitForFunction(() => ['Estado (UF)', 'Bairro', 'Município'].every(placeholder =>
      [...document.querySelectorAll('input')].some(input => input.placeholder === placeholder && input.value.trim())),
      undefined, { timeout: 8_000 });
    stage = 'billing_address';
    await fillByNames(page, [/endere[cç]o|logradouro/i], env.BUYTICKET_ADDRESS);
    stage = 'billing_number';
    await fillByNames(page, [/^N° do endereço$/i], env.BUYTICKET_ADDRESS_NUMBER);
    stage = 'billing_continue';
    await clickFirstButton(page, 'Continuar');
    stage = 'final_review';
    await page.getByText('Resumo da compra', { exact: true }).waitFor({ state: 'visible', timeout: NAVIGATION_TIMEOUT });
    const finalButton = page.getByRole('button', { name: 'Comprar agora', exact: true });
    await finalButton.waitFor({ state: 'visible', timeout: NAVIGATION_TIMEOUT });
    const review = parseFinalReview(await pageText(page));
    if (!review || review.total !== finalPrice) {
      return done({ status: 'price_changed' });
    }
    if (dryRun) return done({ status: withinBounds ? 'ready' : 'outside_range', finalPrice, formReady: true, review, finalActionClicked: false });
    if (!withinBounds) return done({ status: 'outside_range', finalPrice });

    let responsePixCode = null;
    page.on('response', response => {
      if (response.request().method() !== 'POST' || !response.url().includes('/workflow/start')) return;
      response.text().then(body => { responsePixCode ||= extractPixCode(body); }).catch(() => {});
    });
    stage = 'commit';
    await beforeCommit({ finalPrice });
    committed = true;
    await finalButton.click();
    const deadline = Date.now() + 15_000;
    let pixCode;
    while (Date.now() < deadline && !(pixCode = responsePixCode || await findPixCodeInPage(page).catch(() => null))) {
      await page.waitForTimeout(300);
    }
    if (!pixCode) throw new Error('pix_response_unknown');
    return done({ status: 'pix_created', finalPrice, pixCode });
  } catch (error) {
    if (committed) throw Object.assign(new Error('purchase_outcome_unknown'), { cause: error });
    const known = ['login_failed', 'checkout_field_missing', 'billing_value_not_accepted'];
    return done({ status: known.includes(error?.message) ? error.message : 'checkout_failed' });
  } finally {
    await browser.close().catch(() => {});
  }
}
