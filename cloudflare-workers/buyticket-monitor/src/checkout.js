import { acquire, connect } from '@cloudflare/playwright';
import { EVENTS, EVENT_SLUG, finalPriceAllowed } from './core.js';
import { parseFinalReview, extractPixTotal, extractPixCode, isPixUnavailable } from './checkout-logic.js';

export { parseBrl, extractPixTotal, extractPixCode, isPixUnavailable } from './checkout-logic.js';

const BASE = 'https://buyticketbrasil.com';
const NAVIGATION_TIMEOUT = 12_000;
const WAITING_ROOM_TIMEOUT = 270_000;

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
  const email = page.getByPlaceholder('Seu email', { exact: true });
  const password = page.getByPlaceholder('Sua senha', { exact: true });
  await email.waitFor({ state: 'visible', timeout: NAVIGATION_TIMEOUT });
  await password.waitFor({ state: 'visible', timeout: NAVIGATION_TIMEOUT });
  await email.fill(env.BUYTICKET_USERNAME);
  await password.fill(env.BUYTICKET_PASSWORD);
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

async function clickCurrentButton(page, label) {
  const buttons = page.getByRole('button', { name: label, exact: true });
  for (let index = await buttons.count() - 1; index >= 0; index--) {
    const button = buttons.nth(index);
    if (await button.isVisible().catch(() => false) && await button.isEnabled().catch(() => false)) {
      await button.click();
      return;
    }
  }
  throw new Error('checkout_button_missing');
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

function eventDateVisible(text, day) {
  if (text.includes(day)) return true;
  const [date, month] = day.split('/').map(Number);
  const months = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'];
  return new RegExp(`\\b${date}\\b[\\s\\S]{0,60}\\b${months[month - 1]}\\b`, 'i').test(text);
}

export async function runCheckout(env, candidate, { dryRun = false, beforeCommit = async () => {}, sessionId = null, onSession = async () => {} } = {}) {
  let browser;
  let page;
  try {
    if (sessionId) browser = await connect(env.BROWSER, sessionId).catch(() => null);
    if (!browser) {
      const acquired = await acquire(env.BROWSER, { keep_alive: 600_000 });
      await onSession(acquired.sessionId);
      browser = await connect(env.BROWSER, acquired.sessionId);
    }
  } catch (error) {
    const message = String(error?.message || '').toLowerCase();
    return {
      status: message.includes('429') || message.includes('rate limit') ? 'browser_rate_limited' : 'browser_unavailable',
      stage: 'browser_started',
      finalActionClicked: false,
      noOrderCreated: true,
    };
  }
  let committed = false;
  let stage = 'browser_started';
  const done = result => ({ ...result, stage });
  try {
    stage = 'context';
    const context = browser.contexts()[0] || await browser.newContext({ locale: 'pt-BR', timezoneId: 'America/Sao_Paulo' });
    page = context.pages()[0] || await context.newPage();
    page.setDefaultTimeout(8_000);
    stage = 'login';
    const buyerName = await ensureLogin(page, env);
    const direct = `${BASE}/r?event=${encodeURIComponent(EVENT_SLUG)}&c_anuncio=${encodeURIComponent(candidate.idRef)}`;
    stage = 'listing';
    await page.goto(direct, { waitUntil: 'domcontentloaded', timeout: NAVIGATION_TIMEOUT });
    const expectedDate = EVENTS[candidate.dayIndex].day;
    const waitingRoom = /Todo mundo quer nossos ingressos/i.test(await page.title());
    await page.getByRole('button', { name: /Comprar agora por/ }).waitFor({ state: 'visible', timeout: waitingRoom ? WAITING_ROOM_TIMEOUT : NAVIGATION_TIMEOUT });
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
    if (!eventDateVisible(text, expectedDate)) return done({ status: 'listing_mismatch' });
    if (isPixUnavailable(text) || !await pix.isEnabled().catch(() => false)) {
      return done({ status: 'pix_unavailable' });
    }
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
    const withinBounds = finalPriceAllowed(finalPrice);
    if (!await pix.isEnabled().catch(() => false)) return done({ status: 'pix_unavailable', finalPrice });

    stage = 'payment_select';
    const pixSelected = await page.evaluate(() => {
      const input = [...document.querySelectorAll('input[type="radio"]')].find(item => {
        const container = item.closest('label') || item.parentElement;
        return /\bPIX\b/i.test(container?.textContent || '');
      });
      if (!input || input.disabled) return false;
      input.click();
      return input.checked;
    });
    if (!pixSelected) return done({ status: 'pix_unavailable', finalPrice });
    stage = 'payment_continue';
    await clickCurrentButton(page, 'Continuar');
    stage = 'quentro_email';
    await fillByNames(page, [/e-?mail.*Quentro/i, /e-?mail/i], env.BUYTICKET_QUENTRO_EMAIL);
    stage = 'quentro_continue';
    await clickCurrentButton(page, 'Continuar');
    stage = 'billing_name';
    await fillByNames(page, [/nome completo/i, /^nome$/i], buyerName);
    stage = 'billing_phone';
    await fillMasked(page, 'Telefone celular', env.BUYTICKET_PHONE);
    stage = 'billing_tax_id';
    await fillMasked(page, 'CPF/CNPJ', env.BUYTICKET_CPF);
    stage = 'billing_postal_code';
    await fillMasked(page, 'CEP (Código postal)', env.BUYTICKET_CEP);
    const addressReady = () => page.waitForFunction(() => ['Estado (UF)', 'Bairro', 'Município'].every(placeholder =>
      [...document.querySelectorAll('input')].some(input => input.placeholder === placeholder && input.value.trim())),
      undefined, { timeout: 4_000 }).then(() => true).catch(() => false);
    let autoAddress = await addressReady();
    if (!autoAddress) {
      await page.getByPlaceholder('CEP (Código postal)', { exact: true }).press('Tab').catch(() => {});
      autoAddress = await addressReady();
    }
    if (!autoAddress) throw new Error('billing_address_autofill_timeout');
    stage = 'billing_address';
    await fillByNames(page, [/endere[cç]o|logradouro/i], env.BUYTICKET_ADDRESS);
    stage = 'billing_number';
    await fillByNames(page, [/^N° do endereço$/i], env.BUYTICKET_ADDRESS_NUMBER);
    stage = 'billing_continue';
    await clickCurrentButton(page, 'Continuar');
    stage = 'final_review';
    await page.getByText(/Resumo (?:da compra|do pedido)/i).first().waitFor({ state: 'visible', timeout: NAVIGATION_TIMEOUT });
    const finalButton = page.getByRole('button', { name: /^(?:Comprar agora|Finalizar compra)$/i }).first();
    await finalButton.waitFor({ state: 'visible', timeout: NAVIGATION_TIMEOUT });
    const review = parseFinalReview(await pageText(page));
    if (!review || review.total !== finalPrice) {
      return done({ status: 'price_changed' });
    }
    if (dryRun) return done({ status: withinBounds ? 'ready' : 'outside_range', finalPrice, formReady: true, review, finalActionClicked: false });

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
    const diagnostic = dryRun && page ? await page.evaluate(() => ({
      title: document.title.slice(0, 120),
      loginVisible: Boolean(document.querySelector('input[type="password"]')),
      soldOutVisible: /esgotad|indispon[ií]vel|an[uú]ncio.*(?:removido|encerrado)/i.test(document.body?.innerText || ''),
      relevantButtons: [...document.querySelectorAll('button')].map(button => button.textContent?.trim())
        .filter(text => text && /comprar|continuar|finalizar|entrar|tentar/i.test(text)).slice(0, 8),
      fields: [...document.querySelectorAll('input')].map(input => ({
        type: input.type,
        name: input.name || null,
        placeholder: input.placeholder || null,
        ariaLabel: input.getAttribute('aria-label'),
      })).filter(field => field.type !== 'hidden').slice(0, 12),
    })).catch(() => null) : null;
    return done({
      status: known.includes(error?.message) ? error.message : 'checkout_failed',
      ...(diagnostic ? { diagnostic: { ...diagnostic, errorKind: error?.name || 'Error' } } : {}),
    });
  } finally {
    await browser.close().catch(() => {});
  }
}
