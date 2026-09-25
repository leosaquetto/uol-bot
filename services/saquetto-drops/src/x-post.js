import { parseHTML } from 'linkedom';
import { structuredPost } from './x-records.js';

export function canonicalPost(value) {
  const m = String(value || '').match(/^https:\/\/(?:www\.)?(?:x\.com|twitter\.com)\/([a-zA-Z0-9_]{1,15})\/status\/(\d{10,25})(?:\?[^#]*)?$/);
  if (!m) throw new Error('invalid_post_url');
  return { author: m[1].toLowerCase(), id: m[2], url: `https://x.com/${m[1].toLowerCase()}/status/${m[2]}` };
}

export function allowedImage(value, avatar = false) {
  if (!value) return '';
  const u = new URL(value);
  if (u.protocol !== 'https:' || u.hostname !== 'pbs.twimg.com' || u.username || u.password || u.port || u.hash ||
      !(avatar ? /^\/profile_images\// : /^\/(media|amplify_video_thumb|ext_tw_video_thumb|profile_images)\//).test(u.pathname)) {
    throw new Error('image_url_not_allowed');
  }
  return u.href;
}

export function parsePost(html, target) {
  const identity = canonicalPost(target);
  const { document } = parseHTML(html);
  const record=structuredPost(document,identity);
  if(record)return {...record,imageUrl:allowedImage(record.imageUrl),avatarUrl:allowedImage(record.avatarUrl,true)};
  for (const e of document.querySelectorAll('script,style')) e.remove();
  const articles = [...document.querySelectorAll('article')];
  const firstIdentity = article => {
    for (const a of article.querySelectorAll('a[href]')) {
      // X puts a linked, absolutely positioned overlay before the post byline.
      // That link can point at a different post (for example, a reply parent).
      if (a.closest('.pointer-events-none.absolute')) continue;
      try { return canonicalPost(new URL(a.getAttribute('href'),'https://x.com').href); } catch {}
    }
    return null;
  };
  const article = articles.find(a => {
    const first = firstIdentity(a);
    return first?.id === identity.id && first.author === identity.author;
  });
  if (!article) throw new Error('post_article_missing');
  let type = 'post';
  const social = article.querySelector('[data-testid="socialContext"]')?.textContent || '';
  if (/reposted|retweet|republicou/i.test(social)) type = 'repost';
  else if (article.querySelector('[data-testid="replyingTo"]') || /(?:^|\n)\s*(?:Replying to|Respondendo a)\b/i.test(article.textContent)) type = 'reply';
  else if ([...article.querySelectorAll('a[href]')].some(a => {
    try { return canonicalPost(new URL(a.getAttribute('href'),'https://x.com').href).id !== identity.id; } catch { return false; }
  })) type = 'quote';
  // Only the author's first text node; never append a quoted author's text.
  const textNode = article.querySelector('[data-testid="tweetText"]') || article.querySelector('div[dir="auto"].whitespace-pre-wrap');
  if (!textNode) throw new Error('post_text_missing');
  const copy = textNode.cloneNode(true);
  for (const img of copy.querySelectorAll('img')) img.replaceWith(document.createTextNode(img.getAttribute('alt') || ''));
  for (const br of copy.querySelectorAll('br')) br.replaceWith(document.createTextNode('\n'));
  for (const a of copy.querySelectorAll('a[href]')) {
    const href = a.getAttribute('href');
    if (/^https?:\/\//i.test(href) && !/^https?:\/\/(?:www\.)?(?:x\.com|twitter\.com)\//i.test(href)) {
      a.replaceWith(document.createTextNode(href));
    }
  }
  const text = copy.textContent.trim();
  if (!text || article.querySelector('[data-testid="tweet-text-show-more-link"]')) throw new Error('post_text_incomplete');
  const metadata = name => document.querySelector(`meta[property="${name}"],meta[name="${name}"]`)?.getAttribute('content') || '';
  const title = metadata('og:title') || metadata('twitter:title');
  const name = title.match(new RegExp(`^(.*?)\\s*\\(@${identity.author}\\)`,'i'))?.[1]?.trim() || identity.author;
  const images = [...article.querySelectorAll('img[src]')].map(i => i.getAttribute('src'));
  const media = [metadata('og:image'),metadata('twitter:image'),...images]
    .find(u => /^https:\/\/pbs\.twimg\.com\/(media|amplify_video_thumb|ext_tw_video_thumb)\//.test(u)) || '';
  const avatar = images.find(u => /^https:\/\/pbs\.twimg\.com\/profile_images\//.test(u)) || '';
  const avatarUrl = avatar.replace(/_(?:mini|normal|bigger|reasonably_small|200x200|x96)(\.[a-z]+)(?=[?#]|$)/i,'_400x400$1');
  return { ...identity, type, name, text,
    publishedAt: new Date(Number((BigInt(identity.id) >> 22n) + 1288834974657n)).toISOString(),
    imageUrl: allowedImage(media || avatarUrl), avatarUrl: media ? allowedImage(avatarUrl,true) : '' };
}

export function formatPost(post) {
  const link = `${post.url}?s=46`;
  const title = `${post.name} (@${post.author}) no X`;
  const hour = new Date(post.publishedAt).toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit',timeZone:'America/Sao_Paulo'});
  const body = post.text.split('\n').filter(l => l.trim()).map(l => '> ```'+l+'```').join('\n');
  return { link, text: body+'\n> 𝕏 ```'+title+', '+hour+'```\n> ```'+link+'```', format:'whatsapp',
    preview:{ title,summary:'',imageUrl:post.imageUrl,...(post.avatarUrl?{avatarUrl:post.avatarUrl}:{}) } };
}

export async function readLimited(response, max = 2 * 1024 * 1024) {
  if (!response.ok) throw new Error(response.status === 429 ? 'x_rate_limited' : response.status === 404 ? 'post_unavailable' : 'download_failed');
  if (Number(response.headers.get('content-length') || 0) > max) throw new Error('response_too_large');
  const parts = []; let size = 0;
  const reader = response.body.getReader();
  try {
    while (true) {
      const {done,value} = await reader.read();
      if (done) break;
      size += value.length;
      if (size > max) throw new Error('response_too_large');
      parts.push(Buffer.from(value));
    }
  } finally { await reader.cancel().catch(() => {}); }
  return Buffer.concat(parts);
}

export async function fetchPost(target, { fetchImpl = fetch, context } = {}) {
  const identity = canonicalPost(target);
  const {url} = identity;
  let parsed;
  try {
    let response = await fetchImpl(url, {headers:{'User-Agent':'Mozilla/5.0'},redirect:'manual',signal:AbortSignal.timeout(15000)});
    if (response.status >= 300 && response.status < 400) {
      const location = response.headers.get('location');
      if (!location) throw new Error('unexpected_post_redirect');
      const next = new URL(location,url);
      const redirected = canonicalPost(next.href);
      if (redirected.id !== identity.id || redirected.author !== identity.author || next.username || next.password || next.port) {
        throw new Error('unexpected_post_redirect');
      }
      response = await fetchImpl(next.href, {headers:{'User-Agent':'Mozilla/5.0'},redirect:'error',signal:AbortSignal.timeout(15000)});
    }
    parsed = parsePost((await readLimited(response)).toString('utf8'),url);
  } catch (error) {
    if (error.message === 'x_rate_limited' || !context) throw error;
    const page = await context.newPage();
    try {
      await page.goto(url,{waitUntil:'domcontentloaded',timeout:20000});
      await page.locator('article').first().waitFor({timeout:10000});
      parsed = parsePost(await page.content(),url);
    } finally { await page.close(); }
  }
  return parsed;
}
