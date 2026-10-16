// Only structured identity identifies the source. Notification prose is never evidence.
function classifyStoryOwner(payload, profile) {
  // Instagram's native Web Push uses root type/params; retain the wrapped contract.
  const data = payload?.data ?? payload;
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
  const native = data === payload;
  const type = data.type || data.params?.collapse_key;
  if (typeof type === 'string' && (type.startsWith('direct_v2') || ['dm','direct','direct_message'].includes(type))) return null;
  const urls = ['url', 'uri', 'href', 'click_action'].map(name=>data[name]);
  // The official service worker interprets params.ig as a URL only for post.
  if (native && type === 'post') urls.push(data.params?.ig);
  for (const url of urls) {
    if (typeof url !== 'string' || url.length > 4096) continue;
    try {
      const u = new URL(url, 'https://www.instagram.com');
      if (u.protocol !== 'https:' || !['www.instagram.com','instagram.com'].includes(u.hostname) ||
          u.username || u.password || u.port) continue;
      const match = /^\/stories\/([a-zA-Z0-9._]+)(?:\/([0-9]{10,25}))?\/?$/.exec(u.pathname);
      if (match && match[1] === profile) return { profile, storyId: match[2] || null, evidence: 'story_uri' };
    } catch { /* Reject an unrecognized destination. */ }
  }
  if (['story','story_notification','new_story'].includes(data.type) &&
      (data.username === profile || data.user?.username === profile)) {
    const id = data.story_id ?? data.storyId;
    return { profile, storyId: typeof id === 'string' && /^[0-9]{10,25}$/.test(id) ? id : null,
      evidence: 'structured_story_owner' };
  }
  return null;
}

// Production collection remains restricted to the UOL source.
export function classifyInstagramPush(payload) {
  return classifyStoryOwner(payload, 'clubeuol');
}

// Test pushes are persisted for diagnostics; production consumers filter profile=clubeuol.
export function classifyInstagramDiagnosticPush(payload) {
  return classifyStoryOwner(payload, 'leotheblueblur');
}
