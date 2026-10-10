// Only structured identity identifies the source. Notification prose is never evidence.
export function classifyInstagramPush(payload) {
  const data = payload?.data;
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
  for (const name of ['url', 'uri', 'href', 'click_action']) {
    if (typeof data[name] !== 'string' || data[name].length > 4096) continue;
    try {
      const u = new URL(data[name], 'https://www.instagram.com');
      if (u.protocol !== 'https:' || !['www.instagram.com','instagram.com'].includes(u.hostname) ||
          u.username || u.password || u.port) continue;
      const match = /^\/stories\/clubeuol(?:\/([0-9]{10,25}))?\/?$/.exec(u.pathname);
      if (match) return { profile: 'clubeuol', storyId: match[1] || null, evidence: 'story_uri' };
    } catch { /* Reject an unrecognized destination. */ }
  }
  if (['story','story_notification','new_story'].includes(data.type) &&
      (data.username === 'clubeuol' || data.user?.username === 'clubeuol')) {
    const id = data.story_id ?? data.storyId;
    return { profile: 'clubeuol', storyId: typeof id === 'string' && /^[0-9]{10,25}$/.test(id) ? id : null,
      evidence: 'structured_story_owner' };
  }
  return null;
}
