function rawAuthorizationToken(value) {
  return String(value || "").trim().replace(/^bearer\s+/i, "");
}

function jwtPayload(value) {
  const parts = rawAuthorizationToken(value).split(".");
  if (parts.length !== 3) return null;
  try {
    const normalized = parts[1].replaceAll("-", "+").replaceAll("_", "/");
    const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
    return JSON.parse(atob(padded));
  } catch {
    return null;
  }
}

export function authorizationExpiresAt(value) {
  const expiresAt = Number(jwtPayload(value)?.exp || 0) * 1_000;
  return Number.isFinite(expiresAt) && expiresAt > 0
    ? new Date(expiresAt).toISOString()
    : "";
}

export function authorizationDiagnostics(value) {
  const raw = rawAuthorizationToken(value);
  const jwt = raw.split(".").length === 3;
  const payload = jwt ? jwtPayload(raw) : null;
  return {
    present: Boolean(raw),
    format: jwt ? payload ? "jwt" : "jwt_invalid" : raw ? "opaque" : "missing",
    lengthBucket: raw ? Math.ceil(raw.length / 100) * 100 : 0,
    expiresAt: authorizationExpiresAt(raw),
    claimNames: payload ? Object.keys(payload).sort().slice(0, 24) : [],
  };
}
