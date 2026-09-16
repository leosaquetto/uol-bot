export const ACCOUNT_ID = "336445";
export const CHAT_ID = "imsg##thread:0d1d661d521ad54a15db20440c5a00c0782c639cc90ea3d1";
export const MESSAGE = "Você entrou em uma Smart Fit";
export const TOKEN_KEY = "GYMRATS_JWT";

export function localDate(now) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo", year: "numeric", month: "2-digit", day: "2-digit",
  }).format(new Date(now));
}

export async function boundedJson(response) {
  if (!response.body) throw new Error("invalid_json");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let size = 0;
  let text = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > 1_048_576) {
      await reader.cancel();
      throw new Error("response_too_large");
    }
    text += decoder.decode(value, { stream: true });
  }
  try { return JSON.parse(text + decoder.decode()); }
  catch { throw new Error("invalid_json"); }
}

export async function readWorkouts(token, now) {
  const date = localDate(now);
  const url = new URL(`https://www.gymrats.app/api/accounts/${ACCOUNT_ID}/workouts`);
  url.searchParams.set("start_date", `${date}T00:00:00-03:00`);
  url.searchParams.set("end_date", `${date}T23:59:59-03:00`);
  const response = await fetch(url, {
    headers: {
      Authorization: token,
      "rat-timezone": "America/Sao_Paulo",
      "rat-app-version": "2026.9.1",
      "rat-app-platform": "iOS",
      "User-Agent": "GymRats/2026.9.1 CFNetwork/3860.700.1 Darwin/25.6.0",
      Accept: "application/json, text/plain, */*",
    },
    redirect: "manual",
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`gymrats_http_${response.status}`);
  }
  const body = await boundedJson(response);
  const workouts = Array.isArray(body.data) ? body.data : body.data?.workouts;
  if (!Array.isArray(workouts)) throw new Error("gymrats_invalid_schema");
  const tokenNext = typeof body.data?.token === "string" ? body.data.token : null;
  return { workouts, tokenNext };
}

export function candidates(workouts, now, startedAt) {
  const unique = new Map();
  for (const workout of workouts) {
    if (!workout || typeof workout !== "object") continue;
    if (workout.academia?.brand !== "smart_fit" && !/smart\s+fit/i.test(workout.title || "")) continue;
    const id = workout.workout_entry_id;
    // A workout ID is a challenge-specific copy, never a safe fallback identity.
    if (!((typeof id === "number" && Number.isSafeInteger(id) && id > 0) ||
      (typeof id === "string" && /^[1-9]\d{0,19}$/.test(id)))) {
      throw new Error("gymrats_missing_entry_id");
    }
    const occurred = Date.parse(workout.occurred_at);
    if (!Number.isFinite(occurred)) throw new Error("gymrats_missing_occurred_at");
    if (occurred < startedAt || occurred > now || localDate(occurred) !== localDate(now)) continue;
    unique.set(String(id), { id: String(id), occurred });
  }
  return [...unique.values()].sort((a, b) => a.occurred - b.occurred || a.id.localeCompare(b.id));
}

export function beeperUrl(base, messages = false) {
  const url = new URL(base);
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) {
    throw new Error("beeper_invalid_url");
  }
  url.pathname = `${url.pathname.replace(/\/$/, "")}/v1/chats/${encodeURIComponent(CHAT_ID)}${messages ? "/messages" : ""}`;
  return url;
}

export async function checkDestination(env) {
  const response = await fetch(beeperUrl(env.BEEPER_API_URL), {
    headers: { Authorization: `Bearer ${env.BEEPER_API_TOKEN}` },
    redirect: "manual", signal: AbortSignal.timeout(8_000),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`beeper_preflight_http_${response.status}`);
  }
  const chat = await boundedJson(response);
  if (chat.id !== CHAT_ID) throw new Error("beeper_destination_mismatch");
}

export async function sendNotification(env, id) {
  const response = await fetch(beeperUrl(env.BEEPER_API_URL, true), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.BEEPER_API_TOKEN}`,
      "Content-Type": "application/json",
      "Idempotency-Key": `gymrats:${ACCOUNT_ID}:${id}`,
    },
    body: JSON.stringify({ text: MESSAGE }),
    redirect: "manual", signal: AbortSignal.timeout(8_000),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`beeper_send_http_${response.status}`);
  }
  const body = await boundedJson(response);
  if (typeof body.pendingMessageID !== "string" || !body.pendingMessageID) throw new Error("beeper_missing_receipt");
  return body.pendingMessageID;
}

export async function confirmNotification(env, pendingId) {
  const url = beeperUrl(env.BEEPER_API_URL, true);
  url.pathname += `/${encodeURIComponent(pendingId)}`;
  const response = await fetch(url, {
    headers: { Authorization: `Bearer ${env.BEEPER_API_TOKEN}` },
    redirect: "manual", signal: AbortSignal.timeout(8_000),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`beeper_receipt_http_${response.status}`);
  }
  const body = await boundedJson(response);
  if (body.chatID !== CHAT_ID || body.isSender !== true || body.text !== MESSAGE) throw new Error("beeper_receipt_mismatch");
  return body.sendStatus?.status === "SUCCESS";
}
