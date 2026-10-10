import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

describe("Instagram Stories isolated inbox in the existing Durable Object", () => {
  it("dedicated endpoints reject admin credentials and unauthenticated requests", async () => {
    for (const path of ["/ingest-instagram-story", "/instagram-story-status"]) {
      const response = await exports.default.fetch(`https://worker.test${path}`, {
        method: path.startsWith("/ingest") ? "POST" : "GET",
        headers: { Authorization: "Bearer vitest-admin-token-not-a-secret" },
      });
      expect(response.status).toBe(401);
      expect(await response.json()).toEqual({ ok: false, error: "unauthorized" });
    }
  });

  it("RPC uses separate SQL receipts and preserves deduplication without touching offers", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("instagram-rpc-isolation");
    let count = 0;
    await runInDurableObject(stub, instance => {
      instance.setMetadata("delivery_mode_override", "live");
      instance.instagramStoryInbox.readiness = () => ({ main: true, canal2: true, discord: true, beeper: true });
      instance.instagramStoryInbox.transports = Object.fromEntries(["main", "canal2", "discord", "beeper"].map(target => [target, async () => {
        count++;
        return { messageId: `${target}-receipt`, imageConfirmed: target === "beeper", imageProxyUrl: "https://media.discordapp.net/external/story.jpg" };
      }]));
    });
    const now = Date.now();
    const story = {
      storyId: "4004185955500703427",
      publishedAt: new Date(now - 60_000).toISOString(),
      expiresAt: new Date(now + 23 * 60 * 60_000).toISOString(),
      link: "https://clube.uol.com.br/campanhasdeingresso/pQg-2-ingressos-teatro-sp",
      imageUrl: "https://scontent.cdninstagram.com/story.jpg?private=signature",
      imageWidth: 1080, imageHeight: 1920,
    };
    expect((await stub.ingestInstagramStory(story)).status).toBe("delivered");
    expect((await stub.ingestInstagramStory(story)).status).toBe("delivered");
    expect(count).toBe(4);
    const status = await stub.getInstagramStoryStatus(story.storyId);
    expect(status.stories).toHaveLength(1);
    expect(JSON.stringify(status)).not.toContain("signature");
    expect(JSON.stringify(status)).not.toContain("discordapp");
    await runInDurableObject(stub, (_instance, state) => {
      expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM instagram_story_outbox").one().count).toBe(1);
      expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM offers").one().count).toBe(0);
    });
  });

  it("persisted delivery mode override controls Story RPC independently of environment mode", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("instagram-persisted-mode");
    let calls = 0;
    await runInDurableObject(stub, instance => {
      instance.setMetadata("delivery_mode_override", "shadow");
      instance.instagramStoryInbox.readiness = () => ({ main: true, canal2: true, discord: true, beeper: true });
      instance.instagramStoryInbox.transports = Object.fromEntries(["main", "canal2", "discord", "beeper"].map(target => [target, async () => {
        calls++;
        return { messageId: `${target}-receipt`, imageConfirmed: target === "beeper", imageProxyUrl: "https://media.discordapp.net/external/story.jpg" };
      }]));
    });
    const now = Date.now();
    const story = {
      storyId: "4004185955500703427", publishedAt: new Date(now - 60_000).toISOString(),
      expiresAt: new Date(now + 23 * 60 * 60_000).toISOString(),
      link: "https://clube.uol.com.br/campanhasdeingresso/pQg-ingressos-teatro",
      imageUrl: "https://scontent.cdninstagram.com/story.jpg?private=signature",
      imageWidth: 1080, imageHeight: 1920,
    };
    const held = await stub.ingestInstagramStory(story);
    expect(held.status).toBe("held");
    expect(Object.values(held.targets).every(target => target.error === "bot_not_live")).toBe(true);
    expect(calls).toBe(0);
    await runInDurableObject(stub, instance => { instance.setMetadata("delivery_mode_override", "live"); });
    expect((await stub.ingestInstagramStory(story)).status).toBe("delivered");
    expect(calls).toBe(4);
    await runInDurableObject(stub, instance => { instance.setMetadata("delivery_mode_override", "shadow"); });
    expect((await stub.ingestInstagramStory(story)).status).toBe("delivered");
    expect(calls).toBe(4);
  });

  it("Story forwards cannot confirm an ambiguous offer or become discussion work", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("instagram-forward-isolation");
    await runInDurableObject(stub, async (instance, state) => {
      const link = "https://clube.uol.com.br/campanhasdeingresso/pQg-ingressos-teatro";
      const date = new Date().toISOString();
      state.storage.sql.exec(`INSERT INTO offers(
        id,link,preview_title,first_seen_at,last_seen_at,status,decision_at,would_send_main,
        delivery_mode,delivery_generation,delivery_unknown_at,delivery_unknown_target,main_delivery_unknown_at)
        VALUES(?,?,?,?,?,'delivery_unknown',?,1,'live',1,?,'main',?)`,
      "campaign-pQg", link, "Ingresso", date, date, date, date, date);
      const result = await instance.handleTelegramUpdate({ message: {
        is_automatic_forward: true, message_id: 902, chat: { id: -100222 },
        caption: `🎟️ Story do @clubeuol\n${link}`, photo: [{ file_id: "fixture" }],
        forward_origin: { type: "channel", chat: { id: -100111 }, message_id: 702,
          date: Math.floor(Date.now()/1000) },
      } });
      expect(result.outcome).toBe("ignored_story_forward");
      const offer = state.storage.sql.exec("SELECT main_message_id,main_sent_at,main_delivery_unknown_at FROM offers WHERE id='campaign-pQg'").one();
      expect(Number(offer.main_message_id)).toBe(0);
      expect(offer.main_sent_at).toBe("");
      expect(offer.main_delivery_unknown_at).toBe(date);
      expect(state.storage.sql.exec("SELECT count(*) AS n FROM pending_discussion_forwards").one().n).toBe(0);
    });
  });
});
