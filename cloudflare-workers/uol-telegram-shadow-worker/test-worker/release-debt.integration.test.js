import { env, exports } from 'cloudflare:workers';
import { runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';

describe('private release debt identities', () => {
  it('rejects unauthenticated access', async () => {
    const response = await exports.default.fetch('https://worker.test/release-debt');
    expect(response.status).toBe(401);
  });
  it('includes old debt, granular maintenance targets and all WhatsApp rows without writes', async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName('release-debt-complete'), (instance, state) => {
      state.storage.sql.exec(`INSERT INTO offers(id, link, preview_title, first_seen_at, last_seen_at, status,
        delivery_unknown_at, delivery_unknown_target, discord_delivery_unknown_at)
        VALUES ('old','https://clube.uol.com.br/old','PRIVATE TITLE','2026-01-01','','delivery_unknown','2026-01-02','discord','2026-01-02')`);
      state.storage.sql.exec(`INSERT INTO offers(id,link,preview_title,first_seen_at,last_seen_at,status,sold_out_at,
        would_send_canal2,canal2_message_id,main_sold_out_attempts,canal2_sold_out_attempts)
        VALUES ('sold','https://clube.uol.com.br/sold','','','','sold_out','2026-10-01',1,22,10,10)`);
      for (let i=0;i<513;i++) state.storage.sql.exec(
        "INSERT INTO beeper_delivery_queue(offer_id,attempts,last_error) VALUES (?,10,'ambiguous:timeout')", `wa-${i}`);
      state.storage.sql.exec(`INSERT INTO incidents(key,status,severity,summary,first_detected_at,last_detected_at)
        VALUES ('delivery:test','active','critical','PRIVATE SUMMARY','2026-10-01','2026-10-02')`);
      instance.setMetadata('ops_ntfy_episode:delivery:test', 'first-episode');
      const before=state.storage.sql.exec('SELECT total_changes() AS n').one().n;
      const result=instance.getReleaseDebt();
      expect(state.storage.sql.exec('SELECT total_changes() AS n').one().n).toBe(before);
      expect(result.checks).toEqual({criticalIncidents:1,deadLetters:513,unknown:514,maintenanceDeadLetters:1});
      expect(result.identities.maintenanceDeadLetters).toHaveLength(2);
      expect(result.identities.deadLetters).toHaveLength(513);
      expect(result.identities.unknown.some(id=>id.includes('old'))).toBe(true);
      expect(JSON.stringify(result)).not.toContain('PRIVATE');
      expect(instance.getReleaseDebt().identities).toEqual(result.identities);
      instance.setMetadata('ops_ntfy_episode:delivery:test', 'second-episode');
      expect(instance.getReleaseDebt().identities.criticalIncidents).not.toEqual(result.identities.criticalIncidents);
    });
  });
});
