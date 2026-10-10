import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest

from monitor import Monitor, MAX_DAILY_REQUESTS, ticket_link


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.now = 1791585600
        self.m = Monitor(self.path, {}, clock=lambda: self.now)
        (self.path/'session.json').write_text(json.dumps({'user_agent':'fixture-agent'}))
        self.addCleanup(self.m.db.close)

    def story(self, ticket=True):
        iso = lambda offset: dt.datetime.fromtimestamp(self.now+offset,dt.timezone.utc).isoformat()
        return {'storyId':'4004391581161830448','publishedAt':iso(-60),'expiresAt':iso(3600),
                'destinations':['https://clube.uol.com.br/campanhasdeingresso/pQg-teatro' if ticket else 'https://clube.uol.com.br/fotoregistro/pO8-foto'],
                'imageUrl':'https://instagram.example.fbcdn.net/v/t.jpg?sig=fake', 'imageWidth':828,'imageHeight':1472,
                'imageBase64':'/9j/4A==','imageMime':'image/jpeg'}

    def result(self, story=None):
        return {'checkedAt':'2026-10-09T19:39:26+00:00','status':'found','requests':2,
                'stories':[story or self.story()], 'reason':'','duration_ms':900,'body_bytes':100}

    def media(self, p, ua):
        return {'imageBase64':'/9j/4A==','imageMime':'image/jpeg'}

    def test_only_campaigns_queued_and_current_story_eligible(self):
        self.m.record(self.result(self.story(False)))
        self.assertEqual(self.m.db.execute('SELECT count(*) FROM outbox').fetchone()[0],0)
        self.m.record(self.result())
        self.assertEqual(self.m.db.execute('SELECT count(*) FROM outbox').fetchone()[0],1)

    def test_restarts_and_refreshed_image_never_repeat_delivered(self):
        self.m.record(self.result())
        calls=[]
        def send(c,p):
            calls.append(p)
            return {'ok':True,'status':'delivered','targets':{n:{'status':'confirmed'} for n in ('main','canal2','discord','beeper')}}
        self.m.flush(send,self.media)
        other=Monitor(self.path,{},clock=lambda:self.now)
        self.addCleanup(other.db.close)
        story=self.story();story['imageUrl']+='2'
        other.record(self.result(story));other.flush(send,self.media)
        self.assertEqual(len(calls),1)

    def test_unknown_send_not_repeated_but_other_destinations_can_retry(self):
        self.m.record(self.result())
        calls=[]
        def unknown(c,p):
            calls.append(p)
            return {'ok':True,'status':'unknown','targets':{'main':{'status':'unknown'}}}
        self.m.flush(unknown,self.media)
        self.now+=3600
        self.m.flush(unknown,self.media)
        self.assertEqual(len(calls),1)

    def test_missing_image_is_held_and_expiration_not_sent(self):
        story=self.story();story['imageUrl']=''
        self.m.record(self.result(story))
        calls=[]
        self.m.flush(lambda c,p:calls.append(p))
        self.assertEqual(calls,[])
        self.now+=3601;self.m.flush(lambda c,p:calls.append(p))
        self.assertEqual(self.m.state['outbox'],{'expired':1})

    def test_poll_crash_keeps_interval_and_rolling_budget(self):
        self.assertTrue(self.m.reserve_poll())
        restart=Monitor(self.path,{},clock=lambda:self.now)
        self.addCleanup(restart.db.close)
        self.assertFalse(restart.reserve_poll())
        self.now+=121
        restart.db.execute('INSERT INTO polls VALUES(?,?,NULL)',(self.now,MAX_DAILY_REQUESTS))
        restart.db.commit()
        self.assertFalse(restart.reserve_poll())

    def test_non_campaign_and_unsafe_urls_rejected(self):
        for bad in ('https://clube.uol.com.br/campanhasdeingresso/pQg-teatro/utilizar',
                    'https://clube.uol.com.br:443/campanhasdeingresso/pQg-teatro',
                    'https://clube.uol.com.br/campanhasdeingresso/pQg-teatro?token=fake',
                    'https://clube.uol.com.br.evil.test/campanhasdeingresso/pQg-teatro'):
            self.assertEqual(ticket_link(bad),'')

    def test_health_failure_and_recovery_published_without_secrets(self):
        self.m.record(self.result())
        sent=[]
        def report(c,p):
            sent.append(p)
            return True
        self.m.publish_health(report);self.m.publish_health(report)
        self.assertEqual(len(sent),1)
        failed=self.result();failed.update(status='auth_required',reason='login_payload',stories=[])
        self.m.record(failed);self.m.publish_health(report)
        self.assertEqual(sent[-1]['sourceStatus'],'auth_required')
        self.assertEqual(set(sent[-1]),{'sourceStatus','observedAt','lastSuccessAt','reason'})
        self.m.record(self.result());self.m.publish_health(report)
        self.assertEqual(len(sent),3)

    def test_media_is_fetched_once_preserved_during_poll_and_removed_after_delivery(self):
        story=self.story();story.pop('imageBase64');story.pop('imageMime')
        self.m.record(self.result(story))
        fetched=[]
        def media(p,ua):
            fetched.append((p['storyId'],ua))
            return {'imageBase64':'/9j/4A==','imageMime':'image/jpeg'}
        self.m.flush(lambda c,p:{'ok':True,'status':'partial'},media)
        self.m.record(self.result(story))
        self.now+=500
        self.m.flush(lambda c,p:{'ok':True,'status':'delivered'},media)
        self.assertEqual(fetched,[(story['storyId'],'fixture-agent')])
        stored=json.loads(self.m.db.execute('SELECT payload FROM outbox').fetchone()[0])
        self.assertNotIn('imageBase64',stored)
        self.assertEqual(self.m.state['mediaRequests'],1)


if __name__ == '__main__':
    unittest.main()
