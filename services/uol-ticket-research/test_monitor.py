import datetime as dt
from contextlib import closing
import json
import os
from pathlib import Path
import tempfile
import sqlite3
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import monitor as monitor_module

from monitor import Monitor, MAX_DAILY_REQUESTS, SAFETY_PERIOD, StorageUsageWorker, run, ticket_link


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

    def push(self, sequence, status='connected', story_id='default'):
        path=self.path/'push.sqlite'
        with closing(sqlite3.connect(path)) as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS signals(seq INTEGER PRIMARY KEY, profile TEXT,story_id TEXT,received_at INTEGER);
                CREATE TABLE IF NOT EXISTS receiver_state(id INTEGER PRIMARY KEY,status TEXT,observed_at INTEGER);''')
            db.execute('INSERT OR IGNORE INTO signals VALUES(?,?,?,?)',
                       (sequence,'clubeuol',self.story()['storyId'] if story_id == 'default' else story_id,self.now*1000))
            db.execute('INSERT OR REPLACE INTO receiver_state VALUES(1,?,?)',(status,self.now*1000))
            db.commit()
        os.chmod(path,0o600)

    def proof(self):
        return {'signalSequence':1,'storyId':self.story()['storyId'],'browserClosed':True,
                'observedAt':self.result()['checkedAt']}

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
        self.assertEqual(set(sent[-1]),{'sourceStatus','observedAt','lastSuccessAt','reason','coverage','pushReceiver'})
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

    def test_event_mode_requires_server_signal_story_snapshot_and_browser_closed_proof(self):
        self.m.config.update(pushMode='event',pushProof=self.proof())
        self.m.record(self.result())
        self.assertEqual(self.m.state['activePollPeriodSeconds'],120)
        self.push(1)
        self.m.config['pushProof']['browserClosed']=False
        self.m.record(self.result())
        self.assertEqual(self.m.state['activePollPeriodSeconds'],120)
        self.m.config['pushProof']['browserClosed']=True
        self.assertTrue(self.m.reserve_poll())
        self.m.record(self.result())
        self.assertEqual(self.m.state['activePollPeriodSeconds'],SAFETY_PERIOD)
        self.assertEqual(self.m.state['consumedSignalSequence'],1)

    def test_signal_received_during_collection_survives_snapshot_and_restart(self):
        self.m.config.update(pushMode='pilot')
        self.push(1)
        self.assertTrue(self.m.reserve_poll())
        self.push(2)
        self.m.record(self.result())
        self.assertEqual(self.m.state['consumedSignalSequence'],1)
        restart=Monitor(self.path,self.m.config,clock=lambda:self.now)
        self.addCleanup(restart.db.close)
        self.now+=16
        self.assertTrue(restart.reserve_poll())
        restart.record(self.result())
        self.assertEqual(restart.state['consumedSignalSequence'],2)
        self.assertFalse(restart.reserve_poll())

    def test_pending_signal_never_bypasses_authentication_backoff_or_request_budget(self):
        self.m.config['pushMode']='pilot'
        self.push(1)
        self.assertTrue(self.m.reserve_poll())
        failed=self.result();failed.update(status='auth_required',reason='login_payload',stories=[])
        self.m.record(failed)
        self.now+=16;self.push(2)
        self.assertFalse(self.m.reserve_poll())
        self.assertEqual(self.m.state.get('consumedSignalSequence',0),0)
        self.now+=6*3600
        self.m.db.execute('INSERT INTO polls VALUES(?,?,NULL)',(self.now,MAX_DAILY_REQUESTS));self.m.db.commit()
        self.assertFalse(self.m.reserve_poll())
        self.assertEqual(self.m.state['sourceStatus'],'budget_wait')

    def test_disconnected_receiver_preserves_thirty_minute_safety_without_erasing_proof(self):
        self.m.config.update(pushMode='event',pushProof=self.proof())
        self.push(1);self.m.reserve_poll();self.m.record(self.result())
        self.assertEqual(self.m.state['activePollPeriodSeconds'],1800)
        self.now+=121;self.push(1,status='disconnected')
        self.assertFalse(self.m.reserve_poll())
        self.now+=1800
        self.assertTrue(self.m.reserve_poll())
        self.m.record(self.result())
        self.assertEqual(self.m.state['activePollPeriodSeconds'],1800)
        self.assertEqual(self.m.state['consumedSignalSequence'],1)

    def test_signal_reservation_crash_preserves_high_water_and_minimum_interval(self):
        self.m.config['pushMode']='pilot';self.push(1)
        self.assertTrue(self.m.reserve_poll())
        restart=Monitor(self.path,self.m.config,clock=lambda:self.now)
        self.addCleanup(restart.db.close)
        self.assertFalse(restart.reserve_poll())
        self.now+=16
        self.assertTrue(restart.reserve_poll())
        self.assertEqual(restart.state['reservedSignalSequence'],1)

    def test_upgrading_old_authentication_state_preserves_existing_backoff(self):
        self.m.state.update(sourceStatus='auth_required',nextPoll=self.now+6*3600)
        self.m.state.pop('retryNotBefore',None);self.m.save();self.push(1)
        restart=Monitor(self.path,{'pushMode':'pilot'},clock=lambda:self.now)
        self.addCleanup(restart.db.close)
        self.assertFalse(restart.reserve_poll())

    def baseline(self):
        self.m.config['pushMode'] = 'pilot'
        self.push(1)
        empty = self.result();empty.update(status='empty', stories=[])
        self.m.record(empty)

    def coverage(self, story_id=None):
        row = self.m.db.execute('SELECT value FROM story_coverage WHERE story_id=?',
                                (story_id or self.story()['storyId'],)).fetchone()
        return json.loads(row[0])

    def test_idle_ticks_do_not_commit_state_or_rewrite_status(self):
        self.m.reserve_poll()
        before = self.m.db.total_changes
        with patch('monitor.atomic_json', wraps=monitor_module.atomic_json) as write:
            for _ in range(10):
                self.assertFalse(self.m.reserve_poll())
                self.m.flush()
            self.assertEqual(write.call_count, 0)
        self.assertEqual(self.m.db.total_changes, before)

    def test_unchanged_state_still_commits_sql_changes(self):
        self.m.save()
        self.m.db.execute('INSERT INTO polls VALUES(?,?,NULL)', (self.now, 2))
        with patch('monitor.atomic_json') as write:
            self.m.save()
            write.assert_not_called()
        with closing(sqlite3.connect(self.path/'monitor.sqlite')) as reader:
            self.assertEqual(reader.execute('SELECT count(*) FROM polls').fetchone()[0], 1)

    def test_backoff_keeps_local_liveness_at_sixty_seconds(self):
        self.m.state.update(sourceStatus='auth_required', retryNotBefore=self.now+21600, nextPoll=self.now+21600)
        self.m.reserve_poll();self.m.save()
        before = self.m.db.total_changes
        with patch('monitor.atomic_json', wraps=monitor_module.atomic_json) as write:
            self.assertTrue(self.m.loop_checkpoint())
            for _ in range(10):
                self.now += 2
                self.assertFalse(self.m.reserve_poll())
                self.assertFalse(self.m.loop_checkpoint())
                self.m.flush()
            self.now += 40
            self.assertTrue(self.m.loop_checkpoint())
            heartbeats = [call for call in write.call_args_list if call.args[0].name == 'loop-heartbeat.json']
            self.assertEqual(len(heartbeats), 2)
        self.assertEqual(self.m.db.total_changes, before)
        saved = json.loads((self.path/'loop-heartbeat.json').read_text())
        self.assertEqual(saved['loopAliveAt'], dt.datetime.fromtimestamp(self.now,dt.timezone.utc).isoformat())

    def test_media_and_intake_reservations_survive_crash_before_side_effect(self):
        self.m.record(self.result())
        def media(payload, ua):
            with closing(sqlite3.connect(self.path/'monitor.sqlite')) as reader:
                self.assertEqual(reader.execute('SELECT count(*) FROM media_reads').fetchone()[0], 1)
            return self.media(payload, ua)
        def crash(config, payload):
            with closing(sqlite3.connect(self.path/'monitor.sqlite')) as reader:
                row = reader.execute('SELECT attempts,payload,due FROM outbox').fetchone()
                self.assertEqual(row[0], 1)
                self.assertIn('imageBase64', json.loads(row[1]))
                self.assertGreater(row[2], self.now)
            raise RuntimeError('fixture_crash')
        with self.assertRaises(RuntimeError):
            self.m.flush(crash,media)

    def test_initial_snapshot_stories_are_excluded_even_with_exact_push(self):
        self.m.config['pushMode'] = 'pilot';self.push(1)
        self.m.record(self.result())
        entry = self.coverage()
        self.assertTrue(entry['baseline'])
        self.assertEqual(entry['pushCorrelation'], 'exact_story_id')
        self.assertEqual(self.m.state['coverage']['successDenominator'], 0)
        self.assertEqual(entry['firstSeenAt'], dt.datetime.fromtimestamp(self.now,dt.timezone.utc).isoformat())

    def test_missing_push_matures_and_late_exact_signal_repairs_correlation(self):
        self.baseline()
        story = self.story();story['storyId'] = '4004391581161830449'
        self.m.state['lastPollTrigger'] = 'safety'
        self.m.record(self.result(story))
        self.assertEqual(self.coverage(story['storyId'])['collectionTrigger'], 'safety')
        self.assertEqual(self.m.state['coverage']['awaitingPushStories'], 1)
        self.now += 600
        self.push(1)  # Receiver heartbeat stayed current during the correlation window.
        self.m.refresh_coverage(force=True)
        self.assertEqual(self.coverage(story['storyId'])['pushCorrelation'], 'without_corresponding_push')
        self.assertEqual(self.m.state['coverage']['withoutCorrespondingPushStories'], 1)
        self.push(2, story_id=story['storyId'])
        self.m.reserve_poll()
        self.assertEqual(self.coverage(story['storyId'])['pushCorrelation'], 'exact_story_id')
        self.assertEqual(self.m.state['coverage']['correspondingPushStories'], 1)
        self.assertEqual(self.m.state['coverage']['withoutCorrespondingPushStories'], 0)

    def test_generic_signal_remains_uncertain_and_does_not_become_exact(self):
        self.baseline()
        story = self.story();story['storyId'] = '4004391581161830449'
        self.push(2, story_id=None)
        self.m.record(self.result(story))
        entry = self.coverage(story['storyId'])
        self.assertEqual(entry['pushCorrelation'], 'generic_profile_signal_uncertain')
        self.assertEqual(entry['genericProfileSignalSequence'], 2)
        self.now += 600;self.push(2,story_id=None);self.m.refresh_coverage(force=True)
        self.assertEqual(self.coverage(story['storyId'])['pushCorrelation'], 'without_corresponding_push')
        self.assertEqual(self.m.state['coverage']['genericProfileSignalStories'], 1)
        self.assertEqual(self.m.state['coverage']['correspondingPushStories'], 0)

    def test_authentication_and_socket_gap_stories_do_not_enter_denominator(self):
        self.baseline()
        failed = self.result();failed.update(status='auth_required', stories=[])
        self.m.record(failed)
        self.now += 21601;self.push(2)
        self.m.record(self.result())
        self.assertEqual(self.coverage()['excludedReason'], 'coverage_gap')
        self.push(2,status='disconnected')
        story = self.story();story['storyId'] = '4004391581161830449'
        self.m.record(self.result(story))
        self.assertEqual(self.coverage(story['storyId'])['excludedReason'], 'receiver_disconnected')
        self.now += 600;self.m.refresh_coverage(force=True)
        self.assertEqual(self.m.state['coverage']['successDenominator'], 0)
        self.assertEqual(self.m.state['coverage']['excludedStories'], 2)

    def test_weekend_without_stories_is_healthy_empty_snapshot(self):
        self.baseline()
        self.now += 86400
        empty = self.result();empty.update(status='empty',stories=[])
        self.m.record(empty)
        self.assertEqual(self.m.state['sourceStatus'], 'empty')
        self.assertEqual(self.m.state['failures'], 0)
        self.assertEqual(self.m.state['coverage']['successDenominator'], 0)

    def test_receiver_outage_during_pending_correlation_excludes_story(self):
        self.baseline()
        story = self.story();story['storyId'] = '4004391581161830449'
        self.m.record(self.result(story))
        self.now += 601;self.m.refresh_coverage(force=True)
        self.assertEqual(self.coverage(story['storyId'])['excludedReason'], 'receiver_stale')
        self.assertEqual(self.m.state['coverage']['successDenominator'], 0)
        self.assertEqual(self.m.state['coverage']['excludedStories'], 1)

    def test_restart_keeps_generic_correlation_and_existing_verified_proof(self):
        self.baseline()
        story = self.story();story['storyId'] = '4004391581161830449'
        self.push(2,story_id=None)
        self.m.record(self.result(story))
        self.m.state['verifiedPushProof'] = 'historic_verified_proof';self.m.save()
        other = Monitor(self.path,self.m.config,clock=lambda:self.now)
        self.addCleanup(other.db.close)
        other.reserve_poll();other.save()
        self.assertEqual(other.state['coverage']['genericProfileSignalStories'], 1)
        self.assertEqual(other.state['verifiedPushProof'], 'historic_verified_proof')
        self.assertEqual(other.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)

    def test_receiver_snapshot_exposes_freshness_and_backlog_progress_is_receipt_only(self):
        self.baseline()
        self.assertTrue(self.m.state['pushReceiver']['fresh'])
        self.m.record(self.result())
        self.m.flush(lambda c,p:{'status':'pending','targets':{'main':{'status':'pending','attempts':1}}}, self.media)
        progressed = self.m.state['outboxProgress']['lastProgressAt']
        self.assertEqual(self.m.state['outboxProgress']['oldestPendingEpoch'], self.now)
        self.now += 121
        self.m.flush(lambda c,p:{'status':'pending','targets':{'main':{'status':'pending','attempts':2}}}, self.media)
        self.assertEqual(self.m.state['outboxProgress']['lastProgressAt'], progressed)
        self.assertEqual(self.coverage()['targets'], {next(iter(self.coverage()['targets'])): {'main':{'status':'pending','imageConfirmed':None}}})
        self.now += 600;self.m.reserve_poll()
        self.assertFalse(self.m.state['pushReceiver']['fresh'])
        self.assertFalse(self.m.state['pushReceiver']['connected'])

    def test_storage_network_does_not_block_story_collection_or_once_return(self):
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        observer = SimpleNamespace(state={'nextEpoch': 0}, clock=lambda: self.now)
        def observe():
            entered.set()
            release.wait(2)
            finished.set()
            return {'outcome': 'published'}
        observer.run_if_due = observe
        def collect():
            self.assertTrue(entered.wait(1))
            self.assertFalse(finished.is_set())
            return self.result()
        (self.path / 'storage-usage.json').write_text('{}')
        try:
            with patch('monitor.private_config', return_value={}), \
                    patch('monitor.Monitor', return_value=self.m), \
                    patch('storage_usage.StorageUsageObserver', return_value=observer), \
                    patch('monitor.InstagramClient') as instagram, \
                    patch.object(self.m, 'flush'), patch.object(self.m, 'publish_health'):
                instagram.return_value.collect.side_effect = collect
                state = run(self.path, self.path / 'config.json', once=True)
            self.assertFalse(finished.is_set())
            self.assertEqual(state['cycles'], 1)
        finally:
            release.set()
            self.assertTrue(finished.wait(1))


class StorageUsageWorkerTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000
        self.observer = SimpleNamespace(
            state={'nextEpoch': 1001, 'lastOutcome': 'published'}, clock=lambda: self.now)

    def worker(self):
        worker = StorageUsageWorker(self.observer, 900)
        self.addCleanup(worker.close)
        return worker

    def test_due_only_single_worker_and_no_overlapping_collections(self):
        entered, release = threading.Event(), threading.Event()
        calls = []
        def observe():
            calls.append(threading.get_ident())
            entered.set()
            release.wait(2)
            return {'outcome': 'published'}
        self.observer.run_if_due = observe
        worker = self.worker()
        try:
            self.assertEqual(worker.tick(), 'published')
            self.assertIsNone(worker.future)
            self.now = 1001
            worker.tick()
            self.assertTrue(entered.wait(1))
            original = worker.future
            self.now += 900
            for _ in range(3):
                self.assertEqual(worker.tick(), 'published')
                self.assertIs(worker.future, original)
            self.assertEqual(len(calls), 1)
            release.set()
            original.result(timeout=1)
            worker.tick()
            worker.future.result(timeout=1)
            worker.tick()
            self.assertIsNone(worker.future)
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0], calls[1])
        finally:
            release.set()

    def test_failures_keep_cadence_and_health_until_recovery(self):
        def fail():
            raise ValueError('private_error_not_exposed')
        self.observer.run_if_due = fail
        self.now = 1001
        worker = self.worker()
        worker.tick()
        worker.future.result(timeout=1)
        self.assertEqual(worker.tick(), 'failed')
        self.assertIsNone(worker.future)
        self.now += 899
        self.assertEqual(worker.tick(), 'failed')
        self.assertIsNone(worker.future)
        self.observer.run_if_due = lambda: {'outcome': 'published'}
        self.now += 1
        worker.tick()
        worker.future.result(timeout=1)
        self.assertEqual(worker.tick(), 'published')


if __name__ == '__main__':
    unittest.main()
