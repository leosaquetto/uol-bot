import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import ssl
import tempfile
import unittest
from unittest.mock import patch
import urllib.parse
import zlib

from fbns_probe import (BoundedTransport, Journal, MAX_PAYLOAD, Probe, ProbeError,
                        bounded_payload, diagnostic_story, locked_directory, main)
from instagram import InstagramClient
from vendor.instagram_fbns.mqttot import write_publish_packet


class Response:
    def __init__(self, status=200, raw=b'{"status":"ok"}', headers=None):
        self.status, self.raw, self.headers = status, raw, headers or {}
    def read(self, limit):
        return self.raw[:limit]
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False


class Opener:
    def __init__(self, response=None, callback=None):
        self.calls = []
        self.response, self.callback = response or Response(), callback
    def open(self, request, timeout):
        self.calls.append(request)
        if self.callback:
            self.callback(request)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class FakeTransport:
    def __init__(self, path, packets, on_send=None):
        self.path, self.packets, self.on_send = path, list(packets), on_send
        self.sent, self.connected, self.closed = [], False, False
        self.timeout = 20
    def connect(self):
        self.connected = True
    def send(self, packet):
        if self.on_send:
            self.on_send(packet)
        self.sent.append(packet)
    def recv_packet(self):
        if not self.packets:
            raise TimeoutError()
        return self.packets.pop(0)
    def disconnect(self):
        self.closed = True


def connack(auth=None):
    raw = json.dumps(auth if auth is not None else {
        'ck': 987, 'cs': 'broker-password', 'di': 'broker-device', 'ds': 'broker-secret'}).encode()
    body = b'\x00\x00' + zlib.compress(raw)
    assert len(body) < 128
    return b'\x20' + bytes([len(body)]) + body


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.now = 1700000000.0
        session = {'user_agent': 'ExistingProtectedSession/1.0', 'cookie_header':
                   'sessionid=123%3Aprivate; ds_user_id=123; csrftoken=private-csrf'}
        path = self.root / 'session.json'
        path.write_text(json.dumps(session))
        path.chmod(0o600)
        self.session_raw = path.read_bytes()
        self.db = sqlite3.connect(self.root / 'monitor.sqlite')
        self.db.execute('CREATE TABLE state(id INTEGER PRIMARY KEY,value TEXT)')
        self.set_cooldown(0)
        self.opener = Opener()
        self.transports = []

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def set_cooldown(self, until):
        self.db.execute('INSERT OR REPLACE INTO state VALUES(1,?)',
                        (json.dumps({'retryNotBefore': until}),))
        self.db.commit()

    def client(self, session):
        return InstagramClient(session, opener_factory=lambda: self.opener)

    def transport(self, packets=None, on_send=None):
        transport = FakeTransport(self.root, packets or [connack(), write_publish_packet(
            '80', b'{"token":"private-token"}', packet_id=9), b'\xd0\x00'], on_send)
        self.transports.append(transport)
        return lambda: transport

    def probe(self, factory=None):
        return Probe(self.root, transport_factory=factory or self.transport(),
                     instagram_factory=self.client, clock=lambda: self.now)

    def prepared(self):
        probe = self.probe()
        self.assertTrue(probe.prepare()['ok'])
        return probe

    def test_prepare_persists_auth_and_token_before_next_packet_ack(self):
        def inspect(packet):
            state = json.loads((self.root / 'fbns-state.json').read_text())
            if packet[0] >> 4 in (8, 3):
                self.assertEqual(state['auth']['password'], 'broker-password')
            if packet.startswith(b'\x40'):
                self.assertEqual(state['token'], 'private-token')
                self.assertTrue((self.root / 'fbns-journal.jsonl').read_bytes())
        probe = self.probe(self.transport(on_send=inspect))
        result = probe.prepare()
        self.assertTrue(result['ok'])
        self.assertFalse(result['accountAssociated'])
        self.assertFalse(self.opener.calls)
        self.assertTrue(self.transports[-1].closed)
        self.assertEqual((self.root / 'session.json').read_bytes(), self.session_raw)
        self.assertEqual((self.root / 'fbns-state.json').stat().st_mode & 0o777, 0o600)

    def test_prepare_requires_proven_broker_credentials(self):
        probe = self.probe(self.transport([connack({})]))
        with self.assertRaisesRegex(ProbeError, 'broker_auth_unproven'):
            probe.prepare()
        self.assertFalse(probe.state['token'])

    def test_prepare_refuses_cooldown_without_connect(self):
        self.set_cooldown(self.now + 100)
        probe = self.probe()
        with self.assertRaisesRegex(ProbeError, 'source_cooldown_active'):
            probe.prepare()
        self.assertFalse(self.transports[-1].connected)

    def test_prepare_reuses_identity_and_token_after_restart(self):
        first = self.prepared()
        identities = {key: first.state[key] for key in ('uuid', 'phoneId', 'familyDeviceId', 'token')}
        probe = self.probe(self.transport([connack(), b'\xd0\x00']))
        self.assertTrue(probe.prepare()['ok'])
        self.assertEqual({key: probe.state[key] for key in identities}, identities)
        self.assertFalse(any(packet[0] >> 4 == 3 for packet in self.transports[-1].sent))

    def test_association_current_account_main_channel_and_intent_before_post(self):
        probe = self.prepared()
        def inspect(request):
            state = json.loads((self.root / 'fbns-state.json').read_text())
            self.assertEqual(state['association']['status'], 'uncertain')
            self.assertEqual(state['association']['userId'], '123')
        self.opener.callback = inspect
        self.assertTrue(probe.associate()['ok'])
        request = self.opener.calls[0]
        self.assertEqual(request.full_url, 'https://i.instagram.com/api/v1/push/register/')
        data = urllib.parse.parse_qs(request.data.decode())
        self.assertEqual(data['is_main_push_channel'], ['true'])
        self.assertEqual(data['users'], ['123'])
        self.assertEqual(data['phone_id'], [probe.state['phoneId']])
        self.assertEqual(data['family_device_id'], [probe.state['familyDeviceId']])
        self.assertIn('sessionid=', request.get_header('Cookie'))
        self.assertEqual((self.root / 'session.json').read_bytes(), self.session_raw)
        with self.assertRaisesRegex(ProbeError, 'association_already_attempted'):
            self.probe().associate()
        self.assertEqual(len(self.opener.calls), 1)

    def test_association_lost_response_stays_uncertain_without_second_post(self):
        probe = self.prepared()
        self.opener.response = TimeoutError('private-token must not leak')
        self.assertEqual(probe.associate()['status'], 'uncertain')
        restart = self.probe()
        self.assertEqual(restart.state['association']['status'], 'uncertain')
        with self.assertRaisesRegex(ProbeError, 'association_already_attempted'):
            restart.associate()
        self.assertEqual(len(self.opener.calls), 1)
        self.assertFalse(restart.serve(max_windows=1)['ok'])
        self.assertEqual(len(self.opener.calls), 1)

    def test_association_5xx_invalid_json_and_nonok_are_uncertain(self):
        for response in (Response(503), Response(200, b'not-json'), Response(200, b'{"status":"fail"}')):
            with self.subTest(response=response):
                probe = self.prepared()
                probe.state.pop('association', None)
                probe.state['connections'] = []
                probe.save()
                self.opener.response = response
                self.assertEqual(probe.associate()['status'], 'uncertain')

    def test_association_rejection_is_terminal_and_429_persists_guard(self):
        probe = self.prepared()
        self.opener.response = Response(429, headers={'Retry-After': '30000'})
        self.assertEqual(probe.associate()['status'], 'rejected')
        self.assertEqual(probe.state['httpRetryNotBefore'], self.now + 30000)
        with self.assertRaisesRegex(ProbeError, 'association_already_attempted'):
            self.probe().associate()

    def test_association_guard_precedes_http_and_intent(self):
        probe = self.prepared()
        self.set_cooldown(self.now + 100)
        with self.assertRaisesRegex(ProbeError, 'source_cooldown_active'):
            probe.associate()
        self.assertNotIn('association', probe.state)
        self.assertFalse(self.opener.calls)

    def source_lookup_backoff(self, until):
        self.db.execute('INSERT OR REPLACE INTO state VALUES(1,?)', (json.dumps({
            'retryNotBefore': until,
            'sourceStatus': 'rate_limited', 'lastResult': {'reason': 'http_429'}}),))
        self.db.commit()

    def test_registration_probe_audits_exception_before_broker_and_one_post(self):
        self.source_lookup_backoff(self.now + 21600)
        before = self.db.execute('SELECT value FROM state').fetchone()
        def inspect(packet):
            audit = json.loads((self.root / 'fbns-state.json').read_text())['sourceBackoffException']
            self.assertEqual(audit['scope'], 'fbns_registration_probe')
            self.assertEqual(audit['sourceRetryNotBefore'], self.now + 21600)
        probe = self.probe(self.transport(on_send=inspect))
        probe.registration_probe = True
        self.assertTrue(probe.prepare()['ok'])
        self.assertTrue(probe.associate()['ok'])
        audit = dict(probe.state['sourceBackoffException'])
        restart = self.probe()
        restart.registration_probe = True
        with patch.object(restart, 'observe', return_value={'records': 0, 'testStoryMatches': 0}):
            self.assertTrue(restart.serve(max_windows=1)['ok'])
        self.assertEqual(len(self.opener.calls), 1)
        self.assertEqual(restart.state['sourceBackoffException'], audit)
        self.assertEqual(before, self.db.execute('SELECT value FROM state').fetchone())

    def test_registration_probe_never_bypasses_fbns_429_or_new_source_guard(self):
        probe = self.prepared()
        probe.registration_probe = True
        self.source_lookup_backoff(self.now + 21600)
        self.assertEqual(probe.retry_not_before(), 0)
        probe.state['httpRetryNotBefore'] = self.now + 30000
        probe.save()
        self.assertEqual(probe.retry_not_before(), self.now + 30000)
        with self.assertRaisesRegex(ProbeError, 'source_cooldown_active'):
            probe.associate()
        self.assertFalse(self.opener.calls)
        probe.state['httpRetryNotBefore'] = 0
        self.source_lookup_backoff(self.now + 21601)
        self.assertEqual(probe.retry_not_before(), self.now + 21601)

    def test_registration_probe_cannot_bypass_auth_or_unspecified_cooldown(self):
        probe = self.probe()
        probe.registration_probe = True
        self.set_cooldown(self.now + 100)
        self.assertEqual(probe.retry_not_before(), self.now + 100)
        self.assertNotIn('sourceBackoffException', probe.state)
        self.db.execute('UPDATE state SET value=?', (json.dumps({
            'retryNotBefore': self.now + 100, 'collectionBackend': 'json',
            'sourceStatus': 'auth_required', 'lastResult': {'reason': 'http_auth_required'}}),))
        self.db.commit()
        self.assertEqual(probe.retry_not_before(), self.now + 100)

    def test_cookie_identity_conflict_refused(self):
        probe = self.prepared()
        original = self.client
        def conflicted(path):
            client = original(path)
            import copy
            cookie = copy.copy(next(c for c in client.jar if c.name == 'ds_user_id'))
            cookie.domain, cookie.value = 'www.instagram.com', '456'
            client.jar.set_cookie(cookie)
            return client
        probe.instagram_factory = conflicted
        with self.assertRaisesRegex(ProbeError, 'session_identity_unproven'):
            probe.associate()
        self.assertFalse(self.opener.calls)

    def test_accepted_restart_reuses_association_but_checks_current_account(self):
        probe = self.prepared()
        probe.associate()
        restart = self.probe()
        with patch.object(restart, 'observe', return_value={'records': 1, 'testStoryMatches': 1}) as observe:
            result = restart.serve(max_windows=1)
        self.assertTrue(result['ok'])
        observe.assert_called_once_with(3600, internal=True)
        self.assertEqual(len(self.opener.calls), 1)
        self.assertEqual(restart.state['serviceTotals']['testStoryMatches'], 1)
        restart.state['association']['userId'] = '456'
        restart.save()
        with self.assertRaisesRegex(ProbeError, 'session_account_changed'):
            restart.serve(max_windows=1)

    def test_serve_waits_before_prepare_and_associate(self):
        probe = self.probe()
        self.set_cooldown(self.now + 70)
        sleeps = []
        def sleep(value):
            sleeps.append(value)
            self.assertFalse(self.transports[-1].connected)
            self.now += value
        with patch.object(probe, 'observe', return_value={'records': 0, 'testStoryMatches': 0}):
            self.assertTrue(probe.serve(sleep=sleep, max_windows=1)['ok'])
        self.assertEqual(sleeps, [30, 30, 10])
        self.assertEqual(len(self.opener.calls), 1)

    def test_connection_budget_survives_restart_and_counts_failed_connect(self):
        probe = self.prepared()
        probe.state['connections'] += [self.now, self.now]
        probe.save()
        restart = self.probe(self.transport([connack(), b'\xd0\x00']))
        with self.assertRaisesRegex(ProbeError, 'connection_budget_exhausted'):
            restart.prepare()
        self.assertFalse(self.transports[-1].connected)
        self.now += 3601
        self.assertTrue(restart.prepare()['ok'])

    def test_serve_reconnects_network_only_and_stops_protocol(self):
        probe = self.prepared()
        probe.associate()
        sleeps = []
        with patch.object(probe, 'observe', side_effect=[OSError(), {'records': 0, 'testStoryMatches': 0}]):
            self.assertTrue(probe.serve(sleep=sleeps.append, max_windows=1)['ok'])
        self.assertEqual(sleeps, [60])
        with patch.object(probe, 'observe', side_effect=ProbeError('payload_limit')):
            with self.assertRaisesRegex(ProbeError, 'payload_limit'):
                probe.serve(sleep=sleeps.append, max_windows=1)
        self.assertEqual(probe.state['service']['status'], 'review_required')
        with patch.object(probe, 'observe', side_effect=ssl.SSLError('untrusted')):
            with self.assertRaises(ssl.SSLError):
                probe.serve(sleep=sleeps.append, max_windows=1)
        self.assertEqual(sleeps, [60])

    def test_existing_broker_identity_cannot_be_replaced(self):
        first = self.prepared()
        original = dict(first.state['auth'])
        restart = self.probe(self.transport([connack({
            'ck': 988, 'cs': 'different-secret', 'di': 'new-device', 'ds': 'another-secret'})]))
        with self.assertRaisesRegex(ProbeError, 'broker_identity_changed'):
            restart.prepare()
        self.assertEqual(json.loads((self.root / 'fbns-state.json').read_text())['auth'], original)

    def test_observe_public_duration_is_capped(self):
        probe = self.prepared()
        with self.assertRaisesRegex(ProbeError, 'invalid_observation_duration'):
            probe.observe(601)

    def test_observe_rejects_disconnect_unknown_and_invalid_controls_without_ack(self):
        self.prepared()
        for raw, reason in ((b'\xe0\x00', 'broker_disconnect'),
                            (b'\xa0\x00', 'unexpected_broker_packet'),
                            (b'\x40\x01\x00', 'invalid_broker_puback'),
                            (b'\x90\x03\x00\x01\x80', 'broker_subscription_rejected')):
            with self.subTest(reason=reason):
                probe = self.probe(self.transport([raw]))
                client, _ = probe._client()
                with self.assertRaisesRegex(ProbeError, reason):
                    client.read_once()
                self.assertFalse(self.transports[-1].sent)

    def test_read_once_allows_puback_suback_and_pingresp(self):
        probe = self.probe(self.transport([b'\x40\x02\x00\x01',
                                          b'\x90\x03\x00\x01\x01', b'\xd0\x00']))
        client, journal = probe._client()
        self.assertEqual([client.read_once().packet_type for _ in range(3)], ['4', '9', 'pingresp'])
        self.assertFalse(journal.records)

    def test_observe_missing_pong_cannot_report_healthy(self):
        self.prepared()
        probe = self.probe(self.transport([connack()]))
        tick = [0]
        def monotonic():
            tick[0] += 1
            return tick[0]
        probe.monotonic = monotonic
        with self.assertRaisesRegex(ProbeError, 'broker_pong_timeout'):
            probe.observe(120)
        self.assertIn(b'\xc0\x00', self.transports[-1].sent)
        self.assertTrue(self.transports[-1].closed)

    def test_observe_pong_clears_deadline_and_allows_next_keepalive(self):
        self.prepared()
        transport = self.transport([connack()])()
        def on_send(packet):
            if packet == b'\xc0\x00':
                transport.packets.append(b'\xd0\x00')
        transport.on_send = on_send
        probe = self.probe(lambda: transport)
        tick = [0]
        def monotonic():
            tick[0] += 1
            return tick[0]
        probe.monotonic = monotonic
        self.assertTrue(probe.observe(130)['ok'])
        self.assertEqual(transport.sent.count(b'\xc0\x00'), 2)

    def test_diagnostic_story_requires_raw_structured_type_action_and_owner(self):
        valid = {'collapse_key': 'first_reel_post', 'ig': 'user?username=leotheblueblur&launch_reel=1'}
        self.assertTrue(diagnostic_story(valid))
        self.assertTrue(diagnostic_story({'fbpushnotif': json.dumps(valid)}))
        for update in ({'collapse_key': 'direct_v2_message'}, {'ig': 'user?username=clubeuol&launch_reel=1'},
                       {'ig': 'user?username=leotheblueblur'}, {'ig': 'user?username=x&username=leotheblueblur&launch_reel=1'},
                       {'ig': 'https://evil.test/?username=leotheblueblur&launch_reel=1'}, {'collapse_key': 'new_story'}):
            self.assertFalse(diagnostic_story({**valid, **update}))
        self.assertFalse(diagnostic_story({'t': 'leotheblueblur', 'm': 'New Story'}))

    def test_observe_journals_before_ack_without_production_mutation(self):
        probe = self.prepared()
        payload = {'fbpushnotif': json.dumps({'collapse_key': 'first_reel_post',
                   'ig': 'user?username=leotheblueblur&launch_reel=1'})}
        def inspect(packet):
            if packet.startswith(b'\x40'):
                lines = (self.root / 'fbns-journal.jsonl').read_text().splitlines()
                self.assertTrue(json.loads(lines[-1])['testStory'])
        transport = self.transport([connack(), write_publish_packet('76', json.dumps(payload).encode(), packet_id=12)], inspect)
        restart = self.probe(transport)
        times = iter([0, 0, 1, 2, 3, 4])
        restart.monotonic = lambda: next(times, 5)
        before = self.db.execute('SELECT value FROM state').fetchone()
        result = restart.observe(2)
        self.assertEqual(result['testStoryMatches'], 1)
        self.assertEqual(before, self.db.execute('SELECT value FROM state').fetchone())
        self.assertFalse((self.root / 'push.sqlite').exists())
        self.assertFalse(self.opener.calls)

    def test_limits_decompression_frame_tls_and_journal(self):
        with self.assertRaisesRegex(ProbeError, 'payload_limit'):
            bounded_payload(zlib.compress(b'x' * (MAX_PAYLOAD + 1)))
        self.assertEqual(bounded_payload(zlib.compress(b'{"ok":true}')), b'{"ok":true}')
        transport = BoundedTransport()
        self.assertTrue(transport.tls_context.check_hostname)
        self.assertEqual(transport.tls_context.verify_mode, ssl.CERT_REQUIRED)
        class FakeSocket:
            def __init__(self): self.raw = io.BytesIO(b'\x30\xff\xff\x7f')
            def recv(self, size): return self.raw.read(size)
        transport.sock = FakeSocket()
        with self.assertRaisesRegex(ProbeError, 'frame_limit'):
            transport.recv_packet()
        journal = Journal(self.root / 'other-journal.jsonl')
        with patch('fbns_probe.MAX_RECORDS', 0):
            with self.assertRaisesRegex(ProbeError, 'journal_limit'):
                journal.append('76', {})

    def test_cli_sanitizes_transport_errors_and_service_exits_78(self):
        output = io.StringIO()
        with patch('fbns_probe.Probe.serve', side_effect=OSError('private-csrf private-token')), contextlib.redirect_stdout(output):
            code = main(['--serve', '--directory', str(self.root)])
        self.assertEqual(code, 78)
        self.assertNotIn('private', output.getvalue())
        self.assertEqual(json.loads(output.getvalue())['reason'], 'diagnostic_failed')

    def test_symlink_and_concurrent_lock_refused(self):
        with locked_directory(self.root):
            with self.assertRaisesRegex(ProbeError, 'probe_already_running'):
                with locked_directory(self.root):
                    pass
        target = self.root / 'real-state'
        target.write_text('{}')
        target.chmod(0o600)
        (self.root / 'fbns-state.json').symlink_to(target)
        with self.assertRaises(OSError):
            self.probe()


if __name__ == '__main__':
    unittest.main()
