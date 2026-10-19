import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from instagram_browser import BrowserInstagramClient, _run_browser, browser_gate_fallback
from test_instagram import SECRET, STORY_ID, item
from monitor import Monitor


class BrowserTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.session = self.directory / 'session.json'
        self.session.write_text(json.dumps({'user_agent': 'Existing local browser',
                                           'cookie_header': 'sessionid=' + SECRET}))
        self.session.chmod(0o600)

    def client(self, response):
        self.runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps(response), ''))
        return BrowserInstagramClient(self.session, runner=self.runner)

    def loaded(self, **extra):
        raw = json.dumps({'reels_media': [{'user': {'username': 'clubeuol'}, 'items': [item(pk=int(STORY_ID))]}]})
        return {'status': 'loaded', 'reason': '', 'requests': 2, 'scripts': [raw], 'gateClicked': True, **extra}

    def test_gate_result_reuses_parser_and_preserves_long_numeric_id(self):
        result = self.client(self.loaded()).collect()
        self.assertEqual(result['status'], 'found')
        self.assertEqual(result['stories'][0]['storyId'], STORY_ID)
        self.assertTrue(result['gateClicked'])
        self.assertNotIn(SECRET, json.dumps(result))
        command, options = self.runner.call_args.args[0], self.runner.call_args.kwargs
        self.assertNotIn(SECRET, ' '.join(command))
        self.assertIn(SECRET, options['input'])  # private stdin, never argv/output

    def test_other_profile_cannot_become_a_story(self):
        result = self.client(self.loaded(scripts=[json.dumps({'user': {'username': 'other'}, 'items': [item()]})])).collect()
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['stories'], [])

    def test_rate_limit_and_auth_are_terminal_and_sanitized(self):
        for status, reason in [('rate_limited', 'http_429'), ('auth_required', 'http_auth_required')]:
            with self.subTest(status=status):
                result = self.client({'status': status, 'reason': reason, 'requests': 1,
                                      'retryAfterSeconds': 7200, 'cookie': SECRET}).collect()
                self.assertEqual(result['status'], status)
                self.assertEqual(result['stories'], [])
                self.assertNotIn(SECRET, json.dumps(result))

    def test_invalid_output_and_exception_never_expose_private_text(self):
        for response in [{'status': 'loaded', 'requests': 1000}, {'status': 'unknown', 'reason': SECRET, 'requests': 1}]:
            result = self.client(response).collect()
            self.assertEqual(result['status'], 'unknown')
            self.assertNotIn(SECRET, json.dumps(result))
        client = self.client(self.loaded())
        self.runner.side_effect = RuntimeError(SECRET)
        self.assertNotIn(SECRET, json.dumps(client.collect()))

    def test_fallback_only_follows_inconclusive_structure(self):
        factory = Mock()
        for status, reason in [('found', ''), ('empty', ''), ('auth_required', 'login_payload'),
                               ('rate_limited', 'http_429'), ('unknown', 'network_error'),
                               ('unknown', 'malformed_story_payload')]:
            primary = {'status': status, 'reason': reason, 'requests': 2}
            self.assertIs(browser_gate_fallback(primary, self.session, True, factory), primary)
        factory.assert_not_called()
        primary = {'status': 'unknown', 'reason': 'no_validated_story_structure', 'requests': 2}
        factory.return_value.collect.return_value = {'status': 'rate_limited', 'reason': 'http_429', 'requests': 1}
        result = browser_gate_fallback(primary, self.session, True, factory)
        self.assertEqual(result['status'], 'rate_limited')
        self.assertEqual(result['requests'], 3)
        self.assertTrue(result['browserFallback'])

    def test_monitor_reserves_fallback_before_network_and_preserves_429(self):
        monitor = Monitor(self.directory, {'collectionBackend': 'html', 'browserGateFallback': True,
                                          'pollIntervalSeconds': 300, 'maxDailyRequests': 600}, clock=lambda: 1792435000)
        self.addCleanup(monitor.db.close)
        self.assertEqual(monitor.collection_cost(), 4)
        self.assertTrue(monitor.reserve_poll())
        self.assertEqual(monitor.db.execute('SELECT sum(reserved) FROM polls').fetchone()[0], 4)
        monitor.state['retryNotBefore'] = 1792436000
        self.assertFalse(monitor.reserve_poll())

    def test_timeout_kills_browser_process_group(self):
        child = Mock(pid=123)
        child.communicate.side_effect = [subprocess.TimeoutExpired('node', 45), ('', None)]
        context = Mock()
        context.__enter__ = Mock(return_value=child)
        context.__exit__ = Mock(return_value=False)
        with patch('instagram_browser.subprocess.Popen', return_value=context), patch('instagram_browser.os.killpg') as kill:
            with self.assertRaises(subprocess.TimeoutExpired):
                _run_browser(['node', 'helper.mjs'], input='{}', timeout=45)
            kill.assert_called_once()
            self.assertEqual(kill.call_args.args[0], 123)


if __name__ == '__main__':
    unittest.main()
