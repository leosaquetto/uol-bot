"""Bounded browser collection using only the Oracle's existing local session.

The child process receives cookies over a private pipe. Only validated Story
records reach the monitor; browser HTML, session data and exception text do not.
"""
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess

from instagram import InstagramClient, MAX_BYTES, parse_stories

HELPER = Path(__file__).with_name('instagram_browser.mjs')
MAX_OUTPUT = 2 * MAX_BYTES
BROWSER_REASONS = frozenset(('browser_unavailable', 'browser_timeout', 'browser_failed',
                            'browser_output_invalid', 'browser_body_limit', 'http_429',
                            'http_auth_required', 'http_status', 'login_page',
                            'unexpected_page', 'story_gate_not_found', 'story_gate_ambiguous',
                            'story_gate_did_not_load', 'no_validated_story_structure'))


def _run_browser(command, **options):
    """Kill the whole disposable browser process group if its deadline expires."""
    with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL, text=True, start_new_session=True) as child:
        try:
            stdout, _ = child.communicate(options['input'], timeout=options['timeout'])
            return subprocess.CompletedProcess(command, child.returncode, stdout, '')
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.communicate()
            raise


class BrowserInstagramClient(InstagramClient):
    def __init__(self, session_path, runner=None, clock=None):
        super().__init__(session_path, opener_factory=lambda: None, clock=clock)
        self.runner = runner or _run_browser

    def collect(self):
        started = self.clock()
        result = {'checkedAt': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  'status': 'unknown', 'reason': 'browser_failed', 'requests': 0,
                  'duration_ms': 0, 'body_bytes': 0, 'stories': []}
        try:
            process = self.runner(['/usr/local/bin/node', str(HELPER)],
                                  input=json.dumps({'userAgent': self.config['user_agent'],
                                                    'cookies': self._cookies()}),
                                  text=True, capture_output=True, timeout=45, check=False)
            if process.returncode != 0:
                return result
            raw = process.stdout
            if not isinstance(raw, str) or len(raw.encode('utf-8')) > MAX_OUTPUT:
                result['reason'] = 'browser_output_invalid'
                return result
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError('invalid_browser_output')
            requests = data.get('requests')
            if type(requests) is not int or not 0 <= requests <= 2:
                raise ValueError('invalid_browser_count')
            result['requests'] = requests
            status = data.get('status')
            reason = data.get('reason')
            if status in ('auth_required', 'rate_limited', 'unknown'):
                result.update(status=status, reason=reason if reason in BROWSER_REASONS else 'browser_failed')
                if status == 'rate_limited':
                    delay = data.get('retryAfterSeconds')
                    result['retryAfterSeconds'] = (min(delay, 365 * 86400)
                                                   if type(delay) is int and delay >= 0 else 3600)
                return result
            if status != 'loaded':
                raise ValueError('invalid_browser_status')
            scripts = data.get('scripts')
            if (not isinstance(scripts, list) or not all(isinstance(value, str) for value in scripts)
                    or len(scripts) > 512):
                raise ValueError('invalid_browser_scripts')
            # Never inject raw closing tags into the inert HTML parser. JSON escapes
            # preserve IDs and strings exactly, without a JavaScript number roundtrip.
            body = ''.join('<script type="application/json">' + value.replace('<', '\\u003c') + '</script>'
                           for value in scripts)
            result['body_bytes'] = len(body.encode('utf-8'))
            if result['body_bytes'] > MAX_BYTES:
                result['reason'] = 'browser_body_limit'
                return result
            result.update(parse_stories(body))
            result['gateClicked'] = data.get('gateClicked') is True
            if result['status'] == 'unknown' and result.get('reason') == 'no_validated_story_structure':
                result['reason'] = ('story_gate_did_not_load' if result['gateClicked']
                                    else 'no_validated_story_structure')
        except subprocess.TimeoutExpired:
            result.update(reason='browser_timeout', requests=2)
        except FileNotFoundError:
            result['reason'] = 'browser_unavailable'
        except (ValueError, TypeError, UnicodeError):
            result['reason'] = 'browser_output_invalid'
        except Exception:
            result['reason'] = 'browser_failed'
        finally:
            result['duration_ms'] = max(0, round((self.clock() - started) * 1000))
        return result


def browser_gate_fallback(primary, session_path, enabled=False, client_factory=BrowserInstagramClient):
    """Only an inconclusive HTML/JSON structure can trigger the reserved fallback.

    A real rate limit, authentication failure, network error or malformed payload
    never gets another route or retry here. Successful snapshots stay HTTP-only.
    """
    if (enabled is not True or primary.get('status') != 'unknown'
            or primary.get('reason') != 'no_validated_story_structure'):
        return primary
    result = client_factory(session_path).collect()
    result['requests'] = primary.get('requests', 0) + result.get('requests', 0)
    result['duration_ms'] = primary.get('duration_ms', 0) + result.get('duration_ms', 0)
    result['body_bytes'] = primary.get('body_bytes', 0) + result.get('body_bytes', 0)
    result['browserFallback'] = True
    if 'profileIdentity' in primary:
        result['profileIdentity'] = primary['profileIdentity']
    return result
