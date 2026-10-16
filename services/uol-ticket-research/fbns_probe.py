"""Bounded FBNS diagnostic and optional server listener; no destination delivery.

Account association is attempted once; an explicit mobile-contract correction
can follow only a confirmed legacy HTTP 400 and a fresh same-account check.
Device credentials and received payloads belong only in the protected directory.
"""
import argparse
import base64
import contextlib
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import sqlite3
import ssl
import stat
import tempfile
import time
from types import SimpleNamespace
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib

from instagram import InstagramClient, SessionError, _retry_after_seconds
from vendor.instagram_fbns.fbns import FbnsClient, FbnsDeviceAuth, FBNS_HOST, FBNSTopics
from vendor.instagram_fbns.mqttot import (SocketMQTToTTransport, decode_packet,
                                        write_pingreq_packet)

MAX_STATE = 64 * 1024
MAX_FRAME = 1024 * 1024
MAX_PAYLOAD = 64 * 1024
MAX_JOURNAL = 4 * 1024 * 1024
MAX_RECORDS = 1000
REGISTER_URL = 'https://i.instagram.com/api/v1/push/register/'
CURRENT_USER_URL = 'https://i.instagram.com/api/v1/accounts/current_user/?edit=true'
MOBILE_CONTRACT = 'android_449_igt_v1'
MAX_HTTP_BODY = 32768
MOBILE_PROFILE = {
    'appVersion': '449.0.0.52.84', 'versionCode': '1079242191',
    'userAgent': 'Instagram 449.0.0.52.84 Android (34/14; 480dpi; 1344x2992; '
                 'Google/google; Pixel 8 Pro; husky; husky; en_US; 1079242191)',
    'appId': '567067343352427', 'capabilities': '3brTv10=', 'locale': 'en_US',
    'bloksVersion': '799db3e387d7a21a19c72918b8698b6cd28a1516ade363792c100ed45a0f9f0b',
}
TEST_PROFILE = 'leotheblueblur'
STORY_TYPES = frozenset(('first_reel_post', 'resurrected_reel_post', 'first_bestie_post'))


def mobile_headers(state, values):
    """Build the pinned native session header without decoding its cookie value."""
    profile = state['mobileProfile']
    authorization = {'ds_user_id': values['ds_user_id'], 'sessionid': values['sessionid'],
                     'should_use_header_over_cookies': True}
    bearer = base64.b64encode(json.dumps(authorization, separators=(',', ':')).encode()).decode()
    return {'Authorization': 'Bearer IGT:2:' + bearer, 'User-Agent': profile['userAgent'],
            'Accept': 'application/json', 'Accept-Encoding': 'identity',
            'Accept-Language': 'en-US', 'X-IG-App-ID': profile['appId'],
            'X-IG-Capabilities': profile['capabilities'], 'X-IG-Device-ID': state['uuid'],
            'X-IG-Family-Device-ID': state['phoneId'], 'X-IG-Android-ID': profile['androidId'],
            'X-IG-App-Locale': profile['locale'], 'X-IG-Device-Locale': profile['locale'],
            'X-IG-Mapped-Locale': profile['locale'], 'X-Bloks-Version-Id': profile['bloksVersion'],
            'X-IG-Connection-Type': 'WIFI', 'IG-INTENDED-USER-ID': values['ds_user_id'],
            'IG-U-DS-USER-ID': values['ds_user_id'], 'X-CSRFToken': values['csrftoken']}


def _response_reason(value):
    if not isinstance(value, dict):
        return 'unclassified_response'
    if isinstance(value.get('challenge'), dict):
        return 'challenge_required'
    for field in ('error_type', 'message'):
        reason = value.get(field)
        if reason in ('challenge_required', 'checkpoint_required', 'login_required',
                      'consent_required', 'feedback_required', 'rate_limit_error',
                      'invalid_credentials', 'invalid_user'):
            return reason
    return 'unclassified_response'


class ProbeError(Exception):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def _private_info(fd):
    info = os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.getuid()):
        raise ProbeError('protected_file_required')
    return info


def _read_json(path, missing=False):
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    except FileNotFoundError:
        if missing:
            return None
        raise ProbeError('state_missing') from None
    with os.fdopen(fd, 'rb') as handle:
        _private_info(handle.fileno())
        raw = handle.read(MAX_STATE + 1)
    if len(raw) > MAX_STATE:
        raise ProbeError('state_limit')
    try:
        result = json.loads(raw)
        if not isinstance(result, dict):
            raise ValueError()
        return result
    except (ValueError, TypeError):
        raise ProbeError('invalid_state') from None


def _atomic_json(path, value):
    raw = json.dumps(value, separators=(',', ':'), ensure_ascii=True).encode()
    if len(raw) > MAX_STATE:
        raise ProbeError('state_limit')
    fd, temporary = tempfile.mkstemp(prefix='.fbns-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)


@contextlib.contextmanager
def locked_directory(directory):
    directory = Path(directory)
    info = directory.lstat()
    if (not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077
            or info.st_uid != os.getuid()):
        raise ProbeError('protected_directory_required')
    fd = os.open(directory / 'fbns-probe.lock', os.O_WRONLY | os.O_CREAT
                 | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        _private_info(fd)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ProbeError('probe_already_running') from None
        yield
    finally:
        os.close(fd)


def bounded_payload(raw):
    if raw[:1] == b'\x78':
        try:
            decoder = zlib.decompressobj()
            body = decoder.decompress(raw, MAX_PAYLOAD + 1)
            if len(body) > MAX_PAYLOAD or decoder.unconsumed_tail:
                raise ProbeError('payload_limit')
            if not decoder.eof or decoder.unused_data:
                raise ProbeError('invalid_compressed_payload')
        except zlib.error:
            raise ProbeError('invalid_compressed_payload') from None
    else:
        body = raw
    if len(body) > MAX_PAYLOAD:
        raise ProbeError('payload_limit')
    if len(body) >= 3 and int.from_bytes(body[:2], 'big') == len(body) - 2:
        body = body[2:]
    return body


class BoundedTransport(SocketMQTToTTransport):
    def __init__(self, timeout=20):
        context = ssl.create_default_context()
        context.minimum_version = max(context.minimum_version, ssl.TLSVersion.TLSv1_2)
        super().__init__(FBNS_HOST, timeout=timeout, tls_context=context)

    def connect(self):
        raw = socket.create_connection((self.host, self.port), timeout=self.timeout)
        try:
            self.sock = self.tls_context.wrap_socket(raw, server_hostname=self.host)
            self.sock.settimeout(self.timeout)
        except Exception:
            raw.close()
            raise

    def recv_packet(self):
        if self.sock is None:
            raise ProbeError('broker_not_connected')

        started = False

        def exact(size):
            nonlocal started
            result = bytearray()
            while len(result) < size:
                try:
                    block = self.sock.recv(size - len(result))
                except TimeoutError:
                    if started:
                        raise ProbeError('partial_frame_timeout') from None
                    raise
                if not block:
                    raise ProbeError('broker_closed')
                started = True
                result.extend(block)
            return bytes(result)

        first = exact(1)
        size, multiplier, encoded = 0, 1, bytearray()
        for _ in range(4):
            byte = exact(1)[0]
            encoded.append(byte)
            size += (byte & 127) * multiplier
            if size > MAX_FRAME:
                raise ProbeError('frame_limit')
            if not byte & 128:
                return first + bytes(encoded) + exact(size)
            multiplier *= 128
        raise ProbeError('invalid_frame_length')


def diagnostic_story(payload):
    """Never infer a Story or owner from prose, recipient, or a DM."""
    if not isinstance(payload, dict):
        return False
    push = payload.get('fbpushnotif', payload)
    if isinstance(push, str):
        try:
            push = json.loads(push)
        except ValueError:
            return False
    if not isinstance(push, dict) or push.get('collapse_key') not in STORY_TYPES:
        return False
    action = push.get('ig')
    if not isinstance(action, str) or len(action) > 4096:
        return False
    try:
        parsed = urllib.parse.urlsplit(action if action.startswith('ig://') else 'ig://' + action)
        pairs = urllib.parse.parse_qsl(parsed.query, strict_parsing=True)
        values = dict(pairs)
        return (parsed.scheme == 'ig' and parsed.netloc == 'user' and parsed.path in ('', '/')
                and not parsed.fragment and len(pairs) == len(values)
                and values.get('username', '').lower() == TEST_PROFILE
                and values.get('launch_reel') == '1')
    except ValueError:
        return False


class Journal:
    def __init__(self, path, clock=time.time):
        self.path, self.clock = Path(path), clock
        self.records = self.story_matches = 0
        try:
            fd = os.open(self.path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        except FileNotFoundError:
            return
        with os.fdopen(fd, 'rb') as handle:
            info = _private_info(handle.fileno())
            if info.st_size > MAX_JOURNAL:
                raise ProbeError('journal_limit')
            for line in handle:
                self.records += 1
                if self.records > MAX_RECORDS or len(line) > MAX_JOURNAL:
                    raise ProbeError('journal_limit')
                try:
                    self.story_matches += int(json.loads(line).get('testStory') is True)
                except (ValueError, TypeError, AttributeError):
                    raise ProbeError('invalid_journal') from None

    def append(self, topic, payload):
        if self.records >= MAX_RECORDS:
            raise ProbeError('journal_limit')
        if isinstance(payload, bytes):
            record_payload = {'binaryBase64': base64.b64encode(payload).decode()}
            test_story = False
        else:
            record_payload, test_story = payload, diagnostic_story(payload)
        record = {'at': self.clock(), 'topic': str(topic), 'testStory': test_story,
                  'payload': record_payload}
        raw = json.dumps(record, ensure_ascii=True, separators=(',', ':')).encode() + b'\n'
        fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT
                     | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(fd, 'wb') as handle:
            info = _private_info(handle.fileno())
            if info.st_size + len(raw) > MAX_JOURNAL:
                raise ProbeError('journal_limit')
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        self.records += 1
        self.story_matches += int(test_story)


class ProbeClient(FbnsClient):
    def __init__(self, client, transport, auth, journal, save_token):
        super().__init__(client, transport=transport, auth=auth)
        self.journal, self.save_token = journal, save_token

    @staticmethod
    def parse_packet_payload(payload):
        body = bounded_payload(payload)
        try:
            return json.loads(body)
        except (ValueError, UnicodeDecodeError):
            return body

    def connect(self, register=False):
        if register:
            raise ProbeError('automatic_association_forbidden')
        self.transport.connect()
        self._send(self.connect_packet())
        packet = decode_packet(self._recv_packet())
        if packet.packet_type != 'connack' or packet.return_code != 0:
            raise ProbeError('broker_connect_rejected')
        payload = self.parse_packet_payload(packet.payload)
        if not isinstance(payload, dict):
            raise ProbeError('invalid_broker_auth')
        previous_identity = (self.auth.user_id, self.auth.device_id)
        self.auth.read(payload)
        if (not 0 < self.auth.user_id < 2**63
                or not isinstance(self.auth.password, str) or not 1 <= len(self.auth.password) <= 4096
                or not isinstance(self.auth.device_id, str) or not 1 <= len(self.auth.device_id) <= 1024
                or not isinstance(self.auth.device_secret, str) or not 1 <= len(self.auth.device_secret) <= 4096):
            raise ProbeError('broker_auth_unproven')
        if (previous_identity[0] and previous_identity[0] != self.auth.user_id
                or previous_identity[1] and previous_identity[1] != self.auth.device_id):
            raise ProbeError('broker_identity_changed')
        self.auth.save(self.client)
        self.emit('auth', self.auth.to_settings())
        self.connected = True

    def dispatch_packet(self, topic, payload):
        parsed = self.parse_packet_payload(payload)
        self.journal.append(topic, parsed)  # durable before the caller's PUBACK
        return super().dispatch_packet(topic, payload)

    def _validated_packet(self):
        raw = self._recv_packet()
        try:
            packet = decode_packet(raw)
        except Exception:
            raise ProbeError('invalid_broker_packet') from None
        if packet.packet_type == 'disconnect':
            raise ProbeError('broker_disconnect')
        if packet.packet_type == 'publish':
            if packet.topic is None or packet.qos not in (0, 1):
                raise ProbeError('invalid_broker_publish')
        elif packet.packet_type == 'pingresp':
            if packet.payload or raw[0] != 0xd0:
                raise ProbeError('invalid_broker_pingresp')
        elif packet.packet_type == '4':  # PUBACK
            # FBNS returned 0x42 with the same two-byte packet ID on the
            # authenticated broker diagnostic; token response and PONG followed.
            if len(packet.payload) != 2 or raw[0] not in (0x40, 0x42):
                raise ProbeError('invalid_broker_puback')
        elif packet.packet_type == '9':  # SUBACK for our single-topic subscription
            if len(packet.payload) != 3 or raw[0] != 0x90:
                raise ProbeError('invalid_broker_suback')
            if packet.payload[2] not in (0, 1):
                raise ProbeError('broker_subscription_rejected')
        else:
            raise ProbeError('unexpected_broker_packet')
        return packet

    def read_once(self):
        packet = self._validated_packet()
        if packet.packet_type == 'publish':
            self.dispatch_packet(packet.topic, packet.payload)
            self._ack(packet)
        return packet

    def ping(self, max_packets=5):
        if not self.connected:
            raise ProbeError('broker_not_connected')
        self._send(write_pingreq_packet())
        for _ in range(max_packets):
            if self.read_once().packet_type == 'pingresp':
                return True
        return False

    def register_token(self, max_packets=10):
        self._publish_bytes(FBNSTopics.REG_REQUEST, json.dumps({
            'pkg_name': 'com.instagram.android', 'appid': 567310203415052},
            separators=(',', ':')).encode())
        for _ in range(max_packets):
            packet = self._validated_packet()
            if packet.packet_type != 'publish':
                continue
            if packet.topic == FBNSTopics.REG_RESPONSE:
                payload = self.parse_packet_payload(packet.payload)
                self.journal.append(packet.topic, payload)
                if not isinstance(payload, dict) or payload.get('error'):
                    raise ProbeError('broker_token_rejected')
                token = payload.get('token')
                if not isinstance(token, str) or not 1 <= len(token) <= 8192 or any(c.isspace() for c in token):
                    raise ProbeError('invalid_broker_token')
                self.save_token(token)
                self._ack(packet)
                return token
            self.dispatch_packet(packet.topic, packet.payload)
            self._ack(packet)
        raise ProbeError('broker_token_timeout')


class Probe:
    def __init__(self, directory, session=None, transport_factory=BoundedTransport,
                 instagram_factory=InstagramClient, clock=time.time, monotonic=time.monotonic,
                 registration_probe=False):
        self.directory = Path(directory)
        self.state_path = self.directory / 'fbns-state.json'
        self.session = Path(session) if session else self.directory / 'session.json'
        self.transport_factory, self.instagram_factory = transport_factory, instagram_factory
        self.clock, self.monotonic = clock, monotonic
        self.registration_probe = registration_probe is True
        self.state = _read_json(self.state_path, missing=True)
        if self.state is None:
            self.state = {'schema': 1, 'uuid': str(uuid.uuid4()), 'phoneId': str(uuid.uuid4()),
                          'familyDeviceId': str(uuid.uuid4()), 'auth': {}, 'token': ''}
            self.save()
        if (self.state.get('schema') != 1 or not isinstance(self.state.get('auth'), dict)
                or not isinstance(self.state.get('token'), str)):
            raise ProbeError('invalid_state')
        for field in ('uuid', 'phoneId', 'familyDeviceId'):
            try:
                uuid.UUID(self.state[field])
            except (ValueError, TypeError, KeyError, AttributeError):
                raise ProbeError('invalid_identity') from None

    def save(self):
        _atomic_json(self.state_path, self.state)

    def _client(self):
        local = self.instagram_factory(self.session)
        self._check_account(local)
        adapter = SimpleNamespace(settings={'fbns_auth': self.state['auth']},
                                  phone_id=self.state['phoneId'], user_agent=local.config['user_agent'])
        auth = FbnsDeviceAuth.from_client(adapter)
        journal = Journal(self.directory / 'fbns-journal.jsonl', self.clock)

        def save_auth(value):
            self.state['auth'] = value
            self.save()

        def save_token(value):
            if self.state.get('association') and value != self.state['token']:
                raise ProbeError('associated_token_change_forbidden')
            self.state['token'] = value
            self.state['preparedAt'] = self.clock()
            self.save()

        client = ProbeClient(adapter, self.transport_factory(), auth, journal, save_token)
        client.on('auth', save_auth)
        return client, journal

    def prepare(self):
        if self.state.get('association') and not self.state.get('token'):
            raise ProbeError('associated_token_missing')
        client, journal = self._client()
        try:
            self._connect(client)
            client.subscribe(FBNSTopics.MESSAGE)
            if not self.state['token']:
                client.register_token()
            pong = client.ping()
            return {'ok': pong, 'mode': 'prepare', 'status': 'prepared' if pong else 'ping_unconfirmed',
                    'accountAssociated': bool(self.state.get('association', {}).get('status') == 'accepted')}
        finally:
            client.disconnect()

    def retry_not_before(self):
        path = self.directory / 'monitor.sqlite'
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise ProbeError('invalid_monitor_state')
        try:
            with contextlib.closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro',
                                                   uri=True, timeout=2)) as db:
                row = db.execute('SELECT value FROM state WHERE id=1').fetchone()
            value = json.loads(row[0]) if row and len(row[0]) <= MAX_STATE else None
            until = value.get('retryNotBefore') if isinstance(value, dict) else None
            if type(until) not in (int, float) or not math.isfinite(until):
                raise ValueError()
        except (ValueError, TypeError, sqlite3.Error):
            raise ProbeError('invalid_monitor_state') from None
        own = self.state.get('httpRetryNotBefore', 0)
        if type(own) not in (int, float) or not math.isfinite(own):
            raise ProbeError('invalid_state')
        # Explicit, audited exception for the source backoff verified by the operator.
        # It never edits the source DB or bypasses an FBNS HTTP guard.
        last = value.get('lastResult', {})
        eligible = (self.registration_probe and until > self.clock()
                    and value.get('sourceStatus') == 'rate_limited'
                    and isinstance(last, dict) and last.get('reason') == 'http_429')
        if eligible:
            exception = self.state.get('sourceBackoffException')
            if exception is None:
                exception = {'authorizedAt': self.clock(), 'sourceRetryNotBefore': until,
                             'scope': 'fbns_registration_probe'}
                self.state['sourceBackoffException'] = exception
                self.save()
            bound = exception.get('sourceRetryNotBefore') if isinstance(exception, dict) else None
            if (isinstance(exception, dict) and exception.get('scope') == 'fbns_registration_probe'
                    and type(bound) in (int, float) and math.isfinite(bound) and until <= bound):
                until = 0
        return max(until, own)

    def _cooldown(self):
        if self.clock() < self.retry_not_before():
            raise ProbeError('source_cooldown_active')

    def _connect(self, client):
        self._cooldown()
        now = self.clock()
        previous = self.state.get('connections', [])
        if (not isinstance(previous, list) or any(type(value) not in (int, float)
                or not math.isfinite(value) for value in previous)):
            raise ProbeError('invalid_connection_budget')
        attempts = [value for value in previous if value > now - 3600]
        if len(attempts) >= 3:
            raise ProbeError('connection_budget_exhausted')
        self.state['connections'] = attempts + [now]
        self.save()  # Reboots and interrupted handshakes do not reset this cap.
        client.connect(register=False)

    @staticmethod
    def _identity(local, include_session=False):
        values = {}
        names = ('ds_user_id', 'csrftoken', 'sessionid') if include_session else ('ds_user_id', 'csrftoken')
        for name in names:
            candidates = {c.value for c in local.jar if c.name == name and c.value}
            if len(candidates) != 1:
                raise ProbeError('session_identity_unproven')
            values[name] = candidates.pop()
            if (not isinstance(values[name], str) or len(values[name]) > 8192
                    or any(ord(c) < 33 or ord(c) == 127 for c in values[name])):
                raise ProbeError('session_identity_unproven')
        if not re.fullmatch(r'[0-9]{1,30}', values['ds_user_id']):
            raise ProbeError('session_identity_unproven')
        return values

    def _check_account(self, local):
        association = self.state.get('association')
        if association and association.get('userId') != self._identity(local)['ds_user_id']:
            raise ProbeError('session_account_changed')

    def _mobile_profile(self):
        android_id = 'android-' + hashlib.sha256(self.state['uuid'].encode()).hexdigest()[:16]
        expected = {**MOBILE_PROFILE, 'androidId': android_id}
        profile = self.state.get('mobileProfile')
        if profile is None:
            self.state['mobileProfile'] = expected
            self.save()
        elif profile != expected:
            raise ProbeError('mobile_profile_changed')

    def _binding(self, values):
        return {'tokenSha256': hashlib.sha256(self.state['token'].encode()).hexdigest(),
                'userSha256': hashlib.sha256(values['ds_user_id'].encode()).hexdigest(),
                'sessionSha256': hashlib.sha256(values['sessionid'].encode()).hexdigest()}

    def _http_exchange(self, local, request):
        code, raw, truncated, result, transport_reason = None, b'', False, None, ''
        try:
            try:
                response = local.opener.open(request, timeout=20)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                code = getattr(response, 'status', getattr(response, 'code', None))
                if code == 429:
                    self.state['httpRetryNotBefore'] = self.clock() + max(
                        21600, _retry_after_seconds(response.headers.get('Retry-After')))
                    self.save()
                raw = response.read(MAX_HTTP_BODY + 1)
                truncated, raw = len(raw) > MAX_HTTP_BODY, raw[:MAX_HTTP_BODY]
                if not truncated:
                    try:
                        result = json.loads(raw)
                    except (ValueError, UnicodeDecodeError, RecursionError):
                        pass
        except (OSError, ValueError, TypeError, urllib.error.URLError):
            transport_reason = 'transport_error'
        return code, raw, truncated, result, transport_reason

    def _http_diagnostic(self, stage, exchange, include_error_body=False):
        code, raw, truncated, result, transport_reason = exchange
        diagnostic = {'httpStatus': code if type(code) is int else None, 'bytes': len(raw),
                      'sha256': hashlib.sha256(raw).hexdigest(), 'truncated': truncated,
                      'reason': transport_reason or _response_reason(result)}
        if include_error_body:
            diagnostic['bodyBase64'] = base64.b64encode(raw).decode()
        Journal(self.directory / 'fbns-http-journal.jsonl', self.clock).append(stage, diagnostic)
        return diagnostic['reason']

    def validate_account(self):
        if self.state.get('accountValidation'):
            raise ProbeError('account_validation_already_attempted')
        if not self.state.get('token') or not self.state.get('auth'):
            raise ProbeError('prepare_required')
        self._cooldown()
        local = self.instagram_factory(self.session)
        self._check_account(local)
        values = self._identity(local, include_session=True)
        self._legacy_eligibility(values)
        self._mobile_profile()
        request = urllib.request.Request(CURRENT_USER_URL, method='GET',
                                         headers=mobile_headers(self.state, values))
        self.state['accountValidation'] = {'status': 'uncertain', 'matched': False,
                                          'startedAt': self.clock(), 'contractVersion': MOBILE_CONTRACT,
                                          **self._binding(values)}
        self.save()  # A crash or lost GET response cannot authorize a repeated check.
        exchange = self._http_exchange(local, request)
        code, raw, truncated, result, transport_reason = exchange
        matched = False
        if (code == 200 and not truncated and isinstance(result, dict)
                and result.get('status') == 'ok' and isinstance(result.get('user'), dict)):
            pk = result['user'].get('pk')
            matched = type(pk) in (str, int) and str(pk) == values['ds_user_id']
        status = 'accepted' if matched else 'rejected' if (
            code == 200 or type(code) is int and 400 <= code < 500) else 'uncertain'
        reason = 'same_account_verified' if matched else (
            'account_mismatch' if code == 200 and isinstance(result, dict)
            and result.get('status') == 'ok' and isinstance(result.get('user'), dict)
            else 'http_' + str(code) if type(code) is int and 400 <= code < 500
            else 'response_unproven')
        # A successful account body contains PII and is never persisted.
        if not matched:
            self._http_diagnostic('validate_account', exchange)
        self.state['accountValidation'].update(status=status, matched=matched,
                                                reason=reason, checkedAt=self.clock())
        self.save()
        return {'ok': matched, 'mode': 'validate_account', 'status': status, 'matched': matched,
                'reason': reason, 'contractVersion': MOBILE_CONTRACT}

    def _legacy_eligibility(self, values):
        old = self.state.get('association')
        history = self.state.get('associationHistory', [])
        def valid_record(item):
            if (not isinstance(item, dict) or item.get('status') not in ('accepted', 'rejected', 'uncertain')
                    or not isinstance(item.get('userId'), str)
                    or not re.fullmatch(r'[0-9]{1,30}', item['userId'])
                    or not isinstance(item.get('tokenSha256'), str)
                    or not re.fullmatch(r'[a-f0-9]{64}', item['tokenSha256'])
                    or type(item.get('startedAt')) not in (int, float)
                    or not math.isfinite(item['startedAt'])):
                return False
            try:
                uuid.UUID(item.get('attemptId'))
                return True
            except (ValueError, TypeError, AttributeError):
                return False
        if (not isinstance(old, dict) or old.get('status') != 'rejected'
                or old.get('reason') != 'http_400' or old.get('contractVersion') or old.get('corrective')
                or old.get('errorReason') in ('challenge_required', 'checkpoint_required', 'login_required',
                                               'consent_required', 'feedback_required')
                or not valid_record(old) or not isinstance(history, list)
                or any(not valid_record(item) for item in history)
                or any(item.get('contractVersion') == MOBILE_CONTRACT or item.get('corrective') for item in history)
                or old.get('userId') != values['ds_user_id']
                or old.get('tokenSha256') != self._binding(values)['tokenSha256']):
            raise ProbeError('corrective_association_ineligible')
        return old, history

    def _corrective_eligibility(self, values):
        old, history = self._legacy_eligibility(values)
        validation = self.state.get('accountValidation')
        checked = validation.get('checkedAt') if isinstance(validation, dict) else None
        if (not isinstance(validation, dict) or validation.get('status') != 'accepted'
                or validation.get('matched') is not True or validation.get('contractVersion') != MOBILE_CONTRACT
                or type(checked) not in (int, float) or not math.isfinite(checked)
                or not 0 <= self.clock() - checked <= 300
                or any(validation.get(key) != value for key, value in self._binding(values).items())):
            raise ProbeError('fresh_account_validation_required')
        return old, history

    def associate(self, correct=False):
        if self.state.get('association') and not correct:
            raise ProbeError('association_already_attempted')
        if not self.state.get('token') or not self.state.get('auth'):
            raise ProbeError('prepare_required')
        self._cooldown()
        local = self.instagram_factory(self.session)
        values = self._identity(local, include_session=correct)
        prior = self._corrective_eligibility(values) if correct else None
        if correct:
            self._mobile_profile()
        data = {'device_type': 'android_mqtt', 'is_main_push_channel': 'true',
                'device_sub_type': '2', 'device_token': self.state['token'],
                '_csrftoken': values['csrftoken'], 'guid': self.state['uuid'],
                'uuid': self.state['uuid'], 'users': values['ds_user_id'],
                'family_device_id': self.state['familyDeviceId'], 'phone_id': self.state['phoneId']}
        headers = mobile_headers(self.state, values) if correct else {
            'Accept': 'application/json', 'User-Agent': local.config['user_agent'],
            'X-CSRFToken': values['csrftoken']}
        headers['Content-Type'] = 'application/x-www-form-urlencoded'
        request = urllib.request.Request(REGISTER_URL, urllib.parse.urlencode(data).encode(), method='POST', headers=headers)
        if not correct:
            local.jar.add_cookie_header(request)
            if not request.has_header('Cookie'):
                raise ProbeError('session_cookie_scope_invalid')
        if correct:
            old, history = prior
            self.state['associationHistory'] = [*history, dict(old)]
        self.state['association'] = {'status': 'uncertain', 'attemptId': str(uuid.uuid4()),
                                     'startedAt': self.clock(), 'userId': values['ds_user_id'],
                                     'isMainPushChannel': True,
                                     'tokenSha256': hashlib.sha256(self.state['token'].encode()).hexdigest()}
        if correct:
            self.state['association'].update(contractVersion=MOBILE_CONTRACT, corrective=True)
        self.save()  # An interrupted/lost response never authorizes a second POST.
        status, reason = 'uncertain', 'response_uncertain'
        exchange = self._http_exchange(local, request)
        code, raw, truncated, result, transport_reason = exchange
        if type(code) is int and 400 <= code < 500:
            status, reason = 'rejected', 'http_' + str(code)
        elif (type(code) is int and 200 <= code < 300 and not truncated
              and isinstance(result, dict) and result.get('status') == 'ok'):
            status, reason = 'accepted', 'register_accepted'
        diagnostic_reason = ''
        if status != 'accepted':
            diagnostic_reason = self._http_diagnostic('register', exchange, include_error_body=True)
        self.state['association'].update(status=status, reason=reason, finishedAt=self.clock(),
                                         errorReason=diagnostic_reason)
        self.save()
        return {'ok': status == 'accepted', 'mode': 'correct_association' if correct else 'associate',
                'status': status, 'reason': reason}

    def correct_association(self):
        return self.associate(correct=True)

    def observe(self, seconds, internal=False):
        if type(seconds) is not int or not 1 <= seconds <= (3600 if internal else 600):
            raise ProbeError('invalid_observation_duration')
        if not self.state.get('token'):
            raise ProbeError('prepare_required')
        client, journal = self._client()
        start, before, matched = self.monotonic(), journal.records, journal.story_matches
        deadline, ping_at, pong_deadline = start + seconds, start + 45, None
        try:
            client.transport.timeout = min(20, seconds)
            self._connect(client)
            client.subscribe(FBNSTopics.MESSAGE)
            while self.monotonic() < deadline:
                now = self.monotonic()
                if pong_deadline is not None and now >= pong_deadline:
                    raise ProbeError('broker_pong_timeout')
                if pong_deadline is None and now >= ping_at and deadline - now > 20:
                    client._send(write_pingreq_packet())
                    pong_deadline = now + 20
                if getattr(client.transport, 'sock', None):
                    remaining = min(deadline, pong_deadline or deadline) - self.monotonic()
                    client.transport.sock.settimeout(min(5, max(.1, remaining)))
                try:
                    packet = client.read_once()
                    if packet.packet_type == 'pingresp':
                        pong_deadline, ping_at = None, self.monotonic() + 45
                except TimeoutError:
                    pass
            if pong_deadline is not None:
                raise ProbeError('broker_pong_timeout')
            return {'ok': True, 'mode': 'observe', 'status': 'observation_complete',
                    'records': journal.records - before, 'testStoryMatches': journal.story_matches - matched,
                    'seconds': seconds}
        finally:
            client.disconnect()

    def serve(self, sleep=time.sleep, max_windows=None):
        """Diagnostic-only listener; one durable association, bounded read windows.

        main-channel association was explicitly authorized for this test account.
        No journal entry is forwarded into the production push or delivery DBs.
        """
        def status(value, **fields):
            self.state['service'] = {'status': value, 'updatedAt': self.clock(), **fields}
            self.save()

        while self.clock() < self.retry_not_before():
            until = self.retry_not_before()
            status('waiting_source_cooldown', retryNotBefore=until)
            sleep(min(30, until - self.clock()))
        if not self.state.get('token'):
            status('preparing')
            try:
                prepared = self.prepare()
            except Exception:
                status('prepare_failed_review_required')
                raise
            if not prepared['ok']:
                status('prepare_unconfirmed_review_required')
                return prepared
        while not self.state.get('association'):
            until = self.retry_not_before()
            if self.clock() < until:
                status('waiting_source_cooldown', retryNotBefore=until)
                sleep(min(30, until - self.clock()))
                continue
            status('associating')
            result = self.associate()
            if not result['ok']:
                status('association_' + result['status'])
                return result
        association = self.state['association']
        if association.get('status') != 'accepted':
            status('association_' + str(association.get('status', 'uncertain')))
            return {'ok': False, 'mode': 'serve', 'status': 'association_blocked',
                    'reason': 'association_already_attempted'}
        self._check_account(self.instagram_factory(self.session))
        windows, failures = 0, 0
        while max_windows is None or windows < max_windows:
            status('observing', windows=windows, failures=failures)
            try:
                result = self.observe(3600, internal=True)
                failures = 0
                windows += 1
                self.state['serviceTotals'] = {
                    'records': self.state.get('serviceTotals', {}).get('records', 0) + result['records'],
                    'testStoryMatches': self.state.get('serviceTotals', {}).get('testStoryMatches', 0)
                    + result['testStoryMatches']}
                status('window_complete', windows=windows, failures=failures)
            except (ProbeError, OSError, ValueError, IndexError, KeyError) as error:
                if isinstance(error, ssl.SSLError) or not isinstance(error, OSError) and not (
                        isinstance(error, ProbeError) and error.reason == 'broker_closed'):
                    status('review_required')
                    raise
                failures += 1
                delay = min(300, 60 * failures)
                status('broker_reconnect_wait', windows=windows, failures=failures,
                       retryNotBefore=self.clock() + delay)
                sleep(delay)
        return {'ok': True, 'mode': 'serve', 'status': 'windows_complete', 'windows': windows}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    for mode in ('prepare', 'associate', 'observe', 'serve', 'validate-account', 'correct-association'):
        modes.add_argument('--' + mode, action='store_true')
    parser.add_argument('--directory', default='/var/lib/uol-instagram-monitor')
    parser.add_argument('--session')
    parser.add_argument('--seconds', type=int, default=120)
    parser.add_argument('--registration-probe', action='store_true',
                        help='Explicitly authorized FBNS exception to the recorded source backoff')
    args = parser.parse_args(argv)
    mode = ('prepare' if args.prepare else 'associate' if args.associate
            else 'observe' if args.observe else 'validate_account' if args.validate_account
            else 'correct_association' if args.correct_association else 'serve')
    try:
        with locked_directory(args.directory):
            probe = Probe(args.directory, args.session, registration_probe=args.registration_probe)
            result = getattr(probe, mode)(args.seconds) if mode == 'observe' else getattr(probe, mode)()
    except (ProbeError, SessionError) as error:
        result = {'ok': False, 'mode': mode, 'status': 'blocked', 'reason': error.reason}
    except Exception:
        # Broker, urllib, and JSON exceptions may include secrets or payloads.
        result = {'ok': False, 'mode': mode, 'status': 'failed', 'reason': 'diagnostic_failed'}
    print(json.dumps(result, separators=(',', ':')))
    # A service must not restart an uncertain association or a protocol/auth gate.
    return 0 if result['ok'] else 78 if mode == 'serve' else 1


if __name__ == '__main__':
    raise SystemExit(main())
