"""Narrow, verified local MacSend registry patch. Does not restart or stage it."""
import ast
import hashlib
import os
import tempfile
from pathlib import Path

source = Path.home() / 'Library/Application Support/LocalSendClipboardSync/macsend_server.py'
original = source.read_bytes()

def registry(code):
    for item in ast.parse(code).body:
        if isinstance(item, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'SCRIPTABLE_SCRIPTS' for t in item.targets):
            return ast.literal_eval(item.value)
    raise ValueError('Missing registry')

before = registry(original)
desired = {**before, 'clube-uol': {**before['clube-uol'], 'filename': 'Clube UOL Premium.js', 'display_name': 'Clube UOL Premium'}}
updated = original.replace(b'"filename": "Clube UOL - Stories Hunter.js"', b'"filename": "Clube UOL Premium.js"').replace(b'"display_name": "Clube UOL - Stories Hunter"', b'"display_name": "Clube UOL Premium"')
assert registry(updated) == desired, 'Unexpected registry; preserve original'
if updated != original:
    directory = Path.home() / '.config/uol-ticket-research/premium-macsend-backups'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    backup = directory / ('macsend-' + hashlib.sha256(original).hexdigest() + '.py')
    backup.write_bytes(original)
    backup.chmod(0o600)
    assert backup.read_bytes() == original
    assert source.read_bytes() == original, 'Concurrent source change'
    fd, temporary = tempfile.mkstemp(prefix='.macsend-premium-', dir=source.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(updated)
        os.chmod(temporary, source.stat().st_mode & 0o777)
        os.replace(temporary, source)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
    assert source.read_bytes() == updated
print('MacSend registry verified: only UOL filename/display_name changed; other scripts and routing preserved.')
