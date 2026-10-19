"""Generate importable shortcuts from versioned, credential-free sources."""
import argparse
import json
import plistlib
import subprocess
import tempfile
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--sign', action='store_true')
args = parser.parse_args()
root = Path(__file__).resolve().parent
for name in ['Update saquettoOS', 'saquettoOS']:
    data = json.loads((root / (name + '.json')).read_text())
    raw = plistlib.dumps(data, fmt=plistlib.FMT_XML, sort_keys=True)
    if args.sign:
        with tempfile.TemporaryDirectory(prefix='saquettoos-shortcut-') as directory:
            source = Path(directory) / (name + '.shortcut')
            source.write_bytes(raw)
            subprocess.run(['shortcuts', 'sign', '--mode', 'anyone', '--input', str(source),
                            '--output', str(root / (name + '.shortcut'))], check=True, timeout=90)
    else:
        print(name + ': ' + str(len(raw)) + ' bytes unsigned; --sign uses Apple signing service')
