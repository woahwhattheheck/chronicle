import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time

ROOT = Path.cwd()
EVIDENCE = ROOT / 'flight-evidence'
EVIDENCE.mkdir()
BASE = '0c6cb51397f4c564e6c8595139796b04bb144b95'
TEST = 'arrow_flight_sql_test.go'
PRODUCT = 'arrow_flight_sql.go'
COMMAND = ['go', 'test', '-short', '-count=1', '-timeout=90s', '-run',
           '^TestFlightSQLServer_(HandleStatementQuery|StatementIdentity)$', '-json', '.']

def git(*args, cwd=ROOT):
    return subprocess.check_output(['git', *args], cwd=cwd).decode().strip()

def blob(path):
    data = Path(path).read_bytes()
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()

def run(label, directory):
    out = EVIDENCE / (label + '.jsonl')
    err = EVIDENCE / (label + '.stderr.log')
    started = time.time()
    with out.open('wb') as stdout, err.open('wb') as stderr:
        result = subprocess.run(COMMAND, cwd=directory, stdout=stdout, stderr=stderr, timeout=300)
    rows = []
    for line in out.read_text(encoding='utf-8').splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    tests = [row for row in rows if row.get('Test') and row.get('Action') in ('pass', 'fail', 'skip')]
    record = {
        'label': label,
        'exit_code': result.returncode,
        'wall_seconds': time.time() - started,
        'command': COMMAND,
        'tests': tests,
        'passed_events': sum(row['Action'] == 'pass' for row in tests),
        'failed_events': sum(row['Action'] == 'fail' for row in tests),
        'skipped_events': sum(row['Action'] == 'skip' for row in tests),
        'identity_observations': [row['Output'].strip() for row in rows if 'changed_bindings=' in row.get('Output', '')],
        'source_blobs': {name: blob(directory / name) for name in (PRODUCT, TEST)},
        'diff_stat': git('diff', '--stat', cwd=directory),
    }
    (EVIDENCE / (label + '.summary.json')).write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(record, indent=2), flush=True)
    return record

BASELINE = ROOT.parent / 'flight-baseline'
subprocess.run(['git', 'worktree', 'add', '--detach', str(BASELINE), BASE], check=True)
shutil.copyfile(ROOT / 'work/validation/chronicle104-flightsql-relay17/baseline-test.txt', BASELINE / TEST)
# The baseline adds only the same public-API regression; its product and original
# now/now+1ns query fixture stay unchanged.
subprocess.run(['gofmt', '-w', str(BASELINE / TEST)], check=True)
subprocess.run(['gofmt', '-w', PRODUCT, TEST], check=True)
formatter = subprocess.check_output(['gofmt', '-l', PRODUCT, TEST]).decode()
assert not formatter.strip(), formatter
provenance = {
    'baseline_commit': BASE,
    'controller_commit': git('rev-parse', 'HEAD'),
    'platform': platform.platform(),
    'go_environment': json.loads(subprocess.check_output(['go', 'env', '-json', 'GOVERSION', 'GOOS', 'GOARCH', 'CGO_ENABLED'])),
    'scope': 'Native Windows complete repository, two focused root-package groups; no mocks, altered clock, full suite or network-client claim.',
    'formatter_output': formatter,
}
(EVIDENCE / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
before = run('baseline', BASELINE)
after = run('candidate', ROOT)
for label, directory in [('baseline', BASELINE), ('candidate', ROOT)]:
    for name in (PRODUCT, TEST):
        shutil.copyfile(directory / name, EVIDENCE / (label + '-' + name + '.txt'))
    (EVIDENCE / (label + '.diff')).write_text(git('diff', '--', PRODUCT, TEST, cwd=directory) + '\n')
(EVIDENCE / 'comparison.json').write_text(json.dumps({'provenance': provenance, 'baseline': before, 'candidate': after}, indent=2) + '\n')
assert after['exit_code'] == 0, 'candidate focused tests failed'
assert after['skipped_events'] == 0, 'candidate skipped a selected test'
assert any(row['Action'] == 'fail' and row['Test'].startswith('TestFlightSQLServer_StatementIdentity/') for row in before['tests']), 'baseline did not reproduce ticket identity failure'
