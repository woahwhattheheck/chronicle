import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time

ROOT = Path.cwd()
EVIDENCE = ROOT / 'grpc-evidence'
EVIDENCE.mkdir()
BASE = 'efb80aefe3af26183e765607ea013ca2d3ab2b1b'
PRODUCT = 'grpc_ingestion.go'
TEST = 'grpc_ingestion_write_test.go'
COMMAND = ['go', 'test', '-short', '-count=1', '-timeout=90s', '-run',
           '^(TestGRPCIngestionEngine|TestGRPCHandleQuery(WithAggregation|AggregateBuckets|ReturnsResults|NilRequest))$', '-json', '.']

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
        'label': label, 'exit_code': result.returncode,
        'wall_seconds': time.time() - started, 'command': COMMAND, 'tests': tests,
        'passed_events': sum(row['Action'] == 'pass' for row in tests),
        'failed_events': sum(row['Action'] == 'fail' for row in tests),
        'skipped_events': sum(row['Action'] == 'skip' for row in tests),
        'source_blobs': {name: blob(directory / name) for name in (PRODUCT, TEST, 'grpc_ingestion_test.go', 'internal/query/parser.go')},
        'diff_stat': git('diff', '--stat', cwd=directory),
    }
    (EVIDENCE / (label + '.summary.json')).write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(record, indent=2), flush=True)
    return record

BASELINE = ROOT.parent / 'grpc-baseline'
subprocess.run(['git', 'worktree', 'add', '--detach', str(BASELINE), BASE], check=True)
# Run the identical maintained tests on both production implementations.
# Only the appended value/control regression is overlaid on the baseline.
shutil.copyfile(ROOT / TEST, BASELINE / TEST)
subprocess.run(['gofmt', '-w', str(BASELINE / TEST)], check=True)
subprocess.run(['gofmt', '-w', PRODUCT, TEST], check=True)
formatter = subprocess.check_output(['gofmt', '-l', PRODUCT, TEST]).decode()
assert not formatter.strip(), formatter
assert blob(BASELINE / TEST) == blob(ROOT / TEST), 'tests differ between baseline and candidate'
assert blob(BASELINE / PRODUCT) == 'ce7a61a3734742714bcb3a77764f0a88e8ff30ef', 'baseline product changed'
provenance = {
    'baseline_commit': BASE,
    'controller_commit': git('rev-parse', 'HEAD'),
    'controller_tree': git('rev-parse', 'HEAD^{tree}'),
    'platform': platform.platform(),
    'go_environment': json.loads(subprocess.check_output(['go', 'env', '-json', 'GOVERSION', 'GOOS', 'GOARCH', 'CGO_ENABLED'])),
    'scope': 'Native Windows complete repository, five focused root-package groups; real public request handler, database and aggregation. No mocks, replacement clock, full suite, HTTP/network transport or performance claim.',
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
failed = {row['Test'] for row in before['tests'] if row['Action'] == 'fail'}
assert 'TestGRPCIngestionEngine/query_with_aggregation' in failed, 'maintained aggregation case did not reproduce'
assert 'TestGRPCHandleQueryWithAggregation' in failed, 'maintained standalone aggregation case did not reproduce'
assert 'TestGRPCHandleQueryAggregateBuckets/sum' in failed, 'new value regression did not reproduce'
