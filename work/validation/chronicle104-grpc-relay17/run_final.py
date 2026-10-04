import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import time

ROOT = Path.cwd()
EVIDENCE = ROOT / 'grpc-final-evidence'
EVIDENCE.mkdir()
SOURCES = ['grpc_ingestion.go', 'grpc_ingestion_write_test.go', 'index.go', 'index_test.go']
COMMAND = ['go', 'test', '-short', '-count=1', '-timeout=90s', '-run', '^(TestGRPCIngestionEngine|TestGRPCHandleQuery(WithAggregation|AggregateBuckets|ReturnsResults|NilRequest)|TestIndex_(GetOrCreatePartition|FindPartitions(_Empty)?|RemovePartitionsBefore(_Empty)?|RemoveOldestPartition(_Empty)?|RemovePartitionByID(_NotFound)?))$', '-json', '.']
def git(*args):
    return subprocess.check_output(['git', *args]).decode().strip()
def blob(path):
    data = Path(path).read_bytes()
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()

subprocess.run(['gofmt', '-w', *SOURCES], check=True)
formatter = subprocess.check_output(['gofmt', '-l', *SOURCES]).decode()
assert not formatter.strip(), formatter
provenance = {
    'initial_baseline_commit': 'efb80aefe3af26183e765607ea013ca2d3ab2b1b',
    'initial_comparison_run': 37202462701,
    'retained_failure_inspection_run': 37202749969,
    'controller_commit': git('rev-parse', 'HEAD'),
    'controller_tree': git('rev-parse', 'HEAD^{tree}'),
    'platform': platform.platform(),
    'go_environment': json.loads(subprocess.check_output(['go', 'env', '-json', 'GOVERSION', 'GOOS', 'GOARCH', 'CGO_ENABLED'])),
    'scope': 'One native Windows candidate-only selection after the observed overlap-selection failure: the unchanged gRPC values and required maintained index insert/find/recovery/remove coverage. Original baseline is retained without replay. Real database and aggregation; no mock, clock replacement, full suite, transport or performance claim.',
    'formatter_output': formatter,
    'source_blobs': {name: blob(ROOT / name) for name in SOURCES},
}
out = EVIDENCE / 'candidate.jsonl'
err = EVIDENCE / 'candidate.stderr.log'
started = time.time()
with out.open('wb') as stdout, err.open('wb') as stderr:
    result = subprocess.run(COMMAND, stdout=stdout, stderr=stderr, timeout=300)
rows = []
for line in out.read_text(encoding='utf-8').splitlines():
    try:
        rows.append(json.loads(line))
    except json.JSONDecodeError:
        pass
tests = [row for row in rows if row.get('Test') and row.get('Action') in ('pass', 'fail', 'skip')]
record = {
    'label': 'final_candidate', 'exit_code': result.returncode,
    'wall_seconds': time.time() - started, 'command': COMMAND, 'tests': tests,
    'passed_events': sum(row['Action'] == 'pass' for row in tests),
    'failed_events': sum(row['Action'] == 'fail' for row in tests),
    'skipped_events': sum(row['Action'] == 'skip' for row in tests),
    'source_blobs': provenance['source_blobs'],
}
summary = {'provenance': provenance, 'candidate': record}
(EVIDENCE / 'comparison.json').write_text(json.dumps(summary, indent=2) + '\n')
print('SUMMARY_JSON_BEGIN', flush=True)
print(json.dumps(summary), flush=True)
print('SUMMARY_JSON_END', flush=True)
postimages = {name: (ROOT / name).read_text(encoding='utf-8') for name in SOURCES}
print('POSTIMAGE_JSON_BEGIN', flush=True)
print(json.dumps(postimages), flush=True)
print('POSTIMAGE_JSON_END', flush=True)
print('RAW_STDOUT_BEGIN', flush=True)
print(out.read_text(encoding='utf-8'), flush=True)
print('RAW_STDOUT_END', flush=True)
for name in SOURCES:
    shutil.copyfile(ROOT / name, EVIDENCE / (name + '.txt'))
(EVIDENCE / 'source.diff').write_text(git('diff', '--', *SOURCES) + '\n')
assert record['exit_code'] == 0, 'candidate focused tests failed'
assert record['skipped_events'] == 0, 'candidate skipped a selected test'
