"""Exact-source, focused native lifecycle execution; never changes the PR ref."""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

BASE = 'cf57d7c658acfb91f23b4e1ccc66794187607ce0'
RESULT_REF = 'validation/chronicle104-raft-lifecycle-result-20261004'
root = Path(sys.argv[1]).resolve()
controller = Path(__file__).resolve().parent
evidence = root.parent / 'evidence'
evidence.mkdir(exist_ok=True)

def git(*args):
    return subprocess.check_output(['git', *args], cwd=root, text=True).strip()

def replace_once(path, old, new):
    text = path.read_text(encoding='utf-8')
    if text.count(old) != 1:
        raise RuntimeError('Exact source segment changed: ' + str(path))
    path.write_bytes(text.replace(old, new, 1).encode('utf-8'))

assert sys.platform == 'win32', 'Must execute on native Windows'
assert git('rev-parse', 'HEAD') == BASE
assert git('rev-parse', 'HEAD:internal/raft/core.go') == 'db1c289be88acd496ab20834e8de871cadcfb0d6'
assert git('rev-parse', 'HEAD:internal/raft/log.go') == 'c375931004dc03beedf19b7831f962ccc651c911'
assert not git('status', '--porcelain')
test = root / 'internal/raft/resource_lifecycle_windows_test.go'
assert not test.exists()
shutil.copyfile(controller / test.name, test)
subprocess.run(['gofmt', '-w', str(test)], check=True)
test_digest = hashlib.sha256(test.read_bytes()).hexdigest()
command = ['go', 'test', '-count=1', '-short', '-p=2', '-timeout=90s', '-json', '-run', '^TestRaftResourceLifecycle$', './internal/raft']
record = {'baseline': BASE, 'os': platform.platform(), 'python': sys.version, 'go': subprocess.check_output(['go','version'], text=True).strip(), 'command': command, 'test_sha256': test_digest, 'source_blobs_before': {'core.go': git('hash-object', 'internal/raft/core.go'), 'log.go': git('hash-object', 'internal/raft/log.go')}, 'previous_run': 37191917494, 'harness_correction': 'Use actual Windows file removal, not os.ErrClosed identity from Stat; the first run reproduced all three leaks but falsely failed the closed-handle control.'}
(evidence / 'provenance.json').write_text(json.dumps(record, indent=2), encoding='utf-8')

def execute(label):
    with (evidence / (label + '.jsonl')).open('w', encoding='utf-8') as out, (evidence / (label + '-stderr.txt')).open('w', encoding='utf-8') as err:
        result = subprocess.run(command, cwd=root, stdout=out, stderr=err, timeout=600)
    events = [json.loads(line) for line in (evidence / (label + '.jsonl')).read_text(encoding='utf-8').splitlines() if line.strip()]
    summary = {'exit': result.returncode, 'passed': [e['Test'] for e in events if e.get('Action') == 'pass' and 'Test' in e], 'failed': [e['Test'] for e in events if e.get('Action') == 'fail' and 'Test' in e]}
    (evidence / (label + '-summary.json')).write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(label, json.dumps(summary), flush=True)
    print((evidence / (label + '-stderr.txt')).read_text(encoding='utf-8')[-4000:], flush=True)
    return summary

before = execute('before')
expected_failures = {'TestRaftResourceLifecycle/stop_before_start', 'TestRaftResourceLifecycle/invalid_state', 'TestRaftResourceLifecycle/invalid_log'}
assert before['exit'] != 0 and expected_failures.issubset(before['failed']), 'The exact resource leaks must reproduce before changing source'
assert 'TestRaftResourceLifecycle/started_node' in before['passed'], 'Normal started lifecycle control did not pass'

core = root / 'internal/raft/core.go'
replace_once(core, '\trunning   atomic.Bool\n', '\trunning   atomic.Bool\n\tstopOnce  sync.Once\n\tstopErr   error\n')
replace_once(core, 'if err := rn.loadState(); err != nil {\n\t\tcancel()', 'if err := rn.loadState(); err != nil {\n\t\t_ = log.Close()\n\t\tcancel()')
replace_once(core, 'func (rn *RaftNode) Start() error {\n', 'func (rn *RaftNode) Start() error {\n\tif rn.ctx.Err() != nil {\n\t\treturn errors.New("raft node has been stopped")\n\t}\n')
replace_once(core, '// Stop stops Raft operations.\nfunc (rn *RaftNode) Stop() error {\n\tif !rn.running.Load() {\n\t\treturn nil\n\t}', '// Stop releases node resources, including a log opened before Start.\n// A stopped node cannot be restarted; create a new node to reopen persisted state.\nfunc (rn *RaftNode) Stop() error {\n\trn.stopOnce.Do(func() {\n\t\trn.stopErr = rn.stop()\n\t})\n\treturn rn.stopErr\n}\n\nfunc (rn *RaftNode) stop() error {\n\tif !rn.running.Load() {\n\t\trn.cancel()\n\t\treturn rn.log.Close()\n\t}')
log = root / 'internal/raft/log.go'
replace_once(log, 'if err := rl.load(); err != nil {\n\t\t\treturn nil, err', 'if err := rl.load(); err != nil {\n\t\t\t_ = file.Close()\n\t\t\treturn nil, err')
subprocess.run(['gofmt', '-w', str(core), str(log)], check=True)
assert hashlib.sha256(test.read_bytes()).hexdigest() == test_digest
paths = ['internal/raft/core.go', 'internal/raft/log.go', 'internal/raft/resource_lifecycle_windows_test.go']
assert set(git('diff', '--name-only').splitlines()) == set(paths[:2])
git('add', '--', *paths)
git('-c', 'user.name=woahwhattheheck', '-c', 'user.email=293286387+woahwhattheheck@users.noreply.github.com', 'commit', '-m', 'fix(raft): release logs on constructor errors and pre-start shutdown [skip ci]')
record['candidate'] = git('rev-parse', 'HEAD')
record['candidate_tree'] = git('rev-parse', 'HEAD^{tree}')
record['source_blobs_after'] = {path: git('rev-parse', 'HEAD:' + path) for path in paths}
(evidence / 'candidate.patch').write_bytes(subprocess.check_output(['git', 'diff', BASE, 'HEAD', '--', *paths], cwd=root))
for path in paths:
    destination = evidence / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(root / path, destination)
(evidence / 'provenance.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
after = execute('after')
assert after['exit'] == 0 and not after['failed'], 'Candidate lifecycle failed'
assert expected_failures.issubset(after['passed'])
assert 'TestRaftResourceLifecycle/started_node' in after['passed']
assert not git('status', '--porcelain'), 'Execution changed tracked source'
# A new result ref only. Original bounty-9-windows-testing is never advanced here.
subprocess.run(['git', 'push', 'origin', 'HEAD:refs/heads/' + RESULT_REF], cwd=root, check=True)
record['result_ref'] = RESULT_REF
record['before'] = before
record['after'] = after
(evidence / 'provenance.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
print(json.dumps(record, indent=2), flush=True)
