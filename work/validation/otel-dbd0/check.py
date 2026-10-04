from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

BASE = '7b8512b6fcbe6ac1942b73cfe7911fcdad63660d'
PRODUCTION = 'internal/oteldistro/chronicle_o_tel_distro.go'
EXISTING_TEST = 'internal/oteldistro/oteldistro_test.go'
NEW_TEST = 'internal/oteldistro/metrics_lifecycle_test.go'
EXPECTED = {PRODUCTION: 'b0a5f54452e32e3081c14e157ac2c61da9ce2e45', EXISTING_TEST: '8dbc994da4a34e00399de15f5b5a7b5efbfb8ec8'}
OLD_PUSH = '''\tif !ok || !pipeline.running {
\t\treturn
\t}

\tatomic.AddInt64(&d.metrics.MetricsReceived, 1)
'''
NEW_PUSH = '''\tif !ok {
\t\treturn
\t}

\t// stopPipeline closes dataChan under this same lock. Keep the running
\t// check and nonblocking send in one admission interval with shutdown.
\tpipeline.mu.Lock()
\tdefer pipeline.mu.Unlock()
\tif !pipeline.running {
\t\treturn
\t}

\tatomic.AddInt64(&d.metrics.MetricsReceived, 1)
'''
OLD_CLOCK = '''\tif metrics.Uptime <= 0 {
\t\tt.Error("Expected positive uptime")
\t}
'''
NEW_CLOCK = '''\t// Construction and observation may occur in the same clock tick.
\tif metrics.Uptime < 0 {
\t\tt.Error("Expected nonnegative initial uptime")
\t}

\t// Exercise positive elapsed time without relying on sleeps or clock
\t// resolution. No distro workers have been started in this fixture.
\tstart := time.Now().Add(-time.Second)
\tdistro.metrics.mu.Lock()
\tdistro.metrics.StartTime = start
\tdistro.metrics.mu.Unlock()
\tmetrics = distro.GetMetrics()
\tif !metrics.StartTime.Equal(start) {
\t\tt.Error("Expected the recorded start time to be preserved")
\t}
\tif metrics.Uptime < time.Second || metrics.Uptime > time.Since(start) {
\t\tt.Errorf("Expected uptime derived from the elapsed fixture interval, got %v", metrics.Uptime)
\t}
'''

def git(*args: str) -> str:
    return subprocess.check_output(['git', *args], text=True, cwd=subject).strip()

def blob(data: bytes) -> str:
    return hashlib.sha1(f'blob {len(data)}\0'.encode()+data).hexdigest()

def run_test(label: str) -> dict:
    command = ['go','test','-short','-count=1','-timeout=60s','-json','-run','^(TestDistroMetrics|TestPushMetricsDuringStop)$','./internal/oteldistro']
    start = time.monotonic()
    result = subprocess.run(command, cwd=subject, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    (evidence/f'{label}.jsonl').write_text(result.stdout,encoding='utf8')
    (evidence/f'{label}.stderr').write_text(result.stderr,encoding='utf8')
    events=[]
    for line in result.stdout.splitlines():
        try: event=json.loads(line)
        except ValueError: continue
        if event.get('Action') in {'pass','fail','skip'} and event.get('Test'):
            events.append({k:event[k] for k in ('Action','Test','Elapsed') if k in event})
        if event.get('Action')=='output' and event.get('Test'):
            print(event.get('Output','').rstrip())
    print(result.stderr)
    return {'command':command,'exit_code':result.returncode,'seconds':round(time.monotonic()-start,6),'events':events}

subject = Path(sys.argv[1]).resolve()
evidence = Path(sys.argv[2]).resolve()
evidence.mkdir(parents=True,exist_ok=False)
receipt={'base':BASE,'repository':os.environ['GITHUB_REPOSITORY'],'run_id':os.environ['GITHUB_RUN_ID'],'os':os.name,'python':sys.version,'go':subprocess.check_output(['go','version'],text=True).strip(),'status':'STARTED'}
try:
    assert git('rev-parse','HEAD')==BASE, 'unexpected subject commit'
    assert git('status','--porcelain')=='', 'dirty baseline'
    original={path:(subject/path).read_bytes() for path in EXPECTED}
    for path,expected in EXPECTED.items():
        assert blob(original[path])==expected, (path,'blob mismatch')
    assert not (subject/NEW_TEST).exists(), 'new regression already exists'
    shutil.copyfile(Path(__file__).with_name('lifecycle_test.go'), subject/NEW_TEST)
    # Baseline retains the old clock assertion and all original production.
    receipt['before']=run_test('before')
    prod=original[PRODUCTION].decode('utf8')
    tests=original[EXISTING_TEST].decode('utf8')
    assert prod.count(OLD_PUSH)==1 and tests.count(OLD_CLOCK)==1, 'patch context mismatch'
    (subject/PRODUCTION).write_bytes(prod.replace(OLD_PUSH,NEW_PUSH).encode('utf8'))
    (subject/EXISTING_TEST).write_bytes(tests.replace(OLD_CLOCK,NEW_CLOCK).encode('utf8'))
    subprocess.run(['gofmt','-w',PRODUCTION,EXISTING_TEST,NEW_TEST],cwd=subject,check=True)
    receipt['after']=run_test('after')
    passes={e['Test'] for e in receipt['after']['events'] if e['Action']=='pass'}
    assert receipt['after']['exit_code']==0 and passes=={'TestDistroMetrics','TestPushMetricsDuringStop'}, 'selected candidate cases did not pass'
    assert not any(e['Action'] in {'fail','skip'} for e in receipt['after']['events']), 'failed/skipped candidate case'
    baseline_failures={e['Test'] for e in receipt['before']['events'] if e['Action']=='fail'}
    assert 'TestPushMetricsDuringStop' in baseline_failures, 'shutdown defect did not reproduce; do not publish an unmeasured repair'
    paths=[PRODUCTION,EXISTING_TEST,NEW_TEST]
    receipt['blobs']={path:blob((subject/path).read_bytes()) for path in paths}
    receipt['sha256']={path:hashlib.sha256((subject/path).read_bytes()).hexdigest() for path in paths}
    for path in paths:
        output=evidence/'source'/path
        output.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(subject/path,output)
    (evidence/'change.patch').write_text(git('diff','--',PRODUCTION,EXISTING_TEST)+'\n',encoding='utf8')
    git('add','--',*paths)
    assert set(git('diff','--cached','--name-only').splitlines())==set(paths), 'unexpected staged paths'
    git('-c','user.name=woahwhattheheck','-c','user.email=293286387+woahwhattheheck@users.noreply.github.com','commit','-m','fix(otel): serialize ingestion with pipeline shutdown and stabilize uptime fixture [skip ci]')
    receipt['candidate']=git('rev-parse','HEAD')
    receipt['tree']=git('rev-parse','HEAD^{tree}')
    assert git('status','--porcelain')=='', 'unexpected candidate drift'
    # Retain one new candidate only; the original bounty branch is not pushed here.
    ref='fix/windows-otel-lifecycle-dbd0-20261004'
    git('push','origin',f'HEAD:refs/heads/{ref}')
    receipt['candidate_ref']=ref
    receipt['status']='PASSED_AND_RETAINED'
finally:
    (evidence/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf8')
    print(json.dumps(receipt,indent=2))
