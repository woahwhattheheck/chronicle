import base64, hashlib, json, os, pathlib, platform, shutil, subprocess, sys, time

ROOT = pathlib.Path(__file__).resolve().parent
SOURCE = pathlib.Path(sys.argv[1]).resolve()
OUT = pathlib.Path(sys.argv[2]).resolve()
OUT.mkdir(parents=True, exist_ok=True)
BASE = 'cf57d7c658acfb91f23b4e1ccc66794187607ce0'
ENGINE = 'internal/continuousquery/continuous_query_engine.go'
TYPES = 'internal/continuousquery/types.go'
TEST = 'internal/continuousquery/cq_create_regression_test.go'
PINS = {ENGINE: '4a64a1e1dffe38d2fa16197068ebcf01dd094db2', TYPES: '3a516e1d4756ce2242f00c11c2db2a0ca9ef597b'}

def git(*args):
    return subprocess.check_output(['git', *args], cwd=SOURCE, text=True).strip()

def blob(data):
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()

def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError('source anchor does not occur exactly once: ' + repr(old))
    return text.replace(old, new, 1)

def check(label, pattern=None):
    command = ['go', 'test', '-short', '-count=1', '-timeout=90s', '-json']
    if pattern:
        command += ['-run', pattern]
    command += ['./internal/continuousquery']
    start = time.monotonic()
    with (OUT / (label + '.jsonl')).open('wb') as stdout, (OUT / (label + '.stderr')).open('wb') as stderr:
        result = subprocess.run(command, cwd=SOURCE, stdout=stdout, stderr=stderr, timeout=300)
    events = []
    for line in (OUT / (label + '.jsonl')).read_text(encoding='utf-8-sig').splitlines():
        try:
            events.append(json.loads(line))
        except ValueError:
            pass
    return {'command': command, 'exit_code': result.returncode, 'wall_seconds': time.monotonic()-start,
            'passed': [e.get('Test') for e in events if e.get('Action') == 'pass' and e.get('Test')],
            'failed': [e.get('Test') for e in events if e.get('Action') == 'fail' and e.get('Test')],
            'skipped': [e.get('Test') for e in events if e.get('Action') == 'skip' and e.get('Test')]}

if git('rev-parse', 'HEAD') != BASE or git('status', '--porcelain'):
    raise RuntimeError('expected clean pinned subject')
originals = {}
for name, expected in PINS.items():
    data = (SOURCE / name).read_bytes()
    if blob(data) != expected:
        raise RuntimeError('unexpected source blob: ' + name)
    originals[name] = data.decode('utf-8')
    (OUT / ('before-' + pathlib.Path(name).name)).write_bytes(data)
if (SOURCE / TEST).exists():
    raise RuntimeError('regression path already exists')
shutil.copyfile(ROOT / 'cq_create_regression_test.go', SOURCE / TEST)
subprocess.run(['gofmt', '-w', str(SOURCE / TEST)], check=True)
before = check('before', '^(TestContinuousQueryEngine|TestContinuousQueryEngine_Stats|TestContinuousQueryEngine_CreateBurst|TestContinuousQueryEngine_ConcurrentAdmission)$')
if before['exit_code'] == 0 or 'TestContinuousQueryEngine_ConcurrentAdmission' not in before['failed']:
    raise RuntimeError('deterministic original admission failure was not reproduced')
engine = replace_once(originals[ENGINE], '\t\tID:      fmt.Sprintf("cq-%d", time.Now().UnixNano()),\n', '')
engine = replace_once(engine, '\te.queryMu.Lock()\n\te.queries[query.ID] = query\n', '''\te.queryMu.Lock()
\t// Planning runs outside the registry lock. Recheck capacity at insertion so
\t// simultaneous creates cannot both consume the same remaining slot.
\tif len(e.queries) >= e.config.MaxQueries {
\t\te.queryMu.Unlock()
\t\tcancel()
\t\treturn nil, errors.New("max queries reached")
\t}
\t// Windows clock resolution can give successive creates the same timestamp.
\t// Keep IDs monotonic for this engine, including after deletion or clock drift.
\tnextID := time.Now().UnixNano()
\tif nextID <= e.lastQueryID {
\t\tnextID = e.lastQueryID + 1
\t}
\te.lastQueryID = nextID
\tquery.ID = fmt.Sprintf("cq-%d", nextID)
\te.queries[query.ID] = query
''')
types = replace_once(originals[TYPES], '\tqueries map[string]*ContinuousQueryV2\n\tqueryMu sync.RWMutex\n', '\tqueries     map[string]*ContinuousQueryV2\n\tqueryMu     sync.RWMutex\n\tlastQueryID int64 // guarded by queryMu; never reset when a query is deleted\n')
(SOURCE / ENGINE).write_bytes(engine.encode())
(SOURCE / TYPES).write_bytes(types.encode())
subprocess.run(['gofmt', '-w', str(SOURCE / ENGINE), str(SOURCE / TYPES)], check=True)
after = check('after')
report = {'source_base': BASE, 'platform': platform.platform(), 'go': subprocess.check_output(['go','version'], text=True).strip(),
          'CGO_ENABLED': os.environ.get('CGO_ENABLED'), 'before': before, 'after': after,
          'source_before_blobs': PINS, 'source_after_blobs': {p: blob((SOURCE/p).read_bytes()) for p in [ENGINE,TYPES,TEST]}}
(OUT / 'report.json').write_text(json.dumps(report, indent=2)+'\n')
for name in [ENGINE,TYPES,TEST]:
    shutil.copyfile(SOURCE/name, OUT/('after-'+pathlib.Path(name).name))
(OUT / 'source.patch').write_bytes(subprocess.check_output(['git','diff','--',ENGINE,TYPES], cwd=SOURCE))
if after['exit_code'] or after['failed'] or after['skipped']:
    raise RuntimeError('focused candidate package did not pass without skipped cases')
changes = set(git('diff', '--name-only').splitlines())
if changes != {ENGINE,TYPES}:
    raise RuntimeError('unexpected tracked changes: ' + repr(changes))
git('add','--',ENGINE,TYPES,TEST)
git('-c','user.name=woahwhattheheck','-c','user.email=293286387+woahwhattheheck@users.noreply.github.com','commit','-m','fix: retain distinct continuous queries and enforce concurrent capacity [skip ci]')
report['candidate_commit'] = git('rev-parse','HEAD')
report['candidate_tree'] = git('rev-parse','HEAD^{tree}')
(OUT / 'candidate.txt').write_text(report['candidate_commit']+'\n')
(OUT / 'report.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
