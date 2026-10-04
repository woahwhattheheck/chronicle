"""Execute the committed sidecar example in a disposable local Kubernetes cluster."""
import json
import os
from pathlib import Path
import subprocess
import time
import traceback
import urllib.request

ROOT = Path(os.environ['GITHUB_WORKSPACE'])
SUBJECT = ROOT / 'subject'
OUT = ROOT / 'evidence'
OUT.mkdir(exist_ok=True)
PIN = '87d28f7ee80960f0d70e29e2bb73e47048771771'
NAME = 'quarry-chronicle105'
receipt = {'source_sha': PIN, 'success': False, 'checks': {}, 'commands': []}
forward = None

def run(name, args, timeout=300, check=True):
    started = time.monotonic()
    with (OUT / (name + '.log')).open('w') as log:
        result = subprocess.run(args, cwd=SUBJECT, stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
    receipt['commands'].append({'name': name, 'argv': args, 'exit_code': result.returncode,
                                'seconds': time.monotonic() - started})
    if check and result.returncode:
        raise RuntimeError(f'{name}: exit {result.returncode}')
    return (OUT / (name + '.log')).read_text()

def request(port, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(f'http://127.0.0.1:{port}{path}', data=data,
                                 headers={'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest'})
    with urllib.request.urlopen(req, timeout=5) as response:
        body = response.read().decode()
        return json.loads(body) if body.strip().startswith(('{', '[')) else body

try:
    actual = run('source-head', ['git', 'rev-parse', 'HEAD']).strip()
    assert actual == PIN, actual
    run('source-status-before', ['git', 'status', '--porcelain'])
    run('source-archive', ['git', 'archive', '--format=tar.gz', '-o', str(OUT / 'source.tar.gz'), 'HEAD'])
    run('go-version', ['go', 'version'])
    run('kind-version', ['kind', 'version'])
    run('docker-version', ['docker', 'version'])
    run('kubectl-version', ['kubectl', 'version', '--client', '-o', 'yaml'])
    run('docker-build', ['docker', 'build', '-f', 'examples/kubernetes-sidecar/Dockerfile',
                         '-t', 'chronicle-kubernetes-sidecar:local', '.'], timeout=600)
    receipt['checks']['committed_dockerfile_build'] = True
    run('image-inspect', ['docker', 'image', 'inspect', 'chronicle-kubernetes-sidecar:local'])
    run('kind-create', ['kind', 'create', 'cluster', '--name', NAME, '--wait', '120s'], timeout=300)
    run('cluster-version', ['kubectl', 'version', '-o', 'json'])
    run('kind-load', ['kind', 'load', 'docker-image', '--name', NAME, 'chronicle-kubernetes-sidecar:local'])
    run('manifest-apply', ['kubectl', 'apply', '-f', 'examples/kubernetes-sidecar/manifests/'])
    run('rollout', ['kubectl', 'rollout', 'status', 'deployment/demo-app-with-chronicle', '--timeout=180s'])
    receipt['checks']['committed_manifests_rollout'] = True
    pods = json.loads(run('pods', ['kubectl', 'get', 'pods', '-l', 'app=demo-app', '-o', 'json']))
    pod = pods['items'][0]
    assert len(pod['status']['containerStatuses']) == 2
    assert all(c['ready'] for c in pod['status']['containerStatuses'])
    expected = {'pod': pod['metadata']['name'], 'namespace': pod['metadata']['namespace'],
                'node': pod['spec']['nodeName']}
    receipt['expected_tags'] = expected
    receipt['checks']['two_ready_containers'] = True
    run('resources', ['kubectl', 'get', 'deployment,pod,service,configmap', '-l', 'app=demo-app', '-o', 'json'])
    log = (OUT / 'port-forward.log').open('w')
    forward = subprocess.Popen(['kubectl', 'port-forward', 'svc/demo-app-chronicle',
                                '18080:8080', '18086:8086'], stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 150
    last = None
    while time.monotonic() < deadline:
        try:
            stats = request(18080, '/stats')
            if stats.get('points_written', 0) > 0:
                break
        except Exception as exc:
            last = str(exc)
        time.sleep(2)
    else:
        raise RuntimeError(f'No stored points before deadline: {last}')
    (OUT / 'stats.json').write_text(json.dumps(stats, indent=2))
    (OUT / 'ready.json').write_text(json.dumps(request(18080, '/ready'), indent=2))
    (OUT / 'targets.json').write_text(json.dumps(request(18080, '/targets'), indent=2))
    query = request(18086, '/query', {'metric': 'demo_up'})
    (OUT / 'query.json').write_text(json.dumps(query, indent=2))
    points = query['points']
    assert points, query
    matching = [p for p in points if p.get('Metric') == 'demo_up' and p.get('Value') == 1
                and all(p.get('Tags', {}).get(k) == v for k, v in expected.items())]
    assert matching, query
    receipt['checks']['actual_service_stats_written'] = True
    receipt['checks']['stored_demo_up_value_and_downward_api_tags'] = True
    receipt['observed_stats'] = stats
    receipt['matching_points'] = len(matching)
    diff = run('source-diff', ['git', 'diff', '--exit-code'])
    assert not diff
    receipt['success'] = True
except Exception:
    receipt['error'] = traceback.format_exc()
finally:
    for label, args in [
        ('pod-describe', ['kubectl', 'describe', 'pods', '-l', 'app=demo-app']),
        ('app-logs', ['kubectl', 'logs', 'deployment/demo-app-with-chronicle', '-c', 'demo-app']),
        ('sidecar-logs', ['kubectl', 'logs', 'deployment/demo-app-with-chronicle', '-c', 'chronicle-sidecar']),
        ('cluster-events', ['kubectl', 'get', 'events', '--sort-by=.metadata.creationTimestamp']),
    ]:
        try:
            run(label, args, timeout=30, check=False)
        except Exception as exc:
            receipt.setdefault('collection_errors', []).append(str(exc))
    if forward is not None:
        forward.terminate()
        try:
            forward.wait(timeout=5)
        except subprocess.TimeoutExpired:
            forward.kill()
            forward.wait()
    try:
        run('kind-delete', ['kind', 'delete', 'cluster', '--name', NAME], timeout=90, check=False)
    except Exception as exc:
        receipt['cleanup_error'] = str(exc)
    (OUT / 'receipt.json').write_text(json.dumps(receipt, indent=2))
    print(json.dumps(receipt, indent=2))
if not receipt['success']:
    raise SystemExit(1)
