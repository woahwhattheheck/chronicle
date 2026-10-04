#!/usr/bin/env bash
set -euo pipefail

lock_root=$PWD
mkdir -p receipt
go version > receipt/runtime.txt
go env GOOS GOARCH CGO_ENABLED GOTOOLCHAIN >> receipt/runtime.txt
git -C source rev-parse HEAD > receipt/source-commit.txt
test "$(cat receipt/source-commit.txt)" = 3bff9d255e45c27f1ad66b0e0571850969c8e8ac
test "$(git hash-object source/k8s_sidecar.go)" = ce5aa1aaeecd1d592b642f3e0eac71872338ec39
cp source/k8s_sidecar.go receipt/baseline-k8s_sidecar.go
gofmt -w payload/k8s_sidecar.go payload/k8s_sidecar_response_test.go isolation_support.go
cp payload/k8s_sidecar.go receipt/candidate-k8s_sidecar.go
cp payload/k8s_sidecar_response_test.go receipt/k8s_sidecar_response_test.go
cp isolation_support.go receipt/isolation_support.go
cp payload/k8s_sidecar_response_test.go source/k8s_sidecar_response_test.go

run_selected() {
  local lock_dir=$1
  local lock_stage=$2
  set +e
  (cd "$lock_dir" && timeout 300s go test -json -count=1 -timeout=30s \
    -run '^TestK8sSidecarTargetResponseLock$' .) > "receipt/$lock_stage.jsonl" 2>&1
  local lock_exit=$?
  set -e
  printf '%s\n' "$lock_exit" > "receipt/$lock_stage.exit"
}

has_event() {
  python3 - "$1" "$2" <<'PY'
import json, sys
for line in open(sys.argv[1]):
    try:
        event = json.loads(line)
    except ValueError:
        continue
    if event.get('Test') == 'TestK8sSidecarTargetResponseLock' and event.get('Action') == sys.argv[2]:
        raise SystemExit(0)
raise SystemExit(1)
PY
}

run_selected source full-before
if has_event receipt/full-before.jsonl fail; then
  printf '%s\n' full-package > receipt/execution-scope.txt
  cp payload/k8s_sidecar.go source/k8s_sidecar.go
  run_selected source full-after
  test "$(cat receipt/full-after.exit)" = 0
  has_event receipt/full-after.jsonl pass
elif has_event receipt/full-before.jsonl pass; then
  printf '%s\n' 'Original selected test unexpectedly passed; no repair established.' >&2
  exit 1
else
  # Preserve the complete normal-package failure before any isolated fallback.
  test "$(cat receipt/full-before.exit)" != 0
  printf '%s\n' full-sidecar-source-isolation > receipt/execution-scope.txt
  mkdir isolated
  printf 'module example.invalid/sidecar-response-lock\n\ngo 1.24.0\n' > isolated/go.mod
  cp receipt/baseline-k8s_sidecar.go isolated/k8s_sidecar.go
  cp payload/k8s_sidecar_response_test.go isolated/k8s_sidecar_response_test.go
  cp isolation_support.go isolated/isolation_support.go
  run_selected isolated isolated-before
  has_event receipt/isolated-before.jsonl fail
  cp payload/k8s_sidecar.go isolated/k8s_sidecar.go
  run_selected isolated isolated-after
  test "$(cat receipt/isolated-after.exit)" = 0
  has_event receipt/isolated-after.jsonl pass
fi

python3 - <<'PY' > receipt/result.json
from pathlib import Path
import hashlib, json
root = Path('receipt')
def blob(path):
    data = path.read_bytes()
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
result = {
    'source_commit': (root/'source-commit.txt').read_text().strip(),
    'execution_scope': (root/'execution-scope.txt').read_text().strip(),
    'runtime': (root/'runtime.txt').read_text().splitlines(),
    'blobs': {name: blob(root/name) for name in (
        'baseline-k8s_sidecar.go', 'candidate-k8s_sidecar.go',
        'k8s_sidecar_response_test.go', 'isolation_support.go')},
    'runs': {},
}
for path in sorted(root.glob('*.jsonl')):
    events = []
    for line in path.read_text().splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get('Test') == 'TestK8sSidecarTargetResponseLock':
            events.append(event)
    result['runs'][path.stem] = {
        'exit': int(path.with_suffix('.exit').read_text()),
        'selected_events': events,
    }
print(json.dumps(result, indent=2))
PY
cat receipt/result.json

