#!/usr/bin/env bash
set -euo pipefail

: "${CHRONICLE_BASELINE_SHA:?baseline commit required}"
: "${EVIDENCE_DIR:?evidence directory required}"
task_dir="${RUNNER_TEMP:?}/chronicle105-run"
tool_dir="$task_dir/bin"
baseline_dir="$task_dir/baseline"
candidate_dir="$PWD"
cluster_name=chronicle105
mkdir -p "$tool_dir" "$baseline_dir" "$EVIDENCE_DIR"
export PATH="$tool_dir:$PATH"
export KUBECONFIG="$task_dir/kubeconfig"
forward_pid=""

finish() {
  result=$?
  trap - EXIT
  if [[ -n "$forward_pid" ]]; then kill "$forward_pid" 2>/dev/null || true; fi
  if command -v kubectl >/dev/null && [[ -f "$KUBECONFIG" ]]; then
    for stage in baseline candidate; do
      ns="chronicle105-$stage"
      kubectl --request-timeout=10s -n "$ns" get pods,deployments,services,configmaps -o yaml > "$EVIDENCE_DIR/$stage-resources.yaml" 2>&1 || true
      kubectl --request-timeout=10s -n "$ns" get events -o json > "$EVIDENCE_DIR/$stage-events.json" 2>&1 || true
      kubectl --request-timeout=10s -n "$ns" logs deployment/demo-app-with-chronicle --all-containers --tail=100 > "$EVIDENCE_DIR/$stage-container.log" 2>&1 || true
    done
  fi
  if command -v kind >/dev/null; then timeout 60 kind delete cluster --name "$cluster_name" > "$EVIDENCE_DIR/cleanup.log" 2>&1 || true; fi
  exit "$result"
}
trap finish EXIT

git rev-parse HEAD HEAD^{tree} > "$EVIDENCE_DIR/candidate-source.txt"
if ! git cat-file -e "$CHRONICLE_BASELINE_SHA^{commit}" 2>/dev/null; then
  git fetch --no-tags --depth=1 origin "$CHRONICLE_BASELINE_SHA"
fi
git rev-parse "$CHRONICLE_BASELINE_SHA" "$CHRONICLE_BASELINE_SHA^{tree}" > "$EVIDENCE_DIR/baseline-source.txt"
git diff "$CHRONICLE_BASELINE_SHA" HEAD -- config.go http_server.go examples/kubernetes-sidecar > "$EVIDENCE_DIR/candidate.patch"
git archive "$CHRONICLE_BASELINE_SHA" | tar -x -C "$baseline_dir"

curl -fLsS --connect-timeout 10 --max-time 90 https://github.com/kubernetes-sigs/kind/releases/download/v0.24.0/kind-linux-amd64 -o "$tool_dir/kind-linux-amd64"
curl -fLsS --connect-timeout 10 --max-time 90 https://github.com/kubernetes-sigs/kind/releases/download/v0.24.0/kind-linux-amd64.sha256sum -o "$tool_dir/kind-linux-amd64.sha256sum"
(cd "$tool_dir" && sha256sum -c kind-linux-amd64.sha256sum) | tee "$EVIDENCE_DIR/kind-checksum.txt"
install -m 755 "$tool_dir/kind-linux-amd64" "$tool_dir/kind"
curl -fLsS --connect-timeout 10 --max-time 90 https://dl.k8s.io/release/v1.31.0/bin/linux/amd64/kubectl -o "$tool_dir/kubectl"
curl -fLsS --connect-timeout 10 --max-time 90 https://dl.k8s.io/release/v1.31.0/bin/linux/amd64/kubectl.sha256 -o "$tool_dir/kubectl.sha256"
(cd "$tool_dir" && printf '%s  kubectl\n' "$(cat kubectl.sha256)" | sha256sum -c -) | tee "$EVIDENCE_DIR/kubectl-checksum.txt"
chmod +x "$tool_dir/kubectl"
kind version > "$EVIDENCE_DIR/kind-version.txt"
kubectl version --client -o json > "$EVIDENCE_DIR/kubectl-version.json"
docker version > "$EVIDENCE_DIR/docker-version.txt"

for stage in baseline candidate; do
  source_dir="$baseline_dir"
  if [[ "$stage" == candidate ]]; then source_dir="$candidate_dir"; fi
  image="chronicle-kubernetes-sidecar:$stage"
  SECONDS=0
  timeout 420 docker build --progress=plain -f "$source_dir/examples/kubernetes-sidecar/Dockerfile" -t "$image" "$source_dir" 2>&1 | tee "$EVIDENCE_DIR/$stage-build.log"
  printf '%s\n' "$SECONDS" > "$EVIDENCE_DIR/$stage-build-seconds.txt"
  docker image inspect --format '{{json .}}' "$image" > "$EVIDENCE_DIR/$stage-image.json"
done

docker run --rm --network=none --read-only -v "$candidate_dir:/src:ro" -w /src --entrypoint gofmt golang:1.24-alpine -l config.go http_server.go examples/kubernetes-sidecar/main.go > "$EVIDENCE_DIR/gofmt.txt"
test ! -s "$EVIDENCE_DIR/gofmt.txt"

timeout 240 kind create cluster --name "$cluster_name" --image kindest/node:v1.31.0@sha256:53df588e04085fd41ae12de0c3fe4c72f7013bba32a20e7325357a1ac94ba865 --wait 120s 2>&1 | tee "$EVIDENCE_DIR/cluster-create.log"
kind load docker-image --name "$cluster_name" chronicle-kubernetes-sidecar:baseline chronicle-kubernetes-sidecar:candidate
kubectl get nodes -o json > "$EVIDENCE_DIR/nodes.json"

for stage in baseline candidate; do
  source_dir="$baseline_dir"
  if [[ "$stage" == candidate ]]; then source_dir="$candidate_dir"; fi
  ns="chronicle105-$stage"
  stage_dir="$EVIDENCE_DIR/$stage"
  mkdir -p "$stage_dir/manifests"
  cp "$source_dir/examples/kubernetes-sidecar/manifests/"*.yaml "$stage_dir/manifests/"
  sed -i "s#image: chronicle-kubernetes-sidecar:local#image: chronicle-kubernetes-sidecar:$stage#g" "$stage_dir/manifests/deployment.yaml"
  kubectl create namespace "$ns"
  SECONDS=0
  kubectl -n "$ns" apply -f "$stage_dir/manifests/"
  kubectl -n "$ns" rollout status deployment/demo-app-with-chronicle --timeout=150s 2>&1 | tee "$stage_dir/rollout.log"
  printf '%s\n' "$SECONDS" > "$stage_dir/rollout-seconds.txt"
  kubectl -n "$ns" get pods -l app=demo-app -o json > "$stage_dir/pods.json"
  kubectl -n "$ns" port-forward --address 127.0.0.1 svc/demo-app-chronicle 18080:8080 18086:8086 > "$stage_dir/port-forward.log" 2>&1 &
  forward_pid=$!
  python3 - "$stage_dir" <<'PY'
import json, pathlib, sys, time, urllib.error, urllib.request
p = pathlib.Path(sys.argv[1])
pods = json.loads((p / "pods.json").read_text())["items"]
assert len(pods) == 1, pods
pod = pods[0]
expected = {"pod": pod["metadata"]["name"], "namespace": pod["metadata"]["namespace"], "node": pod["spec"]["nodeName"], "pod_label_app": "demo-app"}
security = pod["spec"]["securityContext"]
assert security["runAsNonRoot"] and security["runAsUser"] == security["runAsGroup"] == security["fsGroup"] == 65532
assert len(pod["status"]["containerStatuses"]) == 2
assert all(c["ready"] and c["restartCount"] == 0 for c in pod["status"]["containerStatuses"])
def get(path, payload=None):
    port = 18086 if path == "/query" else 18080
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)
deadline = time.monotonic() + 75
started = time.monotonic()
last = None
while time.monotonic() < deadline:
    try:
        ready = get("/ready")
        stats = get("/stats")
        query = get("/query", {"metric": "demo_up"})
        points = query.get("points") or []
        if stats["scrape_count"] >= 2 and stats["points_written"] >= 8 and points:
            break
    except (OSError, ValueError, urllib.error.URLError) as exc:
        last = str(exc)
    time.sleep(1)
else:
    raise AssertionError(f"no two-scrape stored result within75s: {last}")
assert ready["status"] == "ready"
assert all(point["Metric"] == "demo_up" and point["Value"] == 1 and all(point["Tags"].get(k) == v for k, v in expected.items()) for point in points), query
requests = get("/query", {"metric": "demo_requests_total"})
assert requests.get("points"), requests
assert all(point["Tags"].get("method") == "GET" and point["Tags"].get("status") == "200" for point in requests["points"]), requests
for name, value in {"ready": ready, "stats": stats, "query": query, "requests-query": requests, "expected-tags": expected}.items():
    (p / (name + ".json")).write_text(json.dumps(value, indent=2) + "\n")
(p / "observation.json").write_text(json.dumps({"seconds_after_rollout": time.monotonic() - started, "last_scrape_duration_ns": stats["last_scrape_duration"]}, indent=2) + "\n")
print(json.dumps({"stage": p.name, "scrape_count": stats["scrape_count"], "points_written": stats["points_written"], "stored_demo_up_points": len(points), "tags": expected}))
PY
  kill "$forward_pid"
  wait "$forward_pid" 2>/dev/null || true
  forward_pid=""
  kubectl -n "$ns" run service-query --image=curlimages/curl:8.10.1 --restart=Never --command -- /bin/sh -ec 'curl --fail --silent --show-error --connect-timeout 5 --max-time 10 http://demo-app-chronicle:8080/ready -o /dev/null
printf "HEALTH_OK\n"
exec curl --fail-with-body --silent --show-error --connect-timeout 5 --max-time 10 -H "Content-Type: application/json" --data '\''{"metric":"demo_up"}'\'' http://demo-app-chronicle:8086/query'
  python3 - "$stage" "$ns" "$stage_dir" <<'PY'
import json, pathlib, subprocess, sys, time
stage, ns, directory = sys.argv[1:]
p = pathlib.Path(directory)
deadline = time.monotonic() + 90
while time.monotonic() < deadline:
    result = subprocess.run(["kubectl", "--request-timeout=10s", "-n", ns, "get", "pod/service-query", "-o", "json"], check=True, capture_output=True, text=True)
    pod = json.loads(result.stdout)
    if pod["status"].get("phase") in ("Succeeded", "Failed"):
        break
    time.sleep(1)
else:
    raise AssertionError("Service query pod did not finish within90s")
(p / "service-query-pod.json").write_text(json.dumps(pod, indent=2) + "\n")
logs = subprocess.run(["kubectl", "--request-timeout=10s", "-n", ns, "logs", "service-query"], check=True, capture_output=True, text=True).stdout
(p / "service-query.log").write_text(logs)
exit_code = pod["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"]
assert logs.startswith("HEALTH_OK\n"), f"Service8080 health did not succeed: exit={exit_code}; {logs}"
query_body = logs[len("HEALTH_OK\n"):]
if stage == "baseline":
    assert exit_code == 7, f"baseline did not reproduce connection failure: exit={exit_code}; {logs}"
else:
    assert exit_code == 0, f"candidate Service query failed: exit={exit_code}; {logs}"
    query = json.loads(query_body)
    expected = json.loads((p / "expected-tags.json").read_text())
    points = query.get("points") or []
    assert points and all(point["Metric"] == "demo_up" and point["Value"] == 1 and all(point["Tags"].get(k) == v for k, v in expected.items()) for point in points), query
(p / "service-result.json").write_text(json.dumps({"stage": stage, "exit_code": exit_code, "expectation_met": True}, indent=2) + "\n")
print(json.dumps({"stage": stage, "service_query_exit_code": exit_code, "expectation_met": True}))
PY
done
