# Kubernetes image and deployment acceptance

Bounty #11 / issue #99 / existing PR #105.

## Executed source and result

The committed Dockerfile and Kubernetes manifests at **87d28f7ee80960f0d70e29e2bb73e47048771771** were built and deployed without modification on October 4, 2026. [Run 37191687891](https://github.com/woahwhattheheck/chronicle/actions/runs/37191687891), job 111405027813, completed successfully.

This closes the previously unexecuted image/cluster portion of this example for that source. It is not merely a cross-compile, parser unit test or two-process host simulation. Subsequent source commits, including the target-registry changes composed before this report was published, were not part of this execution.

The isolated [controller source](https://github.com/woahwhattheheck/chronicle/tree/7c2c878dd236ab5cce1ee6827e33731005400cd2) contains the complete workflow and `acceptance.py`. The workflow remains outside the contribution branch and does not run a general test matrix.

## Observed acceptance

- The repository-root Docker build with `examples/kubernetes-sidecar/Dockerfile` exited 0 in 44.82 seconds. The resulting Linux amd64 image is `sha256:7d56fca6cac4dfe5f4ef445ab2c690b07089125feaf4c8b982bd3fc634df76d6`, 23,138,764 bytes by Docker image inspection, and uses UID 65532.
- The same image was loaded into a disposable kind cluster. `kubectl apply -f examples/kubernetes-sidecar/manifests/` and the Deployment rollout both exited 0. Rollout took 18.10 seconds in this one run.
- The `demo-app` and `chronicle-sidecar` containers both became ready under the committed non-root pod configuration. They remained separate containers sharing the pod network.
- Requests through the `demo-app-chronicle` Service returned `/ready` status `ready`. The first observed `/stats` result was one scrape, zero scrape errors, four points collected and four points written, with one target.
- An actual POST to `/query` returned `demo_up` with value 1, not just a positive counter. Its tags matched the actual Kubernetes pod, namespace and node. The Downward API label tags also survived:

```json
{
  "Metric": "demo_up",
  "Value": 1,
  "Tags": {
    "namespace": "default",
    "node": "quarry-chronicle105-control-plane",
    "pod": "demo-app-with-chronicle-7745655df4-gglf4",
    "pod_label_app": "demo-app",
    "pod_label_pod-template-hash": "7745655df4"
  },
  "Timestamp": 1791105598942560070
}
```

The tracked-source diff remained empty. The port-forward was stopped and `kind delete cluster` exited 0. No registry push or existing cluster was involved.

## Reproduction

Use the pinned source above, Docker, kind v0.27.0 and a disposable cluster. From the repository root:

```bash
docker build -f examples/kubernetes-sidecar/Dockerfile -t chronicle-kubernetes-sidecar:local .
kind create cluster --name quarry-chronicle105 --wait 120s
kind load docker-image --name quarry-chronicle105 chronicle-kubernetes-sidecar:local
kubectl apply -f examples/kubernetes-sidecar/manifests/
kubectl rollout status deployment/demo-app-with-chronicle --timeout=180s
kubectl port-forward svc/demo-app-chronicle 18080:8080 18086:8086
```

With that port-forward active, query `/ready`, `/stats` and `/targets` on port 18080. On port 18086, POST `{"metric":"demo_up"}` to `/query` with `Content-Type: application/json` and `X-Requested-With: XMLHttpRequest`. Wait for the committed 15-second scrape interval. Compare the returned tags with `kubectl get pods -l app=demo-app -o json`, rather than hard-coding this run's pod name. Stop port-forwarding and delete the disposable cluster afterward. The linked controller automates these steps and captures all exit codes and responses.

## Evidence and limits

[Artifact 11299586256](https://github.com/woahwhattheheck/chronicle/actions/runs/37191687891/artifacts/11299586256) contains the raw receipt, command logs, image inspection, pod status/events, application/sidecar logs, Service responses and exact source archive. The downloaded 15,569,135-byte ZIP matched the provider SHA-256:

`7ff64e19129a88e2f57e7cf53c6394635e1f1cc47c3ed60d70a8782631a28e5a`

The artifact expires October 18, 2026; this report and the controller/source pins remain in Git.

The host used Go 1.24.4 and kind v0.27.0. The cluster server was Kubernetes v1.32.2. The hosted kubectl client was v1.37.1 and emitted an unsupported version-skew warning; the listed operations succeeded despite that warning. This is an observed execution, not a claim of a supported client/server compatibility matrix. The Docker build uses the committed `golang:1.24-alpine` builder separately from the host toolchain.

These are single-run acceptance observations, not performance benchmarks. Durable-volume recovery, restarts, production traffic, multi-node operation, other architectures and a full repository suite were not exercised. The example's `emptyDir` remains intentionally temporary. No production deployment, upstream approval, bounty award or payment is implied.
