# Kubernetes Sidecar Example

Deploy Chronicle as a sidecar that scrapes Prometheus metrics from a co-located
application container on `localhost`, tagging series with Kubernetes Downward API
metadata (`POD_NAME`, `POD_NAMESPACE`, `NODE_NAME`).

Bounty #11 from `docs/BOUNTY_PROGRAM.md` ($50).

## What you get

| Path | Purpose |
|------|---------|
| `main.go` | Local demo: fake app on `:9090/metrics` + `K8sSidecar` health/stats on `:8080` |
| `manifests/` | Deployment, Service, ConfigMap for a real cluster |
| Library API | Uses `chronicle.NewK8sSidecar` / `DefaultK8sSidecarConfig` |

## Run locally (no cluster)

```bash
cd examples/kubernetes-sidecar
go run .

# in another shell
curl -s http://127.0.0.1:9090/metrics | head
curl -s http://127.0.0.1:8080/health
curl -s http://127.0.0.1:8080/stats
curl -s http://127.0.0.1:8080/targets
curl -s 'http://127.0.0.1:8086/health'
```

You should see scrape counts increase every ~2s and points collected from `demo_*` metrics.

## Deploy to Kubernetes

```bash
kubectl apply -f manifests/
kubectl get pods -l app=demo-app
kubectl port-forward svc/demo-app-chronicle 8080:8080 8086:8086
curl -s http://127.0.0.1:8080/stats
```

Replace the `demo-app` container image with your workload that exposes
Prometheus text metrics on port `9090`. Keep the `chronicle-sidecar` container
and the `podinfo` Downward API volume.

## Verify collection

1. Hit the app until it emits metrics.
2. `curl sidecar:8080/stats` — `scrape_count` and `points_written` should rise.
3. Query Chronicle HTTP (`:8086`) for scraped metric names such as `demo_up`.

## Design notes

- Sidecar scrapes **localhost only** (same network namespace as the app container).
- Pod metadata comes from Downward API env vars and `/etc/podinfo`.
- Local retention defaults to 24h / 100MB (see `DefaultK8sSidecarConfig`).
- Optional remote write can be enabled via `RemoteWriteEnabled` / `RemoteWriteURL`.

See also: [docs/K8S_DEPLOYMENT.md](../../docs/K8S_DEPLOYMENT.md).
