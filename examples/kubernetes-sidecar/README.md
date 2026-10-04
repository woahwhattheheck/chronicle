# Kubernetes Sidecar Example

Deploy Chronicle as a sidecar that scrapes Prometheus metrics from a co-located
application container on `localhost`, tagging series with Kubernetes Downward API
metadata (`POD_NAME`, `POD_NAMESPACE`, `NODE_NAME`).

Bounty #11 from `docs/BOUNTY_PROGRAM.md` ($50).

## Components

The example binary has three modes:

| Mode | Behavior |
|------|----------|
| `demo` (default) | Runs the metrics application and Chronicle sidecar in one process for local exploration |
| `app` | Runs only the demonstration application, exposing Prometheus text metrics |
| `sidecar` | Runs only Chronicle and `NewK8sSidecar`, scraping the application over localhost |

The Kubernetes Deployment runs `app` and `sidecar` in separate containers using
the same example image. The sidecar does not start a second application or bind
the application's metrics port. The example image is built from this directory's
`Dockerfile`; its entrypoint is the example binary.

## Run locally (no cluster)

Requires Go 1.24 or later. From this directory:

```bash
go run .
```

In another shell:

```bash
curl -fsS http://127.0.0.1:9090/metrics
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/stats
curl -fsS http://127.0.0.1:8080/targets
```

The default demo scrapes every 2 seconds. `scrape_count`, `points_collected`, and
`points_written` should increase. Stop with Ctrl-C; the example stops scraping,
closes its servers, and closes the local database.

To run the same process separation used by the Deployment, build once:

```bash
go build -o /tmp/chronicle-sidecar-example .
```

Run the application in one shell:

```bash
/tmp/chronicle-sidecar-example -mode=app
```

Run the sidecar in a second shell:

```bash
POD_NAME=local-app POD_NAMESPACE=default NODE_NAME=local-node \
  /tmp/chronicle-sidecar-example -mode=sidecar -data-dir=./sidecar-data
```

Stop the combined demo before running separate processes on the same ports.
Only `demo` mode supplies fallback pod metadata. `sidecar` mode uses the actual
Downward API values when provided.

## Build the Kubernetes image

Build from the **repository root** because this example's `go.mod` uses the local
Chronicle library through `replace github.com/chronicle-db/chronicle => ../..`:

```bash
docker build -f examples/kubernetes-sidecar/Dockerfile \
  -t chronicle-kubernetes-sidecar:local .
```

For an existing kind cluster, load that image (supply `--name` for a named cluster):

```bash
kind load docker-image chronicle-kubernetes-sidecar:local
```

For another cluster, push the image to a registry accessible to its nodes and
replace **both** image references in `manifests/deployment.yaml`. The supplied
`imagePullPolicy: IfNotPresent` allows an image loaded into a local cluster.

## Deploy to Kubernetes

After building and making the image available to the cluster, run from this
directory:

```bash
kubectl apply -f manifests/
kubectl rollout status deployment/demo-app-with-chronicle
kubectl get pods -l app=demo-app
kubectl port-forward svc/demo-app-chronicle 8080:8080 8086:8086
```

Keep the port-forward running and use another shell to verify collection. The
ConfigMap changes the Deployment's scrape interval to 15 seconds, so allow at
least one interval after startup:

```bash
curl -fsS http://127.0.0.1:8080/ready
curl -fsS http://127.0.0.1:8080/stats
curl -fsS http://127.0.0.1:8080/targets
curl -fsS http://127.0.0.1:8086/query \
  -H 'Content-Type: application/json' -d '{"metric":"demo_up"}'
```

The Deployment explicitly sets `-http-bind-address=0.0.0.0`, allowing the query
API to accept traffic addressed to the pod through the ClusterIP Service. A
client in the Deployment's namespace can query that Service directly:

```bash
curl -fsS http://demo-app-chronicle:8086/query \
  -H 'Content-Type: application/json' -d '{"metric":"demo_up"}'
```

The query response contains `points` with `Metric: "demo_up"`, `Value: 1`, and
`Tags` including `pod`, `namespace`, and `node`. A ready response indicates the
sidecar is running; increasing write counts and returned points verify successful
collection.

## Configuration

Both containers consume `chronicle-sidecar-config` through `envFrom`; the
Deployment expands those values into the binary's flags.

| ConfigMap key | Flag | Default in the Deployment |
|---------------|------|---------------------------|
| `metrics_port` | `-metrics-port` | `9090` |
| `metrics_path` | `-metrics-path` | `/metrics` |
| `scrape_interval` | `-scrape-interval` | `15s` |
| `scrape_timeout` | `-scrape-timeout` | `10s` |
| `health_port` | `-health-port` | `8080` |
| `retention` | `-retention` | `24h` |

Run the binary with `-help` for all flags, including `-http-port` (default `8086`),
`-http-bind-address` (default `127.0.0.1`), and `-data-dir` (also configurable with
`CHRONICLE_DATA_DIR`). The three listening ports must be distinct in demo and
sidecar modes. Durations must be positive.

Local runs bind the query API to loopback unless `-http-bind-address` is supplied.
The corresponding library setting is `Config.HTTP.HTTPBindAddress`; an empty
value also retains the `127.0.0.1` default. Supply a host or IP address without a
port. The provided Deployment opts into `0.0.0.0` for its ClusterIP Service; the
example does not enable query authentication.

After changing the ConfigMap, restart the Deployment to reload environment
values:

```bash
kubectl apply -f manifests/configmap.yaml
kubectl rollout restart deployment/demo-app-with-chronicle
```

If changing a port or metrics path, also update the corresponding container port,
probe, Service mapping, and pod annotation in the manifests. Kubernetes does not
expand environment values into those fields.

To use your own application, replace the `demo-app` container with your workload
that exposes Prometheus text metrics. Keep the sidecar container and the Downward
API environment variables and volume; set the ConfigMap to your application's
metrics endpoint.

## Storage and development

- Both containers share the pod network, so the sidecar scrapes `localhost`.
- The supplied image runs as a non-root user. The pod's `fsGroup` makes its
  `emptyDir` database volume writable by that user.
- `emptyDir` is temporary storage and is lost when the pod is removed. Use a
  suitable persistent volume for data that must outlive the pod.
- Pod labels and annotations are read from the mounted `/etc/podinfo` files.
- Optional remote write is available through the library's
  `RemoteWriteEnabled` / `RemoteWriteURL` settings; this example keeps collection
  local.

The example includes focused checks for mode/flag validation, real HTTP metrics,
port conflicts and shutdown, and preserving supplied pod metadata. Run them from
this directory:

```bash
go test ./...
go vet ./...
```

See the [Kubernetes image and rollout evidence](KUBERNETES_ACCEPTANCE.md) and
[direct Service query before-and-after results](SERVICE_NETWORK_ACCEPTANCE.md)
for executed deployment checks, source identities, and reproduction instructions.

See also: [Kubernetes deployment guide](../../docs/K8S_DEPLOYMENT.md).
