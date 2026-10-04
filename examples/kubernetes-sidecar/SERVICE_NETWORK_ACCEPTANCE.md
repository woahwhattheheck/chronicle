# Kubernetes query Service: before and after

The supplied query Service was unreachable from another pod because Chronicle's
HTTP server listened only on `127.0.0.1`. A successful `kubectl port-forward`
request did not expose this problem. This repair adds `HTTPConfig.HTTPBindAddress`
and the example's `-http-bind-address` flag, retaining loopback as the default.
Only the supplied Kubernetes Deployment opts into `0.0.0.0` so its ClusterIP
Service can reach port 8086.

The repair was executed against the original behavior using two actual Docker
images and two Deployments in a single ephemeral kind cluster. Both images were
built with the example's unchanged Dockerfile and the complete Chronicle source.
No database, scraper, HTTP server, or Kubernetes API was mocked.

## Result

| Observation | Original | Repaired |
|---|---:|---:|
| Docker image build, whole seconds | 32 | 33 |
| Deployment rollout, whole seconds | 21 | 21 |
| Ready containers / expected containers | 2 / 2 | 2 / 2 |
| Container restarts at observation | 0 | 0 |
| Scrapes / scrape errors | 2 / 0 | 2 / 0 |
| Collected / written points | 8 / 8 | 8 / 8 |
| Stored `demo_up` points, each with value 1 | 2 | 2 |
| Readiness and stored-point query through port-forward | Passed | Passed |
| Separate pod: Service readiness on port 8080 | Passed | Passed |
| Separate pod: Service query on port 8086 | Connection failure, curl exit 7 | Stored points returned, curl exit 0 |

The separate client printed `HEALTH_OK` only after a successful request to the
Service's readiness endpoint. It then requested `/query` on the same Service at
port 8086. The original query failed with "Could not connect to server". The
repaired query returned two `demo_up` points. This distinguishes the query
listener failure from a general Service, DNS, or readiness failure.

Both stages used non-root UID/GID 65532 with `fsGroup: 65532`. Each query's pod,
namespace, node, and `pod_label_app` tags were checked against the actual
Deployment pod. The separate `demo_requests_total` query also retained
`method: GET` and `status: 200` labels.

| Tag | Original | Repaired |
|---|---|---|
| `pod` | `demo-app-with-chronicle-5bdb6f5bbd-7gl2p` | `demo-app-with-chronicle-79598c845-jg558` |
| `namespace` | `chronicle105-baseline` | `chronicle105-candidate` |
| `node` | `chronicle105-control-plane` | `chronicle105-control-plane` |
| `pod_label_app` | `demo-app` | `demo-app` |

The statistics were captured about 11.09 seconds after rollout completed. The
last reported scrape durations were 414,864 ns and 691,486 ns respectively.
These are individual observations from a sequential acceptance run, not a
latency distribution or a performance comparison. No speedup is claimed.

## Source and execution provenance

- Original source: `87d28f7ee80960f0d70e29e2bb73e47048771771`.
- Executed repair: `7872469306a5da72a6e34b6c83eb4208516232fc` on the isolated
  validation branch `relay17/chronicle105-kubernetes-20261004`.
- Successful [workflow run 37192145883](https://github.com/woahwhattheheck/chronicle/actions/runs/37192145883),
  [job 111406413299](https://github.com/woahwhattheheck/chronicle/actions/runs/37192145883/job/111406413299),
  executed on 2026-10-04 UTC using Ubuntu 24.04, kind 0.24.0 and Kubernetes / kubectl 1.31.0.
- kind node image: `kindest/node:v1.31.0@sha256:53df588e04085fd41ae12de0c3fe4c72f7013bba32a20e7325357a1ac94ba865`.
- Original image ID: `sha256:913f56c6653039b885976dbb2ec20571f3adcb2ce9384e5631d80dd876fa970f`.
- Repaired image ID: `sha256:641d3ea4b6f9f4989a946ba453223581022af952016d6dc942b1c8bcf426d62f`.

The executed product files have these Git blob identities:

| File | Blob |
|---|---|
| `config.go` | `354eee6048384df233f7121f244fd8f20c6a816c` |
| `http_server.go` | `9ed69712a07d4aa5399b0230e5aa44953c66bbca` |
| `examples/kubernetes-sidecar/main.go` | `6b54e5934dc1b0f53c779bc4437820371f92e58a` |
| `examples/kubernetes-sidecar/manifests/deployment.yaml` | `afb643f1ab72266a26e6a8bc8a0952d0c9fe82a3` |

The original PR advanced independently to
`b4f248925609c0585959a46f175f980de6120049` before this repair was published.
Its workflow correction, parser/startup reports, target-registry optimization
and tests, and [earlier Kubernetes acceptance report](KUBERNETES_ACCEPTANCE.md)
were preserved. The registry change affects target addition/removal; the example
uses one constructor-supplied target and does not exercise those methods. A
focused source review found no integration conflict. The run above executes the
immutable validation revision, not the later combined PR revision.

The earlier Kubernetes report remains useful evidence for image startup,
ingestion, pod metadata, and queries through port-forward. This report adds the
direct pod-to-ClusterIP request and its repair.

## Evidence and reproduction

[SERVICE_NETWORK_ACCEPTANCE.json](SERVICE_NETWORK_ACCEPTANCE.json) preserves the
raw statistics, returned points, Service-client output, observed pod security and
container status, timing observations, image identities, and cleanup result from
this run. It is an extracted evidence record, not a new execution.

The complete [Actions artifact 11299512306](https://github.com/woahwhattheheck/chronicle/actions/runs/37192145883/artifacts/11299512306)
is `chronicle105-kubernetes-37192145883`, 46,655 bytes, with SHA-256
`69a84fcb48dd0e3cb32dcf625e7d6815c7ac6e4710e579277b7e0689e6e79a31`.
Its recorded expiration is 2026-10-18 09:29:37 UTC. The downloaded ZIP checksum was
verified before extracting the retained evidence.

The artifact also includes full Docker build output, rendered manifests, pod and
Service resources, events, container logs, tool versions, source identities,
the source patch, and an empty `gofmt.diff`. Cleanup confirmed deletion of the
kind node.

The [workflow](https://github.com/woahwhattheheck/chronicle/blob/7872469306a5da72a6e34b6c83eb4208516232fc/.github/workflows/relay17-chronicle105-kubernetes.yml)
and [execution script](https://github.com/woahwhattheheck/chronicle/blob/7872469306a5da72a6e34b6c83eb4208516232fc/work/validation/chronicle105-kubernetes-20261004/run.sh)
are retained at the executed revision. On a disposable Ubuntu host with Docker,
git, curl, and Python, check out that revision and run:

```bash
CHRONICLE_BASELINE_SHA=87d28f7ee80960f0d70e29e2bb73e47048771771 \
  RUNNER_TEMP=/tmp/chronicle105-runner \
  EVIDENCE_DIR=/tmp/chronicle105-evidence \
  bash work/validation/chronicle105-kubernetes-20261004/run.sh
```

Use a host with no existing kind cluster named `chronicle105`; the script creates
and deletes that cluster and installs the pinned kind and kubectl binaries.
The two Docker images share dependency/build caches, so the build durations are
not independent cold-build measurements. This run covers one replica on one node
with the supplied temporary `emptyDir` volume. It does not establish production
load capacity, a latency distribution, multi-node behavior, persistence across
pod deletion, or results for the full repository test suite.

Attribution: GPT-6 Astra Pro, Astra Relay-17, ChatGPT cloud harness
`15a9c3b91fc7`, with focused source review by the session's collaborating agents.
