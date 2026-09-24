# Kubernetes Deployment Guide

This guide covers deploying Chronicle on Kubernetes using the included Helm chart.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.x
- `kubectl` configured for your cluster

## Deployment Modes

Chronicle supports two deployment modes on Kubernetes:

### Standalone (Default)

A dedicated pod running Chronicle as its main process. Best for shared metric collection where multiple applications write to a central Chronicle instance.

```bash
helm install chronicle ./k8s/helm/chronicle
```

**Resource profile (default):**

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 100m | 500m |
| Memory | 64Mi | 256Mi |
| Storage | 1Gi PVC | — |

### Sidecar

Chronicle runs alongside your application pod, scraping Prometheus-format metrics locally. Best for per-pod metric collection with optional remote write to a central store.

Enable sidecar mode in your values:

```yaml
sidecar:
  enabled: true
```

**Resource profile (sidecar):**

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 50m | 100m |
| Memory | 64Mi | 128Mi |
| Storage | 100MB local | — |

The sidecar automatically discovers pod metadata via the Kubernetes Downward API. See [ENVIRONMENT_VARIABLES.md](./ENVIRONMENT_VARIABLES.md) for the environment variables it expects.

## Installation

### From Local Chart

```bash
# Install with defaults
helm install chronicle ./k8s/helm/chronicle

# Install with custom values
helm install chronicle ./k8s/helm/chronicle -f my-values.yaml

# Dry run to preview manifests
helm install chronicle ./k8s/helm/chronicle --dry-run --debug
```

### Upgrade

```bash
helm upgrade chronicle ./k8s/helm/chronicle -f my-values.yaml
```

## Helm Values Reference

### Core Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `replicaCount` | int | `1` | Number of Chronicle pods |
| `image.repository` | string | `ghcr.io/chronicle-db/chronicle` | Container image |
| `image.tag` | string | `latest` | Image tag |
| `image.pullPolicy` | string | `IfNotPresent` | Pull policy |

### Chronicle Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `chronicle.maxMemory` | string | `"64MB"` | Maximum memory budget |
| `chronicle.partitionDuration` | string | `"1h"` | Time span per partition |
| `chronicle.retentionDuration` | string | `"168h"` | Data retention (7 days) |
| `chronicle.httpEnabled` | bool | `true` | Enable HTTP API |
| `chronicle.httpPort` | int | `8086` | HTTP listen port |

### Service

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `service.type` | string | `ClusterIP` | Service type (`ClusterIP`, `NodePort`, `LoadBalancer`) |
| `service.port` | int | `8086` | Service port |

### Persistence

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `persistence.enabled` | bool | `true` | Enable persistent storage |
| `persistence.size` | string | `1Gi` | PVC size |
| `persistence.storageClass` | string | `""` | Storage class (empty = default) |

### Resources

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `resources.requests.memory` | string | `"64Mi"` | Memory request |
| `resources.requests.cpu` | string | `"100m"` | CPU request |
| `resources.limits.memory` | string | `"256Mi"` | Memory limit |
| `resources.limits.cpu` | string | `"500m"` | CPU limit |

### Security

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `securityContext.runAsNonRoot` | bool | `true` | Run as non-root user |
| `securityContext.runAsUser` | int | `65534` | UID (nobody) |
| `securityContext.readOnlyRootFilesystem` | bool | `true` | Read-only root FS |
| `tls.enabled` | bool | `false` | Enable TLS |
| `tls.secretName` | string | `chronicle-tls` | TLS secret name |
| `networkPolicy.enabled` | bool | `true` | Enable network policy |

### High Availability

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `podDisruptionBudget.enabled` | bool | `true` | Enable PDB |
| `podDisruptionBudget.minAvailable` | int | `1` | Min available pods |
| `antiAffinity.enabled` | bool | `false` | Enable pod anti-affinity |
| `antiAffinity.type` | string | `soft` | `soft` or `hard` |

### Monitoring

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `serviceMonitor.enabled` | bool | `false` | Create Prometheus ServiceMonitor |
| `serviceMonitor.interval` | string | `15s` | Scrape interval |
| `probes.liveness.enabled` | bool | `true` | Enable liveness probe |
| `probes.readiness.enabled` | bool | `true` | Enable readiness probe |

### Sidecar

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sidecar.enabled` | bool | `false` | Deploy as sidecar |

## Resource Sizing Recommendations

| Workload | CPU Request | Memory Request | Storage | Retention |
|----------|------------|----------------|---------|-----------|
| **Dev/Test** | 50m | 64Mi | 512Mi | 24h |
| **IoT Edge** | 100m | 64Mi | 1Gi | 7d |
| **Small Production** | 250m | 256Mi | 10Gi | 30d |
| **Medium Production** | 500m | 512Mi | 50Gi | 90d |
| **Large Production** | 2000m | 2Gi | 100Gi | 365d |

Set `chronicle.maxMemory` to roughly 50–75% of the memory limit to leave room for Go runtime overhead.

## Example Configurations

### Minimal Development

```yaml
replicaCount: 1
persistence:
  enabled: false
resources:
  requests:
    memory: "64Mi"
    cpu: "50m"
  limits:
    memory: "128Mi"
    cpu: "200m"
chronicle:
  retentionDuration: "24h"
```

### Production with Monitoring

```yaml
replicaCount: 2
persistence:
  enabled: true
  size: 50Gi
  storageClass: gp3
resources:
  requests:
    memory: "512Mi"
    cpu: "500m"
  limits:
    memory: "1Gi"
    cpu: "2000m"
chronicle:
  maxMemory: "512MB"
  retentionDuration: "2160h"  # 90 days
antiAffinity:
  enabled: true
  type: hard
serviceMonitor:
  enabled: true
tls:
  enabled: true
  secretName: chronicle-tls
```

### Sidecar for Application Pods

Add the Chronicle sidecar to your application deployment:

```yaml
sidecar:
  enabled: true
persistence:
  enabled: false
chronicle:
  maxMemory: "32MB"
  retentionDuration: "24h"
resources:
  requests:
    memory: "64Mi"
    cpu: "50m"
  limits:
    memory: "128Mi"
    cpu: "100m"
```

The sidecar scrapes Prometheus-format metrics from localhost:9090 every 15 seconds by default.

## Health Checks

Chronicle exposes two health endpoints used by Kubernetes probes:

- **Liveness**: `GET /health/live` — returns 200 if the process is running
- **Readiness**: `GET /health/ready` — returns 200 when the database is ready to accept queries

## See Also

- [ENVIRONMENT_VARIABLES.md](./ENVIRONMENT_VARIABLES.md) — Environment variables for K8s integration
- [CONFIGURATION.md](./CONFIGURATION.md) — Full configuration reference
- [FEATURE_MATURITY.md](../FEATURE_MATURITY.md) — Feature maturity levels

## Runnable sidecar example

See [`examples/kubernetes-sidecar`](../examples/kubernetes-sidecar) for a local demo and Kubernetes manifests that scrape a co-located app via `K8sSidecar`.
