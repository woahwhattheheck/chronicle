# Docker Compose monitoring stack

Run Chronicle with Prometheus, Alertmanager, and Grafana. The example provides its own container entry point because the repository's generic Dockerfile refers to a CLI directory that is not present. Chronicle's embedded HTTP listener uses container loopback; this entry point forwards it to the Compose network and serves a Prometheus text exposition at `/metrics`.

Bounty #12 from `docs/BOUNTY_PROGRAM.md` ($50).

## Quick start

```bash
cd examples/docker-compose-monitoring
docker compose up --build -d
docker compose ps
```

Open Grafana at http://localhost:3000 (admin / chronicle), Prometheus at http://localhost:9090, and Alertmanager at http://localhost:9093. The published ports are bound to the host's loopback address for this local example.

```bash
curl -f http://localhost:8086/health
curl -f http://localhost:8086/metrics
curl -fs 'http://localhost:9090/api/v1/query?query=chronicle_metric_count'
```

The `metric-gen` service posts one line-protocol point every five seconds. After its first write, `chronicle_metric_count` should become at least 1. The dashboard shows `up{job="chronicle"}` and Prometheus scrape activity. The `ChronicleTargetDown` alert fires if the scrape fails for one minute; `ChronicleNoSampleData` fires if the sample generator has not produced any metric after one minute.

The Alertmanager receiver records alerts in its UI without sending to an external webhook. Configure a real receiver before expecting notifications. Chronicle's built-in `/metrics` returns a JSON metric-name list; the example's container entry point emits Prometheus text for process availability and the metric-name count instead.

```bash
docker compose down
```

Avoid `down -v` when you want to retain the demo database.

## Contents

| Service | Role |
|---|---|
| chronicle | Time-series DB, HTTP API, and scrapeable example gauges |
| metric-gen | Posts sample data to Chronicle |
| prometheus | Scrapes Chronicle and evaluates alert rules |
| alertmanager | Displays alerts in the local UI |
| grafana | Provisioned dashboard and data source |

To customize, edit `prometheus/alerts.yml`, replace `metric-gen` with an emitter, and configure a notification receiver in `alertmanager/alertmanager.yml`.
