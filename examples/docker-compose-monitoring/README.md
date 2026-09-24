# Docker Compose Monitoring Stack

Chronicle + Prometheus + Alertmanager + Grafana in one compose file.

Bounty #12 from `docs/BOUNTY_PROGRAM.md` ($50).

## Quick start

```bash
cd examples/docker-compose-monitoring
docker compose up --build -d

# UIs
open http://localhost:3000   # Grafana (admin / chronicle)
open http://localhost:9090   # Prometheus
open http://localhost:9093   # Alertmanager
curl -s http://localhost:8086/health
```

Stop:

```bash
docker compose down -v
```

## What is included

| Service | Port | Role |
|---------|------|------|
| chronicle | 8086 | Time-series DB + HTTP API / Prometheus-compatible surface |
| prometheus | 9090 | Scrapes Chronicle `/metrics`, evaluates alert rules |
| alertmanager | 9093 | Routes alert notifications (webhook stub by default) |
| grafana | 3000 | Provisioned datasources (Chronicle + Prometheus) + starter dashboard |
| metric-gen | — | Posts sample points to Chronicle so the stack is not empty |

## Verify

```bash
docker compose ps
curl -s http://localhost:8086/health
curl -s 'http://localhost:9090/api/v1/targets' | head
curl -s http://localhost:9093/-/healthy
```

In Grafana, open folder **Chronicle** → **Chronicle Monitoring Stack**.

## Customization

- Edit `prometheus/alerts.yml` for new recording/alerting rules.
- Point Alertmanager at Slack/PagerDuty in `alertmanager/alertmanager.yml`.
- Swap `metric-gen` for your real emitters (OpenTelemetry, Prom remote write, etc.).

Related: `examples/docker-quickstart` is a smaller Chronicle+Grafana pair; this stack adds scraping and alerting.
