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

The `metric-gen` service posts one line-protocol point every five seconds. After its first write, `chronicle_metric_count` should become at least 1. The dashboard retains `up{job="chronicle"}` and Prometheus scrape activity, and adds a stored `demo_cpu_percent{host="compose",env="dev"}` time-series panel using the `Chronicle` datasource. The `ChronicleTargetDown` alert fires if the scrape fails for one minute; `ChronicleNoSampleData` fires if the sample generator has not produced any metric after one minute.

The emitter sends `X-Requested-With: XMLHttpRequest` and `Content-Type: text/plain` on its non-browser write requests. Chronicle's CSRF validation remains enabled; omitting the required request header caused the earlier emitter to receive HTTP 403 instead of ingesting data.

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

## Executed acceptance evidence — October 4, 2026

The maintained stack at source commit `a991190eef51d555c1bcaa4dacc960256797120e` was built and run on a hosted Linux runner with Docker Compose **v2.38.2**. This is a historical, source-bound result; the later direct-data panel and partition correction have separate evidence below.

[Successful run and complete logs](https://github.com/woahwhattheheck/chronicle/actions/runs/37189161693) · [Evidence artifact](https://github.com/woahwhattheheck/chronicle/actions/runs/37189161693/artifacts/11298288760) · [Exact acceptance workflow](https://github.com/woahwhattheheck/chronicle/blob/a5de59deb36a3524a0ea48ab320f01ea6884c0ad/.github/workflows/verify-compose-106.yml)

The container build/start completed, `promtool check config` and `amtool check-config` passed, and **all 19 HTTP/state acceptance checks passed**:

| Area | Executed evidence |
|---|---|
| Service readiness | Chronicle, Prometheus, Alertmanager, and Grafana became healthy/ready |
| Actual ingestion | The maintained BusyBox emitter produced `demo_cpu_percent{host="compose",env="dev"}` with a value in its expected 20–34 range |
| Prometheus | Chronicle's target was up, its scraped metric count was nonzero, and both maintained alert rules were loaded with healthy evaluations |
| Grafana | The provisioned `Prometheus` datasource pointed to the maintained Prometheus service; the provisioned two-panel dashboard referenced that UID; an actual query through Grafana's datasource proxy returned the up target |
| Alert delivery | Stopping Chronicle made the target go down, caused `ChronicleTargetDown` to enter the firing state, and delivered an active alert to Alertmanager's `local-ui` receiver |
| Persistence and recovery | A uniquely named point was written, both the emitter and Chronicle were stopped, and only Chronicle was restarted. The exact timestamp/value/tag remained present, and Prometheus observed the target recover |

The persistence point was `acceptance_persistence_37189161693{source="workflow"}`. Before and after restart, the API returned timestamp `1791102720.813659` and value `47.5`. The generator remained stopped throughout the persistence check, so the result was not produced by rewriting the sample after restart.

The earlier [source-head run](https://github.com/woahwhattheheck/chronicle/actions/runs/37187992365) exposed the emitter's missing CSRF request header. That was fixed in the original PR branch without disabling server validation. The [second run](https://github.com/woahwhattheheck/chronicle/actions/runs/37188869820) passed the first 15 checks through actual alert delivery but stopped because the acceptance harness used `docker compose start --wait`, which the installed Compose version does not support. The final harness uses `docker compose start chronicle` followed by an HTTP readiness check; the product source did not change between those last two runs.

The passing artifact contains 36 files: the structured results, raw service/build logs, resolved Compose configuration, actual image digests, runtime/source provenance, before/after query responses, the executed probe, and an empty tracked-source diff. ZIP SHA-256: `83fe86c5f837e7b04a3692214647701e36ddddb5ef28a793675121be4e9db69d`. The hosted artifact is retained for 14 days from the run.

These checks exercised the real maintained containers and APIs, not mocked services. They establish local Alertmanager receipt and Grafana provisioning/query behavior, not external notifications or browser rendering. They do not assert other-platform support, upstream CI approval, maintainer acceptance, or bounty payment.

## Direct stored-data dashboard — October 5, 2026

The source continuation from `be2161b3486d2ab94bc8feb019d0ad41e200ab44` remains on the existing [PR #106](https://github.com/josedab/chronicle/pull/106) and [claim #100](https://github.com/josedab/chronicle/issues/100).

The Chronicle datasource has the stable UID `Chronicle` and uses GET. Grafana's Prometheus datasource supports this provisioned [`httpMethod` option](https://grafana.com/docs/grafana/latest/administration/provisioning/). Chronicle's `/api/v1/query` and `/api/v1/query_range` handlers accept GET query parameters. This uses the existing read route without weakening server validation or changing the emitter's write headers.

The third panel queries `demo_cpu_percent{host="compose",env="dev"}` directly from Chronicle. Those labels and the metric name match the unchanged `metric-gen` service. The two existing panels still use the separate `Prometheus` UID; that datasource remains the default and retains POST. A healthy Prometheus target alone does not establish stored sample visibility.

For an already running stack, restart Grafana after applying the provisioning changes. For the partition-query repair below, rebuild and recreate Chronicle with `docker compose up --build -d`. No database volume reset is needed.

### Reproduced short-window failure and repair

A live run of source `3ec9685653eef096c1af075fecc12982cda77ce6` started the real maintained containers with Grafana **11.5.2**. A native instant query returned a stored sample at timestamp `1791228289.002437`, value `21`. An explicit five-minute range `[1791227993, 1791228293)` contained that timestamp but returned an empty matrix. The same query through Grafana's datasource proxy returned no series, and `/api/ds/query` returned HTTP 200 with an empty frame.

[Baseline run](https://github.com/woahwhattheheck/chronicle/actions/runs/37363033115) · [Raw request/response artifact](https://github.com/woahwhattheheck/chronicle/actions/runs/37363033115/artifacts/11366683937). ZIP SHA-256: `b4b00c2489538220572c0c5e35766b1fb4c1f6bc86cf0a23eba14e728753043e`.

`Index.FindPartitions` searched partition start keys inside the requested window, excluding an earlier-starting partition even when it overlapped the query. Source repair `27613bcbfb086fd990879c225079fee46b898532` bounds the sorted partition slice by the exclusive query end and checks interval ends against the inclusive query start. It preserves unbounded queries and handles overlapping partitions without assuming their end times are sorted. The previous B-tree range implementation visited every leaf; this repair does not claim logarithmic lookup or a measured performance speedup.

The repair adds one focused, 40-line regression covering nine interval cases. The selected command is:

```bash
go test -p 2 -run '^TestIndexFindPartitionsOverlappingWindow$' -count=1 -v .
```

[Repaired-source run](https://github.com/woahwhattheheck/chronicle/actions/runs/37364242050) · [Exact workflow](https://github.com/woahwhattheheck/chronicle/blob/4137b97daacc2e18d141664068662bdf25959bd9/.github/workflows/compose-direct-data.yml).

At this documentation update, the repaired-source job was still queued: **its regression and live integration results are not yet established**. The workflow checks the same native, Grafana proxy and Grafana backend queries, and runs the focused regression against both original and repaired source. It does not replay the earlier 19-check suite or claim browser rendering, external notification, upstream acceptance, or bounty payment.
