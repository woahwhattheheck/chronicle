# Real MQTT delivery receipt

Executed on 2026-10-04 against collector source
`271d0c94be180e501d0c48d462e6cbc9867c62a0`. This report adds no collector changes.

[Successful run](https://github.com/woahwhattheheck/chronicle/actions/runs/37200226673)
uses the supplied Mosquitto Compose service, not a fake in-process broker.
The fixture publishes controlled sensor-shaped messages; no physical sensor,
Zigbee/Z-Wave radio, Home Assistant installation or Raspberry Pi was involved.

## Executed results

| Check | Result |
|---|---|
| Build the example | PASS |
| Existing example tests with race detector | PASS: 9 top-level tests, 45 subtests |
| `go vet ./...` in the example module | PASS |
| Four device identities across normalized MQTT/Z-Wave and Zigbee2MQTT topics | PASS |
| Initial seven points covering all six metrics, with room mapping | PASS |
| Malformed reports and control/availability topics do not invent readings | PASS |
| Read queries during ingestion | PASS |
| Read queries while the broker is stopped | PASS |
| Broker restart, resubscription and new report ingestion | PASS |
| Graceful collector shutdown | PASS: exit 0, both processes |
| Reopen the same database | PASS: exact nine-point temperature history retained |
| Other five metric histories retained and new ingestion after reopen | PASS |

The first collector reported 15 messages, 14 written points, two rejected and two
ignored reports. The second wrote one additional point. This proves the exercised
graceful-restart path, not power-loss durability or exactly-once delivery.

The README's `Content-Type: application/json` query returned HTTP 200 without
`X-Requested-With`; adding that header also returned 200. No header fix is needed.
The historical runner label `query_endpoint_with_required_header` does not mean
that the header is required.

## Small-fixture measurements

Ubuntu 24.04.5 x86_64; Go 1.24.13; Mosquitto 2.0.20;
`GOMAXPROCS=2`, `GOGC=50`, `GOFLAGS=-p=2`.

- Collector process peak RSS observed through `/proc`: **32,140 KiB**.
- **36** small, local HTTP queries: p50 **0.663 ms**, p95 **0.894 ms**, maximum **0.917 ms**.
- End-to-end fixture: **3.811 seconds**, excluding compilation and container setup.

These are observations of a small cloud fixture, not a sustained-load benchmark,
a default-GC measurement, or Raspberry Pi resource claims. Memory excludes the
broker and build tools. The race detector covered the existing example tests;
the real-transport fixture used the normally built collector binary.

## Reproduce and inspect

The [isolated runner source](https://github.com/woahwhattheheck/chronicle/tree/41122d3bc4b5a3fbd648836e1af0a45f995d1d4a)
contains the exact workflow and `mqtt_delivery.py`. It checks out the collector
by immutable SHA separately from the runner and runs only this example module.
To reproduce manually, use a complete Chronicle checkout at the source SHA above,
copy that runner's Python script outside the checkout, and run:

```bash
cd examples/home-automation
go build -o home-automation .
go test -race -count=1 ./...
go vet ./...
docker compose up -d mosquitto
# Run the downloaded/copied runner script from this example directory:
SOURCE_SHA=271d0c94be180e501d0c48d462e6cbc9867c62a0 \
  python3 /path/to/mqtt_delivery.py
docker compose down
```

The fixture uses a temporary database, port 18086 for Chronicle, and the supplied
local broker on port 1883. Run on a disposable/local fixture, not an existing
household broker: it deliberately stops and restarts the Compose broker.

[Raw artifact: receipt, test events, environment, broker and collector logs](https://github.com/woahwhattheheck/chronicle/actions/runs/37200226673/artifacts/11302647395)
(14-day retention from the run). ZIP SHA-256:
`310fd1a1d72bedb16f322d06654b0252a00ced17592cd1d0b547406e148ef129`.
Broker image ID:
`sha256:29d935845523d9ab4d6c0f05fc9f458c9586fba08be6811412744e6a4d4cc2b0`.

This receipt does not replace maintainer acceptance, repository-wide CI or the
original bounty claim. It does not assert a merge or payment.
