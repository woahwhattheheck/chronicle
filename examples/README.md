# Chronicle Examples

This directory contains example applications demonstrating Chronicle's features.

## Examples

### [simple](./simple)
Basic usage showing write and query operations.

```bash
cd simple && go run main.go
```

### [http-server](./http-server)
Full HTTP API server with automatic sample data generation.

```bash
cd http-server && go run main.go
# Then: curl http://localhost:8086/health
```

Features demonstrated:
- HTTP API endpoints
- Prometheus remote write
- GraphQL playground
- Admin dashboard

### [iot-collector](./iot-collector)
IoT sensor data collection with schema validation and downsampling.

```bash
cd iot-collector && go run main.go
```

Features demonstrated:
- Schema validation
- Downsampling rules
- Constrained environment configuration
- Aggregation queries

### [prometheus-compatible](./prometheus-compatible)
Prometheus-compatible backend with PromQL queries.

```bash
cd prometheus-compatible && go run main.go
```

Features demonstrated:
- Prometheus remote write ingestion
- PromQL queries (rate, sum, etc.)
- Histogram metrics
- Prometheus-style metrics format

### [analytics-dashboard](./analytics-dashboard)
Analytics features including forecasting, alerting, and histograms.

```bash
cd analytics-dashboard && go run main.go
```

Features demonstrated:
- Time-series forecasting (Holt-Winters)
- Recording rules (pre-computed queries)
- Native histograms with percentiles
- Alerting rules


### [home-automation](./home-automation)
MQTT / Zigbee / Z-Wave style sensor ingestion for smart-home data.

For a broker-free demonstration, run from `examples/`:

```bash
cd home-automation && go run . -mode simulate
```

Without `-mode simulate`, the default `mqtt` mode requires a reachable broker.
See the [home-automation guide](./home-automation/README.md#run-with-the-local-broker)
for the local broker setup and commands to publish sensor reports.

Features demonstrated:
- Multi-protocol topic simulation
- Schema-tagged writes and aggregations
- Optional Mosquitto compose broker

### [plugin](./plugin)
Custom plugin development with aggregator and transformer plugins.

```bash
cd plugin && go run main.go
```

Features demonstrated:
- Plugin registry and manifest
- Custom aggregator (EWMA)
- Custom transformer (unit conversion)
- Plugin invocation via SDK

## Running All Examples

```bash
# Run each example
for dir in simple http-server iot-collector prometheus-compatible analytics-dashboard; do
    echo "=== Running $dir ==="
    cd $dir
    go run main.go &
    sleep 5
    kill $!
    cd ..
done
```

## Creating Your Own Example

1. Create a new directory under `examples/`
2. Add a `main.go` with your example code
3. Include comments explaining what the example demonstrates
4. Update this README

## Common Patterns

### Opening a Database

```go
db, err := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### Writing Data

```go
// Single point
db.Write(chronicle.Point{
    Metric:    "temperature",
    Tags:      map[string]string{"sensor": "living-room"},
    Value:     22.5,
    Timestamp: time.Now().UnixNano(),
})

// Batch write (faster)
db.WriteBatch(points)
```

### Querying Data

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "temperature",
    Tags:   map[string]string{"sensor": "living-room"},
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   5 * time.Minute,
    },
})
```

### Enabling HTTP API

```go
db, _ := chronicle.Open("data.db", chronicle.Config{
    Path:        "data.db",
    HTTPEnabled: true,
    HTTPPort:    8086,
})
```

## Troubleshooting

### "file is locked" error
Only one process can open a database file. Stop other processes using the file.

### High memory usage
Reduce `MaxMemory` in config or decrease buffer sizes.

### Slow queries
Add time bounds (Start/End) to queries and use tag filters.

## More Resources

- [Documentation](https://chronicle-db.github.io/chronicle)
- [API Reference](https://pkg.go.dev/github.com/chronicle-db/chronicle)
- [GitHub Issues](https://github.com/chronicle-db/chronicle/issues)
