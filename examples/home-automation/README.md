# Home Automation Example (MQTT / Zigbee / Z-Wave)

Subscribe to a real MQTT broker and store sensor readings in Chronicle. The
collector understands Zigbee2MQTT JSON reports and a normalized topic format for
MQTT, Zigbee and Z-Wave sensors. A separate simulation mode works without a broker.

Bounty #13 from [the bounty program](../../docs/BOUNTY_PROGRAM.md).

## Run with the local broker

Requires Go 1.24 or later and Docker Compose for the supplied broker. Run from a
checkout of the complete Chronicle repository because this example's Go module
uses the repository root.

```bash
cd examples/home-automation
docker compose up -d mosquitto
go run . -rooms rooms.example.json
```

Wait for `MQTT subscribed: filters=2 qos=1`. In another terminal, publish reports:

```bash
cd examples/home-automation

# A native Zigbee2MQTT report: three Chronicle points.
docker compose exec -T mosquitto mosquitto_pub -h localhost -q 1 \
  -t zigbee2mqtt/living-room-sensor \
  -m '{"temperature":22.5,"humidity":45,"occupancy":true}'

# A Z-Wave gateway report mapped to the normalized format.
docker compose exec -T mosquitto mosquitto_pub -h localhost -q 1 \
  -t home/zwave/front-door/door-1/contact_open -m '{"value":true}'

# A generic MQTT temperature reading, already expressed in Celsius.
docker compose exec -T mosquitto mosquitto_pub -h localhost -q 1 \
  -t home/mqtt/office/esp-1/temperature_c -m '21.75'

curl -fsS http://127.0.0.1:8086/health
curl -fsS http://127.0.0.1:8086/query \
  -H 'Content-Type: application/json' -d '{"metric":"temperature_c"}'
```

Each accepted report is flushed before its MQTT acknowledgment, so its points
are available to queries immediately. Points carry `device`, `room`, `protocol`
and the complete original `topic` as tags.

Stop the collector with Ctrl+C; it closes the broker connection, flushes and
closes the database, then reports message, point, rejection and ignored counts.
Stop the local broker with `docker compose down`.

The supplied anonymous broker is published only on the host's loopback interface.
For an existing broker, omit Compose and use `-broker tcp://broker-host:1883`.

## Configuration

| Option | Default | Purpose |
|--------|---------|---------|
| `-mode` | `mqtt` | `mqtt` or `simulate` |
| `-broker` | `MQTT_BROKER`, then `tcp://127.0.0.1:1883` | Broker URL; `tcp`, `ssl`, `tls`, `ws`, `wss` |
| `-topics` | `home/#,zigbee2mqtt/#` | Comma-separated MQTT subscription filters |
| `-client-id` | A generated client ID | Use a different ID for each running collector |
| `-rooms` | Unset | JSON file mapping Zigbee2MQTT friendly names to rooms |
| `-db` | `CHRONICLE_DB`, then `home_automation.db` | Database path |
| `-http-port` | `8086` | Chronicle HTTP port; `0` disables the HTTP server |
| `-connect-timeout` | `10s` | Connection/subscription timeout; greater than zero, at most `1m` |

Set `MQTT_USERNAME` and `MQTT_PASSWORD` when the broker requires credentials.
Credentials in broker URLs are rejected. TLS URLs use the system certificate
trust store. Subscription filters select incoming reports; they do not rename
topics or change the decoder's supported prefixes.

The collector uses MQTT 3.1.1, requests QoS 1 and resubscribes after a connection
loss. Initial connection failures, refused subscriptions and database write,
flush or close errors produce a nonzero exit. Malformed reports are counted and
skipped, with no raw payload logging. Bridge/control messages are counted as
ignored. Reports over 64 KiB are rejected.

This example uses a clean MQTT session. It does not provide an offline message
queue or an exactly-once guarantee: reports sent while disconnected can be
missed, and retained or redelivered reports can produce duplicate readings.
SIGINT/SIGTERM finish the report being written; queued, unprocessed messages are
not drained. Choose broker retention and publisher QoS for your installation.

## Supported sensor formats

### Normalized MQTT, Zigbee and Z-Wave

```text
home/<mqtt|zigbee|zwave>/<room>/<device>/<metric>
```

Metrics are `temperature_c`, `humidity_pct`, `motion`, `contact_open`, `power_w`
and `energy_kwh`. Send a finite JSON number, or an object with `value`:

```json
{"value":22.5,"timestamp":"2026-10-03T06:00:01.123456789Z"}
```

`motion` and `contact_open` also accept JSON booleans. For these two metrics,
`true`/1 means motion detected or contact open; `false`/0 means inactive or closed.
The other four metrics require numbers. Protocol, room and device come from the
topic, and numeric values must already use the units in the metric name.

The older `home/<protocol>/<room>/<metric>` format is accepted only with an
explicit device in its object payload, for example:

```text
home/zwave/laundry/power_w
{"device":"plug-1","value":18}
```

Native Z-Wave JS UI and ESPHome topic layouts are not decoded automatically.
Configure your gateway or automation to publish the normalized format after
mapping its sensor units. No Zigbee or Z-Wave radio adapter is opened by this
example; those devices reach Chronicle through an MQTT gateway.

### Zigbee2MQTT

Use the default `zigbee2mqtt/<friendly_name>` topic prefix with JSON output.
The complete friendly name is preserved, including slash-separated components.

| Report field | Chronicle metric | Conversion |
|--------------|------------------|------------|
| `temperature` | `temperature_c` | Celsius |
| `humidity` | `humidity_pct` | Percent |
| `occupancy` | `motion` | Boolean or numeric 0/1 |
| `contact` | `contact_open` | Inverted: Zigbee2MQTT `true` means closed |
| `power` | `power_w` | Watts |
| `energy` | `energy_kwh` | Kilowatt-hours |

Room precedence is a matching entry in `-rooms`, then a valid `room` string in
the payload, then `unknown`. See [rooms.example.json](rooms.example.json).
Unrecognized fields add no readings. One invalid recognized field rejects the
whole report.

### Timestamps and labels

An optional `timestamp` accepts RFC3339 (including fractional seconds) or integer
Unix **milliseconds**. Native Zigbee2MQTT reports also accept `last_seen`; if both
are present they must refer to the same time. Missing timestamps use reception
time. Invalid, fractional numeric, overflowing, conflicting and zero Unix
timestamps are rejected; dates must fit Chronicle's signed nanosecond range.

Device and room components must be nonempty UTF-8 labels without surrounding
whitespace, control characters or MQTT wildcards. Do not use `set`, `get`,
`availability` or `status` as ambiguous Zigbee device-name components, or
`bridge` as the first component: these overlap with ignored control topics.

## Run the simulation

```bash
go run . -mode simulate
```

This generates six tagged readings once per second, stores them through the same
Chronicle write path and prints a sample temperature-query count at shutdown.
It requires no MQTT broker or hardware.

## Development checks

Run these from `examples/home-automation`, which is a separate Go module:

```bash
go build ./...
go test ./...
go test -race ./...
go vet ./...
```

The tests cover payload units, contact inversion, malformed reports, timestamps,
device identity, room-map configuration and MQTT filter validation. Use the
broker publishing and HTTP query commands above to exercise real transport and
storage. To check reconnection, restart the broker, wait for another subscription
message, publish a new reading and query it.

## Protocol references

- [Zigbee2MQTT topics and payloads](https://www.zigbee2mqtt.io/guide/usage/mqtt_topics_and_messages.html)
- [Zigbee2MQTT contact semantics](https://www.zigbee2mqtt.io/devices/STSS-MULT-001.html)
- [Zigbee2MQTT power and energy units](https://www.zigbee2mqtt.io/devices/ZNCZ04LM.html)
- [Zigbee2MQTT output and timestamp settings](https://www.zigbee2mqtt.io/guide/configuration/all-settings.html)
- [Eclipse Paho Go MQTT client](https://github.com/eclipse-paho/paho.mqtt.golang)
