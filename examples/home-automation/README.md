# Home Automation Example (MQTT / Zigbee / Z-Wave)

Collect sensor readings from a simulated home-automation fabric into Chronicle.

Bounty #13 from `docs/BOUNTY_PROGRAM.md` ($50).

## Run

```bash
cd examples/home-automation
go run .

# optional real broker
docker compose up -d
```

The demo synthesizes MQTT-style topics:

```
home/zigbee/living-room/temperature_c
home/zigbee/hallway/motion
home/zwave/front-door/contact_open
home/zwave/laundry/power_w
home/mqtt/office/temperature_c
```

Each reading is stored with tags `device`, `room`, `protocol`, and `topic`.

## Verify

```bash
curl -s http://127.0.0.1:8086/health
# After Ctrl+C the process prints a sample aggregated temperature query.
```

Query programmatically (see end of `main.go`) filtering `protocol IN (zigbee, mqtt)`.

## Mapping real bridges

| Bridge | Typical topics | Chronicle metric |
|--------|----------------|------------------|
| Zigbee2MQTT | `zigbee2mqtt/<friendly_name>` | map JSON fields → `temperature_c`, `humidity_pct`, `motion` |
| Z-Wave JS UI | MQTT messages under `zwave/#` | map `binary_sensor` / `sensor` classes |
| ESPHome | `esphome/<node>/sensor/#` | one metric per sensor name |

Wire a small subscriber that unmarshals payloads and calls `db.Write` / `WriteBatch` using the same tag schema.

## Files

| File | Purpose |
|------|---------|
| `main.go` | Collector + simulator + sample query |
| `docker-compose.yml` | Optional Mosquitto |
| `mosquitto.conf` | Anonymous listener for local experiments |
