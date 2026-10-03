package main

import (
	"strings"
	"testing"
	"time"
)

func TestDecodeMessageHome(t *testing.T) {
	receivedAt := time.Date(2026, 10, 3, 6, 30, 0, 123456789, time.UTC)
	for _, tc := range []struct {
		name, topic, payload, protocol, device, room, metric string
		value                                                float64
		timestamp                                            time.Time
	}{
		{"number", "home/mqtt/office/esp-1/temperature_c", "22.5", "mqtt", "esp-1", "office", "temperature_c", 22.5, receivedAt},
		{"boolean", "home/zwave/entry/door-1/contact_open", "true", "zwave", "door-1", "entry", "contact_open", 1, receivedAt},
		{"timestamp", "home/zigbee/hall/motion-1/motion", `{"value":false,"timestamp":"2026-10-03T06:00:01.123456789Z"}`, "zigbee", "motion-1", "hall", "motion", 0, time.Date(2026, 10, 3, 6, 0, 1, 123456789, time.UTC)},
		{"legacy", "home/zwave/laundry/power_w", `{"device":"plug-1","value":18,"timestamp":1791009000123}`, "zwave", "plug-1", "laundry", "power_w", 18, time.UnixMilli(1791009000123)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events, err := decodeMessage(tc.topic, []byte(tc.payload), receivedAt, nil)
			if err != nil || len(events) != 1 {
				t.Fatalf("decodeMessage returned %d events, error %v", len(events), err)
			}
			ev := events[0]
			if ev.Topic != tc.topic || ev.Protocol != tc.protocol || ev.Device != tc.device || ev.Room != tc.room || ev.Metric != tc.metric || ev.Value != tc.value || !ev.Timestamp.Equal(tc.timestamp) {
				t.Fatalf("unexpected normalized event: %+v", ev)
			}
		})
	}
}

func TestDecodeMessageZigbeeReport(t *testing.T) {
	receivedAt := time.Date(2026, 10, 3, 6, 30, 0, 0, time.UTC)
	topic := "zigbee2mqtt/downstairs/multi-sensor"
	payload := []byte(`{"temperature":22.5,"humidity":51,"occupancy":true,"contact":true,"power":12.5,"energy":1.25,"battery":90,"room":"payload-room","last_seen":"2026-10-03T06:00:01.123Z"}`)
	events, err := decodeMessage(topic, payload, receivedAt, map[string]string{"downstairs/multi-sensor": "living-room"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"temperature_c": 22.5, "humidity_pct": 51, "motion": 1, "contact_open": 0, "power_w": 12.5, "energy_kwh": 1.25}
	if len(events) != len(want) {
		t.Fatalf("got %d events, want %d", len(events), len(want))
	}
	for _, ev := range events {
		value, ok := want[ev.Metric]
		if !ok || ev.Value != value || ev.Topic != topic || ev.Device != "downstairs/multi-sensor" || ev.Room != "living-room" || ev.Protocol != "zigbee" || !ev.Timestamp.Equal(time.Date(2026, 10, 3, 6, 0, 1, 123000000, time.UTC)) {
			t.Fatalf("unexpected Zigbee reading: %+v", ev)
		}
		delete(want, ev.Metric)
	}
}

func TestDecodeMessageZigbeeRoomFallback(t *testing.T) {
	receivedAt := time.Date(2026, 10, 3, 6, 30, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, payload, room string
	}{
		{"payload room", `{"contact":false,"room":"front-door"}`, "front-door"},
		{"missing room", `{"contact":false}`, "unknown"},
		{"invalid room", `{"contact":false,"room":42}`, "unknown"},
		{"control in room", `{"contact":false,"room":"front\ndoor"}`, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events, err := decodeMessage("zigbee2mqtt/door-1", []byte(tc.payload), receivedAt, map[string]string{"door-1": ""})
			if err != nil || len(events) != 1 {
				t.Fatalf("decodeMessage returned %d events, error %v", len(events), err)
			}
			if events[0].Room != tc.room || events[0].Value != 1 || !events[0].Timestamp.Equal(receivedAt) {
				t.Fatalf("unexpected open-contact event: %+v", events[0])
			}
		})
	}
}

func TestDecodeMessageRejectsInvalidReports(t *testing.T) {
	receivedAt := time.Date(2026, 10, 3, 6, 30, 0, 0, time.UTC)
	for _, tc := range []struct{ name, topic, payload string }{
		{"missing reading", "home/mqtt/office/esp-1/temperature_c", `{}`},
		{"nonfinite number", "home/mqtt/office/esp-1/temperature_c", `1e999`},
		{"boolean numeric reading", "home/mqtt/office/esp-1/temperature_c", `true`},
		{"invalid binary reading", "home/zwave/entry/door-1/contact_open", `2`},
		{"trailing JSON", "home/mqtt/office/esp-1/temperature_c", `21 {"secret":"do-not-log-me"}`},
		{"invalid timestamp", "home/mqtt/office/esp-1/temperature_c", `{"value":21,"timestamp":"do-not-log-me"}`},
		{"null timestamp", "home/mqtt/office/esp-1/temperature_c", `{"value":21,"timestamp":null}`},
		{"fractional milliseconds", "home/mqtt/office/esp-1/temperature_c", `{"value":21,"timestamp":1791009000123.5}`},
		{"reserved zero timestamp", "home/mqtt/office/esp-1/temperature_c", `{"value":21,"timestamp":"1970-01-01T00:00:00Z"}`},
		{"timestamp overflow", "home/mqtt/office/esp-1/temperature_c", `{"value":21,"timestamp":"2300-01-01T00:00:00Z"}`},
		{"millisecond overflow", "home/mqtt/office/esp-1/temperature_c", `{"value":21,"timestamp":9223372036854775807}`},
		{"missing legacy device", "home/zwave/laundry/power_w", `{"value":12}`},
		{"unknown metric", "home/mqtt/office/esp-1/arbitrary", `21`},
		{"invalid topic", "home/mqtt//esp-1/temperature_c", `21`},
		{"missing Zigbee name", "zigbee2mqtt", `{"temperature":21}`},
		{"invalid Zigbee JSON", "zigbee2mqtt/sensor-1", `{"temperature":`},
		{"partial Zigbee report", "zigbee2mqtt/sensor-1", `{"temperature":21,"humidity":"do-not-log-me"}`},
		{"conflicting timestamps", "zigbee2mqtt/sensor-1", `{"temperature":21,"timestamp":"2026-10-03T06:00:00Z","last_seen":"2026-10-03T06:00:01Z"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events, err := decodeMessage(tc.topic, []byte(tc.payload), receivedAt, nil)
			if err == nil || len(events) != 0 {
				t.Fatalf("invalid report returned %d events, error %v", len(events), err)
			}
			if strings.Contains(err.Error(), "do-not-log-me") {
				t.Fatal("error exposed payload contents")
			}
		})
	}
}

func TestDecodeMessageIgnoresControlAndNonSensorMessages(t *testing.T) {
	receivedAt := time.Date(2026, 10, 3, 6, 30, 0, 0, time.UTC)
	for _, topic := range []string{
		"zigbee2mqtt/bridge/state", "zigbee2mqtt/bridge/devices",
		"zigbee2mqtt/plug-1/set", "zigbee2mqtt/plug-1/get", "zigbee2mqtt/plug-1/set/state",
		"zigbee2mqtt/plug-1/availability", "home/mqtt/office/esp-1/temperature_c/set",
		"homeassistant/sensor/esp-1/config",
	} {
		events, err := decodeMessage(topic, []byte("not a sensor payload"), receivedAt, nil)
		if err != nil || len(events) != 0 {
			t.Errorf("topic %q returned %d events, error %v", topic, len(events), err)
		}
	}
	events, err := decodeMessage("zigbee2mqtt/remote-1", []byte(`{"action":"single","battery":80}`), receivedAt, nil)
	if err != nil || len(events) != 0 {
		t.Fatalf("non-sensor device report returned %d events, error %v", len(events), err)
	}
}
