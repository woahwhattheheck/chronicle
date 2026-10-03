package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// decodeMessage accepts the example's normalized home/<protocol>/<room>/<device>/<metric>
// topics and Zigbee2MQTT's JSON device topics. Z-Wave gateways must normalize their
// sensor values and units to home/zwave/...; native Z-Wave JS UI topic, payload and
// preferred-scale settings are configurable, so this example does not infer them.
// Unsupported topic prefixes and bridge/control messages produce no readings.
// Errors deliberately omit payload contents, which can include private metadata.
func decodeMessage(topic string, payload []byte, receivedAt time.Time, rooms map[string]string) ([]SensorEvent, error) {
	parts := strings.Split(topic, "/")
	switch parts[0] {
	case "home":
		if isControlSuffix(parts) {
			return nil, nil
		}
		return decodeHomeMessage(topic, parts, payload, receivedAt)
	case "zigbee2mqtt":
		if len(parts) > 1 && parts[1] == "bridge" {
			return nil, nil
		}
		if len(parts) > 2 && parts[len(parts)-1] == "availability" {
			return nil, nil
		}
		// /set, /get and their attribute subtopics are commands, not sensor
		// reports. Avoid these ambiguous components in device friendly names.
		for i := 2; i < len(parts); i++ {
			part := parts[i]
			if part == "set" || part == "get" {
				return nil, nil
			}
		}
		return decodeZigbeeMessage(topic, parts, payload, receivedAt, rooms)
	default:
		return nil, nil
	}
}

func isControlSuffix(parts []string) bool {
	switch parts[len(parts)-1] {
	case "set", "get", "availability", "status":
		return true
	default:
		return false
	}
}

func decodeHomeMessage(topic string, parts []string, payload []byte, receivedAt time.Time) ([]SensorEvent, error) {
	if len(parts) != 5 && len(parts) != 4 {
		return nil, errors.New("home topic must contain protocol, room, device and metric")
	}
	if parts[1] != "mqtt" && parts[1] != "zigbee" && parts[1] != "zwave" {
		return nil, errors.New("unsupported home protocol")
	}
	for _, part := range parts[2:] {
		if !validSensorLabel(part) {
			return nil, errors.New("home topic contains an invalid label")
		}
	}
	metric := parts[len(parts)-1]
	if !supportedMetric(metric) {
		return nil, errors.New("unsupported home metric")
	}
	decoded, err := decodeSensorJSON(payload)
	if err != nil {
		return nil, err
	}
	fields, isObject := decoded.(map[string]any)
	value := decoded
	if isObject {
		value = fields["value"]
	}
	device := parts[3]
	if len(parts) == 4 {
		// The older home/<protocol>/<room>/<metric> form has no device in
		// its topic. Require an explicit device rather than merging sensors.
		device, _ = fields["device"].(string)
		if !validSensorLabel(device) {
			return nil, errors.New("legacy home topic requires a device in the JSON payload")
		}
	}
	reading, err := sensorValue(value, metric)
	if err != nil {
		return nil, err
	}
	timestamp, err := sensorTimestamp(fields, receivedAt, "timestamp")
	if err != nil {
		return nil, err
	}
	return []SensorEvent{{
		Topic: topic, Metric: metric, Value: reading, Protocol: parts[1],
		Device: device, Room: parts[2], Timestamp: timestamp,
	}}, nil
}

func decodeZigbeeMessage(topic string, parts []string, payload []byte, receivedAt time.Time, rooms map[string]string) ([]SensorEvent, error) {
	if len(parts) < 2 {
		return nil, errors.New("Zigbee2MQTT topic requires a device name")
	}
	for _, part := range parts[1:] {
		if !validSensorLabel(part) {
			return nil, errors.New("Zigbee2MQTT topic contains an invalid device name")
		}
	}
	// Friendly names may include '/', so retain the complete name as the key.
	device := strings.Join(parts[1:], "/")
	decoded, err := decodeSensorJSON(payload)
	if err != nil {
		return nil, err
	}
	fields, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("Zigbee2MQTT sensor payload must be a JSON object")
	}
	room := rooms[device]
	if !validSensorLabel(room) {
		room, _ = fields["room"].(string)
		if !validSensorLabel(room) {
			room = "unknown"
		}
	}
	timestamp, err := sensorTimestamp(fields, receivedAt, "timestamp", "last_seen")
	if err != nil {
		return nil, err
	}
	mappings := []struct{ field, metric string }{
		{"temperature", "temperature_c"},
		{"humidity", "humidity_pct"},
		{"occupancy", "motion"},
		{"contact", "contact_open"},
		{"power", "power_w"},
		{"energy", "energy_kwh"},
	}
	var events []SensorEvent
	for _, mapping := range mappings {
		value, present := fields[mapping.field]
		if !present {
			continue
		}
		reading, err := sensorValue(value, mapping.metric)
		if err != nil {
			// Reject the entire report rather than return a partially valid batch.
			return nil, err
		}
		if mapping.field == "contact" {
			// Zigbee2MQTT contact=true means closed; contact_open=1 means open.
			reading = 1 - reading
		}
		events = append(events, SensorEvent{
			Topic: topic, Metric: mapping.metric, Value: reading, Protocol: "zigbee",
			Device: device, Room: room, Timestamp: timestamp,
		})
	}
	return events, nil
}

func decodeSensorJSON(payload []byte) (any, error) {
	if !utf8.Valid(payload) {
		return nil, errors.New("sensor payload is not valid UTF-8 JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.New("sensor payload is not valid JSON")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("sensor payload must contain exactly one JSON value")
	}
	return value, nil
}

func supportedMetric(metric string) bool {
	switch metric {
	case "temperature_c", "humidity_pct", "motion", "contact_open", "power_w", "energy_kwh":
		return true
	default:
		return false
	}
}

func sensorValue(raw any, metric string) (float64, error) {
	binary := metric == "motion" || metric == "contact_open"
	var value float64
	switch raw := raw.(type) {
	case json.Number:
		var err error
		value, err = raw.Float64()
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, errors.New("sensor reading must be a finite number")
		}
	case bool:
		if !binary {
			return 0, errors.New("numeric sensor reading cannot be a boolean")
		}
		if raw {
			value = 1
		}
	default:
		return 0, errors.New("sensor reading must be a number or a binary boolean")
	}
	if binary && value != 0 && value != 1 {
		return 0, errors.New("binary sensor reading must be zero, one or a boolean")
	}
	return value, nil
}

// Timestamps are RFC3339 (including fractional seconds) or integer Unix milliseconds.
// Absent timestamps use the reception time. Supplied timestamps must never silently
// fall back to reception time, including Chronicle's reserved zero timestamp.
func sensorTimestamp(fields map[string]any, receivedAt time.Time, keys ...string) (time.Time, error) {
	timestamp := receivedAt
	provided := false
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var parsed time.Time
		var err error
		switch raw := raw.(type) {
		case string:
			parsed, err = time.Parse(time.RFC3339Nano, raw)
		case json.Number:
			var millis int64
			millis, err = raw.Int64()
			if err == nil {
				parsed = time.UnixMilli(millis)
			}
		default:
			err = errors.New("invalid timestamp type")
		}
		if err != nil || !validSensorTime(parsed) {
			return time.Time{}, errors.New("sensor timestamp must be RFC3339 or Unix milliseconds within Chronicle's timestamp range")
		}
		if provided && !parsed.Equal(timestamp) {
			return time.Time{}, errors.New("sensor payload contains conflicting timestamps")
		}
		timestamp, provided = parsed, true
	}
	if !validSensorTime(timestamp) {
		return time.Time{}, errors.New("sensor reception time is outside Chronicle's timestamp range")
	}
	return timestamp, nil
}

func validSensorTime(value time.Time) bool {
	return !value.IsZero() && value.UnixNano() != 0 && time.Unix(0, value.UnixNano()).Equal(value)
}

func validSensorLabel(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || !utf8.ValidString(value) || strings.ContainsAny(value, "/+#") {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
