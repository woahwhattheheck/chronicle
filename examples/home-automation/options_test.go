package main

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func parseTestOptions(args ...string) (options, error) {
	// Explicit defaults make validation independent of the developer's environment.
	defaults := []string{"-db", "options-test.db", "-broker", "tcp://127.0.0.1:1883"}
	return parseOptions(append(defaults, args...), io.Discard)
}

func TestParseOptionsRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"broker without host", []string{"-broker", "tcp:///1883"}},
		{"unsupported broker scheme", []string{"-broker", "https://localhost:1883"}},
		{"broker port out of range", []string{"-broker", "tcp://127.0.0.1:65536"}},
		{"broker credentials", []string{"-broker", "tcp://secret-user:do-not-log-me@localhost:1883"}},
		{"broker fragment", []string{"-broker", "tcp://localhost:1883#fragment"}},
		{"embedded single-level wildcard", []string{"-topics", "home/sen+sor"}},
		{"embedded multi-level wildcard", []string{"-topics", "home/device#"}},
		{"nonterminal multi-level wildcard", []string{"-topics", "home/#/temperature_c"}},
		{"empty subscription filter", []string{"-topics", "home/#,"}},
		{"unknown mode", []string{"-mode", "unknown"}},
		{"negative HTTP port", []string{"-http-port", "-1"}},
		{"HTTP port out of range", []string{"-http-port", "65536"}},
		{"zero timeout", []string{"-connect-timeout", "0s"}},
		{"negative timeout", []string{"-connect-timeout", "-1s"}},
		{"excessive timeout", []string{"-connect-timeout", "61s"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseTestOptions(tc.args...)
			if err == nil {
				t.Fatal("invalid options were accepted")
			}
			if strings.Contains(err.Error(), "secret-user") || strings.Contains(err.Error(), "do-not-log-me") {
				t.Fatal("configuration error exposed broker credentials")
			}
		})
	}
}

func TestParseOptionsCoalescesSubscriptionFilters(t *testing.T) {
	cfg, err := parseTestOptions(
		"-topics", " home/#,zigbee2mqtt/#,home/#,home/+/+/+/temperature_c,zigbee2mqtt/# ",
		"-http-port", "0", "-connect-timeout", "1m",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"home/#", "zigbee2mqtt/#", "home/+/+/+/temperature_c"}
	if !slices.Equal(cfg.topics, want) {
		t.Fatalf("subscription filters = %v, want %v", cfg.topics, want)
	}
	if cfg.httpPort != 0 || cfg.connectTimeout != time.Minute {
		t.Fatalf("valid configuration limits were not preserved: port=%d timeout=%s", cfg.httpPort, cfg.connectTimeout)
	}
}

func TestParseOptionsRoomsPreserveZigbeeFriendlyNames(t *testing.T) {
	roomsFile := filepath.Join(t.TempDir(), "rooms.json")
	if err := os.WriteFile(roomsFile, []byte(`{"downstairs/multi-sensor":"living-room","door-1":"front-door"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := parseTestOptions("-rooms", roomsFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.rooms) != 2 || cfg.rooms["downstairs/multi-sensor"] != "living-room" || cfg.rooms["door-1"] != "front-door" {
		t.Fatalf("room map lost the full device identity: %v", cfg.rooms)
	}
	receivedAt := time.Date(2026, 10, 3, 6, 30, 0, 0, time.UTC)
	events, err := decodeMessage("zigbee2mqtt/downstairs/multi-sensor", []byte(`{"temperature":22.5,"room":"payload-room"}`), receivedAt, cfg.rooms)
	if err != nil || len(events) != 1 {
		t.Fatalf("configured device returned %d readings, error %v", len(events), err)
	}
	if events[0].Device != "downstairs/multi-sensor" || events[0].Room != "living-room" {
		t.Fatalf("configured room was not applied to the device: %+v", events[0])
	}
}

func TestParseOptionsRejectsInvalidRoomsFiles(t *testing.T) {
	for _, tc := range []struct{ name, content string }{
		{"not an object", `null`},
		{"non-string room", `{"sensor-1":42}`},
		{"empty device path component", `{"downstairs//sensor-1":"office"}`},
		{"invalid room label", `{"sensor-1":"office/desk"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roomsFile := filepath.Join(t.TempDir(), "rooms.json")
			if err := os.WriteFile(roomsFile, []byte(tc.content), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := parseTestOptions("-rooms", roomsFile); err == nil {
				t.Fatal("invalid room configuration was accepted")
			}
		})
	}
}

func TestParseOptionsDefaultClientIDIsMQTT311Portable(t *testing.T) {
	cfg, err := parseTestOptions()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.clientID) == 0 || len(cfg.clientID) > 23 {
		t.Fatalf("default client ID length = %d, want 1..23 bytes: %q", len(cfg.clientID), cfg.clientID)
	}
	const portable = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for _, r := range cfg.clientID {
		if !strings.ContainsRune(portable, r) {
			t.Fatalf("default client ID contains non-portable MQTT 3.1.1 character %q: %q", r, cfg.clientID)
		}
	}
}
