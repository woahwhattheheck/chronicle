// Package main collects MQTT sensor reports into Chronicle. Zigbee2MQTT reports
// and normalized MQTT/Zigbee/Z-Wave topics are supported; -mode=simulate runs a
// broker-free demonstration.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/chronicle-db/chronicle"
)

// SensorEvent is a normalized home-automation reading.
type SensorEvent struct {
	Topic     string
	Metric    string
	Value     float64
	Protocol  string // mqtt / zigbee / zwave
	Device    string
	Room      string
	Timestamp time.Time
}

type options struct {
	mode, dbPath, broker, clientID string
	topics                         []string
	rooms                          map[string]string
	httpPort                       int
	connectTimeout                 time.Duration
}

type collectionStats struct {
	messages, written, rejected, ignored int
}

func main() {
	cfg, err := parseOptions(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Print(err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func parseOptions(args []string, output io.Writer) (options, error) {
	var cfg options
	var topics, roomsFile string
	flags := flag.NewFlagSet("home-automation", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.mode, "mode", "mqtt", "mqtt or simulate")
	flags.StringVar(&cfg.dbPath, "db", envOr("CHRONICLE_DB", "home_automation.db"), "database path")
	flags.StringVar(&cfg.broker, "broker", envOr("MQTT_BROKER", "tcp://127.0.0.1:1883"), "MQTT broker URL")
	flags.StringVar(&cfg.clientID, "client-id", defaultMQTTClientID(time.Now()), "MQTT client ID")
	flags.StringVar(&topics, "topics", "home/#,zigbee2mqtt/#", "comma-separated subscription filters")
	flags.StringVar(&roomsFile, "rooms", "", "JSON file mapping Zigbee2MQTT friendly names to rooms")
	flags.IntVar(&cfg.httpPort, "http-port", 8086, "Chronicle HTTP port; 0 disables HTTP")
	flags.DurationVar(&cfg.connectTimeout, "connect-timeout", 10*time.Second, "MQTT connection/subscription timeout")
	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	if flags.NArg() != 0 {
		return cfg, errors.New("unexpected positional arguments")
	}
	if cfg.mode != "mqtt" && cfg.mode != "simulate" {
		return cfg, errors.New("-mode must be mqtt or simulate")
	}
	if strings.TrimSpace(cfg.dbPath) == "" || cfg.httpPort < 0 || cfg.httpPort > 65535 {
		return cfg, errors.New("provide a database path and an HTTP port between 0 and 65535")
	}
	if cfg.connectTimeout <= 0 || cfg.connectTimeout > time.Minute {
		return cfg, errors.New("-connect-timeout must be greater than zero and at most one minute")
	}
	if cfg.mode == "mqtt" {
		broker, err := url.Parse(cfg.broker)
		if err != nil || broker.Hostname() == "" || broker.User != nil || broker.Fragment != "" {
			return cfg, errors.New("invalid broker URL; use MQTT_USERNAME/MQTT_PASSWORD for credentials")
		}
		switch broker.Scheme {
		case "tcp", "ssl", "tls", "ws", "wss":
		default:
			return cfg, errors.New("broker URL must use tcp, ssl, tls, ws or wss")
		}
		if broker.Port() != "" {
			port, err := strconv.Atoi(broker.Port())
			if err != nil || port < 1 || port > 65535 {
				return cfg, errors.New("broker port must be between 1 and 65535")
			}
		}
		if cfg.clientID == "" {
			return cfg, errors.New("-client-id must not be empty")
		}
		seen := make(map[string]bool)
		for _, topic := range strings.Split(topics, ",") {
			topic = strings.TrimSpace(topic)
			if !validTopicFilter(topic) {
				return cfg, errors.New("-topics contains an invalid MQTT subscription filter")
			}
			if !seen[topic] {
				cfg.topics = append(cfg.topics, topic)
				seen[topic] = true
			}
		}
	}
	if roomsFile != "" {
		body, err := os.ReadFile(roomsFile)
		if err != nil {
			return cfg, fmt.Errorf("read rooms file: %w", err)
		}
		if err := json.Unmarshal(body, &cfg.rooms); err != nil || cfg.rooms == nil {
			return cfg, errors.New("rooms file must be a JSON object mapping device names to room strings")
		}
		for device, room := range cfg.rooms {
			if !validSensorLabel(room) {
				return cfg, errors.New("rooms file contains an invalid device or room label")
			}
			for _, part := range strings.Split(device, "/") {
				if !validSensorLabel(part) {
					return cfg, errors.New("rooms file contains an invalid device or room label")
				}
			}
		}
	}
	return cfg, nil
}

func run(ctx context.Context, cfg options) (runErr error) {
	config := chronicle.DefaultConfig(cfg.dbPath)
	config.Storage.MaxMemory = 32 * 1024 * 1024
	config.Storage.PartitionDuration = 30 * time.Minute
	config.Storage.BufferSize = 2000
	config.Retention.RetentionDuration = 7 * 24 * time.Hour
	config.WAL.SyncInterval = 2 * time.Second
	config.HTTP.HTTPEnabled = cfg.httpPort != 0
	config.HTTP.HTTPPort = cfg.httpPort
	for _, metric := range []string{"temperature_c", "humidity_pct", "motion", "contact_open", "power_w", "energy_kwh"} {
		config.Schemas = append(config.Schemas, chronicle.MetricSchema{
			Name: metric,
			Tags: []chronicle.TagSchema{
				{Name: "device", Required: true}, {Name: "room", Required: true},
				{Name: "protocol", Required: true},
			},
		})
	}
	db, err := chronicle.Open(cfg.dbPath, config)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	stats := &collectionStats{}
	defer func() {
		if err := db.Flush(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("flush database: %w", err))
		}
		if err := db.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close database: %w", err))
		}
		log.Printf("stopped: messages=%d points=%d rejected=%d ignored=%d",
			stats.messages, stats.written, stats.rejected, stats.ignored)
	}()
	if cfg.httpPort != 0 {
		log.Printf("Chronicle HTTP API on :%d", cfg.httpPort)
	}
	if cfg.mode == "simulate" {
		runErr = runSimulation(ctx, db, stats)
	} else {
		runErr = runMQTT(ctx, db, cfg, stats)
	}
	if runErr == nil {
		runErr = printSampleQueries(db)
	}
	return runErr
}

func writeEvents(db *chronicle.DB, events []SensorEvent) error {
	points := make([]chronicle.Point, 0, len(events))
	for _, event := range events {
		points = append(points, chronicle.Point{
			Metric: event.Metric, Value: event.Value, Timestamp: event.Timestamp.UnixNano(),
			Tags: map[string]string{
				"device": event.Device, "room": event.Room,
				"protocol": event.Protocol, "topic": event.Topic,
			},
		})
	}
	if err := db.WriteBatch(points); err != nil {
		return fmt.Errorf("write sensor report: %w", err)
	}
	// Sensor reports are small and infrequent. Flush each complete report before
	// acknowledging it, and make it immediately available to HTTP queries.
	if err := db.Flush(); err != nil {
		return fmt.Errorf("flush sensor report: %w", err)
	}
	return nil
}

func runSimulation(ctx context.Context, db *chronicle.DB, stats *collectionStats) error {
	log.Print("Simulation mode: generating readings once per second")
	devices := []struct {
		protocol, device, room, metric string
		base                           float64
	}{
		{"zigbee", "aqara-temp-1", "living-room", "temperature_c", 21.5},
		{"zigbee", "aqara-temp-1", "living-room", "humidity_pct", 45},
		{"zigbee", "aqara-motion-hall", "hallway", "motion", 0},
		{"zwave", "zoo-door-1", "front-door", "contact_open", 0},
		{"zwave", "zoo-plug-washer", "laundry", "power_w", 5},
		{"mqtt", "esphome-office", "office", "temperature_c", 22.0},
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			events := make([]SensorEvent, 0, len(devices))
			for _, device := range devices {
				events = append(events, synthesize(device.protocol, device.device, device.room, device.metric, device.base, now))
			}
			if err := writeEvents(db, events); err != nil {
				return err
			}
			stats.written += len(events)
		}
	}
}

func synthesize(protocol, device, room, kind string, base float64, now time.Time) SensorEvent {
	topic := fmt.Sprintf("home/%s/%s/%s/%s", protocol, room, device, kind)
	value := base
	switch kind {
	case "temperature_c":
		value = base + 1.5*math.Sin(float64(now.Unix()%3600)/3600*2*math.Pi) + (rand.Float64()-0.5)*0.2
	case "humidity_pct":
		value = clamp(base+float64(now.Minute()%10)-5+(rand.Float64()-0.5), 15, 90)
	case "motion":
		if rand.Float64() < 0.08 {
			value = 1
		}
	case "contact_open":
		if now.Second() < 3 {
			value = 1
		}
	case "power_w":
		if now.Minute()%7 < 3 {
			value = 400 + rand.Float64()*80
		} else {
			value = base + rand.Float64()*2
		}
	}
	return SensorEvent{
		Topic: topic, Metric: kind, Value: value, Protocol: protocol,
		Device: device, Room: room, Timestamp: now,
	}
}

func printSampleQueries(db *chronicle.DB) error {
	result, err := db.Execute(&chronicle.Query{
		Metric: "temperature_c",
		TagFilters: []chronicle.TagFilter{
			{Key: "protocol", Op: chronicle.TagOpIn, Values: []string{"zigbee", "mqtt"}},
		},
	})
	if err != nil {
		return fmt.Errorf("sample temperature query: %w", err)
	}
	log.Printf("sample temperature_c points=%d (protocol IN zigbee,mqtt)", len(result.Points))
	return nil
}

func clamp(value, low, high float64) float64 {
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}

// MQTT 3.1.1 servers are only required to accept ClientIds made of
// alphanumeric characters and no more than 23 UTF-8 bytes. Keep the generated
// default inside that portable subset while retaining nanosecond-level entropy.
func defaultMQTTClientID(now time.Time) string {
	return fmt.Sprintf("chronicle%014x", uint64(now.UnixNano())&0x00ffffffffffffff)
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
