// Package main demonstrates collecting home-automation sensor data into Chronicle.
//
// It simulates MQTT topics for Zigbee and Z-Wave bridges (temperature, humidity,
// motion, contact, energy) and writes them as tagged time series. Optional
// docker-compose.yml runs Mosquitto if you want a real broker in the loop.
//
//	cd examples/home-automation && go run .
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
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

func main() {
	dbPath := envOr("CHRONICLE_DB", "home_automation.db")
	db, err := chronicle.Open(dbPath, chronicle.Config{
		Path:              dbPath,
		MaxMemory:         32 * 1024 * 1024,
		PartitionDuration: 30 * time.Minute,
		RetentionDuration: 7 * 24 * time.Hour,
		BufferSize:        2000,
		SyncInterval:      2 * time.Second,
		HTTPEnabled:       true,
		HTTPPort:          8086,
		Schemas: []chronicle.MetricSchema{
			{Name: "temperature_c", Tags: []chronicle.TagSchema{{Name: "device", Required: true}, {Name: "room", Required: true}, {Name: "protocol", Required: true}}},
			{Name: "humidity_pct", Tags: []chronicle.TagSchema{{Name: "device", Required: true}, {Name: "room", Required: true}, {Name: "protocol", Required: true}}},
			{Name: "motion", Tags: []chronicle.TagSchema{{Name: "device", Required: true}, {Name: "room", Required: true}, {Name: "protocol", Required: true}}},
			{Name: "contact_open", Tags: []chronicle.TagSchema{{Name: "device", Required: true}, {Name: "room", Required: true}, {Name: "protocol", Required: true}}},
			{Name: "power_w", Tags: []chronicle.TagSchema{{Name: "device", Required: true}, {Name: "room", Required: true}, {Name: "protocol", Required: true}}},
		},
	})
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()

	log.Println("Home automation collector started on :8086")
	log.Println("Simulating MQTT topics from Zigbee/Z-Wave bridges (Ctrl+C to stop)")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	devices := []struct {
		protocol, device, room, kind string
		base                         float64
	}{
		{"zigbee", "aqara-temp-1", "living-room", "temperature_c", 21.5},
		{"zigbee", "aqara-temp-1", "living-room", "humidity_pct", 45},
		{"zigbee", "aqara-motion-hall", "hallway", "motion", 0},
		{"zwave", "zoo-door-1", "front-door", "contact_open", 0},
		{"zwave", "zoo-plug-washer", "laundry", "power_w", 5},
		{"mqtt", "esphome-office", "office", "temperature_c", 22.0},
	}

	written := 0
	for {
		select {
		case <-stop:
			_ = db.Flush()
			log.Printf("stopped after %d points", written)
			printSampleQueries(db)
			return
		case now := <-ticker.C:
			var batch []chronicle.Point
			for _, d := range devices {
				ev := synthesize(d.protocol, d.device, d.room, d.kind, d.base, now)
				batch = append(batch, chronicle.Point{
					Metric:    ev.Metric,
					Value:     ev.Value,
					Timestamp: ev.Timestamp.UnixNano(),
					Tags: map[string]string{
						"device":   ev.Device,
						"room":     ev.Room,
						"protocol": ev.Protocol,
						"topic":    ev.Topic,
					},
				})
				if written%15 == 0 {
					payload, _ := json.Marshal(map[string]any{"topic": ev.Topic, "value": ev.Value, "ts": ev.Timestamp.Format(time.RFC3339)})
					log.Printf("mqtt %s", payload)
				}
			}
			if err := db.WriteBatch(batch); err != nil {
				log.Printf("write: %v", err)
				continue
			}
			written += len(batch)
			if written%50 == 0 {
				_ = db.Flush()
				log.Printf("flushed %d points", written)
			}
		}
	}
}

func synthesize(protocol, device, room, kind string, base float64, now time.Time) SensorEvent {
	topic := fmt.Sprintf("home/%s/%s/%s", protocol, room, kind)
	val := base
	switch kind {
	case "temperature_c":
		val = base + 1.5*math.Sin(float64(now.Unix()%3600)/3600*2*math.Pi) + (rand.Float64()-0.5)*0.2
	case "humidity_pct":
		val = clamp(base+float64(now.Minute()%10)-5+(rand.Float64()-0.5), 15, 90)
	case "motion":
		if rand.Float64() < 0.08 {
			val = 1
		} else {
			val = 0
		}
	case "contact_open":
		if now.Second() < 3 {
			val = 1
		} else {
			val = 0
		}
	case "power_w":
		if now.Minute()%7 < 3 {
			val = 400 + rand.Float64()*80
		} else {
			val = base + rand.Float64()*2
		}
	}
	return SensorEvent{
		Topic:     topic,
		Metric:    kind,
		Value:     val,
		Protocol:  protocol,
		Device:    device,
		Room:      room,
		Timestamp: now,
	}
}

func printSampleQueries(db *chronicle.DB) {
	q := &chronicle.Query{
		Metric: "temperature_c",
		TagFilters: []chronicle.TagFilter{
			{Key: "protocol", Op: chronicle.TagOpIn, Values: []string{"zigbee", "mqtt"}},
		},
	}
	res, err := db.Execute(q)
	if err != nil {
		log.Printf("query error: %v", err)
		return
	}
	log.Printf("sample temperature_c points=%d (protocol IN zigbee,mqtt)", len(res.Points))
	for i, p := range res.Points {
		if i >= 5 {
			break
		}
		log.Printf("  %s/%s room=%s value=%.2f", p.Tags["device"], p.Tags["protocol"], p.Tags["room"], p.Value)
	}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
