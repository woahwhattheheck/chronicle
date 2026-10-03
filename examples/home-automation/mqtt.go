package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chronicle-db/chronicle"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const maxSensorPayload = 64 * 1024

type receivedMessage struct {
	message mqtt.Message
	at      time.Time
}

func validTopicFilter(filter string) bool {
	if filter == "" || len(filter) > 65535 || !utf8.ValidString(filter) || strings.ContainsRune(filter, 0) {
		return false
	}
	parts := strings.Split(filter, "/")
	for index, part := range parts {
		if strings.Contains(part, "#") && (part != "#" || index != len(parts)-1) {
			return false
		}
		if strings.Contains(part, "+") && part != "+" {
			return false
		}
	}
	return true
}

func runMQTT(parent context.Context, db *chronicle.DB, cfg options, stats *collectionStats) error {
	ctx, cancel := context.WithCancel(parent)
	messages := make(chan receivedMessage, 32)
	failures := make(chan error, 1)
	subscribed := make(chan struct{}, 1)
	fail := func(err error) {
		select {
		case failures <- err:
		case <-ctx.Done():
		default:
		}
	}
	clientOptions := mqtt.NewClientOptions().
		AddBroker(cfg.broker).
		SetClientID(cfg.clientID).
		SetProtocolVersion(4).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(false).
		SetConnectTimeout(cfg.connectTimeout).
		SetWriteTimeout(cfg.connectTimeout).
		SetMaxReconnectInterval(10 * time.Second).
		SetOrderMatters(true).
		SetAutoAckDisabled(true).
		SetUsername(os.Getenv("MQTT_USERNAME")).
		SetPassword(os.Getenv("MQTT_PASSWORD"))
	// The callback only queues work; database writes never block Paho's router.
	// The bounded queue applies backpressure if storage falls behind.
	clientOptions.SetDefaultPublishHandler(func(_ mqtt.Client, message mqtt.Message) {
		received := receivedMessage{message: message, at: time.Now()}
		select {
		case messages <- received:
		case <-ctx.Done():
		}
	})
	clientOptions.SetConnectionLostHandler(func(_ mqtt.Client, _ error) {
		if ctx.Err() == nil {
			log.Print("MQTT connection lost; reconnecting")
		}
	})
	clientOptions.SetOnConnectHandler(func(client mqtt.Client) {
		filters := make(map[string]byte, len(cfg.topics))
		for _, topic := range cfg.topics {
			filters[topic] = 1
		}
		token := client.SubscribeMultiple(filters, nil)
		timer := time.NewTimer(cfg.connectTimeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			fail(errors.New("MQTT subscription timed out"))
		case <-token.Done():
			if err := token.Error(); err != nil {
				fail(fmt.Errorf("MQTT subscription failed: %w", err))
				return
			}
			// MQTT 3.1.1 may return a successful SUBACK packet containing
			// an individual filter rejection (0x80).
			if result, ok := token.(*mqtt.SubscribeToken); ok {
				for _, code := range result.Result() {
					if code == 0x80 {
						fail(errors.New("MQTT broker rejected a subscription filter"))
						return
					}
				}
			}
			select {
			case subscribed <- struct{}{}:
			case <-ctx.Done():
			default:
			}
		}
	})
	client := mqtt.NewClient(clientOptions)
	defer func() {
		cancel() // Release a callback waiting to enqueue before disconnecting.
		client.Disconnect(1000)
	}()
	connection := client.Connect()
	connected := connection.Done()
	startup := time.NewTimer(2 * cfg.connectTimeout)
	defer startup.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-failures:
			return err
		case <-startup.C:
			return errors.New("MQTT startup timed out")
		case <-connected:
			if err := connection.Error(); err != nil {
				return fmt.Errorf("MQTT connection failed: %w", err)
			}
			connected = nil
		case <-subscribed:
			startup.Stop()
			log.Printf("MQTT subscribed: filters=%d qos=1", len(cfg.topics))
		case received := <-messages:
			message := received.message
			stats.messages++
			if len(message.Payload()) > maxSensorPayload {
				stats.rejected++
				log.Print("rejected sensor report: payload exceeds 64 KiB")
				message.Ack()
				continue
			}
			events, err := decodeMessage(message.Topic(), message.Payload(), received.at, cfg.rooms)
			if err != nil {
				stats.rejected++
				log.Printf("rejected sensor report: %v", err)
				message.Ack() // Malformed reports cannot improve by redelivery.
				continue
			}
			if len(events) == 0 {
				stats.ignored++
				message.Ack()
				continue
			}
			if err := writeEvents(db, events); err != nil {
				// Leave this message unacknowledged and stop on a storage
				// failure rather than count or silently discard the report.
				return err
			}
			stats.written += len(events)
			message.Ack()
		}
	}
}
