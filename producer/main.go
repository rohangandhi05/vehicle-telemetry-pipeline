package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// TelemetryEvent represents a vehicle sensor snapshot
type TelemetryEvent struct {
	VehicleID   string    `json:"vehicle_id"`
	Timestamp   time.Time `json:"timestamp"`
	Speed       float64   `json:"speed_kmh"`
	Battery     float64   `json:"battery_pct"`
	Latitude    float64   `json:"latitude"`
	Longitude   float64   `json:"longitude"`
	EngineTemp  float64   `json:"engine_temp_c"`
	Odometer    float64   `json:"odometer_km"`
	IsCharging  bool      `json:"is_charging"`
	ErrorCode   string    `json:"error_code,omitempty"`
}

// VehicleState holds per-vehicle simulation state
type VehicleState struct {
	ID        string
	Battery   float64
	Odometer  float64
	Lat       float64
	Lon       float64
	Speed     float64
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func newKafkaWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
}

func simulateEvent(state *VehicleState) TelemetryEvent {
	// Simulate movement
	state.Speed = math.Max(0, state.Speed+rand.Float64()*10-5)
	if state.Speed > 130 {
		state.Speed = 130
	}

	// Battery drains with speed, charges when parked
	if state.Speed > 0 {
		state.Battery = math.Max(0, state.Battery-rand.Float64()*0.05)
	} else {
		state.Battery = math.Min(100, state.Battery+rand.Float64()*0.3)
	}

	// GPS drift
	state.Lat += (rand.Float64() - 0.5) * 0.001
	state.Lon += (rand.Float64() - 0.5) * 0.001
	state.Odometer += state.Speed / 3600 // km per second

	errorCode := ""
	if state.Battery < 10 {
		errorCode = "LOW_BATTERY"
	} else if state.Speed > 120 {
		errorCode = "SPEED_WARNING"
	}

	return TelemetryEvent{
		VehicleID:  state.ID,
		Timestamp:  time.Now().UTC(),
		Speed:      math.Round(state.Speed*100) / 100,
		Battery:    math.Round(state.Battery*100) / 100,
		Latitude:   math.Round(state.Lat*1e6) / 1e6,
		Longitude:  math.Round(state.Lon*1e6) / 1e6,
		EngineTemp: 70 + rand.Float64()*30,
		Odometer:   math.Round(state.Odometer*10) / 10,
		IsCharging: state.Speed == 0 && state.Battery < 80,
		ErrorCode:  errorCode,
	}
}

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "vehicle-telemetry")
	intervalMs := 500 // emit every 500ms

	numVehicles := 10
	vehicles := make([]*VehicleState, numVehicles)
	for i := 0; i < numVehicles; i++ {
		vehicles[i] = &VehicleState{
			ID:       fmt.Sprintf("TSL-%04d", i+1),
			Battery:  60 + rand.Float64()*40,
			Odometer: rand.Float64() * 50000,
			Lat:      37.3382 + rand.Float64()*0.1,
			Lon:      -121.8863 + rand.Float64()*0.1,
			Speed:    rand.Float64() * 60,
		}
	}

	writer := newKafkaWriter(broker, topic)
	defer writer.Close()

	log.Printf("Producer started: broker=%s topic=%s vehicles=%d interval=%dms",
		broker, topic, numVehicles, intervalMs)

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	msgCount := 0

	for {
		select {
		case <-sigCh:
			log.Println("Shutting down producer...")
			cancel()
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			messages := make([]kafka.Message, numVehicles)
			for i, v := range vehicles {
				event := simulateEvent(v)
				payload, err := json.Marshal(event)
				if err != nil {
					log.Printf("Marshal error: %v", err)
					continue
				}
				messages[i] = kafka.Message{
					Key:   []byte(event.VehicleID),
					Value: payload,
				}
			}

			if err := writer.WriteMessages(ctx, messages...); err != nil {
				log.Printf("Kafka write error: %v", err)
			} else {
				msgCount += numVehicles
				if msgCount%1000 == 0 {
					log.Printf("Published %d total messages", msgCount)
				}
			}
		}
	}
}
