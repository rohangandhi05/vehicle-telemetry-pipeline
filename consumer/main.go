package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"net/http"
)

// TelemetryEvent mirrors the producer's struct
type TelemetryEvent struct {
	VehicleID  string    `json:"vehicle_id"`
	Timestamp  time.Time `json:"timestamp"`
	Speed      float64   `json:"speed_kmh"`
	Battery    float64   `json:"battery_pct"`
	Latitude   float64   `json:"latitude"`
	Longitude  float64   `json:"longitude"`
	EngineTemp float64   `json:"engine_temp_c"`
	Odometer   float64   `json:"odometer_km"`
	IsCharging bool      `json:"is_charging"`
	ErrorCode  string    `json:"error_code,omitempty"`
}

// VehicleStatus is the enriched state stored in Redis
type VehicleStatus struct {
	VehicleID     string    `json:"vehicle_id"`
	LastSeen      time.Time `json:"last_seen"`
	Speed         float64   `json:"speed_kmh"`
	Battery       float64   `json:"battery_pct"`
	Latitude      float64   `json:"latitude"`
	Longitude     float64   `json:"longitude"`
	EngineTemp    float64   `json:"engine_temp_c"`
	Odometer      float64   `json:"odometer_km"`
	IsCharging    bool      `json:"is_charging"`
	ActiveAlert   string    `json:"active_alert,omitempty"`
	AlertCount    int       `json:"alert_count"`
	TotalMessages int       `json:"total_messages"`
}

var (
	messagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "telemetry_messages_processed_total",
			Help: "Total number of telemetry messages processed",
		},
		[]string{"vehicle_id"},
	)
	alertsTriggered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "telemetry_alerts_triggered_total",
			Help: "Total number of alerts triggered",
		},
		[]string{"alert_type"},
	)
	processingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "telemetry_processing_duration_seconds",
			Help:    "Time spent processing each telemetry message",
			Buckets: prometheus.DefBuckets,
		},
	)
	vehicleBattery = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vehicle_battery_pct",
			Help: "Current battery percentage per vehicle",
		},
		[]string{"vehicle_id"},
	)
	vehicleSpeed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vehicle_speed_kmh",
			Help: "Current speed in km/h per vehicle",
		},
		[]string{"vehicle_id"},
	)
)

func init() {
	prometheus.MustRegister(messagesProcessed, alertsTriggered, processingLatency, vehicleBattery, vehicleSpeed)
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func processEvent(ctx context.Context, rdb *redis.Client, event TelemetryEvent) error {
	start := time.Now()
	defer func() {
		processingLatency.Observe(time.Since(start).Seconds())
	}()

	statusKey := "vehicle:status:" + event.VehicleID
	alertKey := "vehicle:alerts:" + event.VehicleID

	// Load existing status or create new
	var status VehicleStatus
	val, err := rdb.Get(ctx, statusKey).Result()
	if err == redis.Nil {
		status = VehicleStatus{VehicleID: event.VehicleID}
	} else if err != nil {
		return err
	} else {
		if err := json.Unmarshal([]byte(val), &status); err != nil {
			return err
		}
	}

	// Update status fields
	status.LastSeen = event.Timestamp
	status.Speed = event.Speed
	status.Battery = event.Battery
	status.Latitude = event.Latitude
	status.Longitude = event.Longitude
	status.EngineTemp = event.EngineTemp
	status.Odometer = event.Odometer
	status.IsCharging = event.IsCharging
	status.TotalMessages++
	status.ActiveAlert = event.ErrorCode

	// Handle alerts
	if event.ErrorCode != "" {
		status.AlertCount++
		alertsTriggered.WithLabelValues(event.ErrorCode).Inc()

		alertEntry, _ := json.Marshal(map[string]interface{}{
			"code":      event.ErrorCode,
			"timestamp": event.Timestamp,
			"speed":     event.Speed,
			"battery":   event.Battery,
		})
		// Push to alert list, keep last 50
		pipe := rdb.Pipeline()
		pipe.LPush(ctx, alertKey, string(alertEntry))
		pipe.LTrim(ctx, alertKey, 0, 49)
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("Alert pipeline error: %v", err)
		}
		log.Printf("ALERT [%s] %s | battery=%.1f%% speed=%.1fkm/h",
			event.VehicleID, event.ErrorCode, event.Battery, event.Speed)
	}

	// Persist updated status with 24h TTL
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return err
	}
	if err := rdb.Set(ctx, statusKey, statusBytes, 24*time.Hour).Err(); err != nil {
		return err
	}

	// Update Prometheus gauges
	vehicleBattery.WithLabelValues(event.VehicleID).Set(event.Battery)
	vehicleSpeed.WithLabelValues(event.VehicleID).Set(event.Speed)
	messagesProcessed.WithLabelValues(event.VehicleID).Inc()

	return nil
}

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "vehicle-telemetry")
	groupID := getEnv("KAFKA_GROUP_ID", "telemetry-consumer-group")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	metricsPort := getEnv("METRICS_PORT", "2112")

	// Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}
	log.Printf("Connected to Redis at %s", redisAddr)

	// Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
	defer reader.Close()

	log.Printf("Consumer started: broker=%s topic=%s group=%s", broker, topic, groupID)

	// Prometheus metrics endpoint
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		})
		log.Printf("Metrics server listening on :%s", metricsPort)
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			log.Fatalf("Metrics server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down consumer...")
		cancel()
	}()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // graceful shutdown
			}
			log.Printf("Kafka read error: %v", err)
			continue
		}

		var event TelemetryEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("JSON unmarshal error: %v", err)
			continue
		}

		if err := processEvent(ctx, rdb, event); err != nil {
			log.Printf("Process error for %s: %v", event.VehicleID, err)
		}
	}
}
