package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// VehicleStatus mirrors the consumer's stored struct
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
	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "api_http_requests_total",
			Help: "Total HTTP requests",
		},
		[]string{"method", "endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "api_request_duration_seconds",
			Help:    "HTTP request latency",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
)

func init() {
	prometheus.MustRegister(httpRequests, requestDuration)
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// prometheusMiddleware records latency + request count per route
func prometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start).Seconds()
		status := http.StatusText(c.Writer.Status())
		httpRequests.WithLabelValues(c.Request.Method, c.FullPath(), status).Inc()
		requestDuration.WithLabelValues(c.Request.Method, c.FullPath()).Observe(duration)
	}
}

func setupRouter(rdb *redis.Client) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(prometheusMiddleware())

	// Health check
	r.GET("/health", func(c *gin.Context) {
		ctx := c.Request.Context()
		if err := rdb.Ping(ctx).Err(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy", "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Prometheus metrics
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	v1 := r.Group("/api/v1")
	{
		// GET /api/v1/vehicles — list all active vehicles
		v1.GET("/vehicles", func(c *gin.Context) {
			ctx := c.Request.Context()
			keys, err := rdb.Keys(ctx, "vehicle:status:*").Result()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}

			vehicles := make([]VehicleStatus, 0, len(keys))
			for _, key := range keys {
				val, err := rdb.Get(ctx, key).Result()
				if err != nil {
					continue
				}
				var v VehicleStatus
				if err := json.Unmarshal([]byte(val), &v); err == nil {
					vehicles = append(vehicles, v)
				}
			}

			c.JSON(http.StatusOK, gin.H{
				"count":    len(vehicles),
				"vehicles": vehicles,
			})
		})

		// GET /api/v1/vehicles/:id — get single vehicle status
		v1.GET("/vehicles/:id", func(c *gin.Context) {
			ctx := c.Request.Context()
			vehicleID := c.Param("id")
			key := "vehicle:status:" + vehicleID

			val, err := rdb.Get(ctx, key).Result()
			if err == redis.Nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "vehicle not found", "vehicle_id": vehicleID})
				return
			} else if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}

			var status VehicleStatus
			if err := json.Unmarshal([]byte(val), &status); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to parse vehicle data"})
				return
			}

			c.JSON(http.StatusOK, status)
		})

		// GET /api/v1/vehicles/:id/alerts — get recent alerts for a vehicle
		v1.GET("/vehicles/:id/alerts", func(c *gin.Context) {
			ctx := c.Request.Context()
			vehicleID := c.Param("id")
			alertKey := "vehicle:alerts:" + vehicleID

			alerts, err := rdb.LRange(ctx, alertKey, 0, 49).Result()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}

			parsed := make([]map[string]interface{}, 0, len(alerts))
			for _, a := range alerts {
				var entry map[string]interface{}
				if err := json.Unmarshal([]byte(a), &entry); err == nil {
					parsed = append(parsed, entry)
				}
			}

			c.JSON(http.StatusOK, gin.H{
				"vehicle_id": vehicleID,
				"count":      len(parsed),
				"alerts":     parsed,
			})
		})

		// GET /api/v1/fleet/summary — fleet-wide stats
		v1.GET("/fleet/summary", func(c *gin.Context) {
			ctx := c.Request.Context()
			keys, err := rdb.Keys(ctx, "vehicle:status:*").Result()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}

			var totalBattery, totalSpeed float64
			activeAlerts := 0
			charging := 0

			for _, key := range keys {
				val, err := rdb.Get(ctx, key).Result()
				if err != nil {
					continue
				}
				var v VehicleStatus
				if err := json.Unmarshal([]byte(val), &v); err != nil {
					continue
				}
				totalBattery += v.Battery
				totalSpeed += v.Speed
				if v.ActiveAlert != "" {
					activeAlerts++
				}
				if v.IsCharging {
					charging++
				}
			}

			count := len(keys)
			avgBattery, avgSpeed := 0.0, 0.0
			if count > 0 {
				avgBattery = totalBattery / float64(count)
				avgSpeed = totalSpeed / float64(count)
			}

			c.JSON(http.StatusOK, gin.H{
				"total_vehicles":  count,
				"avg_battery_pct": avgBattery,
				"avg_speed_kmh":   avgSpeed,
				"active_alerts":   activeAlerts,
				"charging":        charging,
			})
		})
	}

	return r
}

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	port := getEnv("PORT", "8080")

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     20,
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}
	log.Printf("Connected to Redis at %s", redisAddr)

	router := setupRouter(rdb)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("API server listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Gracefully shutting down API...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}
