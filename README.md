# 🚗 Real-Time Vehicle Telemetry Pipeline

A distributed, event-driven system that ingests, processes, and visualizes real-time vehicle sensor data. Built with Go, Apache Kafka, Redis, Docker, Kubernetes, and Prometheus/Grafana.

## Architecture

```
┌──────────────┐     Kafka Topic      ┌──────────────┐     Redis     ┌──────────────┐
│   Producer   │ ──────────────────► │   Consumer   │ ────────────► │   REST API   │
│  (Go)        │  vehicle-telemetry  │   (Go)       │               │   (Go/Gin)   │
│  10 vehicles │                     │  2 replicas  │               │  2 replicas  │
│  500ms/event │                     │  + Alerts    │               │  + HPA       │
└──────────────┘                     └──────────────┘               └──────────────┘
                                            │                               │
                                     Prometheus ◄─────────────────────────┘
                                            │
                                        Grafana
```

**Data flow:**
1. **Producer** simulates 10 vehicles, publishing telemetry events to Kafka every 500ms (~1,200 msg/min)
2. **Consumer** reads from Kafka, detects anomalies (low battery, speed warnings), caches vehicle state in Redis, and exposes Prometheus metrics
3. **API** serves low-latency vehicle status lookups from Redis
4. **Monitoring** stack (Prometheus + Grafana) visualizes throughput, latency, and per-vehicle metrics

## Tech Stack

| Component | Technology |
|---|---|
| Language | Go 1.22 |
| Message Broker | Apache Kafka |
| Cache / State Store | Redis 7 |
| REST Framework | Gin |
| Containerization | Docker |
| Orchestration | Kubernetes |
| Monitoring | Prometheus + Grafana |
| CI/CD | GitHub Actions |

## Quick Start (Local with Docker Compose)

**Prerequisites:** Docker Desktop, Docker Compose v2

```bash
# Clone and start everything
git clone https://github.com/yourname/vehicle-telemetry-pipeline
cd vehicle-telemetry-pipeline
docker compose up --build
```

Services will be available at:

| Service | URL |
|---|---|
| REST API | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| Consumer metrics | http://localhost:2112/metrics |

## API Reference

### Get all active vehicles
```http
GET /api/v1/vehicles
```
```json
{
  "count": 10,
  "vehicles": [
    {
      "vehicle_id": "RIV-0001",
      "last_seen": "2026-01-15T10:23:45Z",
      "speed_kmh": 72.4,
      "battery_pct": 83.2,
      "latitude": 37.3391,
      "longitude": -121.8951,
      "is_charging": false,
      "active_alert": "",
      "total_messages": 4820
    }
  ]
}
```

### Get single vehicle status
```http
GET /api/v1/vehicles/:id
```

### Get vehicle alert history
```http
GET /api/v1/vehicles/:id/alerts
```

### Fleet summary
```http
GET /api/v1/fleet/summary
```
```json
{
  "total_vehicles": 10,
  "avg_battery_pct": 67.4,
  "avg_speed_kmh": 48.1,
  "active_alerts": 2,
  "charging": 1
}
```

### Health check
```http
GET /health
```

## Kubernetes Deployment

```bash
# Apply all manifests
kubectl apply -f k8s/

# Watch pods come up
kubectl get pods -n telemetry -w

# Get API external IP
kubectl get svc telemetry-api-service -n telemetry
```

The API deployment includes a Horizontal Pod Autoscaler that scales from 2–10 replicas at 70% CPU.

## Monitoring

After `docker compose up`, open Grafana at http://localhost:3000 (admin/admin).

The **Vehicle Telemetry Dashboard** auto-provisions and includes:
- Messages processed per second
- Processing latency (p99)
- Alert trigger rate by type
- Per-vehicle battery percentage
- API request rate and p95 latency

## CI/CD

GitHub Actions runs on every push:

1. **Lint** — golangci-lint for all three services
2. **Unit Tests** — `go test -race` with coverage upload to Codecov
3. **Integration Tests** — spins up Kafka + Redis service containers
4. **Build & Push** — builds Docker images and pushes to GitHub Container Registry
5. **Deploy** — rolls out updated images to Kubernetes with rollout status checks

## Project Structure

```
vehicle-telemetry-pipeline/
├── producer/          # Kafka producer — vehicle sensor simulation
│   ├── main.go
│   └── Dockerfile
├── consumer/          # Kafka consumer — event processing + Redis writes
│   ├── main.go
│   └── Dockerfile
├── api/               # REST API — Redis-backed vehicle status endpoints
│   ├── main.go
│   └── Dockerfile
├── k8s/               # Kubernetes manifests
│   ├── 00-namespace-config.yaml
│   ├── 01-redis.yaml
│   ├── 02-kafka.yaml
│   └── 03-services.yaml
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/provisioning/
├── .github/workflows/
│   └── ci.yml
└── docker-compose.yml
```

## Key Design Decisions

- **Kafka consumer groups** allow horizontal scaling of the consumer — add more replicas to increase partition throughput
- **Redis TTL (24h)** ensures stale vehicle data is automatically evicted
- **Alert list capped at 50 entries** per vehicle via `LTRIM` to bound memory usage
- **Graceful shutdown** in all services via `context.WithCancel` + OS signal handling
- **Multi-stage Docker builds** keep final images under 20MB (Alpine base)
