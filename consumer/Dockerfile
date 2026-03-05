FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o consumer .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/consumer .

ENV KAFKA_BROKER=kafka:9092
ENV KAFKA_TOPIC=vehicle-telemetry
ENV KAFKA_GROUP_ID=telemetry-consumer-group
ENV REDIS_ADDR=redis:6379
ENV METRICS_PORT=2112

EXPOSE 2112

ENTRYPOINT ["./consumer"]
