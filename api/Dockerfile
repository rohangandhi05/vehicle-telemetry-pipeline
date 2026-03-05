FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o api .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/api .

ENV REDIS_ADDR=redis:6379
ENV PORT=8080
ENV GIN_MODE=release

EXPOSE 8080

ENTRYPOINT ["./api"]
