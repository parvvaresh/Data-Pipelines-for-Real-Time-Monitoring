package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type LogEvent struct {
	TS        string `json:"ts"`
	Host      string `json:"host"`
	Service   string `json:"service"`
	Level     string `json:"level"`
	Path      string `json:"path"`
	LatencyMS int    `json:"latency_ms"`
	Message   string `json:"message"`
	TraceID   string `json:"trace_id"`
	RequestID string `json:"request_id"`
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "kafka:9092")
	topic := getEnv("KAFKA_TOPIC", "logs.v1")

	ratePerSec := getEnvFloat("RATE_PER_SEC", 3)
	errorRate := getEnvFloat("ERROR_RATE", 0.15)
	host, _ := os.Hostname()

	// Kafka writer (Producer)
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	fmt.Printf("[fake-app-go] sending logs to %s (topic=%s)\n", brokers, topic)

	services := []string{"auth", "api", "worker", "web"}
	paths := []string{"/v1/login", "/v1/resource", "/v1/jobs", "/v1/report", "/healthz"}
	levels := []string{"info", "warn", "error"}

	interval := time.Second / time.Duration(ratePerSec)
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())

	for {
		id := fmt.Sprintf("%x", rand.Uint64())
		level := "info"
		if rand.Float64() < errorRate {
			level = "error"
		} else {
			level = levels[rand.Intn(len(levels)-1)]
		}

		event := LogEvent{
			TS:        time.Now().UTC().Format(time.RFC3339Nano),
			Host:      host,
			Service:   services[rand.Intn(len(services))],
			Level:     level,
			Path:      paths[rand.Intn(len(paths))],
			LatencyMS: rand.Intn(1000),
			Message:   "synthetic log event",
			TraceID:   id,
			RequestID: id,
		}

		value, _ := json.Marshal(event)

		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(id),
			Value: value,
		})
		if err != nil {
			log.Printf("[ERROR] write: %v", err)
		} else {
			fmt.Println(string(value))
		}

		time.Sleep(interval)
	}
}

func getEnv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if val := os.Getenv(key); val != "" {
		f, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return f
		}
	}
	return def
}
