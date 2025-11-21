package main

import (
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

type Config struct {
	WorkerID      int
	InputQueue    string
	OutputQueue   string
	RabbitMQ      *middleware.ConnectionConfig
	DuplicateRate float64
}

func LoadConfig() *Config {
	workerID, _ := strconv.Atoi(getEnv("WORKER_ID", "1"))

	return &Config{
		WorkerID:    workerID,
		InputQueue:  getEnv("INPUT_QUEUE", "queue-input"),
		OutputQueue: getEnv("OUTPUT_QUEUE", "queue-output"),
		RabbitMQ: &middleware.ConnectionConfig{
			Host:     getEnv("RABBITMQ_HOST", "localhost"),
			Port:     parseInt(getEnv("RABBITMQ_PORT", "5672")),
			Username: getEnv("RABBITMQ_USER", "admin"),
			Password: getEnv("RABBITMQ_PASS", "password"),
			VHost:    "/",
		},
		DuplicateRate: parseFloat(getEnv("DUPLICATE_RATE", "0.0")),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseInt(s string) int {
	val, _ := strconv.Atoi(s)
	return val
}

func parseFloat(s string) float64 {
	val, _ := strconv.ParseFloat(s, 64)
	return val
}
