package main

import (
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

type Config struct {
	OutputQueue string
	NumChunks   int
	RabbitMQ    *middleware.ConnectionConfig
}

func LoadConfig() *Config {
	return &Config{
		OutputQueue: getEnv("OUTPUT_QUEUE", "queue-1-2"),
		NumChunks:   parseInt(getEnv("NUM_CHUNKS", "10")),
		RabbitMQ: &middleware.ConnectionConfig{
			Host:     getEnv("RABBITMQ_HOST", "localhost"),
			Port:     parseInt(getEnv("RABBITMQ_PORT", "5672")),
			Username: getEnv("RABBITMQ_USER", "admin"),
			Password: getEnv("RABBITMQ_PASS", "password"),
			VHost:    "/",
		},
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

