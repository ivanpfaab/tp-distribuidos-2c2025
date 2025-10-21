package main

import (
	"os"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	StreamingExchangeName = "streaming-exchange"
	StreamingRoutingKey   = "streaming"

	// Result queues for all query types
	Query1ResultsQueue = queues.Query1ResultsQueue
	Query2ResultsQueue = queues.Query2ResultsQueue
	Query3ResultsQueue = queues.Query3ResultsQueue
	Query4ResultsQueue = queues.Query4ResultsQueue
)

// Config holds the configuration for the streaming service
type Config struct {
	RabbitMQHost string
	RabbitMQPort string
	RabbitMQUser string
	RabbitMQPass string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		RabbitMQHost: getEnv("RABBITMQ_HOST", "localhost"),
		RabbitMQPort: getEnv("RABBITMQ_PORT", "5672"),
		RabbitMQUser: getEnv("RABBITMQ_USER", "admin"),
		RabbitMQPass: getEnv("RABBITMQ_PASS", "password"),
	}
}

// ToMiddlewareConfig converts Config to middleware.ConnectionConfig
func (c *Config) ToMiddlewareConfig() *middleware.ConnectionConfig {
	return &middleware.ConnectionConfig{
		Host:     c.RabbitMQHost,
		Port:     5672, // Default RabbitMQ port
		Username: c.RabbitMQUser,
		Password: c.RabbitMQPass,
	}
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
