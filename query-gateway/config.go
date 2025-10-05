package main

import (
	"os"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

const (
	ReplyFilterBusQueueName = "reply-filter-bus"
)

// Config holds the configuration for the query gateway
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
