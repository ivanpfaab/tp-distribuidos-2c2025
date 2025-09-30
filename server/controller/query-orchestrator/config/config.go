package config

import (
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// Config holds all configuration for the query orchestrator
type Config struct {
	RabbitMQ RabbitMQConfig
}

// RabbitMQConfig holds RabbitMQ connection configuration
type RabbitMQConfig struct {
	Host     string
	Port     int
	Username string
	Password string
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() (*Config, error) {
	host := getEnv("RABBITMQ_HOST", "localhost")
	port := getEnv("RABBITMQ_PORT", "5672")
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	return &Config{
		RabbitMQ: RabbitMQConfig{
			Host:     host,
			Port:     portInt,
			Username: username,
			Password: password,
		},
	}, nil
}

// ToMiddlewareConfig converts the config to middleware connection config
func (c *Config) ToMiddlewareConfig() *middleware.ConnectionConfig {
	return &middleware.ConnectionConfig{
		Host:     c.RabbitMQ.Host,
		Port:     c.RabbitMQ.Port,
		Username: c.RabbitMQ.Username,
		Password: c.RabbitMQ.Password,
	}
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
