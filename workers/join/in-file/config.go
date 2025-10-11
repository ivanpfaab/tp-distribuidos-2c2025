package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

const (
	// Shared data directory for CSV files
	SharedDataDir = "/shared-data"

	// Queue names
	JoinUserIdDictionaryQueue = "join-userid-dictionary"
	UserIdChunkQueue          = "userid-join-chunks"
	Query4ResultsQueue        = "query4-results-chunks"

	// Default configuration
	DefaultRabbitMQHost = "localhost"
	DefaultRabbitMQPort = "5672"
	DefaultRabbitMQUser = "admin"
	DefaultRabbitMQPass = "password"
)

// loadConfig loads configuration from environment variables
func loadConfig() (*middleware.ConnectionConfig, error) {
	host := getEnvOrDefault("RABBITMQ_HOST", DefaultRabbitMQHost)
	port := getEnvOrDefault("RABBITMQ_PORT", DefaultRabbitMQPort)
	user := getEnvOrDefault("RABBITMQ_USER", DefaultRabbitMQUser)
	pass := getEnvOrDefault("RABBITMQ_PASS", DefaultRabbitMQPass)

	portInt, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("invalid RABBITMQ_PORT: %v", err)
	}

	return &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: user,
		Password: pass,
	}, nil
}

// getEnvOrDefault returns environment variable value or default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
