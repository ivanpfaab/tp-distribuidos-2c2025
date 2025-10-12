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

	// Partition configuration
	NumPartitions = 100 // Number of partition files for hash-based partitioning

	// Buffer configuration
	MaxBufferSize = 200 // Maximum transactions to buffer before sending retry chunk
	MaxRetries    = 10  // Maximum retry attempts before dropping transactions (increased for scaled workers)

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

// getUserPartition returns the partition number for a given user ID
func getUserPartition(userID string) (int, error) {
	// Parse user ID (handle both int and float formats)
	userIDFloat, err := strconv.ParseFloat(userID, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid user ID %s: %w", userID, err)
	}
	userIDInt := int(userIDFloat)

	// Using userIDInt as the partition number and module to determine where the user belongs
	return userIDInt % NumPartitions, nil
}
