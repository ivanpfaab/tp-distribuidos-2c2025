package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	// Reader data directory (volume-specific)
	SharedDataDir = "/app/reader-data"

	// Alias queue names from centralized config
	JoinUserIdDictionaryQueue = queues.JoinUserIdDictionaryQueue
	Query4ResultsQueue        = queues.Query4ResultsQueue

	// Partition configuration
	NumPartitions = 100 // Number of partition files for mod partitioning

	// Default configuration
	DefaultRabbitMQHost = "localhost"
	DefaultRabbitMQPort = "5672"
	DefaultRabbitMQUser = "admin"
	DefaultRabbitMQPass = "password"
	DefaultReaderID     = 1
)

// Config holds the reader-specific configuration
type Config struct {
	ReaderID int // This reader's ID (1-indexed)
}

// loadConfig loads configuration from environment variables
func loadConfig() (*middleware.ConnectionConfig, *Config, error) {
	host := getEnvOrDefault("RABBITMQ_HOST", DefaultRabbitMQHost)
	port := getEnvOrDefault("RABBITMQ_PORT", DefaultRabbitMQPort)
	user := getEnvOrDefault("RABBITMQ_USER", DefaultRabbitMQUser)
	pass := getEnvOrDefault("RABBITMQ_PASS", DefaultRabbitMQPass)

	portInt, err := strconv.Atoi(port)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid RABBITMQ_PORT: %v", err)
	}

	connConfig := &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: user,
		Password: pass,
	}

	// Load reader ID
	readerIDStr := getEnvOrDefault("READER_ID", strconv.Itoa(DefaultReaderID))
	readerID, err := strconv.Atoi(readerIDStr)
	if err != nil || readerID < 1 {
		return nil, nil, fmt.Errorf("invalid READER_ID: %s (must be positive integer)", readerIDStr)
	}

	readerConfig := &Config{
		ReaderID: readerID,
	}

	fmt.Printf("User Join Reader Config: ReaderID=%d, NumPartitions=%d\n",
		readerConfig.ReaderID, NumPartitions)

	return connConfig, readerConfig, nil
}

// getEnvOrDefault returns environment variable value or default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
