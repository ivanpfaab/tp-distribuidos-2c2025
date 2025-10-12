package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

const (
	// Partition configuration
	NumPartitions = 100 // Total number of partition files (must match reader)

	// Buffer configuration
	MaxBufferSize = 200 // Maximum users to buffer per writer before flushing

	// Default configuration
	DefaultRabbitMQHost = "localhost"
	DefaultRabbitMQPort = "5672"
	DefaultRabbitMQUser = "admin"
	DefaultRabbitMQPass = "password"
	DefaultNumWriters   = 1
)

// Config holds the configuration for the user partition splitter
type Config struct {
	NumWriters int // Number of partition writers
}

// loadConfig loads configuration from environment variables
func loadConfig() (*middleware.ConnectionConfig, *Config, error) {
	// Load RabbitMQ connection config
	rabbitHost := getEnv("RABBITMQ_HOST", DefaultRabbitMQHost)
	rabbitPortStr := getEnv("RABBITMQ_PORT", DefaultRabbitMQPort)
	rabbitUser := getEnv("RABBITMQ_USER", DefaultRabbitMQUser)
	rabbitPass := getEnv("RABBITMQ_PASS", DefaultRabbitMQPass)

	// Convert port to int
	rabbitPort, err := strconv.Atoi(rabbitPortStr)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid RABBITMQ_PORT: %s", rabbitPortStr)
	}

	connConfig := &middleware.ConnectionConfig{
		Host:     rabbitHost,
		Port:     rabbitPort,
		Username: rabbitUser,
		Password: rabbitPass,
	}

	// Load splitter-specific config
	numWritersStr := getEnv("NUM_WRITERS", strconv.Itoa(DefaultNumWriters))
	numWriters, err := strconv.Atoi(numWritersStr)
	if err != nil || numWriters < 1 {
		return nil, nil, fmt.Errorf("invalid NUM_WRITERS: %s (must be positive integer)", numWritersStr)
	}

	splitterConfig := &Config{
		NumWriters: numWriters,
	}

	fmt.Printf("User Partition Splitter Config: NumWriters=%d, NumPartitions=%d\n",
		splitterConfig.NumWriters, NumPartitions)

	return connConfig, splitterConfig, nil
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// GetWriterQueueName returns the queue name for a specific writer
func GetWriterQueueName(writerID int) string {
	return fmt.Sprintf("users-post-partition-%d", writerID)
}
