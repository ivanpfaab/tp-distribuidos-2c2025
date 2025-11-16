package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

const (
	// Partition configuration
	NumPartitions = 100 // Total number of partition files

	// Writer data directory (volume-specific)
	SharedDataDir = "/app/writer-data"

	// Default configuration
	DefaultRabbitMQHost = "localhost"
	DefaultRabbitMQPort = "5672"
	DefaultRabbitMQUser = "admin"
	DefaultRabbitMQPass = "password"
	DefaultWriterID     = 1
	DefaultNumWriters   = 1
)

// Config holds the configuration for the user partition writer
type Config struct {
	WriterID   int // This writer's ID (1-indexed)
	NumWriters int // Total number of writers
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

	// Load writer-specific config
	writerIDStr := getEnv("WRITER_ID", strconv.Itoa(DefaultWriterID))
	writerID, err := strconv.Atoi(writerIDStr)
	if err != nil || writerID < 1 {
		return nil, nil, fmt.Errorf("invalid WRITER_ID: %s (must be positive integer)", writerIDStr)
	}

	numWritersStr := getEnv("NUM_WRITERS", strconv.Itoa(DefaultNumWriters))
	numWriters, err := strconv.Atoi(numWritersStr)
	if err != nil || numWriters < 1 {
		return nil, nil, fmt.Errorf("invalid NUM_WRITERS: %s (must be positive integer)", numWritersStr)
	}

	if writerID > numWriters {
		return nil, nil, fmt.Errorf("WRITER_ID (%d) cannot be greater than NUM_WRITERS (%d)", writerID, numWriters)
	}

	writerConfig := &Config{
		WriterID:   writerID,
		NumWriters: numWriters,
	}

	fmt.Printf("User Partition Writer Config: WriterID=%d, NumWriters=%d, NumPartitions=%d\n",
		writerConfig.WriterID, writerConfig.NumWriters, NumPartitions)

	return connConfig, writerConfig, nil
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// GetWriterQueueName returns the queue name for this writer
func GetWriterQueueName(writerID int) string {
	return fmt.Sprintf("users-post-partition-%d", writerID)
}

// OwnsPartition checks if this writer owns a specific partition
func (c *Config) OwnsPartition(partition int) bool {
	// Writer owns partition if: partition % NumWriters == (WriterID - 1)
	// WriterID is 1-indexed, so we subtract 1 for modulo calculation
	return (partition % c.NumWriters) == (c.WriterID - 1)
}
