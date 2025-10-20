package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	// Buffer configuration
	MaxBufferSize = 200 // Maximum records to buffer per partition before flushing

	// Partition configuration
	NumPartitions = 10 // Number of partitions for user_id modulo

	// Default configuration
	DefaultRabbitMQURL = "amqp://guest:guest@localhost:5672/"
)

// PartitionerConfig holds configuration for the partitioner worker
type PartitionerConfig struct {
	QueryType        int
	QueueName        string
	ConnectionConfig *middleware.ConnectionConfig
	NumPartitions    int
	NumWorkers       int
	MaxBufferSize    int
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*PartitionerConfig, error) {
	queryTypeStr := os.Getenv("QUERY_TYPE")
	if queryTypeStr == "" {
		return nil, fmt.Errorf("QUERY_TYPE environment variable is required")
	}

	queryType, err := strconv.Atoi(queryTypeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid QUERY_TYPE: %v", err)
	}

	if queryType < 2 || queryType > 4 {
		return nil, fmt.Errorf("QUERY_TYPE must be 2, 3, or 4")
	}

	// Load RabbitMQ connection details from environment variables
	rabbitMQHost := getEnv("RABBITMQ_HOST", "rabbitmq")
	rabbitMQPort := getEnv("RABBITMQ_PORT", "5672")
	rabbitMQUser := getEnv("RABBITMQ_USER", "admin")
	rabbitMQPass := getEnv("RABBITMQ_PASS", "password")

	// Get the appropriate queue name based on query type
	var queueName string
	switch queryType {
	case 2:
		queueName = queues.Query2GroupByQueue
	case 3:
		queueName = queues.Query3GroupByQueue
	case 4:
		queueName = queues.Query4GroupByQueue
	}

	// Load partition configuration
	numPartitionsStr := os.Getenv("NUM_PARTITIONS")
	numPartitions := NumPartitions
	if numPartitionsStr != "" {
		if parsed, err := strconv.Atoi(numPartitionsStr); err == nil && parsed > 0 {
			numPartitions = parsed
		}
	}

	// Load number of workers
	numWorkersStr := os.Getenv("NUM_WORKERS")
	numWorkers := getNumWorkersForQuery(queryType)
	if numWorkersStr != "" {
		if parsed, err := strconv.Atoi(numWorkersStr); err == nil && parsed > 0 {
			numWorkers = parsed
		}
	}

	maxBufferSizeStr := os.Getenv("MAX_BUFFER_SIZE")
	maxBufferSize := MaxBufferSize
	if maxBufferSizeStr != "" {
		if parsed, err := strconv.Atoi(maxBufferSizeStr); err == nil && parsed > 0 {
			maxBufferSize = parsed
		}
	}

	// Parse port to int
	port, err := strconv.Atoi(rabbitMQPort)
	if err != nil {
		return nil, fmt.Errorf("invalid RABBITMQ_PORT: %v", err)
	}

	connectionConfig := &middleware.ConnectionConfig{
		Host:     rabbitMQHost,
		Port:     port,
		Username: rabbitMQUser,
		Password: rabbitMQPass,
	}

	return &PartitionerConfig{
		QueryType:        queryType,
		QueueName:        queueName,
		ConnectionConfig: connectionConfig,
		NumPartitions:    numPartitions,
		NumWorkers:       numWorkers,
		MaxBufferSize:    maxBufferSize,
	}, nil
}

// getNumWorkersForQuery returns the default number of workers for a specific query type
func getNumWorkersForQuery(queryType int) int {
	switch queryType {
	case 2:
		return 3
	case 3:
		return 3
	case 4:
		return 3 // configurable
	default:
		return 1
	}
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
