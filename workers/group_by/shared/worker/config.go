package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	// Default configuration
	DefaultRabbitMQURL = "amqp://guest:guest@localhost:5672/"
	DefaultNumPartitions = 10

	// Worker count per query
	Query2NumWorkers = 3
	Query3NumWorkers = 3
)

// WorkerConfig holds configuration for the group by worker
type WorkerConfig struct {
	QueryType           int
	WorkerID            int
	NumWorkers          int
	NumPartitions       int
	ExchangeName        string
	RoutingKeys         []string
	OrchestratorExchange string
	ConnectionConfig    *middleware.ConnectionConfig
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*WorkerConfig, error) {
	// Load query type
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

	// Load worker ID (1-based from environment)
	workerIDStr := os.Getenv("WORKER_ID")
	if workerIDStr == "" {
		return nil, fmt.Errorf("WORKER_ID environment variable is required")
	}

	workerID, err := strconv.Atoi(workerIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid WORKER_ID: %v", err)
	}

	// Determine number of workers based on query type
	numWorkers := getNumWorkersForQuery(queryType)

	// Load NUM_WORKERS from environment (configurable for all queries)
	numWorkersStr := os.Getenv("NUM_WORKERS")
	if numWorkersStr != "" {
		if parsed, err := strconv.Atoi(numWorkersStr); err == nil && parsed > 0 {
			numWorkers = parsed
		}
	}

	// Validate worker ID (1-based: 1, 2, 3, ...)
	if workerID < 1 || workerID > numWorkers {
		return nil, fmt.Errorf("WORKER_ID must be between 1 and %d (got %d)", numWorkers, workerID)
	}

	// Keep workerID as 1-based (no conversion needed)

	// Load number of partitions
	numPartitionsStr := os.Getenv("NUM_PARTITIONS")
	numPartitions := DefaultNumPartitions
	if numPartitionsStr != "" {
		if parsed, err := strconv.Atoi(numPartitionsStr); err == nil && parsed > 0 {
			numPartitions = parsed
		}
	}

	// Load RabbitMQ connection details from environment variables
	rabbitMQHost := getEnv("RABBITMQ_HOST", "rabbitmq")
	rabbitMQPort := getEnv("RABBITMQ_PORT", "5672")
	rabbitMQUser := getEnv("RABBITMQ_USER", "admin")
	rabbitMQPass := getEnv("RABBITMQ_PASS", "password")

	// Get exchange name for this query
	exchangeName := queues.GetGroupByExchangeName(queryType)
	if exchangeName == "" {
		return nil, fmt.Errorf("no exchange found for query type %d", queryType)
	}

	// Get routing key for this worker
	routingKey := queues.GetGroupByWorkerRoutingKey(queryType, workerID)
	if routingKey == "" {
		return nil, fmt.Errorf("failed to get routing key for worker %d", workerID)
	}
	routingKeys := []string{routingKey}

	// Get orchestrator exchange name (fanout)
	orchestratorExchange := queues.GetOrchestratorChunksExchangeName(queryType)
	if orchestratorExchange == "" {
		return nil, fmt.Errorf("no orchestrator exchange found for query type %d", queryType)
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

	return &WorkerConfig{
		QueryType:           queryType,
		WorkerID:            workerID,
		NumWorkers:          numWorkers,
		NumPartitions:       numPartitions,
		ExchangeName:        exchangeName,
		RoutingKeys:        routingKeys,
		OrchestratorExchange: orchestratorExchange,
		ConnectionConfig:    connectionConfig,
	}, nil
}

// getNumWorkersForQuery returns the number of workers for a specific query type
func getNumWorkersForQuery(queryType int) int {
	switch queryType {
	case 2:
		return Query2NumWorkers
	case 3:
		return Query3NumWorkers
	case 4:
		// Query 4 is configurable, default to 3
		return 3
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
