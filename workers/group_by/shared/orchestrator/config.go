package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// OrchestratorConfig holds configuration for the group by orchestrator
type OrchestratorConfig struct {
	RabbitMQConfig *middleware.ConnectionConfig
	QueryType      int
	WorkerID       int
	NumPartitions  int
	NumWorkers     int
}

// NewOrchestratorConfig creates a new orchestrator configuration
func NewOrchestratorConfig(queryType int) (*OrchestratorConfig, error) {
	// Load worker ID from environment variable
	workerIDStr := os.Getenv("WORKER_ID")
	if workerIDStr == "" {
		return nil, fmt.Errorf("WORKER_ID environment variable is required")
	}

	workerID, err := strconv.Atoi(workerIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid WORKER_ID: %v", err)
	}

	if workerID < 1 {
		return nil, fmt.Errorf("WORKER_ID must be >= 1 (got %d)", workerID)
	}

	// Load NUM_PARTITIONS and NUM_WORKERS from environment
	numPartitions := 100 // Default
	if numPartitionsStr := os.Getenv("NUM_PARTITIONS"); numPartitionsStr != "" {
		if n, err := strconv.Atoi(numPartitionsStr); err == nil && n > 0 {
			numPartitions = n
		}
	}

	numWorkers := 5 // Default
	if numWorkersStr := os.Getenv("NUM_WORKERS"); numWorkersStr != "" {
		if n, err := strconv.Atoi(numWorkersStr); err == nil && n > 0 {
			numWorkers = n
		}
	}

	return &OrchestratorConfig{
		RabbitMQConfig: &middleware.ConnectionConfig{
			Host:     "rabbitmq",
			Port:     5672,
			Username: "admin",
			Password: "password",
		},
		QueryType:     queryType,
		WorkerID:      workerID,
		NumPartitions: numPartitions,
		NumWorkers:    numWorkers,
	}, nil
}
