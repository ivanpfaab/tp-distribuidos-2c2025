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

	return &OrchestratorConfig{
		RabbitMQConfig: &middleware.ConnectionConfig{
			Host:     "rabbitmq",
			Port:     5672,
			Username: "admin",
			Password: "password",
		},
		QueryType: queryType,
		WorkerID:  workerID,
	}, nil
}
