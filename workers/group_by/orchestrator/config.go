package main

import (
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// OrchestratorConfig holds configuration for the group by orchestrator
type OrchestratorConfig struct {
	RabbitMQConfig *middleware.ConnectionConfig
	QueryType      int
}

// NewOrchestratorConfig creates a new orchestrator configuration
func NewOrchestratorConfig(queryType int) *OrchestratorConfig {
	return &OrchestratorConfig{
		RabbitMQConfig: &middleware.ConnectionConfig{
			Host:     "rabbitmq",
			Port:     5672,
			Username: "admin",
			Password: "password",
		},
		QueryType: queryType,
	}
}

// GetTotalExpectedFiles returns the total expected number of files for a query type
func GetTotalExpectedFiles(queryType int) int {
	if queryType == 2 {
		return 2 // transaction_items
	}
	if queryType == 3 {
		return 2 // transactions
	}
	if queryType == 4 {
		return 2 // transactions
	}
	return 0
}
