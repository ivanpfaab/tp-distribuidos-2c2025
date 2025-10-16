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

// GetExpectedFileCounts returns the expected number of files for each table // TODO: get this dinamically or from the client as parameters
func GetExpectedFileCounts() map[int]int {
	return map[int]int{
		1: 1,  // menu_items
		2: 2,  // transaction_items
		3: 2,  // transactions
		4: 20, // users
		5: 1,  // payment_methods
		6: 1,  // stores
		7: 1,  // vouchers
	}
}
