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
