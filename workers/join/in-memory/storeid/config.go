package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	StoreIdChunkQueue           = queues.StoreIdChunkQueue
	StoreIdDictionaryExchange   = queues.StoreIdDictionaryExchange
	StoreIdDictionaryRoutingKey = queues.StoreIdDictionaryRoutingKey
	Query3ResultsQueue          = queues.Query3ResultsQueue

	// Default values
	DefaultRabbitMQHost = "localhost"
	DefaultRabbitMQPort = "5672"
	DefaultRabbitMQUser = "admin"
	DefaultRabbitMQPass = "password"
	DefaultBatchSize    = 5 // Same as Query 3 reducers count
)

// StoreIdConfig holds configuration for the StoreID join worker
type StoreIdConfig struct {
	*middleware.ConnectionConfig
	BatchSize int
}

// loadConfig loads configuration from environment variables
func loadConfig() (*StoreIdConfig, error) {
	host := getEnv("RABBITMQ_HOST", DefaultRabbitMQHost)
	port := getEnv("RABBITMQ_PORT", DefaultRabbitMQPort)
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %v", err)
	}
	username := getEnv("RABBITMQ_USER", DefaultRabbitMQUser)
	password := getEnv("RABBITMQ_PASS", DefaultRabbitMQPass)

	// Load batch size from environment
	batchSizeStr := getEnv("STOREID_BATCH_SIZE", fmt.Sprintf("%d", DefaultBatchSize))
	batchSize, err := strconv.Atoi(batchSizeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid batch size: %v", err)
	}

	fmt.Printf("StoreID Join Worker: Connecting to RabbitMQ at %s:%s with user %s, batch size: %d\n", host, port, username, batchSize)

	return &StoreIdConfig{
		ConnectionConfig: &middleware.ConnectionConfig{
			Host:     host,
			Port:     portInt,
			Username: username,
			Password: password,
		},
		BatchSize: batchSize,
	}, nil
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
