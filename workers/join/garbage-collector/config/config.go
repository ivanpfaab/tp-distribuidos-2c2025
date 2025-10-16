package config

import (
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// Config holds the configuration for the join garbage collector
type Config struct {
	ConnectionConfig *middleware.ConnectionConfig
	CompletionQueue  string
	StoreIDExchange  string
	ItemIDExchange   string
	UserIDExchange   string
}

// LoadConfig loads the configuration from environment variables
func LoadConfig() (*Config, error) {
	// Get RabbitMQ connection details
	rabbitMQHost := getEnv("RABBITMQ_HOST", "localhost")
	rabbitMQPortStr := getEnv("RABBITMQ_PORT", "5672")
	rabbitMQUser := getEnv("RABBITMQ_USER", "guest")
	rabbitMQPass := getEnv("RABBITMQ_PASS", "guest")

	// Convert port string to int
	rabbitMQPort, err := strconv.Atoi(rabbitMQPortStr)
	if err != nil {
		rabbitMQPort = 5672 // Default port
	}

	// Create connection config
	connectionConfig := &middleware.ConnectionConfig{
		Host:     rabbitMQHost,
		Port:     rabbitMQPort,
		Username: rabbitMQUser,
		Password: rabbitMQPass,
	}

	// Get queue and exchange names
	completionQueue := getEnv("COMPLETION_QUEUE", "join-completion-queue")
	storeIDExchange := getEnv("STOREID_CLEANUP_EXCHANGE", "storeid-cleanup-exchange")
	itemIDExchange := getEnv("ITEMID_CLEANUP_EXCHANGE", "itemid-cleanup-exchange")
	userIDExchange := getEnv("USERID_CLEANUP_EXCHANGE", "userid-cleanup-exchange")

	return &Config{
		ConnectionConfig: connectionConfig,
		CompletionQueue:  completionQueue,
		StoreIDExchange:  storeIDExchange,
		ItemIDExchange:   itemIDExchange,
		UserIDExchange:   userIDExchange,
	}, nil
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
