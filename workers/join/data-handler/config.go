package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	// Alias queue names from centralized config
	FixedJoinDataQueue             = queues.FixedJoinDataQueue
	JoinUserIdDictionaryQueue      = queues.JoinUserIdDictionaryQueue
	JoinItemIdDictionaryExchange   = queues.ItemIdDictionaryExchange
	JoinItemIdDictionaryRoutingKey = queues.ItemIdDictionaryRoutingKey

	JoinStoreIdDictionaryExchange   = queues.StoreIdDictionaryExchange
	JoinStoreIdDictionaryRoutingKey = queues.StoreIdDictionaryRoutingKey

	// Default values
	DefaultRabbitMQHost = "localhost"
	DefaultRabbitMQPort = "5672"
	DefaultRabbitMQUser = "admin"
	DefaultRabbitMQPass = "password"
)

// loadConfig loads configuration from environment variables
func loadConfig() (*middleware.ConnectionConfig, error) {
	host := getEnv("RABBITMQ_HOST", DefaultRabbitMQHost)
	port := getEnv("RABBITMQ_PORT", DefaultRabbitMQPort)
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %v", err)
	}
	username := getEnv("RABBITMQ_USER", DefaultRabbitMQUser)
	password := getEnv("RABBITMQ_PASS", DefaultRabbitMQPass)

	fmt.Printf("Join Data Handler: Connecting to RabbitMQ at %s:%s with user %s\n", host, port, username)

	return &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
	}, nil
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
