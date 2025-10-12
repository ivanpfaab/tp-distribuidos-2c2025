package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

const (
	// Input queues/exchanges
	FixedJoinDataExchange   = "fixed-join-data-exchange"
	FixedJoinDataRoutingKey = "fixed-join-data"

	// Output exchanges for dictionaries (broadcast to all workers)
	JoinItemIdDictionaryExchange   = "itemid-dictionary-exchange"
	JoinItemIdDictionaryRoutingKey = "itemid-dictionary"

	JoinStoreIdDictionaryExchange   = "storeid-dictionary-exchange"
	JoinStoreIdDictionaryRoutingKey = "storeid-dictionary"

	// Output queues for dictionaries (point-to-point)
	JoinUserIdDictionaryQueue = "join-userid-dictionary"

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
