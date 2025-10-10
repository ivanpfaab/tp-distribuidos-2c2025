package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

const (
	// Legacy join exchange (for backward compatibility)
	JoinExchangeName = "join-exchange"
	JoinRoutingKey   = "join"

	// New join strategy queues
	FixedJoinDataExchange      = "fixed-join-data-exchange"
	FixedJoinDataRoutingKey    = "fixed-join-data"
	JoinItemIdDictionaryQueue  = "join-itemid-dictionary"
	JoinStoreIdDictionaryQueue = "join-storeid-dictionary"
	JoinUserIdDictionaryQueue  = "join-userid-dictionary"

	// Input queues for join workers
	ItemIdChunkQueue  = "top-item-classification-chunk"
	StoreIdChunkQueue = "itemid-join-chunks"
	UserIdChunkQueue  = "userid-join-chunks"

	// Output queues for join workers
	Query2ResultsQueue = "query2-results-chunks"
	Query3ResultsQueue = "query3-results-chunks"
	Query4ResultsQueue = "query4-results-chunks"

	DefaultRabbitMQHost          = "localhost"
	DefaultRabbitMQPort          = "5672"
	DefaultRabbitMQUser          = "admin"
	DefaultRabbitMQPass          = "password"
	DefaultJoinCount             = "1"
	DefaultDataConsumerQueueName = "data-consumer-queue"
	DefaultProducerQueueName     = "producer-queue"
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

	// Configuration variables for future use
	_ = getEnvInt("JOIN_COUNT", DefaultJoinCount)
	_ = getEnv("DATA_CONSUMER_QUEUE_NAME", DefaultDataConsumerQueueName)

	fmt.Printf("Join Worker: Connecting to RabbitMQ at %s:%s with user %s\n", host, port, username)

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

// getEnvInt gets an environment variable as integer with a default value
func getEnvInt(key, defaultValue string) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	if intValue, err := strconv.Atoi(defaultValue); err == nil {
		return intValue
	}
	return 1 // fallback
}
