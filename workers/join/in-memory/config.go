package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

const (
	JoinExchangeName           = "join-exchange"
	JoinRoutingKey             = "join"
	DefaultRabbitMQHost        = "localhost"
	DefaultRabbitMQPort        = "5672"
	DefaultRabbitMQUser        = "admin"
	DefaultRabbitMQPass        = "password"
	DefaultChunkQueueName      = "chunk-queue"
	DefaultDictionaryQueueName = "dictionary-queue"
	DefaultOutputQueueName     = "output-queue"
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
	_ = getEnv("CHUNK_QUEUE_NAME", DefaultChunkQueueName)
	_ = getEnv("DICTIONARY_QUEUE_NAME", DefaultDictionaryQueueName)
	_ = getEnv("OUTPUT_QUEUE_NAME", DefaultOutputQueueName)

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
