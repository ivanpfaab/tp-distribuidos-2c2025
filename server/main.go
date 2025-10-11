package main

import (
	"log"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

func main() {
	// Get RabbitMQ configuration from environment variables
	host := getEnv("RABBITMQ_HOST", "localhost")
	portStr := getEnv("RABBITMQ_PORT", "5672")
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		log.Printf("Invalid RabbitMQ port: %v, using default 5672", err)
		portInt = 5672
	}
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	// Create RabbitMQ connection configuration
	config := &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
	}

	// Create RabbitMQ-based server
	server, err := NewRabbitMQServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	log.Printf("Starting RabbitMQ server")

	// Start the server (to consume batches)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	log.Printf("RabbitMQ server started successfully")

	// Keep running
	select {}
}

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
