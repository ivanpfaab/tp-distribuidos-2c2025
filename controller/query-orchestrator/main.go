package main

import (
	"fmt"
	"os"
	"strconv"
	"tp-distribuidos-2c2025/shared/middleware"
)

func main() {
	// Get configuration from environment variables or use defaults
	host := getEnv("RABBITMQ_HOST", "localhost")
	port := getEnv("RABBITMQ_PORT", "5672")
	portInt, err := strconv.Atoi(port)
	if err != nil {
		fmt.Printf("Invalid port: %v\n", err)
		return
	}
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	fmt.Printf("Connecting to RabbitMQ at %s:%s with user %s\n", host, port, username)

	// Create connection configuration
	config := &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
	}

	// Create Query Orchestrator
	orchestrator := NewQueryOrchestrator(config)

	// Initialize the orchestrator (creates exchanges and producers)
	if err := orchestrator.Initialize(); err != 0 {
		fmt.Printf("Failed to initialize orchestrator: %v\n", err)
		return
	}
	defer orchestrator.Close()

	fmt.Println("Query Orchestrator initialized successfully!")
	fmt.Println("Exchanges created: filter-exchange, aggregator-exchange, join-exchange, groupby-exchange")

	fmt.Println("Query Orchestrator is ready to process chunks!")
	fmt.Println("Start the worker nodes to begin processing:")
	fmt.Println("  - Filter worker: go run workers/filter/main.go")
	fmt.Println("  - Aggregator worker: go run workers/aggregator/main.go")
	fmt.Println("  - Join worker: go run workers/join/main.go")
	fmt.Println("  - GroupBy worker: go run workers/group_by/main.go")

	// Keep the orchestrator running
	select {}
}

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
