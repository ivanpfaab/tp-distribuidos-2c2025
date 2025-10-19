package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

func main() {
	fmt.Println("Starting In-Memory Join Orchestrator...")

	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create orchestrator
	orchestrator, err := NewInMemoryJoinOrchestrator(config)
	if err != nil {
		fmt.Printf("Failed to create in-memory join orchestrator: %v\n", err)
		return
	}
	defer orchestrator.Close()

	// Start the orchestrator
	if err := orchestrator.Start(); err != 0 {
		fmt.Printf("Failed to start in-memory join orchestrator: %v\n", err)
		return
	}

	fmt.Println("In-Memory Join Orchestrator started successfully")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down In-Memory Join Orchestrator...")
}

// loadConfig loads configuration from environment variables
func loadConfig() (*middleware.ConnectionConfig, error) {
	host := getEnv("RABBITMQ_HOST", "localhost")
	portStr := getEnv("RABBITMQ_PORT", "5672")
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid RABBITMQ_PORT: %s", portStr)
	}

	return &middleware.ConnectionConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
	}, nil
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
