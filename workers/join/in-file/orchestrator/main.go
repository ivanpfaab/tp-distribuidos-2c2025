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
	fmt.Println("Starting In-File Join Orchestrator...")

	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Create orchestrator
	orchestrator, err := NewInFileJoinOrchestrator(config)
	if err != nil {
		fmt.Printf("Failed to create in-file join orchestrator: %v\n", err)
		os.Exit(1)
	}
	defer orchestrator.Close()

	// Start the orchestrator
	go orchestrator.Start()

	// Wait for termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("In-File Join Orchestrator is running. Press Ctrl+C to stop.")
	<-sigChan

	fmt.Println("Shutting down In-File Join Orchestrator...")
	orchestrator.Close()
	fmt.Println("In-File Join Orchestrator stopped.")
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
