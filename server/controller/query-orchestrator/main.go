package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/server/controller/query-orchestrator/config"
	"github.com/tp-distribuidos-2c2025/server/controller/query-orchestrator/orchestrator"
)

func main() {
	// Load configuration from environment variables
	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	fmt.Printf("Connecting to RabbitMQ at %s:%d with user %s\n",
		cfg.RabbitMQ.Host, cfg.RabbitMQ.Port, cfg.RabbitMQ.Username)

	// Create Query Orchestrator
	queryOrchestrator := orchestrator.NewQueryOrchestrator(cfg)

	// Initialize the orchestrator (creates exchanges and producers)
	if err := queryOrchestrator.Initialize(); err != 0 {
		fmt.Printf("Failed to initialize orchestrator: %v\n", err)
		return
	}
	defer queryOrchestrator.Close()

	// Start consuming messages from both data handler and reply queues
	fmt.Println("Starting consumers for step0-data-queue and orchestrator-reply-queue...")
	if err := queryOrchestrator.StartConsuming(); err != 0 {
		fmt.Printf("Failed to start consuming messages: %v\n", err)
		return
	}
	fmt.Println("Both consumers started successfully!")

	// Keep the orchestrator running
	select {}
}
