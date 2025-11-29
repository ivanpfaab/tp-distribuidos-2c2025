package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tp-distribuidos-2c2025/shared/health_server"
)

func main() {
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8888"
	}
	healthSrv := health_server.NewHealthServer(healthPort)
	go healthSrv.Start()
	defer healthSrv.Stop()

	fmt.Println("Starting Query Gateway...")

	// Load configuration
	config := LoadConfig()
	middlewareConfig := config.ToMiddlewareConfig()

	// Create query gateway
	gateway, err := NewQueryGateway(middlewareConfig)
	if err != nil {
		fmt.Printf("Failed to create query gateway: %v\n", err)
		os.Exit(1)
	}

	// Start the gateway (non-blocking - returns immediately)
	if err := gateway.Start(); err != 0 {
		fmt.Printf("Failed to start query gateway: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Query Gateway started successfully. Waiting for messages from results dispatcher exchange...")

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("Shutting down Query Gateway...")

	// Close the gateway
	gateway.Close()

	fmt.Println("Query Gateway stopped.")
}
