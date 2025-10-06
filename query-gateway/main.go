package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
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

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the gateway in a goroutine
	go func() {
		if err := gateway.Start(); err != 0 {
			fmt.Printf("Failed to start query gateway: %v\n", err)
			os.Exit(1)
		}
	}()

	fmt.Println("Query Gateway started successfully. Waiting for messages from reply-filter-bus...")

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("Shutting down Query Gateway...")

	// Close the gateway
	gateway.Close()

	fmt.Println("Query Gateway stopped.")
}
