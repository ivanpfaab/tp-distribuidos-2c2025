package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("Starting Streaming Service...")

	// Load configuration
	config := LoadConfig()
	middlewareConfig := config.ToMiddlewareConfig()

	// Create streaming worker
	worker, err := NewStreamingWorker(middlewareConfig)
	if err != nil {
		fmt.Printf("Failed to create streaming worker: %v\n", err)
		os.Exit(1)
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the worker in a goroutine
	go func() {
		if err := worker.Start(); err != 0 {
			fmt.Printf("Failed to start streaming worker: %v\n", err)
			os.Exit(1)
		}
	}()

	fmt.Println("Streaming Service started successfully. Waiting for messages...")

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("Shutting down Streaming Service...")

	// Close the worker
	worker.Close()

	fmt.Println("Streaming Service stopped.")
}
