package main

import (
	"os"
	"os/signal"
	"syscall"

	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

func main() {
	// Initialize logger
	testing_utils.InitLogger()

	testing_utils.LogInfo("Streaming Service", "Starting Streaming Service...")

	// Load configuration
	config := LoadConfig()
	middlewareConfig := config.ToMiddlewareConfig()

	// Create streaming worker
	worker, err := NewStreamingWorker(middlewareConfig)
	if err != nil {
		testing_utils.LogError("Streaming Service", "Failed to create streaming worker: %v", err)
		os.Exit(1)
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the worker in a goroutine
	go func() {
		if err := worker.Start(); err != 0 {
			testing_utils.LogError("Streaming Service", "Failed to start streaming worker: %v", err)
			os.Exit(1)
		}
	}()

	testing_utils.LogInfo("Streaming Service", "Streaming Service started successfully. Waiting for messages...")

	// Wait for shutdown signal
	<-sigChan
	testing_utils.LogInfo("Streaming Service", "Shutting down Streaming Service...")

	// Close the worker
	worker.Close()

	testing_utils.LogInfo("Streaming Service", "Streaming Service stopped.")
}
