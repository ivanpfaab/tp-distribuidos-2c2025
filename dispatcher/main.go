package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/tp-distribuidos-2c2025/shared/health_server"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

func main() {
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8888"
	}
	healthSrv := health_server.NewHealthServer(healthPort)
	go healthSrv.Start()
	defer healthSrv.Stop()

	// Initialize logger
	testing_utils.InitLogger()

	testing_utils.LogInfo("Results Dispatcher", "Starting Results Dispatcher...")

	// Load configuration
	config := LoadConfig()
	middlewareConfig := config.ToMiddlewareConfig()

	// Create results dispatcher worker
	worker, err := NewResultsDispatcherWorker(middlewareConfig)
	if err != nil {
		testing_utils.LogError("Results Dispatcher", "Failed to create results dispatcher worker: %v", err)
		os.Exit(1)
	}

	// Start the worker (non-blocking - returns immediately)
	if err := worker.Start(); err != 0 {
		testing_utils.LogError("Results Dispatcher", "Failed to start results dispatcher worker: %v", err)
		os.Exit(1)
	}

	testing_utils.LogInfo("Results Dispatcher", "Results Dispatcher started successfully. Waiting for messages...")

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	testing_utils.LogInfo("Results Dispatcher", "Shutting down Results Dispatcher...")

	// Close the worker
	worker.Close()

	testing_utils.LogInfo("Results Dispatcher", "Results Dispatcher stopped.")
}
