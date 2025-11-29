package main

import (
	"fmt"
	"os"

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

	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create and initialize time filter worker
	worker, err := NewTimeFilterWorker(config)
	if err != nil {
		fmt.Printf("Failed to create time filter worker: %v\n", err)
		return
	}
	defer worker.Close()

	// Start the worker
	if err := worker.Start(); err != 0 {
		fmt.Printf("Failed to start time filter worker: %v\n", err)
		return
	}

	// Keep the worker running
	select {}
}
