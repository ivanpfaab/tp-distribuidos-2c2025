package main

import "fmt"

const (
	DefaultNumWorkers = 4 // Default number of parallel workers
)

func main() {
	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Get number of workers from environment or use default
	numWorkers := getEnvInt("NUM_WORKERS", DefaultNumWorkers)
	fmt.Printf("GroupBy Worker Service: Starting with %d workers\n", numWorkers)

	// Create and initialize group by worker service
	workerService, err := NewGroupByWorkerService(config, numWorkers)
	if err != nil {
		fmt.Printf("Failed to create group by worker service: %v\n", err)
		return
	}
	defer workerService.Close()

	// Start the worker service
	if err := workerService.Start(); err != 0 {
		fmt.Printf("Failed to start group by worker service: %v\n", err)
		return
	}

	// Keep the worker service running
	select {}
}
