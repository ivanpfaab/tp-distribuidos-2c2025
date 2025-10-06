package main

import "fmt"

func main() {
	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create and initialize year filter worker
	worker, err := NewYearFilterWorker(config)
	if err != nil {
		fmt.Printf("Failed to create year filter worker: %v\n", err)
		return
	}
	defer worker.Close()

	// Start the worker
	if err := worker.Start(); err != 0 {
		fmt.Printf("Failed to start year filter worker: %v\n", err)
		return
	}

	// Keep the worker running
	select {}
}
