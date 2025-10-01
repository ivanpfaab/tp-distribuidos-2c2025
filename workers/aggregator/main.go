package main

import "fmt"

func main() {
	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create and initialize aggregator worker
	worker, err := NewAggregatorWorker(config)
	if err != nil {
		fmt.Printf("Failed to create aggregator worker: %v\n", err)
		return
	}
	defer worker.Close()

	// Start the worker
	if err := worker.Start(); err != 0 {
		fmt.Printf("Failed to start aggregator worker: %v\n", err)
		return
	}

	// Keep the worker running
	select {}
}
