package main

import (
	"fmt"
)

func main() {
	// Load configuration
	config, err := LoadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create and initialize partitioner worker
	partitioner, err := NewPartitionerWorker(config)
	if err != nil {
		fmt.Printf("Failed to create partitioner worker: %v\n", err)
		return
	}
	defer partitioner.Close()

	// Start the partitioner
	if err := partitioner.Start(); err != 0 {
		fmt.Printf("Failed to start partitioner worker: %v\n", err)
		return
	}

	// Keep the partitioner running
	select {}
}
