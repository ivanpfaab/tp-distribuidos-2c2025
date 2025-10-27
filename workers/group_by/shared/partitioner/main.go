package main

import (
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

func main() {
	// Initialize logger
	testing_utils.InitLogger()

	// Load configuration
	config, err := LoadConfig()
	if err != nil {
		testing_utils.LogError("Partitioner", "Failed to load configuration: %v", err)
		return
	}

	// Create and initialize partitioner worker
	partitioner, err := NewPartitionerWorker(config)
	if err != nil {
		testing_utils.LogError("Partitioner", "Failed to create partitioner worker: %v", err)
		return
	}
	defer partitioner.Close()

	// Start the partitioner
	if err := partitioner.Start(); err != 0 {
		testing_utils.LogError("Partitioner", "Failed to start partitioner worker: %v", err)
		return
	}

	// Keep the partitioner running
	select {}
}
