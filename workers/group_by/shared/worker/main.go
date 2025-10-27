package main

import (
	"os"

	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

func main() {
	// Initialize logger
	testing_utils.InitLogger()

	// Load configuration from environment variables
	config, err := LoadConfig()
	if err != nil {
		testing_utils.LogError("GroupBy Worker", "Failed to load config: %v", err)
		os.Exit(1)
	}

	// Create worker
	groupByWorker, err := NewGroupByWorker(config)
	if err != nil {
		testing_utils.LogError("GroupBy Worker", "Failed to create group by worker: %v", err)
		os.Exit(1)
	}
	defer groupByWorker.Close()

	testing_utils.LogInfo("GroupBy Worker", "Starting Group By Worker for Query %d (Worker ID: %d)", config.QueryType, config.WorkerID)

	// Start processing
	if err := groupByWorker.Start(); err != 0 {
		testing_utils.LogError("GroupBy Worker", "Failed to start worker: error code %v", err)
		os.Exit(1)
	}

	// Block forever
	select {}
}
