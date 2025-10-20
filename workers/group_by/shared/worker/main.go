package main

import (
	"log"
)

func main() {
	// Load configuration from environment variables
	config, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create worker
	groupByWorker, err := NewGroupByWorker(config)
	if err != nil {
		log.Fatalf("Failed to create group by worker: %v", err)
	}
	defer groupByWorker.Close()

	log.Printf("Starting Group By Worker for Query %d (Worker ID: %d)", config.QueryType, config.WorkerID)

	// Start processing
	if err := groupByWorker.Start(); err != 0 {
		log.Fatalf("Failed to start worker: error code %v", err)
	}

	// Block forever
	select {}
}
