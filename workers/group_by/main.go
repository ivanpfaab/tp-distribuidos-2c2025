package main

import "fmt"

const (
	DefaultNumWorkers = 2 // Default number of parallel workers
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
	fmt.Printf("GroupBy Orchestrator: Starting with %d workers\n", numWorkers)

	// Create and initialize group by orchestrator
	orchestrator, err := NewGroupByOrchestrator(config, numWorkers)
	if err != nil {
		fmt.Printf("Failed to create group by orchestrator: %v\n", err)
		return
	}
	defer orchestrator.Close()

	// Start the orchestrator
	if err := orchestrator.Start(); err != 0 {
		fmt.Printf("Failed to start group by orchestrator: %v\n", err)
		return
	}

	// Keep the orchestrator running
	select {}
}
