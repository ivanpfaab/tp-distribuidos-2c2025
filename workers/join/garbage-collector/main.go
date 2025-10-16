package main

import (
	"log"

	"github.com/tp-distribuidos-2c2025/workers/join/garbage-collector/config"
	"github.com/tp-distribuidos-2c2025/workers/join/garbage-collector/processor"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create garbage collector
	gc, err := processor.NewJoinGarbageCollector(cfg)
	if err != nil {
		log.Fatalf("Failed to create garbage collector: %v", err)
	}

	// Start garbage collector
	log.Println("Join Garbage Collector: Starting...")
	if errCode := gc.Start(); errCode != 0 {
		log.Fatalf("Failed to start garbage collector: error code %d", errCode)
	}

	// Wait for shutdown signal
	select {}
}
