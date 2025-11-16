package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("Starting User Join Reader (Query 4)...")

	// Load configuration
	connConfig, readerConfig, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create reader data directory
	if err := os.MkdirAll(SharedDataDir, 0755); err != nil {
		fmt.Printf("Failed to create reader data directory: %v\n", err)
		return
	}

	// Create Join by User ID Worker (Reader only - writers are separate now)
	worker, err := NewJoinByUserIdWorker(connConfig, readerConfig)
	if err != nil {
		fmt.Printf("Failed to create join by user ID worker: %v\n", err)
		return
	}
	defer worker.Close()

	// Start the reader worker
	if err := worker.Start(); err != 0 {
		fmt.Printf("Failed to start join by user ID worker: %v\n", err)
		return
	}

	fmt.Println("User Join Reader started successfully")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down User Join Reader...")
}
