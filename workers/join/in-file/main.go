package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create shared data directory
	if err := os.MkdirAll("/shared-data", 0755); err != nil {
		fmt.Printf("Failed to create shared data directory: %v\n", err)
		return
	}

	// Create Join Data Writer
	writer, err := NewJoinDataWriter(config)
	if err != nil {
		fmt.Printf("Failed to create join data writer: %v\n", err)
		return
	}
	defer writer.Close()

	// Create Join by User ID Worker
	worker, err := NewJoinByUserIdWorker(config)
	if err != nil {
		fmt.Printf("Failed to create join by user ID worker: %v\n", err)
		return
	}
	defer worker.Close()

	// Start both components
	go func() {
		if err := writer.Start(); err != 0 {
			fmt.Printf("Failed to start join data writer: %v\n", err)
		}
	}()

	go func() {
		if err := worker.Start(); err != 0 {
			fmt.Printf("Failed to start join by user ID worker: %v\n", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down in-file join worker...")
}
