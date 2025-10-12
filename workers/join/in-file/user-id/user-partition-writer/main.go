package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("Starting User Partition Writer...")

	// Load configuration
	connConfig, writerConfig, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create writer
	writer, err := NewUserPartitionWriter(connConfig, writerConfig)
	if err != nil {
		fmt.Printf("Failed to create writer: %v\n", err)
		return
	}
	defer writer.Close()

	// Start writer
	if err := writer.Start(); err != 0 {
		fmt.Printf("Failed to start writer: %v\n", err)
		return
	}

	fmt.Printf("User Partition Writer %d started (handles partitions where partition %% %d == %d)\n",
		writerConfig.WriterID, writerConfig.NumWriters, writerConfig.WriterID-1)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Printf("Shutting down User Partition Writer %d...\n", writerConfig.WriterID)
}
