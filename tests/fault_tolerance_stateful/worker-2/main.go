package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Println("Worker 2: Starting Stateful Worker...")

	// Load configuration
	config := LoadConfig()

	// Create worker
	worker, err := NewWorker(config)
	if err != nil {
		log.Fatalf("Worker 2: Failed to create worker: %v", err)
	}
	defer worker.Close()

	// Start worker
	if err := worker.Start(); err != 0 {
		log.Fatalf("Worker 2: Failed to start worker: %d", err)
	}

	log.Println("Worker 2: Started successfully. Waiting for messages...")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Worker 2: Shutting down...")
}

