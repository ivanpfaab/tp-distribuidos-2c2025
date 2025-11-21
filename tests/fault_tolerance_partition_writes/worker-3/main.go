package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	config := LoadConfig()

	worker, err := NewWorker(config)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}
	defer worker.Close()

	// Start worker
	if err := worker.Start(); err != 0 {
		log.Fatalf("Worker 3: Failed to start: %v", err)
	}

	log.Println("Worker 3: Running")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Worker 3: Shutting down...")
}

