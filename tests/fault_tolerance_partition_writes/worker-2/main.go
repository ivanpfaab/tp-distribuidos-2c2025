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
		log.Fatalf("Worker 2: Failed to start: %v", err)
	}

	log.Println("Worker 2: Running")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Worker 2: Shutting down...")
}
