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

	if err := worker.Start(); err != 0 {
		log.Fatalf("Failed to start worker: %d", err)
	}

	log.Printf("Worker %d: Running", config.WorkerID)

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Printf("Worker %d: Shutting down", config.WorkerID)
}

