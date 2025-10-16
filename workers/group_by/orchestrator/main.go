package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Println("Starting Group By Orchestrator...")

	// Create orchestrator for Query 2
	orchestrator := NewGroupByOrchestrator(2)
	defer orchestrator.Close()

	// Start the orchestrator
	go orchestrator.Start()

	// Wait for termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Group By Orchestrator is running. Press Ctrl+C to stop.")
	<-sigChan

	log.Println("Shutting down Group By Orchestrator...")

	select {}
}
