package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	log.Println("Starting Group By Orchestrator...")

	// Get query type from environment variable
	queryTypeStr := os.Getenv("QUERY_TYPE")
	if queryTypeStr == "" {
		log.Fatal("QUERY_TYPE environment variable is required")
	}

	queryType, err := strconv.Atoi(queryTypeStr)
	if err != nil {
		log.Fatalf("Invalid QUERY_TYPE: %s", queryTypeStr)
	}

	log.Printf("Starting orchestrator for Query %d", queryType)

	// Create orchestrator for the specified query type
	orchestrator, err := NewGroupByOrchestrator(queryType)
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}
	defer orchestrator.Close()

	// Start the orchestrator
	go orchestrator.Start()

	// Wait for termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Group By Orchestrator for Query %d is running. Press Ctrl+C to stop.", queryType)
	<-sigChan

	log.Printf("Shutting down Group By Orchestrator for Query %d...", queryType)
}
