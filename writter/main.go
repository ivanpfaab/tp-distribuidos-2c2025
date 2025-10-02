package main

import "fmt"

func main() {
	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create and initialize data writer worker
	worker, err := NewDataWriterWorker(config)
	if err != nil {
		fmt.Printf("Failed to create data writer worker: %v\n", err)
		return
	}
	defer worker.Close()

	// Declare the queue before starting to consume
	if err := worker.DeclareQueue(); err != 0 {
		fmt.Printf("Failed to declare data writer queue: %v\n", err)
		return
	}

	// Start the worker
	if err := worker.Start(); err != 0 {
		fmt.Printf("Failed to start data writer worker: %v\n", err)
		return
	}

	// Keep the worker running
	select {}
}
