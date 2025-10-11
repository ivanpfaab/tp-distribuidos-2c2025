package main

import "fmt"

func main() {
	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create and initialize join data handler
	handler, err := NewJoinDataHandler(config)
	if err != nil {
		fmt.Printf("Failed to create join data handler: %v\n", err)
		return
	}
	defer handler.Close()

	// Start the handler
	if err := handler.Start(); err != 0 {
		fmt.Printf("Failed to start join data handler: %v\n", err)
		return
	}

	// Keep the handler running
	select {}
}
