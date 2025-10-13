package main

import (
	"log"
	"os"
)

func main() {
	// Get TCP server port from environment
	serverPort := getEnv("SERVER_PORT", "8080")

	log.Printf("Starting TCP server on port %s", serverPort)

	// Create and start TCP server
	server := NewTCPServer(serverPort)
	defer server.Stop()

	// Start the TCP server (blocking)
	if err := server.Start(serverPort); err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}
}

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
