package main

import (
	"log"
	"os"
)

func main() {
	// Get port from environment variable or use default
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	// Create and start TCP server
	server := NewTCPServer(port)

	log.Printf("Starting server on port %s", port)

	if err := server.Start(port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
