package main

import (
	"log"

	"github.com/tp-distribuidos-2c2025/proxy/config"
)

func main() {
	// Get TCP server configuration
	serverConfig := config.NewServerConfig()

	log.Printf("Starting TCP server on port %s", serverConfig.Port)

	// Create and start TCP server
	server := NewTCPServer(serverConfig.Port)
	defer server.Stop()

	// Start the TCP server (blocking)
	if err := server.Start(serverConfig.Port); err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}
}
