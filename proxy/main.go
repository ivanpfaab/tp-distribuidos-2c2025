package main

import (
	"log"
	"os"

	"github.com/tp-distribuidos-2c2025/proxy/config"
	"github.com/tp-distribuidos-2c2025/shared/health_server"
)

func main() {
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8888"
	}
	healthSrv := health_server.NewHealthServer(healthPort)
	go healthSrv.Start()
	defer healthSrv.Stop()

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
