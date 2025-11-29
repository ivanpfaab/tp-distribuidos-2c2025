package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	fmt.Println("Starting User Partition Splitter...")

	// Load configuration
	connConfig, splitterConfig, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create splitter
	splitter, err := NewUserPartitionSplitter(connConfig, splitterConfig)
	if err != nil {
		fmt.Printf("Failed to create splitter: %v\n", err)
		return
	}
	defer splitter.Close()

	// Start splitter
	if err := splitter.Start(); err != 0 {
		fmt.Printf("Failed to start splitter: %v\n", err)
		return
	}

	fmt.Printf("User Partition Splitter started with %d writers\n", splitterConfig.NumWriters)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down User Partition Splitter...")
}
