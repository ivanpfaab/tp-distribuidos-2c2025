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

	fmt.Println("Starting User Join Reader (Query 4)...")

	// Load configuration
	connConfig, readerConfig, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create reader data directory with 0777 permissions
	// This allows both writer and reader containers to create/delete files
	if err := os.MkdirAll(SharedDataDir, 0777); err != nil {
		fmt.Printf("Failed to create reader data directory: %v\n", err)
		return
	}
	// Ensure directory has correct permissions (in case it already existed)
	if err := os.Chmod(SharedDataDir, 0777); err != nil {
		// Log but don't fail - this is best effort
		fmt.Printf("User Join Reader: Warning - failed to set directory permissions on %s: %v\n",
			SharedDataDir, err)
	}

	// Create Join by User ID Worker (Reader only - writers are separate now)
	worker, err := NewJoinByUserIdWorker(connConfig, readerConfig)
	if err != nil {
		fmt.Printf("Failed to create join by user ID worker: %v\n", err)
		return
	}
	defer worker.Close()

	// Start the reader worker
	if err := worker.Start(); err != 0 {
		fmt.Printf("Failed to start join by user ID worker: %v\n", err)
		return
	}

	fmt.Println("User Join Reader started successfully")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down User Join Reader...")
}
