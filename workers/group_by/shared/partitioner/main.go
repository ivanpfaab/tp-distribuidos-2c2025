package main

import (
	"os"

	"github.com/tp-distribuidos-2c2025/shared/health_server"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

func main() {
	testing_utils.InitLogger()

	config, err := LoadConfig()
	if err != nil {
		testing_utils.LogError("Partitioner", "Failed to load configuration: %v", err)
		return
	}

	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8888"
	}
	healthServer := health_server.NewHealthServer(healthPort)
	go healthServer.Start()

	partitioner, err := NewPartitionerWorker(config)
	if err != nil {
		testing_utils.LogError("Partitioner", "Failed to create partitioner worker: %v", err)
		return
	}
	defer partitioner.Close()

	if err := partitioner.Start(); err != 0 {
		testing_utils.LogError("Partitioner", "Failed to start partitioner worker: %v", err)
		return
	}

	select {}
}
