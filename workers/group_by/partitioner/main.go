package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

func main() {
	// Load configuration
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}

	// Create and initialize partitioner worker
	partitioner, err := NewPartitionerWorker(config)
	if err != nil {
		fmt.Printf("Failed to create partitioner worker: %v\n", err)
		return
	}
	defer partitioner.Close()

	// Start the partitioner
	if err := partitioner.Start(); err != 0 {
		fmt.Printf("Failed to start partitioner worker: %v\n", err)
		return
	}

	// Keep the partitioner running
	select {}
}

// loadConfig loads configuration from environment variables
func loadConfig() (*PartitionerConfig, error) {
	queryTypeStr := os.Getenv("QUERY_TYPE")
	if queryTypeStr == "" {
		return nil, fmt.Errorf("QUERY_TYPE environment variable is required")
	}

	queryType, err := strconv.Atoi(queryTypeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid QUERY_TYPE: %v", err)
	}

	if queryType < 2 || queryType > 4 {
		return nil, fmt.Errorf("QUERY_TYPE must be 2, 3, or 4")
	}

	rabbitMQURL := os.Getenv("RABBITMQ_URL")
	if rabbitMQURL == "" {
		rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	}

	// Get the appropriate queue name based on query type
	var queueName string
	switch queryType {
	case 2:
		queueName = queues.Query2MapQueue
	case 3:
		queueName = queues.Query3MapQueue
	case 4:
		queueName = queues.Query4MapQueue
	}

	connectionConfig := &middleware.ConnectionConfig{
		URL: rabbitMQURL,
	}

	return &PartitionerConfig{
		QueryType:        queryType,
		QueueName:        queueName,
		ConnectionConfig: connectionConfig,
	}, nil
}
