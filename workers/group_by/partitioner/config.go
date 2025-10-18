package main

import "github.com/tp-distribuidos-2c2025/shared/middleware"

// PartitionerConfig holds configuration for the partitioner worker
type PartitionerConfig struct {
	QueryType        int
	QueueName        string
	ConnectionConfig *middleware.ConnectionConfig
}
