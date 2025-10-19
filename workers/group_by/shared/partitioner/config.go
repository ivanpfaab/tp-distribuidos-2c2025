package partitioner

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	// Buffer configuration
	MaxBufferSize = 200 // Maximum records to buffer per partition before flushing

	// Partition configuration
	NumPartitions = 10 // Number of partitions for user_id modulo

	// Default configuration
	DefaultRabbitMQURL = "amqp://guest:guest@localhost:5672/"
)

// PartitionerConfig holds configuration for the partitioner worker
type PartitionerConfig struct {
	QueryType        int
	QueueName        string
	ConnectionConfig *middleware.ConnectionConfig
	NumPartitions    int
	MaxBufferSize    int
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*PartitionerConfig, error) {
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
		rabbitMQURL = DefaultRabbitMQURL
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

	// Load partition configuration
	numPartitionsStr := os.Getenv("NUM_PARTITIONS")
	numPartitions := NumPartitions
	if numPartitionsStr != "" {
		if parsed, err := strconv.Atoi(numPartitionsStr); err == nil && parsed > 0 {
			numPartitions = parsed
		}
	}

	maxBufferSizeStr := os.Getenv("MAX_BUFFER_SIZE")
	maxBufferSize := MaxBufferSize
	if maxBufferSizeStr != "" {
		if parsed, err := strconv.Atoi(maxBufferSizeStr); err == nil && parsed > 0 {
			maxBufferSize = parsed
		}
	}

	connectionConfig := &middleware.ConnectionConfig{
		URL: rabbitMQURL,
	}

	return &PartitionerConfig{
		QueryType:        queryType,
		QueueName:        queueName,
		ConnectionConfig: connectionConfig,
		NumPartitions:    numPartitions,
		MaxBufferSize:    maxBufferSize,
	}, nil
}
