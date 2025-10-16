package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// UserPartitionWriter writes users to partition files
type UserPartitionWriter struct {
	consumer          *workerqueue.QueueConsumer
	config            *middleware.ConnectionConfig
	writerConfig      *Config
	partitionsWritten map[int]int // Track writes per partition
}

// NewUserPartitionWriter creates a new writer instance
func NewUserPartitionWriter(connConfig *middleware.ConnectionConfig, writerConfig *Config) (*UserPartitionWriter, error) {
	// Create consumer for this writer's queue
	queueName := GetWriterQueueName(writerConfig.WriterID)
	consumer := workerqueue.NewQueueConsumer(queueName, connConfig)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create consumer for queue %s", queueName)
	}

	// Ensure shared data directory exists
	if err := os.MkdirAll(SharedDataDir, 0755); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create shared data directory: %w", err)
	}

	return &UserPartitionWriter{
		consumer:          consumer,
		config:            connConfig,
		writerConfig:      writerConfig,
		partitionsWritten: make(map[int]int),
	}, nil
}

// Start starts the writer
func (upw *UserPartitionWriter) Start() middleware.MessageMiddlewareError {
	queueName := GetWriterQueueName(upw.writerConfig.WriterID)
	fmt.Printf("User Partition Writer %d: Starting to listen on queue %s...\n",
		upw.writerConfig.WriterID, queueName)
	return upw.consumer.StartConsuming(upw.createCallback())
}

// Close closes all connections
func (upw *UserPartitionWriter) Close() {
	if upw.consumer != nil {
		upw.consumer.Close()
	}

	// Print statistics
	fmt.Printf("User Partition Writer %d: Final statistics:\n", upw.writerConfig.WriterID)
	totalWrites := 0
	for partition, count := range upw.partitionsWritten {
		fmt.Printf("  Partition %d: %d users\n", partition, count)
		totalWrites += count
	}
	fmt.Printf("  Total: %d users across %d partitions\n", totalWrites, len(upw.partitionsWritten))
}

// createCallback creates the message processing callback
func (upw *UserPartitionWriter) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			if err := upw.processMessage(delivery); err != 0 {
				fmt.Printf("User Partition Writer %d: Failed to process message: %v\n",
					upw.writerConfig.WriterID, err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// processMessage processes a single user chunk
func (upw *UserPartitionWriter) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("User Partition Writer %d: Failed to deserialize chunk: %v\n",
			upw.writerConfig.WriterID, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Check for EOS marker
	if chunkMsg.ChunkNumber == -1 {
		fmt.Printf("User Partition Writer %d: Received EOS marker\n", upw.writerConfig.WriterID)
		return 0
	}

	fmt.Printf("User Partition Writer %d: Processing chunk - ChunkNumber: %d, Size: %d, IsLastChunk: %t\n",
		upw.writerConfig.WriterID, chunkMsg.ChunkNumber, chunkMsg.ChunkSize, chunkMsg.IsLastChunk)

	// Write users to partitions
	if err := upw.writeUsersToPartitions(chunkMsg.ChunkData, chunkMsg.ClientID); err != nil {
		fmt.Printf("User Partition Writer %d: Failed to write users: %v\n",
			upw.writerConfig.WriterID, err)
		return middleware.MessageMiddlewareMessageError
	}

	return 0
}

// writeUsersToPartitions writes users to their respective partition files
func (upw *UserPartitionWriter) writeUsersToPartitions(csvData string, clientID string) error {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) < 1 {
		return fmt.Errorf("no data in chunk")
	}

	userCount := 0
	skippedCount := 0

	for i := 0; i < len(records); i++ {
		record := records[i]
		if strings.Contains(record[0], "user_id") {
			continue
		}
		if len(record) < 4 {
			fmt.Printf("User Partition Writer %d: Skipping malformed record: %v\n",
				upw.writerConfig.WriterID, record)
			continue
		}

		userID := record[0]

		// Calculate partition
		partition, err := getUserPartition(userID)
		if err != nil {
			fmt.Printf("User Partition Writer %d: Failed to get partition for user %s: %v\n",
				upw.writerConfig.WriterID, userID, err)
			continue
		}

		// Verify this writer owns this partition
		if !upw.writerConfig.OwnsPartition(partition) {
			// This shouldn't happen if splitter logic is correct
			fmt.Printf("User Partition Writer %d: WARNING - Received user for partition %d which is not owned by this writer\n",
				upw.writerConfig.WriterID, partition)
			skippedCount++
			continue
		}

		// Write to partition file
		if err := upw.appendUserToPartition(partition, record, clientID); err != nil {
			fmt.Printf("User Partition Writer %d: Failed to write user %s to partition %d: %v\n",
				upw.writerConfig.WriterID, userID, partition, err)
			continue
		}

		upw.partitionsWritten[partition]++
		userCount++
	}

	if skippedCount > 0 {
		fmt.Printf("User Partition Writer %d: WARNING - Skipped %d users from wrong partitions\n",
			upw.writerConfig.WriterID, skippedCount)
	}

	fmt.Printf("User Partition Writer %d: Wrote %d users across %d partitions\n",
		upw.writerConfig.WriterID, userCount, len(upw.partitionsWritten))

	return nil
}

// appendUserToPartition appends a user record to the appropriate partition file
func (upw *UserPartitionWriter) appendUserToPartition(partition int, record []string, clientID string) error {
	filename := fmt.Sprintf("%s-users-partition-%03d.csv", clientID, partition)
	filePath := filepath.Join(SharedDataDir, filename)

	// Check if file exists to determine if we need to write header
	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
	}

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open partition file %s: %w", filePath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if this is a new file
	if !fileExists {
		header := []string{"user_id", "gender", "birthdate", "registered_at"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Write the user record
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

// getUserPartition calculates the partition for a user ID (must match splitter and reader logic)
func getUserPartition(userID string) (int, error) {
	// Parse user ID (handle both int and float formats)
	userIDFloat, err := strconv.ParseFloat(userID, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid user ID %s: %w", userID, err)
	}
	userIDInt := int(userIDFloat)

	// Simple modulo partitioning (must match splitter and reader logic exactly!)
	return userIDInt % NumPartitions, nil
}
