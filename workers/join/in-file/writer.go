package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// JoinDataWriter handles writing user data to CSV files with specific naming convention
type JoinDataWriter struct {
	consumer *workerqueue.QueueConsumer
	config   *middleware.ConnectionConfig
}

// NewJoinDataWriter creates a new JoinDataWriter instance
func NewJoinDataWriter(config *middleware.ConnectionConfig) (*JoinDataWriter, error) {
	// Create consumer for user ID dictionary queue
	consumer := workerqueue.NewQueueConsumer(
		JoinUserIdDictionaryQueue,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create user ID dictionary consumer")
	}

	// Declare the queue using QueueMiddleware
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		JoinUserIdDictionaryQueue,
		config,
	)
	if queueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}

	// Declare the queue
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare user ID dictionary queue: %v", err)
	}

	return &JoinDataWriter{
		consumer: consumer,
		config:   config,
	}, nil
}

// Start starts the join data writer
func (jdw *JoinDataWriter) Start() middleware.MessageMiddlewareError {
	fmt.Println("Join Data Writer: Starting to listen for user data...")
	return jdw.consumer.StartConsuming(jdw.createCallback())
}

// Close closes all connections
func (jdw *JoinDataWriter) Close() {
	if jdw.consumer != nil {
		jdw.consumer.Close()
	}
}

// createCallback creates the message processing callback
func (jdw *JoinDataWriter) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			if err := jdw.processMessage(delivery); err != 0 {
				fmt.Printf("Join Data Writer: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// processMessage processes a single user data chunk
func (jdw *JoinDataWriter) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("Join Data Writer: Received user data chunk\n")

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Join Data Writer: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Write users to partition files based on hash
	if err := jdw.writeUsersToPartitions(string(chunkMsg.ChunkData)); err != nil {
		fmt.Printf("Join Data Writer: Failed to write users to partitions: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Join Data Writer: Successfully processed chunk\n")
	return 0
}

// writeUsersToPartitions writes users to partition files based on hash
func (jdw *JoinDataWriter) writeUsersToPartitions(csvData string) error {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) < 1 {
		return fmt.Errorf("no data in chunk")
	}

	// Track which partitions we write to for logging
	partitionsWritten := make(map[int]int)

	// Process each user record
	for i := 0; i < len(records); i++ {
		record := records[i]
		if record[0] == "user_id" {
			continue
		}

		userID := record[0]

		// Determine partition for this user
		partition, err := getUserPartition(userID)
		if err != nil {
			fmt.Printf("Join Data Writer: Failed to get partition for user %s: %v\n", userID, err)
			continue
		}

		// Append user to partition file
		if err := jdw.appendUserToPartition(partition, record); err != nil {
			fmt.Printf("Join Data Writer: Failed to write user %s to partition %d: %v\n", userID, partition, err)
			continue
		}

		partitionsWritten[partition]++
	}

	// Log summary
	fmt.Printf("Join Data Writer: Wrote %d users across %d partitions\n", len(records)-1, len(partitionsWritten))
	return nil
}

// appendUserToPartition appends a user record to the appropriate partition file
func (jdw *JoinDataWriter) appendUserToPartition(partition int, record []string) error {
	filename := fmt.Sprintf("users-partition-%03d.csv", partition)
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
