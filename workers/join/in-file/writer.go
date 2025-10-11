package main

import (
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

	// Extract first and last user IDs from sorted CSV chunk data
	firstUserID, lastUserID, err := jdw.extractUserIDRange(string(chunkMsg.ChunkData))
	if err != nil {
		fmt.Printf("Join Data Writer: Failed to extract user ID range: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Join Data Writer: Processing chunk with user ID range %d-%d\n", firstUserID, lastUserID)

	// Write the entire chunk data to the appropriate file(s)
	if err := jdw.writeChunkToFiles(string(chunkMsg.ChunkData), firstUserID, lastUserID); err != nil {
		fmt.Printf("Join Data Writer: Failed to write chunk to files: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Join Data Writer: Successfully processed chunk with range %d-%d\n", firstUserID, lastUserID)
	return 0
}

// extractUserIDRange extracts the first and last user IDs from sorted CSV chunk data
func (jdw *JoinDataWriter) extractUserIDRange(csvData string) (int, int, error) {
	lines := strings.Split(strings.TrimSpace(csvData), "\n")
	if len(lines) < 2 {
		return 0, 0, fmt.Errorf("chunk data must have at least header and one data row")
	}

	// Skip header, get first data line
	firstLine := lines[1]
	firstFields := strings.Split(firstLine, ",")
	if len(firstFields) < 1 {
		return 0, 0, fmt.Errorf("first data line is invalid")
	}

	firstUserID, err := strconv.Atoi(firstFields[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid first user ID %s: %w", firstFields[0], err)
	}

	// Get last data line
	lastLine := lines[len(lines)-1]
	lastFields := strings.Split(lastLine, ",")
	if len(lastFields) < 1 {
		return 0, 0, fmt.Errorf("last data line is invalid")
	}

	lastUserID, err := strconv.Atoi(lastFields[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid last user ID %s: %w", lastFields[0], err)
	}

	return firstUserID, lastUserID, nil
}

// writeChunkToFiles writes the entire chunk data to a CSV file
// Uses the actual first and last user IDs from the chunk data for file naming
func (jdw *JoinDataWriter) writeChunkToFiles(csvData string, firstUserID, lastUserID int) error {
	// Use actual first and last user IDs for file naming
	filename := fmt.Sprintf("users-%05d-%05d.csv", firstUserID, lastUserID)
	filepath := filepath.Join(SharedDataDir, filename)

	fmt.Printf("Join Data Writer: Writing chunk with range %d-%d to file %s\n", firstUserID, lastUserID, filename)

	// Create and write to file
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filepath, err)
	}
	defer file.Close()

	// Write the entire chunk data
	if _, err := file.WriteString(csvData); err != nil {
		return fmt.Errorf("failed to write chunk data to file %s: %w", filepath, err)
	}

	fmt.Printf("Join Data Writer: Wrote chunk data to file %s\n", filename)
	return nil
}
