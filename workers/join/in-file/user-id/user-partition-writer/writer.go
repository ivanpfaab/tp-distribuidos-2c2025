package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/common"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// UserPartitionWriter writes users to partition files
type UserPartitionWriter struct {
	consumer           *workerqueue.QueueConsumer
	cleanupConsumer    *exchange.ExchangeConsumer
	queueProducer      *workerqueue.QueueMiddleware // For self-requeuing cleanup messages
	completionProducer *workerqueue.QueueMiddleware // For notifying orchestrator
	config             *middleware.ConnectionConfig
	writerConfig       *Config
	partitionsWritten  map[int]int     // Track writes per partition
	stoppedClients     map[string]bool // Track clients that should stop writing
	clientMutex        sync.RWMutex
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

	// Create cleanup consumer for userid cleanup signals
	cleanupConsumer := exchange.NewExchangeConsumer(
		"userid-cleanup-exchange",
		[]string{fmt.Sprintf("userid-cleanup-writer-%d", writerConfig.WriterID)}, // Writer-specific routing key
		connConfig,
	)
	if cleanupConsumer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create cleanup consumer")
	}

	// Create queue producer for self-requeuing cleanup messages
	queueProducer := workerqueue.NewMessageMiddlewareQueue(queueName, connConfig)
	if queueProducer == nil {
		consumer.Close()
		cleanupConsumer.Close()
		return nil, fmt.Errorf("failed to create queue producer for queue %s", queueName)
	}

	// Create completion producer for notifying orchestrator
	completionProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.UserPartitionCompletionQueue,
		connConfig,
	)
	if completionProducer == nil {
		consumer.Close()
		cleanupConsumer.Close()
		queueProducer.Close()
		return nil, fmt.Errorf("failed to create completion producer")
	}

	return &UserPartitionWriter{
		consumer:           consumer,
		cleanupConsumer:    cleanupConsumer,
		queueProducer:      queueProducer,
		completionProducer: completionProducer,
		config:             connConfig,
		writerConfig:       writerConfig,
		partitionsWritten:  make(map[int]int),
		stoppedClients:     make(map[string]bool),
	}, nil
}

// Start starts the writer
func (upw *UserPartitionWriter) Start() middleware.MessageMiddlewareError {
	queueName := GetWriterQueueName(upw.writerConfig.WriterID)
	fmt.Printf("User Partition Writer %d: Starting to listen on queue %s...\n",
		upw.writerConfig.WriterID, queueName)

	// Start cleanup consumer
	if err := upw.cleanupConsumer.StartConsuming(upw.createCleanupCallback()); err != 0 {
		fmt.Printf("User Partition Writer %d: Failed to start cleanup consumer: %v\n", upw.writerConfig.WriterID, err)
	}

	return upw.consumer.StartConsuming(upw.createCallback())
}

// Close closes all connections
func (upw *UserPartitionWriter) Close() {
	if upw.consumer != nil {
		upw.consumer.Close()
	}
	if upw.cleanupConsumer != nil {
		upw.cleanupConsumer.Close()
	}
	if upw.queueProducer != nil {
		upw.queueProducer.Close()
	}
	if upw.completionProducer != nil {
		upw.completionProducer.Close()
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

// createCallback creates the message processing callback with message type detection
func (upw *UserPartitionWriter) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			if err := upw.processQueueMessage(delivery); err != 0 {
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

// createCleanupCallback creates the cleanup message processing callback
func (upw *UserPartitionWriter) createCleanupCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := upw.processCleanupMessage(delivery)
			if err != 0 {
				done <- fmt.Errorf("failed to process cleanup message: %v", err)
				return
			}
		}
	}
}

// processQueueMessage processes messages from queue with type detection
func (upw *UserPartitionWriter) processQueueMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Get message type
	msgType, err := deserializer.GetMessageType(delivery.Body)
	if err != nil {
		fmt.Printf("User Partition Writer %d: Failed to get message type: %v\n", upw.writerConfig.WriterID, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Route based on message type
	switch msgType {
	case common.ChunkMessageType: // Type 2
		return upw.processMessage(delivery)
	case common.JoinCleanupSignalType: // Type 6
		return upw.processCleanupMessage(delivery)
	default:
		fmt.Printf("User Partition Writer %d: Unknown message type: %d\n", upw.writerConfig.WriterID, msgType)
		return middleware.MessageMiddlewareMessageError
	}
}

// processCleanupMessage processes cleanup signals with double-cleanup logic
func (upw *UserPartitionWriter) processCleanupMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the cleanup signal
	cleanupSignal, err := signals.DeserializeJoinCleanupSignal(delivery.Body)
	if err != nil {
		fmt.Printf("User Partition Writer %d: Failed to deserialize cleanup signal: %v\n", upw.writerConfig.WriterID, err)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("User Partition Writer %d: Processing cleanup for client: %s\n", upw.writerConfig.WriterID, cleanupSignal.ClientID)

	// Process cleanup (idempotent operation)
	upw.performCleanup(cleanupSignal.ClientID)

	// Only requeue if this came from the exchange consumer (not from queue consumer)
	// We can detect this by checking if the delivery has exchange routing info
	if delivery.Exchange != "" {
		// This came from exchange consumer - requeue for double-cleanup
		serializedMsg, err := signals.SerializeJoinCleanupSignal(cleanupSignal)
		if err != nil {
			fmt.Printf("User Partition Writer %d: Failed to serialize cleanup signal for requeue: %v\n", upw.writerConfig.WriterID, err)
			return middleware.MessageMiddlewareMessageError
		}

		// Send cleanup message to same queue
		if err := upw.queueProducer.Send(serializedMsg); err != 0 {
			fmt.Printf("User Partition Writer %d: Failed to requeue cleanup signal: %v\n", upw.writerConfig.WriterID, err)
			return err
		}

		fmt.Printf("User Partition Writer %d: Requeued cleanup signal for client: %s\n", upw.writerConfig.WriterID, cleanupSignal.ClientID)
	} else {
		// This came from queue consumer - don't requeue to avoid infinite loop
		fmt.Printf("User Partition Writer %d: Processed cleanup from queue for client: %s (no requeue)\n", upw.writerConfig.WriterID, cleanupSignal.ClientID)
	}

	return 0
}

// performCleanup performs cleanup operations (idempotent)
func (upw *UserPartitionWriter) performCleanup(clientID string) {
	fmt.Printf("User Partition Writer %d: Performing cleanup for client: %s\n", upw.writerConfig.WriterID, clientID)

	// Clean up client-specific state
	upw.clientMutex.Lock()
	defer upw.clientMutex.Unlock()

	// Mark client as stopped
	upw.stoppedClients[clientID] = true

	// Clean up partition files for this client
	upw.cleanupClientFiles(clientID)

	fmt.Printf("User Partition Writer %d: Completed cleanup for client: %s\n", upw.writerConfig.WriterID, clientID)
}

// cleanupClientFiles deletes all partition files for a specific client
func (upw *UserPartitionWriter) cleanupClientFiles(clientID string) {
	// Delete all partition files for this client from the shared data directory
	pattern := filepath.Join(SharedDataDir, fmt.Sprintf("%s-users-partition-*.csv", clientID))
	fmt.Printf("User Partition Writer %d: Looking for files with pattern: %s\n", upw.writerConfig.WriterID, pattern)

	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Printf("User Partition Writer %d: Error finding files for client %s: %v\n", upw.writerConfig.WriterID, clientID, err)
		return
	}

	fmt.Printf("User Partition Writer %d: Found %d files for client %s: %v\n", upw.writerConfig.WriterID, len(files), clientID, files)

	for _, file := range files {
		if err := os.Remove(file); err != nil {
			fmt.Printf("User Partition Writer %d: Error deleting file %s: %v\n", upw.writerConfig.WriterID, file, err)
		} else {
			fmt.Printf("User Partition Writer %d: Deleted file %s for client %s\n", upw.writerConfig.WriterID, file, clientID)
		}
	}

	fmt.Printf("User Partition Writer %d: Cleaned up %d files for client %s\n", upw.writerConfig.WriterID, len(files), clientID)
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

	// Check if client is stopped (should drop this chunk)
	upw.clientMutex.RLock()
	isStopped := upw.stoppedClients[chunkMsg.ClientID]
	upw.clientMutex.RUnlock()

	if isStopped {
		fmt.Printf("User Partition Writer %d: Dropping chunk for stopped client %s\n", upw.writerConfig.WriterID, chunkMsg.ClientID)
		return 0 // Successfully dropped
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

	// Send chunk to orchestrator for completion tracking
	upw.sendChunkToOrchestrator(chunkMsg)

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

// sendChunkToOrchestrator sends chunk to orchestrator for completion tracking
func (upw *UserPartitionWriter) sendChunkToOrchestrator(chunkMsg *chunk.Chunk) {
	notification := signals.NewChunkNotification(
		chunkMsg.ClientID,
		chunkMsg.FileID,
		fmt.Sprintf("user-partition-writer-%d", upw.writerConfig.WriterID),
		int(chunkMsg.TableID),
		int(chunkMsg.ChunkNumber),
		chunkMsg.IsLastChunk,
		chunkMsg.IsLastFromTable,
	)

	messageData, err := signals.SerializeChunkNotification(notification)
	if err != nil {
		fmt.Printf("User Partition Writer %d: Failed to serialize chunk notification for orchestrator: %v\n", upw.writerConfig.WriterID, err)
		return
	}

	if err := upw.completionProducer.Send(messageData); err != 0 {
		fmt.Printf("User Partition Writer %d: Failed to send chunk notification to orchestrator: %v\n", upw.writerConfig.WriterID, err)
	} else {
		fmt.Printf("User Partition Writer %d: Sent chunk notification to orchestrator for client %s (chunk %d)\n",
			upw.writerConfig.WriterID, chunkMsg.ClientID, chunkMsg.ChunkNumber)
	}
}
