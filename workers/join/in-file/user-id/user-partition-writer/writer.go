package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/common"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	completioncleaner "github.com/tp-distribuidos-2c2025/shared/completion_cleaner"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	partitionmanager "github.com/tp-distribuidos-2c2025/shared/partition_manager"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
)

// UserPartitionWriter writes users to partition files
type UserPartitionWriter struct {
	consumer            *workerqueue.QueueConsumer
	cleanupConsumer     *exchange.ExchangeConsumer
	queueProducer       *workerqueue.QueueMiddleware // For self-requeuing cleanup messages
	completionProducer  *workerqueue.QueueMiddleware // For notifying orchestrator
	config              *middleware.ConnectionConfig
	writerConfig        *Config
	partitionsWritten   map[int]int     // Track writes per partition
	stoppedClients      map[string]bool // Track clients that should stop writing
	clientMutex         sync.RWMutex
	messageManager      *messagemanager.MessageManager
	partitionManager    *partitionmanager.PartitionManager
	firstChunkProcessed bool // Tracks if first chunk after restart has been processed
}

// NewUserPartitionWriter creates a new writer instance
func NewUserPartitionWriter(connConfig *middleware.ConnectionConfig, writerConfig *Config) (*UserPartitionWriter, error) {
	// Get dynamic queue name
	queueName := GetWriterQueueName(writerConfig.WriterID)

	// Use builder to create all resources
	stateDir := filepath.Join(SharedDataDir, "state")
	stateFilePath := filepath.Join(stateDir, "processed-chunks.txt")

	builder := worker_builder.NewWorkerBuilder(fmt.Sprintf("User Partition Writer %d", writerConfig.WriterID)).
		WithConfig(connConfig).
		// Queue consumer (dynamic queue name)
		WithQueueConsumer(queueName, true).
		// Exchange consumer for cleanup signals
		WithExchangeConsumer("userid-cleanup-exchange", []string{fmt.Sprintf("userid-cleanup-writer-%d", writerConfig.WriterID)}, false).
		// Queue producers
		WithQueueProducer(queueName, true). // Self-requeuing producer (same queue)
		WithQueueProducer(queues.UserPartitionCompletionQueue, true).
		// State management
		WithDirectory(SharedDataDir, 0755). // Will chmod to 0777 after
		WithDirectory(stateDir, 0755).      // Will chmod to 0777 after
		WithMessageManager(stateFilePath).
		// PartitionManager
		WithPartitionManager(worker_builder.PartitionManagerConfig{
			PartitionsDir:           SharedDataDir,
			NumPartitions:           NumPartitions,
			RecoverIncompleteWrites: true,
		})

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Set special permissions for shared directory (allows writer and reader to access)
	if err := os.Chmod(SharedDataDir, 0777); err != nil {
		fmt.Printf("User Partition Writer %d: Warning - failed to set directory permissions on %s: %v\n",
			writerConfig.WriterID, SharedDataDir, err)
	}
	if err := os.Chmod(stateDir, 0777); err != nil {
		fmt.Printf("User Partition Writer %d: Warning - failed to set state directory permissions: %v\n",
			writerConfig.WriterID, err)
	}

	// Extract resources from builder
	consumer := builder.GetQueueConsumer(queueName)
	cleanupConsumer := builder.GetExchangeConsumer("userid-cleanup-exchange")
	queueProducer := builder.GetQueueProducer(queueName)
	completionProducer := builder.GetQueueProducer(queues.UserPartitionCompletionQueue)

	if consumer == nil || cleanupConsumer == nil || queueProducer == nil || completionProducer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get resources from builder"))
	}

	// Extract MessageManager from builder
	messageManager := builder.GetResourceTracker().Get(
		worker_builder.ResourceTypeMessageManager,
		"message-manager",
	)
	if messageManager == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get message manager from builder"))
	}
	mm, ok := messageManager.(*messagemanager.MessageManager)
	if !ok {
		return nil, builder.CleanupOnError(fmt.Errorf("message manager has wrong type"))
	}

	// Add CompletionCleaner with MessageManager as cleanup handler
	// Use WORKER_ID from environment (service name) for cleanup queue name
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return nil, builder.CleanupOnError(fmt.Errorf("WORKER_ID environment variable is required"))
	}
	builder.WithCompletionCleaner(
		queues.ClientCompletionCleanupExchange,
		workerID,
		[]completioncleaner.CleanupHandler{mm},
	)

	// Extract PartitionManager from builder
	partitionManager := builder.GetPartitionManager()
	if partitionManager == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get partition manager from builder"))
	}

	fmt.Printf("User Partition Writer %d: PartitionManager initialized with %d partitions\n",
		writerConfig.WriterID, NumPartitions)

	return &UserPartitionWriter{
		consumer:            consumer,
		cleanupConsumer:     cleanupConsumer,
		queueProducer:       queueProducer,
		completionProducer:  completionProducer,
		config:              connConfig,
		writerConfig:        writerConfig,
		partitionsWritten:   make(map[int]int),
		stoppedClients:      make(map[string]bool),
		messageManager:      mm,
		partitionManager:    partitionManager,
		firstChunkProcessed: false,
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
	if upw.messageManager != nil {
		upw.messageManager.Close()
	}
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
			// Get message type in the callback (middleware layer)
			msgType, err := deserializer.GetMessageType(delivery.Body)
			if err != nil {
				fmt.Printf("User Partition Writer %d: Failed to get message type: %v\n", upw.writerConfig.WriterID, err)
				delivery.Ack(false)
				continue
			}

			// Route based on message type
			switch msgType {
			case common.ChunkMessageType: // Type 2
				// Deserialize the chunk message
				chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
				if err != nil {
					fmt.Printf("User Partition Writer %d: Failed to deserialize chunk: %v\n",
						upw.writerConfig.WriterID, err)
					delivery.Ack(false)
					continue
				}

				if err := upw.processMessage(chunkMsg); err != 0 {
					fmt.Printf("User Partition Writer %d: Failed to process message: %v\n",
						upw.writerConfig.WriterID, err)
					delivery.Nack(false, true) // Reject and requeue
					continue
				}
				delivery.Ack(false)
			default:
				fmt.Printf("User Partition Writer %d: Unknown message type: %d\n", upw.writerConfig.WriterID, msgType)
				delivery.Ack(false)
			}
		}
		done <- nil
	}
}

func (upw *UserPartitionWriter) processMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Check if chunk was already processed
	if upw.messageManager.IsProcessed(chunkMsg.ClientID, chunkMsg.ID) {
		fmt.Printf("User Partition Writer %d: Chunk %s already processed, skipping\n",
			upw.writerConfig.WriterID, chunkMsg.ID)
		return 0
	}

	// Check if client is stopped (should drop this chunk)
	upw.clientMutex.RLock()
	isStopped := upw.stoppedClients[chunkMsg.ClientID]
	upw.clientMutex.RUnlock()

	if isStopped {
		fmt.Printf("User Partition Writer %d: Dropping chunk for stopped client %s\n", upw.writerConfig.WriterID, chunkMsg.ClientID)
		return 0 // Successfully dropped
	}

	fmt.Printf("User Partition Writer %d: Processing chunk - ChunkNumber: %d, Size: %d, IsLastChunk: %t\n",
		upw.writerConfig.WriterID, chunkMsg.ChunkNumber, chunkMsg.ChunkSize, chunkMsg.IsLastChunk)

	// Write users to partitions
	if err := upw.writeUsersToPartitions(chunkMsg.ChunkData, chunkMsg.ClientID, chunkMsg.ID); err != nil {
		fmt.Printf("User Partition Writer %d: Failed to write users: %v\n",
			upw.writerConfig.WriterID, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send chunk to orchestrator for completion tracking ONLY after file is synced
	upw.sendChunkToOrchestrator(chunkMsg)

	// Mark chunk as processed after successful write
	if err := upw.messageManager.MarkProcessed(chunkMsg.ClientID, chunkMsg.ID); err != nil {
		fmt.Printf("User Partition Writer %d: Failed to mark chunk as processed: %v\n",
			upw.writerConfig.WriterID, err)
		return middleware.MessageMiddlewareMessageError
	}

	return 0
}

// writeUsersToPartitions writes users to their respective partition files
func (upw *UserPartitionWriter) writeUsersToPartitions(csvData string, clientID string, chunkID string) error {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) < 1 {
		return fmt.Errorf("no data in chunk")
	}

	// Group records by partition
	partitionRecords := make(map[int][][]string)
	skippedCount := 0

	for i := 0; i < len(records); i++ {
		record := records[i]
		if strings.Contains(record[0], "user_id") {
			continue // Skip header
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
			fmt.Printf("User Partition Writer %d: WARNING - Received user for partition %d which is not owned by this writer\n",
				upw.writerConfig.WriterID, partition)
			skippedCount++
			continue
		}

		partitionRecords[partition] = append(partitionRecords[partition], record)
	}

	if skippedCount > 0 {
		fmt.Printf("User Partition Writer %d: WARNING - Skipped %d users from wrong partitions\n",
			upw.writerConfig.WriterID, skippedCount)
	}

	// Write each partition using PartitionManager
	opts := partitionmanager.WriteOptions{
		FilePrefix: "users-partition",
		Header:     []string{"user_id", "gender", "birthdate", "registered_at"},
		ClientID:   clientID,
		DebugMode:  false,
	}

	userCount := 0
	for partition, records := range partitionRecords {
		// Convert records to CSV lines format
		lines := make([]string, 0, len(records))
		for _, record := range records {
			csvLine := strings.Join(record, ",") + "\n"
			lines = append(lines, csvLine)
		}

		partitionData := partitionmanager.PartitionData{
			Number: partition,
			Lines:  lines,
		}

		// Check if this is the first chunk after restart
		if !upw.firstChunkProcessed {
			// First chunk after restart - check for duplicates/incomplete writes
			filePath := upw.partitionManager.GetPartitionFilePath(opts, partition)
			linesCount := len(lines) * 2 // To avoid duplicates we leave some space for the last lines
			lastLines, err := upw.partitionManager.GetLastLines(filePath, linesCount)
			if err != nil {
				return fmt.Errorf("failed to get last lines for partition %d: %w", partition, err)
			}

			if err := upw.partitionManager.WriteOnlyMissingLines(filePath, lastLines, lines, opts); err != nil {
				return fmt.Errorf("failed to write missing lines to partition %d: %w", partition, err)
			}

			fmt.Printf("User Partition Writer %d: Wrote missing lines to partition %d from chunk %s (first chunk after restart)\n",
				upw.writerConfig.WriterID, partition, chunkID)
		} else {
			// Normal write (WritePartition handles incomplete writes automatically)
			if err := upw.partitionManager.WritePartition(partitionData, opts); err != nil {
				return fmt.Errorf("failed to write partition %d: %w", partition, err)
			}

			fmt.Printf("User Partition Writer %d: Wrote partition %d from chunk %s\n",
				upw.writerConfig.WriterID, partition, chunkID)
		}

		upw.partitionsWritten[partition] += len(records)
		userCount += len(records)
	}

	// After processing the first chunk, mark it as processed
	if !upw.firstChunkProcessed {
		upw.firstChunkProcessed = true
		fmt.Printf("User Partition Writer %d: First chunk processed, switching to normal write mode\n",
			upw.writerConfig.WriterID)
	}

	fmt.Printf("User Partition Writer %d: Wrote %d users across %d partitions\n",
		upw.writerConfig.WriterID, userCount, len(partitionRecords))

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
