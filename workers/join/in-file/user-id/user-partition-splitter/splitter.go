package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
)

// UserRecord represents a single user row
type UserRecord struct {
	UserID       string
	Gender       string
	Birthdate    string
	RegisteredAt string
}

// UserPartitionSplitter splits user data across multiple writers
type UserPartitionSplitter struct {
	consumer       *workerqueue.QueueConsumer
	writerQueues   []*workerqueue.QueueMiddleware
	config         *middleware.ConnectionConfig
	splitterConfig *Config
	buffers        [][]UserRecord // One buffer per writer
	messageManager *messagemanager.MessageManager
}

// NewUserPartitionSplitter creates a new splitter instance
func NewUserPartitionSplitter(connConfig *middleware.ConnectionConfig, splitterConfig *Config) (*UserPartitionSplitter, error) {
	// Use builder to create all resources
	builder := worker_builder.NewWorkerBuilder("User Partition Splitter").
		WithConfig(connConfig).
		// Queue consumer
		WithQueueConsumer(JoinUserIdDictionaryQueue, true)

	// Add queue producers for each writer
	for i := 0; i < splitterConfig.NumWriters; i++ {
		queueName := GetWriterQueueName(i + 1) // Writers are 1-indexed
		builder = builder.WithQueueProducer(queueName, true)
		fmt.Printf("User Partition Splitter: Adding queue %s for writer %d\n", queueName, i+1)
	}

	// Add state management
	builder = builder.
		WithStandardStateDirectory("/app/worker-data").
		WithMessageManager("/app/worker-data/processed-ids.txt")

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract consumer from builder
	consumer := builder.GetQueueConsumer(JoinUserIdDictionaryQueue)
	if consumer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get consumer from builder"))
	}

	// Extract writer queue producers from builder
	writerQueues := make([]*workerqueue.QueueMiddleware, splitterConfig.NumWriters)
	for i := 0; i < splitterConfig.NumWriters; i++ {
		queueName := GetWriterQueueName(i + 1)
		producer := builder.GetQueueProducer(queueName)
		if producer == nil {
			return nil, builder.CleanupOnError(fmt.Errorf("failed to get producer for writer %d", i+1))
		}
		writerQueues[i] = producer
		fmt.Printf("User Partition Splitter: Created queue %s for writer %d\n", queueName, i+1)
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

	// Initialize buffers
	buffers := make([][]UserRecord, splitterConfig.NumWriters)
	for i := range buffers {
		buffers[i] = make([]UserRecord, 0, MaxBufferSize)
	}

	return &UserPartitionSplitter{
		consumer:       consumer,
		writerQueues:   writerQueues,
		config:         connConfig,
		splitterConfig: splitterConfig,
		buffers:        buffers,
		messageManager: mm,
	}, nil
}

// Start starts the splitter
func (ups *UserPartitionSplitter) Start() middleware.MessageMiddlewareError {
	fmt.Println("User Partition Splitter: Starting to listen for user data...")
	return ups.consumer.StartConsuming(ups.createCallback())
}

// Close closes all connections
func (ups *UserPartitionSplitter) Close() {
	if ups.messageManager != nil {
		ups.messageManager.Close()
	}
	if ups.consumer != nil {
		ups.consumer.Close()
	}
	for _, producer := range ups.writerQueues {
		if producer != nil {
			producer.Close()
		}
	}
}

// createCallback creates the message processing callback
func (ups *UserPartitionSplitter) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			
			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				fmt.Printf("User Partition Splitter: Failed to deserialize chunk: %v\n", err)
				delivery.Ack(false)
				continue
			}

			if err := ups.processMessage(chunkMsg); err != 0 {
				fmt.Printf("User Partition Splitter: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// processMessage processes a single user chunk
func (ups *UserPartitionSplitter) processMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Check if chunk was already processed
	if ups.messageManager.IsProcessed(chunkMsg.ID) {
		fmt.Printf("User Partition Splitter: Chunk %s already processed, skipping\n", chunkMsg.ID)
		return 0
	}

	// Only process user files (US prefix)
	if !strings.HasPrefix(chunkMsg.FileID, "US") {
		fmt.Printf("User Partition Splitter: Ignoring non-user file: %s\n", chunkMsg.FileID)
		return 0
	}

	fmt.Printf("User Partition Splitter: Processing user chunk - FileID: %s, ChunkNumber: %d, IsLastChunk: %t\n",
		chunkMsg.FileID, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk)

	// Parse users and distribute to buffers
	if err := ups.distributeUsers(chunkMsg); err != nil {
		fmt.Printf("User Partition Splitter: Failed to distribute users: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Mark chunk as processed after successful distribution
	if err := ups.messageManager.MarkProcessed(chunkMsg.ID); err != nil {
		fmt.Printf("User Partition Splitter: Failed to mark chunk as processed: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	return 0
}

// distributeUsers parses users and routes them to appropriate writer buffers
func (ups *UserPartitionSplitter) distributeUsers(chunkMsg *chunk.Chunk) error {
	reader := csv.NewReader(strings.NewReader(chunkMsg.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) < 1 {
		return fmt.Errorf("no data in chunk")
	}

	userCount := 0
	routingStats := make(map[int]int) // Track users per writer

	for i := 0; i < len(records); i++ {
		record := records[i]
		if strings.Contains(record[0], "user_id") {
			continue
		}
		if len(record) < 4 {
			fmt.Printf("User Partition Splitter: Skipping malformed record: %v\n", record)
			continue
		}

		user := UserRecord{
			UserID:       record[0],
			Gender:       record[1],
			Birthdate:    record[2],
			RegisteredAt: record[3],
		}

		// Calculate partition (same logic as reader)
		partition, err := getUserPartition(user.UserID)
		if err != nil {
			fmt.Printf("User Partition Splitter: Failed to get partition for user %s: %v\n", user.UserID, err)
			continue
		}

		// Route to writer via modulo
		writerID := partition % ups.splitterConfig.NumWriters
		ups.buffers[writerID] = append(ups.buffers[writerID], user)
		routingStats[writerID]++
		userCount++
	}
	for i := 0; i < ups.splitterConfig.NumWriters; i++ {
		if err := ups.flushBuffer(i, chunkMsg); err != 0 {
			return fmt.Errorf("failed to flush buffer for writer %d: %v", i, err)
		}
	}

	fmt.Printf("User Partition Splitter: Distributed %d users across %d writers: %v\n",
		userCount, ups.splitterConfig.NumWriters, routingStats)

	return nil
}

// flushBuffer sends buffered users to a specific writer
func (ups *UserPartitionSplitter) flushBuffer(writerID int, originalChunk *chunk.Chunk) middleware.MessageMiddlewareError {

	// Convert buffer to CSV
	csvData := ups.bufferToCSV(writerID)

	// Create chunk for writer
	writerChunk := chunk.NewChunk(
		originalChunk.ClientID,
		originalChunk.FileID,
		originalChunk.QueryType,
		(originalChunk.ChunkNumber-1)*ups.splitterConfig.NumWriters+(writerID+1),
		originalChunk.IsLastChunk && writerID == ups.splitterConfig.NumWriters-1,
		originalChunk.IsLastFromTable && writerID == ups.splitterConfig.NumWriters-1,
		len(ups.buffers[writerID]),
		originalChunk.TableID,
		csvData,
	)

	// Serialize and send
	chunkMessage := chunk.NewChunkMessage(writerChunk)
	messageData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("User Partition Splitter: Failed to serialize chunk for writer %d: %v\n", writerID+1, err)
		return middleware.MessageMiddlewareMessageError
	}

	if err := ups.writerQueues[writerID].Send(messageData); err != 0 {
		fmt.Printf("User Partition Splitter: Failed to send to writer %d: %v\n", writerID+1, err)
		return err
	}

	fmt.Printf("User Partition Splitter: Flushed %d users to writer %d (IsLastChunk=%t), ChunkNumber=%d\n",
		len(ups.buffers[writerID]), writerID+1, writerChunk.IsLastChunk, writerChunk.ChunkNumber)

	// Clear buffer
	ups.buffers[writerID] = ups.buffers[writerID][:0]

	return 0
}

// bufferToCSV converts a writer buffer to CSV format
func (ups *UserPartitionSplitter) bufferToCSV(writerID int) string {
	var result strings.Builder

	// Write header
	result.WriteString("user_id,gender,birthdate,registered_at\n")

	// Write records
	for _, user := range ups.buffers[writerID] {
		result.WriteString(fmt.Sprintf("%s,%s,%s,%s\n",
			user.UserID, user.Gender, user.Birthdate, user.RegisteredAt))
	}

	return result.String()
}

// getUserPartition calculates the partition for a user ID (must match reader logic)
func getUserPartition(userID string) (int, error) {
	// Parse user ID (handle both int and float formats)
	userIDFloat, err := strconv.ParseFloat(userID, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid user ID %s: %w", userID, err)
	}
	userIDInt := int(userIDFloat)

	// Simple modulo partitioning (must match reader logic exactly!)
	return userIDInt % NumPartitions, nil
}
