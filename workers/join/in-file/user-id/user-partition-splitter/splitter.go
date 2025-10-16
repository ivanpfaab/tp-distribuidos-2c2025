package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
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
}

// NewUserPartitionSplitter creates a new splitter instance
func NewUserPartitionSplitter(connConfig *middleware.ConnectionConfig, splitterConfig *Config) (*UserPartitionSplitter, error) {
	// Create consumer for user data from join-data-handler (consume from user ID dictionary queue)
	consumer := workerqueue.NewQueueConsumer(
		JoinUserIdDictionaryQueue,
		connConfig,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create consumer")
	}

	// Declare the input queue before consuming
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(JoinUserIdDictionaryQueue, connConfig)
	if queueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}

	// Declare the queue
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare fixed join data queue: %v", err)
	}
	queueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create producer queues for each writer
	writerQueues := make([]*workerqueue.QueueMiddleware, splitterConfig.NumWriters)
	for i := 0; i < splitterConfig.NumWriters; i++ {
		queueName := GetWriterQueueName(i + 1) // Writers are 1-indexed
		producer := workerqueue.NewMessageMiddlewareQueue(queueName, connConfig)
		if producer == nil {
			// Clean up already created producers
			for j := 0; j < i; j++ {
				writerQueues[j].Close()
			}
			consumer.Close()
			return nil, fmt.Errorf("failed to create producer for writer %d", i+1)
		}

		// Declare the queue
		if err := producer.DeclareQueue(false, false, false, false); err != 0 {
			// Clean up
			producer.Close()
			for j := 0; j < i; j++ {
				writerQueues[j].Close()
			}
			consumer.Close()
			return nil, fmt.Errorf("failed to declare queue for writer %d: %v", i+1, err)
		}

		writerQueues[i] = producer
		fmt.Printf("User Partition Splitter: Created queue %s for writer %d\n", queueName, i+1)
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
	}, nil
}

// Start starts the splitter
func (ups *UserPartitionSplitter) Start() middleware.MessageMiddlewareError {
	fmt.Println("User Partition Splitter: Starting to listen for user data...")
	return ups.consumer.StartConsuming(ups.createCallback())
}

// Close closes all connections
func (ups *UserPartitionSplitter) Close() {
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
			if err := ups.processMessage(delivery); err != 0 {
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
func (ups *UserPartitionSplitter) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("User Partition Splitter: Failed to deserialize chunk: %v\n", err)
		return middleware.MessageMiddlewareMessageError
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

	// If this is the last chunk, flush all buffers
	if chunkMsg.IsLastChunk {
		fmt.Printf("User Partition Splitter: Last chunk received, flushing all buffers\n")
		if err := ups.flushAllBuffers(chunkMsg); err != 0 {
			return err
		}
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

		// Flush buffer if full
		if len(ups.buffers[writerID]) >= MaxBufferSize {
			if err := ups.flushBuffer(writerID, chunkMsg, false); err != 0 {
				return fmt.Errorf("failed to flush buffer for writer %d: %v", writerID, err)
			}
		}
	}

	fmt.Printf("User Partition Splitter: Distributed %d users across %d writers: %v\n",
		userCount, ups.splitterConfig.NumWriters, routingStats)

	return nil
}

// flushBuffer sends buffered users to a specific writer
func (ups *UserPartitionSplitter) flushBuffer(writerID int, originalChunk *chunk.Chunk, isLastChunk bool) middleware.MessageMiddlewareError {
	if len(ups.buffers[writerID]) == 0 {
		// Empty buffer, just send EOS if needed
		if isLastChunk {
			return ups.sendEOSToWriter(writerID, originalChunk)
		}
		return 0
	}

	// Convert buffer to CSV
	csvData := ups.bufferToCSV(writerID)

	// Create chunk for writer
	writerChunk := &chunk.Chunk{
		ClientID:    originalChunk.ClientID,
		FileID:      originalChunk.FileID,
		QueryType:   originalChunk.QueryType,
		ChunkNumber: originalChunk.ChunkNumber,
		IsLastChunk: isLastChunk,
		Step:        originalChunk.Step,
		ChunkSize:   len(ups.buffers[writerID]),
		TableID:     originalChunk.TableID,
		ChunkData:   csvData,
	}

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

	fmt.Printf("User Partition Splitter: Flushed %d users to writer %d (IsLastChunk=%t)\n",
		len(ups.buffers[writerID]), writerID+1, isLastChunk)

	// Clear buffer
	ups.buffers[writerID] = ups.buffers[writerID][:0]

	return 0
}

// flushAllBuffers flushes all writer buffers
func (ups *UserPartitionSplitter) flushAllBuffers(originalChunk *chunk.Chunk) middleware.MessageMiddlewareError {
	for writerID := 0; writerID < ups.splitterConfig.NumWriters; writerID++ {
		if err := ups.flushBuffer(writerID, originalChunk, true); err != 0 {
			return err
		}
	}
	return 0
}

// sendEOSToWriter sends an end-of-stream marker to a writer
func (ups *UserPartitionSplitter) sendEOSToWriter(writerID int, originalChunk *chunk.Chunk) middleware.MessageMiddlewareError {
	eosChunk := &chunk.Chunk{
		ClientID:    originalChunk.ClientID,
		FileID:      originalChunk.FileID,
		QueryType:   originalChunk.QueryType,
		ChunkNumber: -1, // EOS marker
		IsLastChunk: true,
		Step:        originalChunk.Step,
		ChunkSize:   0,
		TableID:     originalChunk.TableID,
		ChunkData:   "",
	}

	chunkMessage := chunk.NewChunkMessage(eosChunk)
	messageData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		return middleware.MessageMiddlewareMessageError
	}

	return ups.writerQueues[writerID].Send(messageData)
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
