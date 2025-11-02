package main

import (
	"fmt"
	"os"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	joinchunk "github.com/tp-distribuidos-2c2025/workers/join/shared/chunk"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/buffer"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/file"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/parser"
)

const (
	MaxClientsInBufferWorker = 1000 // Maximum number of clients to buffer
)

// JoinByUserIdWorker handles joining top users data with user data from CSV files
type JoinByUserIdWorker struct {
	topUsersConsumer   *workerqueue.QueueConsumer
	completionConsumer *exchange.ExchangeConsumer
	producer           *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig
	workerID           string

	// Shared components
	bufferManager    *buffer.Manager
	partitionManager *file.PartitionManager
	chunkSender      *joinchunk.Sender
	stopChan         chan struct{}
}

// NewJoinByUserIdWorker creates a new JoinByUserIdWorker instance
func NewJoinByUserIdWorker(config *middleware.ConnectionConfig) (*JoinByUserIdWorker, error) {
	// Create consumer for user ID chunks
	topUsersConsumer := workerqueue.NewQueueConsumer(
		UserIdChunkQueue,
		config,
	)
	if topUsersConsumer == nil {
		return nil, fmt.Errorf("failed to create user ID chunk consumer")
	}

	// Declare the user ID chunk queue
	userChunkDeclarer := workerqueue.NewMessageMiddlewareQueue(
		UserIdChunkQueue,
		config,
	)
	if userChunkDeclarer == nil {
		topUsersConsumer.Close()
		return nil, fmt.Errorf("failed to create user chunk queue declarer")
	}
	if err := userChunkDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		topUsersConsumer.Close()
		userChunkDeclarer.Close()
		return nil, fmt.Errorf("failed to declare user ID chunk queue: %v", err)
	}
	userChunkDeclarer.Close()

	// Create completion signal consumer
	completionConsumer := exchange.NewExchangeConsumer(
		queues.UserIdCompletionExchange,
		[]string{queues.UserIdCompletionRoutingKey},
		config,
	)
	if completionConsumer == nil {
		topUsersConsumer.Close()
		return nil, fmt.Errorf("failed to create completion signal consumer")
	}

	// Create producer for query 4 results
	producer := workerqueue.NewMessageMiddlewareQueue(
		Query4ResultsQueue,
		config,
	)
	if producer == nil {
		topUsersConsumer.Close()
		completionConsumer.Close()
		return nil, fmt.Errorf("failed to create query 4 results producer")
	}

	// Declare producer queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		topUsersConsumer.Close()
		completionConsumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to declare query 4 results queue: %v", err)
	}

	// Generate worker ID
	instanceID := os.Getenv("WORKER_INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}
	workerID := fmt.Sprintf("userid-reader-%s", instanceID)

	// Initialize shared components
	bufferManager := buffer.NewManager(MaxClientsInBufferWorker)
	partitionManager := file.NewPartitionManager(SharedDataDir, NumPartitions)
	chunkSender := joinchunk.NewSender(producer)

	return &JoinByUserIdWorker{
		topUsersConsumer:   topUsersConsumer,
		completionConsumer: completionConsumer,
		producer:           producer,
		config:             config,
		workerID:           workerID,
		bufferManager:      bufferManager,
		partitionManager:   partitionManager,
		chunkSender:        chunkSender,
		stopChan:           make(chan struct{}),
	}, nil
}

// Start starts the join by user ID worker
func (jw *JoinByUserIdWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Join by User ID Worker: Starting...")

	// Start completion signal consumer
	go jw.startCompletionSignalConsumer()

	// Start consuming top users messages
	fmt.Println("Join by User ID Worker: Starting to listen for top users chunks...")
	return jw.topUsersConsumer.StartConsuming(jw.createTopUsersCallback())
}

// Close closes all connections
func (jw *JoinByUserIdWorker) Close() {
	fmt.Println("Join by User ID Worker: Shutting down...")

	if jw.stopChan != nil {
		close(jw.stopChan)
	}

	if jw.topUsersConsumer != nil {
		jw.topUsersConsumer.Close()
	}
	if jw.completionConsumer != nil {
		jw.completionConsumer.Close()
	}
	if jw.producer != nil {
		jw.producer.Close()
	}

	fmt.Println("Join by User ID Worker: Shutdown complete")
}

// createTopUsersCallback creates the top users message processing callback
func (jw *JoinByUserIdWorker) createTopUsersCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			if err := jw.processTopUsersMessage(delivery); err != 0 {
				fmt.Printf("Join by User ID Worker: Error processing top users message: %v\n", err)
			}
		}
		done <- nil
	}
}

// startCompletionSignalConsumer starts the completion signal consumer
func (jw *JoinByUserIdWorker) startCompletionSignalConsumer() {
	fmt.Println("Join by User ID Worker: Starting completion signal consumer...")

	err := jw.completionConsumer.StartConsuming(func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			if err := jw.processCompletionSignal(delivery); err != 0 {
				fmt.Printf("Join by User ID Worker: Error processing completion signal: %v\n", err)
			}
		}
		done <- nil
	})
	if err != 0 {
		fmt.Printf("Join by User ID Worker: Failed to start completion signal consumer: %v\n", err)
	}
}

// processTopUsersMessage processes a single top users chunk with buffering
func (jw *JoinByUserIdWorker) processTopUsersMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Join by User ID Worker: Failed to deserialize chunk message: %v\n", err)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Join by User ID Worker: Received chunk - ClientID: %s, ChunkNumber: %d, IsLastChunk: %t\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk)

	// Parse top users data
	topUsers, parseErr := parser.ParseTopUsersData(chunkMsg.ChunkData)
	if parseErr != nil {
		fmt.Printf("Join by User ID Worker: Failed to parse top users data: %v\n", parseErr)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	// Get or create buffer
	buffer, created := jw.bufferManager.GetOrCreateBuffer(chunkMsg.ClientID)
	if !created {
		if buffer == nil {
			fmt.Printf("Join by User ID Worker: Buffer full (%d clients), NACKing message for client %s\n",
				MaxClientsInBufferWorker, chunkMsg.ClientID)
			delivery.Nack(false, false) // NACK and requeue
			return middleware.MessageMiddlewareMessageError
		}
	}

	// Add chunk and records to buffer
	if err := jw.bufferManager.AddChunk(chunkMsg.ClientID, chunkMsg); err != nil {
		fmt.Printf("Join by User ID Worker: Failed to add chunk to buffer: %v\n", err)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

		// Convert TopUserRecord to interface{} for buffer
		recordInterfaces := make([]interface{}, len(topUsers))
		for i, tu := range topUsers {
			recordInterfaces[i] = tu
		}
		if err := jw.bufferManager.AddRecords(chunkMsg.ClientID, recordInterfaces); err != nil {
		fmt.Printf("Join by User ID Worker: Failed to add records to buffer: %v\n", err)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Join by User ID Worker: Buffered chunk for client %s (chunks: %d, users: %d)\n",
		chunkMsg.ClientID, len(buffer.Chunks), len(buffer.Records))

	// Check if last chunk
	if chunkMsg.IsLastChunk {
		fmt.Printf("Join by User ID Worker: Client %s last chunk received with %d buffered users\n",
			chunkMsg.ClientID, len(buffer.Records))

		// Mark chunk data as received
		jw.bufferManager.MarkChunkDataReceived(chunkMsg.ClientID)

		// Check if ready to join
		jw.checkAndJoin(chunkMsg.ClientID)
	}

	delivery.Ack(false)
	return 0
}

// processCompletionSignal processes a completion signal from the orchestrator
func (jw *JoinByUserIdWorker) processCompletionSignal(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the completion signal
	completionSignal, err := signals.DeserializeJoinCompletionSignal(delivery.Body)
	if err != nil {
		fmt.Printf("Join by User ID Worker: Failed to deserialize completion signal: %v\n", err)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Join by User ID Worker: Received completion signal for client %s\n", completionSignal.ClientID)

	// Ensure buffer exists
	buffer, _ := jw.bufferManager.GetOrCreateBuffer(completionSignal.ClientID)
	if buffer == nil {
		fmt.Printf("Join by User ID Worker: Failed to create buffer for client %s\n", completionSignal.ClientID)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	// Mark completion signal as received
	jw.bufferManager.MarkCompletionSignalReceived(completionSignal.ClientID)

	// Check if ready to join
	jw.checkAndJoin(completionSignal.ClientID)

	delivery.Ack(false)
	return 0
}

// checkAndJoin checks if both completion signal and chunk data are ready, then performs join if both are ready
func (jw *JoinByUserIdWorker) checkAndJoin(clientID string) {
	if !jw.bufferManager.IsReady(clientID) {
		return
	}

	// Mark as processed to prevent duplicate joins
	if !jw.bufferManager.MarkProcessed(clientID) {
		fmt.Printf("Join by User ID Worker: Client %s already processed, skipping\n", clientID)
		return
	}

	fmt.Printf("Join by User ID Worker: Both conditions met for client %s, performing join\n", clientID)

	// Perform the join
	jw.processClientIfReady(clientID)
}

// processClientIfReady processes a client when completion signal is received
func (jw *JoinByUserIdWorker) processClientIfReady(clientID string) {
	records := jw.bufferManager.GetRecords(clientID)
	if records == nil {
		fmt.Printf("Join by User ID Worker: No records found for client %s\n", clientID)
		return
	}

	fmt.Printf("Join by User ID Worker: Processing client %s with %d buffered users\n",
		clientID, len(records))

	// Convert records to TopUserRecord
	topUsers := make([]parser.TopUserRecord, len(records))
	for i, r := range records {
		if tu, ok := r.(parser.TopUserRecord); ok {
			topUsers[i] = tu
		}
	}

	// Send joined chunk
	if err := jw.sendJoinedChunk(clientID, topUsers, true); err != 0 {
		fmt.Printf("Join by User ID Worker: Failed to send joined chunk for client %s: %v\n", clientID, err)
	} else {
		fmt.Printf("Join by User ID Worker: Sent complete joined data for client %s (%d users)\n",
			clientID, len(topUsers))
	}

	// Cleanup
	jw.bufferManager.RemoveBuffer(clientID)
	jw.performCleanup(clientID)
}

// sendJoinedChunk sends a chunk with joined user data
func (jw *JoinByUserIdWorker) sendJoinedChunk(clientID string, topUsers []parser.TopUserRecord, isLast bool) middleware.MessageMiddlewareError {
	// Build CSV data with joined users
	var csvBuilder strings.Builder
	csvBuilder.WriteString("user_id,store_id,purchase_count,rank,gender,birthdate,registered_at\n")

	successfulJoins := 0
	failedJoins := 0

	for _, topUser := range topUsers {
		userData, err := jw.lookupUserFromFile(topUser.UserID, clientID)
		if err != nil || userData == nil {
			failedJoins++
			fmt.Printf("Join by User ID Worker: Failed to lookup user %s for client %s: %v\n",
				topUser.UserID, clientID, err)
			continue
		}

		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s\n",
			topUser.UserID,
			topUser.StoreID,
			topUser.PurchaseCount,
			topUser.Rank,
			userData["gender"],
			userData["birthdate"],
			userData["registered_at"],
		))
		successfulJoins++
	}

	fmt.Printf("Join by User ID Worker: Client %s - Successful joins: %d, Failed joins: %d, Total users: %d\n",
		clientID, successfulJoins, failedJoins, len(topUsers))

	csvData := csvBuilder.String()

	// Get metadata from first chunk
	chunkMetadata := jw.bufferManager.GetFirstChunkMetadata(clientID)
	if chunkMetadata == nil {
		return middleware.MessageMiddlewareMessageError
	}

	// Get chunk counter
	chunkCounter := jw.bufferManager.IncrementChunkCounter(clientID)

	// Send chunk
	return jw.chunkSender.SendFromMetadata(
		chunkMetadata,
		chunkCounter,
		isLast,
		len(csvData),
		csvData,
	)
}

// lookupUserFromFile looks up user data from the appropriate partition file
func (jw *JoinByUserIdWorker) lookupUserFromFile(userID string, clientID string) (map[string]string, error) {
	userParser := func(record []string) (map[string]string, error) {
		return parser.ParseUserRecord(record)
	}

	return jw.partitionManager.LookupEntity(clientID, userID, userParser)
}

// performCleanup performs cleanup operations for partition files
func (jw *JoinByUserIdWorker) performCleanup(clientID string) {
	fmt.Printf("Join by User ID Worker: Performing cleanup for client: %s\n", clientID)

	if err := jw.partitionManager.CleanupClientFiles(clientID); err != nil {
		fmt.Printf("Join by User ID Worker: Error during cleanup for client %s: %v\n", clientID, err)
	} else {
		fmt.Printf("Join by User ID Worker: Completed cleanup for client: %s\n", clientID)
	}
}
