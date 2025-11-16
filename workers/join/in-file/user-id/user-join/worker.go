package main

import (
	"fmt"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	joinchunk "github.com/tp-distribuidos-2c2025/workers/join/shared/chunk"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/file"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/parser"
)

// JoinByUserIdWorker handles joining top users data with user data from CSV files
type JoinByUserIdWorker struct {
	topUsersConsumer   *exchange.ExchangeConsumer
	completionConsumer *exchange.ExchangeConsumer
	producer           *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig
	readerConfig       *Config
	workerID           string

	// Completion tracking (no buffering needed)
	completionSignals map[string]bool // clientID -> completion received
	completionMutex   sync.RWMutex

	// Shared components
	partitionManager *file.PartitionManager
	chunkSender      *joinchunk.Sender
	stopChan         chan struct{}
}

// NewJoinByUserIdWorker creates a new JoinByUserIdWorker instance
func NewJoinByUserIdWorker(config *middleware.ConnectionConfig, readerConfig *Config) (*JoinByUserIdWorker, error) {
	// Create exchange consumer for user ID chunks (with reader-specific routing key)
	routingKey := queues.GetUserIdJoinRoutingKey(readerConfig.ReaderID)
	topUsersConsumer := exchange.NewExchangeConsumer(
		queues.UserIdJoinChunksExchange,
		[]string{routingKey},
		config,
	)
	if topUsersConsumer == nil {
		return nil, fmt.Errorf("failed to create user ID chunk exchange consumer")
	}

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

	// Generate worker ID from reader config
	workerID := fmt.Sprintf("userid-reader-%d", readerConfig.ReaderID)

	// Initialize shared components
	partitionManager := file.NewPartitionManager(SharedDataDir, NumPartitions)
	chunkSender := joinchunk.NewSender(producer)

	return &JoinByUserIdWorker{
		topUsersConsumer:   topUsersConsumer,
		completionConsumer: completionConsumer,
		producer:           producer,
		config:             config,
		readerConfig:       readerConfig,
		workerID:           workerID,
		completionSignals:  make(map[string]bool),
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

// processTopUsersMessage processes a single top users chunk with NACK/requeue pattern
func (jw *JoinByUserIdWorker) processTopUsersMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Join by User ID Worker: Failed to deserialize chunk message: %v\n", err)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Join by User ID Worker: Received chunk - ClientID: %s, ChunkNumber: %d (reader %d), IsLastChunk: %t\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk)

	// Check if completion signal has been received for this client
	jw.completionMutex.RLock()
	completionReceived := jw.completionSignals[chunkMsg.ClientID]
	jw.completionMutex.RUnlock()

	if !completionReceived {
		// Completion signal not received yet, NACK and requeue
		fmt.Printf("Join by User ID Worker: Completion signal not received for client %s, NACKing and requeuing chunk %d\n",
			chunkMsg.ClientID, chunkMsg.ChunkNumber)
		delivery.Nack(false, true) // Requeue
		return middleware.MessageMiddlewareMessageError
	}

	// Completion signal received, process chunk immediately
	fmt.Printf("Join by User ID Worker: Processing chunk %d for client %s (completion signal received)\n",
		chunkMsg.ChunkNumber, chunkMsg.ClientID)

	// Parse top users data
	topUsers, parseErr := parser.ParseTopUsersData(chunkMsg.ChunkData)
	if parseErr != nil {
		fmt.Printf("Join by User ID Worker: Failed to parse top users data: %v\n", parseErr)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	// Process and send joined chunk
	if err := jw.processAndSendChunk(chunkMsg, topUsers); err != 0 {
		fmt.Printf("Join by User ID Worker: Failed to process chunk: %v\n", err)
		delivery.Ack(false) // Still ACK to avoid infinite requeue loop
		return err
	}

	// Cleanup when last chunk is processed
	if chunkMsg.IsLastChunk {
		fmt.Printf("Join by User ID Worker: Last chunk processed for client %s, performing cleanup\n", chunkMsg.ClientID)
		jw.performCleanup(chunkMsg.ClientID)

		// Remove completion signal tracking for this client
		jw.completionMutex.Lock()
		delete(jw.completionSignals, chunkMsg.ClientID)
		jw.completionMutex.Unlock()
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

	// Mark completion signal as received
	jw.completionMutex.Lock()
	jw.completionSignals[completionSignal.ClientID] = true
	jw.completionMutex.Unlock()

	// Chunks will be processed when they arrive (or re-arrive after requeue)
	fmt.Printf("Join by User ID Worker: Client %s marked as ready, chunks can now be processed\n", completionSignal.ClientID)

	delivery.Ack(false)
	return 0
}

// processAndSendChunk processes a chunk and sends the joined result
func (jw *JoinByUserIdWorker) processAndSendChunk(chunkMsg *chunk.Chunk, topUsers []parser.TopUserRecord) middleware.MessageMiddlewareError {
	// Build CSV data with joined users
	var csvBuilder strings.Builder
	csvBuilder.WriteString("user_id,store_id,purchase_count,rank,gender,birthdate,registered_at\n")

	successfulJoins := 0
	failedJoins := 0

	for _, topUser := range topUsers {
		userData, err := jw.lookupUserFromFile(topUser.UserID, chunkMsg.ClientID)
		if err != nil || userData == nil {
			failedJoins++
			fmt.Printf("Join by User ID Worker: Failed to lookup user %s for client %s: %v\n",
				topUser.UserID, chunkMsg.ClientID, err)
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

	fmt.Printf("Join by User ID Worker: Client %s, Chunk %d - Successful joins: %d, Failed joins: %d, Total users: %d\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber, successfulJoins, failedJoins, len(topUsers))

	csvData := csvBuilder.String()

	// Send chunk with reader ID as chunk number
	return jw.chunkSender.SendFromMetadata(
		chunkMsg,
		int(chunkMsg.ChunkNumber), // Use reader ID as chunk number
		chunkMsg.IsLastChunk,
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
	fmt.Printf("Join by User ID Worker: Performing cleanup for client: %s (deletion commented out for testing)\n", clientID)

	// TODO: Re-enable cleanup once we fix the partition-specific deletion logic
	// Each reader should only delete partitions it owns (partition % numReaders == readerID - 1)
	// if err := jw.partitionManager.CleanupClientFiles(clientID); err != nil {
	// 	fmt.Printf("Join by User ID Worker: Error during cleanup for client %s: %v\n", clientID, err)
	// } else {
	// 	fmt.Printf("Join by User ID Worker: Completed cleanup for client: %s\n", clientID)
	// }
}
