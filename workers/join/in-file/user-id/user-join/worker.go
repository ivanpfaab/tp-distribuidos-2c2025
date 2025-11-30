package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
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
	messageManager     *messagemanager.MessageManager

	// Completion tracking (no buffering needed)
	completionSignals     map[string]bool // clientID -> completion received
	completionMutex       sync.RWMutex
	completionSignalsPath string // Path to persistence file

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

	// Initialize MessageManager for fault tolerance
	// Directory is created in Dockerfile with correct ownership, but ensure it exists as safety measure
	stateDir := "/app/worker-data"
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		topUsersConsumer.Close()
		completionConsumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	stateFilePath := filepath.Join(stateDir, "processed-ids.txt")
	messageManager := messagemanager.NewMessageManager(stateFilePath)
	if err := messageManager.LoadProcessedIDs(); err != nil {
		fmt.Printf("Join by User ID Worker: Warning - failed to load processed chunks: %v (starting with empty state)\n", err)
	} else {
		count := messageManager.GetProcessedCount()
		fmt.Printf("Join by User ID Worker: Loaded %d processed chunks\n", count)
	}

	// Initialize completion signals persistence
	completionSignalsPath := filepath.Join(stateDir, "completion-signals.txt")
	completionSignals := make(map[string]bool)
	if err := loadCompletionSignals(completionSignalsPath, completionSignals); err != nil {
		fmt.Printf("Join by User ID Worker: Warning - failed to load completion signals: %v (starting with empty state)\n", err)
	} else {
		count := len(completionSignals)
		if count > 0 {
			fmt.Printf("Join by User ID Worker: Loaded %d completion signals\n", count)
		}
	}

	// Initialize shared components
	partitionManager := file.NewPartitionManager(SharedDataDir, NumPartitions)
	chunkSender := joinchunk.NewSender(producer)

	return &JoinByUserIdWorker{
		topUsersConsumer:      topUsersConsumer,
		completionConsumer:    completionConsumer,
		producer:              producer,
		config:                config,
		readerConfig:          readerConfig,
		workerID:              workerID,
		messageManager:        messageManager,
		completionSignals:     completionSignals,
		partitionManager:      partitionManager,
		chunkSender:           chunkSender,
		stopChan:              make(chan struct{}),
		completionSignalsPath: completionSignalsPath,
	}, nil
}

// Start starts the join by user ID worker
func (jw *JoinByUserIdWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Join by User ID Worker: Starting...")

	// Set queue names for persistent queues
	topUsersQueueName := fmt.Sprintf("userid-join-reader-%d-queue", jw.readerConfig.ReaderID)
	jw.topUsersConsumer.SetQueueName(topUsersQueueName)
	completionQueueName := fmt.Sprintf("userid-join-completion-reader-%d-queue", jw.readerConfig.ReaderID)
	jw.completionConsumer.SetQueueName(completionQueueName)

	// Start completion signal consumer
	go jw.startCompletionSignalConsumer()

	// Start consuming top users messages
	fmt.Println("Join by User ID Worker: Starting to listen for top users chunks...")
	return jw.topUsersConsumer.StartConsuming(jw.createTopUsersCallback())
}

// Close closes all connections
func (jw *JoinByUserIdWorker) Close() {
	fmt.Println("Join by User ID Worker: Shutting down...")

	if jw.messageManager != nil {
		jw.messageManager.Close()
	}

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

			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				fmt.Printf("Join by User ID Worker: Failed to deserialize chunk message: %v\n", err)
				delivery.Ack(false)
				continue
			}

			if err := jw.processTopUsersMessage(chunkMsg); err != 0 {
				fmt.Printf("Join by User ID Worker: Error processing top users message: %v\n", err)
				// Determine if we should nack or ack based on error type
				if err == middleware.MessageMiddlewareMessageError {
					delivery.Nack(false, true) // Requeue on message error
				} else {
					delivery.Ack(false)
				}
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// startCompletionSignalConsumer starts the completion signal consumer
func (jw *JoinByUserIdWorker) startCompletionSignalConsumer() {
	fmt.Println("Join by User ID Worker: Starting completion signal consumer...")

	err := jw.completionConsumer.StartConsuming(func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {

			completionSignal, err := signals.DeserializeJoinCompletionSignal(delivery.Body)
			if err != nil {
				fmt.Printf("Join by User ID Worker: Failed to deserialize completion signal: %v\n", err)
				delivery.Ack(false)
				continue
			}

			if err := jw.processCompletionSignal(completionSignal); err != 0 {
				fmt.Printf("Join by User ID Worker: Error processing completion signal: %v\n", err)
				delivery.Nack(false, false)
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	})
	if err != 0 {
		fmt.Printf("Join by User ID Worker: Failed to start completion signal consumer: %v\n", err)
	}
}

// processTopUsersMessage processes a single top users chunk with NACK/requeue pattern
func (jw *JoinByUserIdWorker) processTopUsersMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Check if chunk was already processed
	if jw.messageManager.IsProcessed(chunkMsg.ID) {
		fmt.Printf("Join by User ID Worker: Chunk %s already processed, skipping\n", chunkMsg.ID)
		return 0
	}

	fmt.Printf("Join by User ID Worker: Received chunk - ClientID: %s, ChunkNumber: %d (reader %d), IsLastChunk: %t\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk)

	// Check if completion signal has been received for this client
	jw.completionMutex.RLock()
	completionReceived := jw.completionSignals[chunkMsg.ClientID]
	jw.completionMutex.RUnlock()

	if !completionReceived {
		// Completion signal not received yet, return error to trigger NACK and requeue
		fmt.Printf("Join by User ID Worker: Completion signal not received for client %s, NACKing and requeuing chunk %d\n",
			chunkMsg.ClientID, chunkMsg.ChunkNumber)
		return middleware.MessageMiddlewareMessageError
	}

	// Completion signal received, process chunk immediately
	fmt.Printf("Join by User ID Worker: Processing chunk %d for client %s (completion signal received)\n",
		chunkMsg.ChunkNumber, chunkMsg.ClientID)

	// Parse top users data
	topUsers, parseErr := parser.ParseTopUsersData(chunkMsg.ChunkData)
	if parseErr != nil {
		fmt.Printf("Join by User ID Worker: Failed to parse top users data: %v\n", parseErr)
		return middleware.MessageMiddlewareMessageError
	}

	// Process and send joined chunk
	if err := jw.processAndSendChunk(chunkMsg, topUsers); err != 0 {
		fmt.Printf("Join by User ID Worker: Failed to process chunk: %v\n", err)
		return err
	}

	// Mark chunk as processed after successful processing
	if err := jw.messageManager.MarkProcessed(chunkMsg.ID); err != nil {
		fmt.Printf("Join by User ID Worker: Failed to mark chunk as processed: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Perform cleanup for partition files (reader shares volume with paired writer)
	jw.performCleanup(chunkMsg.ClientID)

	// Remove completion signal tracking for this client (after all chunks processed)
	jw.completionMutex.Lock()
	delete(jw.completionSignals, chunkMsg.ClientID)
	jw.completionMutex.Unlock()

	// Remove from persistence file
	if err := removeCompletionSignal(jw.completionSignalsPath, chunkMsg.ClientID); err != nil {
		fmt.Printf("Join by User ID Worker: Warning - failed to remove completion signal for client %s: %v\n", chunkMsg.ClientID, err)
	}

	return 0
}

// processCompletionSignal processes a completion signal from the orchestrator
func (jw *JoinByUserIdWorker) processCompletionSignal(completionSignal *signals.JoinCompletionSignal) middleware.MessageMiddlewareError {
	fmt.Printf("Join by User ID Worker: Received completion signal for client %s\n", completionSignal.ClientID)

	// Mark completion signal as received
	jw.completionMutex.Lock()
	jw.completionSignals[completionSignal.ClientID] = true
	jw.completionMutex.Unlock()

	// Persist completion signal
	if err := saveCompletionSignal(jw.completionSignalsPath, completionSignal.ClientID); err != nil {
		fmt.Printf("Join by User ID Worker: Warning - failed to persist completion signal for client %s: %v\n", completionSignal.ClientID, err)
	}

	// Chunks will be processed when they arrive (or re-arrive after requeue)
	fmt.Printf("Join by User ID Worker: Client %s marked as ready, chunks can now be processed\n", completionSignal.ClientID)

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
// Reader shares a volume with its paired writer (same compute node simulation)
func (jw *JoinByUserIdWorker) performCleanup(clientID string) {
	fmt.Printf("Join by User ID Worker: Performing cleanup for client: %s (reader %d)\n",
		clientID, jw.readerConfig.ReaderID)

	if err := jw.partitionManager.CleanupClientFiles(clientID); err != nil {
		fmt.Printf("Join by User ID Worker: Error during cleanup for client %s: %v\n", clientID, err)
	} else {
		fmt.Printf("Join by User ID Worker: Completed cleanup for client: %s (reader %d)\n", clientID, jw.readerConfig.ReaderID)
	}
}

// loadCompletionSignals loads completion signals from a file
func loadCompletionSignals(filePath string, signals map[string]bool) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet (first run), return nil
			return nil
		}
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			signals[line] = true
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	return nil
}

// saveCompletionSignal appends a client ID to the completion signals file
func saveCompletionSignal(filePath string, clientID string) error {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if _, err := file.WriteString(clientID + "\n"); err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	return nil
}

// removeCompletionSignal removes a client ID from the completion signals file
func removeCompletionSignal(filePath string, clientID string) error {
	// Read all lines
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File doesn't exist, nothing to remove
		}
		return fmt.Errorf("failed to open file: %w", err)
	}

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && line != clientID {
			lines = append(lines, line)
		}
	}
	file.Close()

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Write back all lines except the removed one
	file, err = os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	for _, line := range lines {
		if _, err := file.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}
	}

	return nil
}
