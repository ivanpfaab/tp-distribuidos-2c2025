package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	MaxClientsInBufferWorker = 1000 // Maximum number of clients to buffer
)

// TopUserRecord represents a top user from classification
type TopUserRecord struct {
	UserID        string
	StoreID       string
	PurchaseCount string
	Rank          string
}

// ClientBufferWorker holds buffered chunks for a client
type ClientBufferWorker struct {
	Chunks       []*chunk.Chunk  // Buffered chunks
	TopUsers     []TopUserRecord // Parsed top users from chunks
	ChunkCounter int             // Sequential chunk numbering for output
	Ready        bool            // True when completion signal received (files are written)

	// Double-check pattern fields
	CompletionSignalReceived bool // True when completion signal arrived
	ChunkDataReceived        bool // True when chunk data arrived
	JoinProcessed            bool // True when join has been processed (prevents duplicates)
}

// JoinByUserIdWorker handles joining top users data with user data from CSV files
type JoinByUserIdWorker struct {
	topUsersConsumer   *workerqueue.QueueConsumer
	completionConsumer *exchange.ExchangeConsumer
	producer           *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig
	workerID           string

	clientBuffers map[string]*ClientBufferWorker
	bufferMutex   sync.RWMutex

	stopChan chan struct{}
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

	// Declare the user ID chunk queue using QueueMiddleware
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

	return &JoinByUserIdWorker{
		topUsersConsumer:   topUsersConsumer,
		completionConsumer: completionConsumer,
		producer:           producer,
		config:             config,
		workerID:           workerID,
		clientBuffers:      make(map[string]*ClientBufferWorker),
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

	// Stop background worker
	if jw.stopChan != nil {
		close(jw.stopChan)
	}

	// Close connections
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
	topUsers, parseErr := jw.parseTopUsersData(chunkMsg.ChunkData)
	if parseErr != nil {
		fmt.Printf("Join by User ID Worker: Failed to parse top users data: %v\n", parseErr)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	// Check buffer capacity and NACK if full
	jw.bufferMutex.Lock()
	defer jw.bufferMutex.Unlock()

	// Check if we can add this client to buffer
	if jw.clientBuffers[chunkMsg.ClientID] == nil {
		if len(jw.clientBuffers) >= MaxClientsInBufferWorker {
			fmt.Printf("Join by User ID Worker: Buffer full (%d clients), NACKing message for client %s\n",
				len(jw.clientBuffers), chunkMsg.ClientID)
			delivery.Nack(false, false) // NACK and requeue
			return middleware.MessageMiddlewareMessageError
		}
		// Create new client buffer
		jw.clientBuffers[chunkMsg.ClientID] = &ClientBufferWorker{
			Chunks:                   make([]*chunk.Chunk, 0),
			TopUsers:                 make([]TopUserRecord, 0),
			ChunkCounter:             1,
			Ready:                    false,
			CompletionSignalReceived: false,
			ChunkDataReceived:        false,
			JoinProcessed:            false,
		}
	}

	clientBuffer := jw.clientBuffers[chunkMsg.ClientID]

	// Add chunk to buffer
	clientBuffer.Chunks = append(clientBuffer.Chunks, chunkMsg)
	clientBuffer.TopUsers = append(clientBuffer.TopUsers, topUsers...)

	fmt.Printf("Join by User ID Worker: Buffered chunk for client %s (chunks: %d, users: %d)\n",
		chunkMsg.ClientID, len(clientBuffer.Chunks), len(clientBuffer.TopUsers))

	// Just log when last chunk is received - no processing yet
	if chunkMsg.IsLastChunk {
		fmt.Printf("Join by User ID Worker: Client %s last chunk received with %d buffered users\n",
			chunkMsg.ClientID, len(clientBuffer.TopUsers))

		// Mark chunk data as received and check if ready to join
		clientBuffer.ChunkDataReceived = true
		fmt.Printf("Join by User ID Worker: Client %s chunk data received\n", chunkMsg.ClientID)

		// Check if both conditions are met and perform join if ready
		jw.bufferMutex.Unlock() // Unlock before calling checkAndJoin
		jw.checkAndJoin(chunkMsg.ClientID)
		jw.bufferMutex.Lock() // Re-lock for the rest of the method
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

	// Mark completion signal as received and check if ready to join
	jw.bufferMutex.Lock()

	// Ensure client buffer exists (create if it doesn't)
	if jw.clientBuffers[completionSignal.ClientID] == nil {
		jw.clientBuffers[completionSignal.ClientID] = &ClientBufferWorker{
			Chunks:                   make([]*chunk.Chunk, 0),
			TopUsers:                 make([]TopUserRecord, 0),
			ChunkCounter:             1,
			Ready:                    false,
			CompletionSignalReceived: false,
			ChunkDataReceived:        false,
			JoinProcessed:            false,
		}
		fmt.Printf("Join by User ID Worker: Created buffer for client %s (completion signal first)\n", completionSignal.ClientID)
	}

	clientBuffer := jw.clientBuffers[completionSignal.ClientID]
	clientBuffer.CompletionSignalReceived = true
	fmt.Printf("Join by User ID Worker: Client %s completion signal received\n", completionSignal.ClientID)

	jw.bufferMutex.Unlock()

	// Check if both conditions are met and perform join if ready
	jw.checkAndJoin(completionSignal.ClientID)

	delivery.Ack(false)
	return 0
}

// checkAndJoin checks if both completion signal and chunk data are ready, then performs join if both are ready
func (jw *JoinByUserIdWorker) checkAndJoin(clientID string) {
	jw.bufferMutex.Lock()
	defer jw.bufferMutex.Unlock()

	clientBuffer, exists := jw.clientBuffers[clientID]
	if !exists {
		return
	}

	// Check if both conditions are met and join hasn't been processed yet
	if clientBuffer.CompletionSignalReceived && clientBuffer.ChunkDataReceived && !clientBuffer.JoinProcessed {
		fmt.Printf("Join by User ID Worker: Both conditions met for client %s, performing join\n", clientID)

		// Mark as processed to prevent duplicate joins
		clientBuffer.JoinProcessed = true

		// Perform the join
		jw.processClientIfReady(clientID)
	} else {
		// Log current status for debugging
		fmt.Printf("Join by User ID Worker: Client %s not ready - CompletionSignal: %t, ChunkData: %t, JoinProcessed: %t\n",
			clientID, clientBuffer.CompletionSignalReceived, clientBuffer.ChunkDataReceived, clientBuffer.JoinProcessed)
	}
}

// processClientIfReady processes a client when completion signal is received
func (jw *JoinByUserIdWorker) processClientIfReady(clientID string) {

	clientBuffer := jw.clientBuffers[clientID]
	fmt.Printf("Join by User ID Worker: Processing client %s with %d buffered users\n",
		clientID, len(clientBuffer.TopUsers))

	// Completion signal received - files are ready, so join all users
	if err := jw.sendJoinedChunk(clientID, clientBuffer, clientBuffer.TopUsers, true); err != 0 {
		fmt.Printf("Join by User ID Worker: Failed to send joined chunk for client %s: %v\n", clientID, err)
	} else {
		fmt.Printf("Join by User ID Worker: Sent complete joined data for client %s (%d users)\n",
			clientID, len(clientBuffer.TopUsers))
	}

	delete(jw.clientBuffers, clientID)
	// Perform cleanup of partition files
	jw.performCleanup(clientID)
}

// sendJoinedChunk sends a chunk with joined user data
func (jw *JoinByUserIdWorker) sendJoinedChunk(clientID string, clientBuffer *ClientBufferWorker, foundUsers []TopUserRecord, isLast bool) middleware.MessageMiddlewareError {
	// Build CSV data with joined users
	var csvBuilder strings.Builder
	csvBuilder.WriteString("user_id,store_id,purchase_count,rank,gender,birthdate,registered_at\n")

	successfulJoins := 0
	failedJoins := 0

	for _, topUser := range foundUsers {
		user, err := jw.lookupUserFromFile(topUser.UserID, clientID)
		if err != nil || user == nil {
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
			user.Gender,
			user.Birthdate,
			user.RegisteredAt,
		))
		successfulJoins++
	}

	fmt.Printf("Join by User ID Worker: Client %s - Successful joins: %d, Failed joins: %d, Total users: %d\n",
		clientID, successfulJoins, failedJoins, len(foundUsers))

	csvData := csvBuilder.String()

	// Use metadata from first chunk
	var chunkMetadata *chunk.Chunk
	if len(clientBuffer.Chunks) > 0 {
		chunkMetadata = clientBuffer.Chunks[0]
	} else {
		// Fallback - this shouldn't happen
		return middleware.MessageMiddlewareMessageError
	}

	// Create chunk for output
	outputChunk := chunk.NewChunk(
		clientID,
		chunkMetadata.FileID,
		chunkMetadata.QueryType,
		clientBuffer.ChunkCounter,
		isLast,
		chunkMetadata.IsLastFromTable,
		chunkMetadata.Step,
		len(csvData),
		chunkMetadata.TableID,
		csvData,
	)

	// Serialize and send
	chunkMsg := chunk.NewChunkMessage(outputChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		fmt.Printf("Join by User ID Worker: Failed to serialize chunk: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	return jw.producer.Send(serializedData)
}

// parseTopUsersData parses top users classification CSV data
func (jw *JoinByUserIdWorker) parseTopUsersData(csvData string) ([]TopUserRecord, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var topUsers []TopUserRecord

	for i := 0; i < len(records); i++ {
		record := records[i]
		if strings.Contains(record[0], "user_id") {
			continue
		}
		if len(record) >= 4 {
			user := TopUserRecord{
				UserID:        record[0],
				StoreID:       record[1],
				PurchaseCount: record[2],
				Rank:          record[3],
			}
			topUsers = append(topUsers, user)
		}
	}

	return topUsers, nil
}

// UserData represents a user record from CSV file
type UserData struct {
	UserID       string
	Gender       string
	Birthdate    string
	RegisteredAt string
}

// lookupUserFromFile looks up user data from the appropriate partition file
func (jw *JoinByUserIdWorker) lookupUserFromFile(userID string, clientID string) (*UserData, error) {
	// Normalize user ID (remove decimal point if present, e.g., "13060.0" -> "13060")
	normalizedUserID := strings.TrimSuffix(userID, ".0")

	// Determine which partition this user belongs to
	partition, err := getUserPartition(normalizedUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition for user %s: %w", normalizedUserID, err)
	}

	// Open the partition file with client ID prefix
	filename := fmt.Sprintf("%s-users-partition-%03d.csv", clientID, partition)
	filePath := filepath.Join(SharedDataDir, filename)

	file, err := os.Open(filePath)
	if err != nil {
		// If file doesn't exist, return error to buffer transaction for retry
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("partition file not ready: %s", filePath)
		}
		return nil, fmt.Errorf("failed to open partition file %s: %w", filePath, err)
	}
	defer file.Close()

	// Read and search for user
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV from file %s: %w", filePath, err)
	}

	// Search for the specific user ID (skip header)
	for i := 0; i < len(records); i++ {
		record := records[i]
		if strings.Contains(record[0], "user_id") {
			continue
		}
		if len(record) >= 4 && record[0] == normalizedUserID {
			return &UserData{
				UserID:       record[0],
				Gender:       record[1],
				Birthdate:    record[2],
				RegisteredAt: record[3],
			}, nil
		}
	}

	// User not found - return error to trigger buffering and retry
	// After MaxRetries attempts, this will be dropped at the processMessage level
	return nil, fmt.Errorf("user %s not found in partition %d", normalizedUserID, partition)
}

// performCleanup performs cleanup operations for partition files (idempotent)
func (jw *JoinByUserIdWorker) performCleanup(clientID string) {
	fmt.Printf("Join by User ID Worker: Performing cleanup for client: %s\n", clientID)

	// Clean up partition files for this client
	jw.cleanupClientFiles(clientID)

	fmt.Printf("Join by User ID Worker: Completed cleanup for client: %s\n", clientID)
}

// cleanupClientFiles deletes all partition files for a specific client
func (jw *JoinByUserIdWorker) cleanupClientFiles(clientID string) {
	// Delete all partition files for this client from the shared data directory
	pattern := filepath.Join(SharedDataDir, fmt.Sprintf("%s-users-partition-*.csv", clientID))
	fmt.Printf("Join by User ID Worker: Looking for files with pattern: %s\n", pattern)

	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Printf("Join by User ID Worker: Error finding files for client %s: %v\n", clientID, err)
		return
	}

	fmt.Printf("Join by User ID Worker: Found %d files for client %s: %v\n", len(files), clientID, files)

	for _, file := range files {
		if err := os.Remove(file); err != nil {
			fmt.Printf("Join by User ID Worker: Error deleting file %s: %v\n", file, err)
		} else {
			fmt.Printf("Join by User ID Worker: Deleted file %s for client %s\n", file, clientID)
		}
	}

	fmt.Printf("Join by User ID Worker: Cleaned up %d files for client %s\n", len(files), clientID)
}
