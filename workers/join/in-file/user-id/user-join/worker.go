package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

const (
	InitialWaitDuration = 2 * time.Second
	JoinTickerInterval  = 1 * time.Second // How often to check for ready clients
	MaxJoinAttempts     = 10              // Maximum join retry attempts per client
)

// TopUserRecord represents a top user from classification
type TopUserRecord struct {
	UserID        string
	StoreID       string
	PurchaseCount string
	Rank          string
}

// ClientState holds the state for a specific client's join process
type ClientState struct {
	pendingUsers  []TopUserRecord // Users waiting to be joined
	chunkMetadata *chunk.Chunk    // Original chunk metadata
	receivedAt    time.Time       // When IsLastChunk was received
	attemptCount  int             // Number of join attempts made
	chunkCounter  int             // Sequential chunk numbering for output
	ready         bool            // True when IsLastChunk received
}

// JoinByUserIdWorker handles joining top users data with user data from CSV files
type JoinByUserIdWorker struct {
	consumer *workerqueue.QueueConsumer
	producer *workerqueue.QueueMiddleware
	config   *middleware.ConnectionConfig

	clientStates map[string]*ClientState
	stateMutex   sync.RWMutex

	ticker   *time.Ticker
	stopChan chan struct{}
}

// NewJoinByUserIdWorker creates a new JoinByUserIdWorker instance
func NewJoinByUserIdWorker(config *middleware.ConnectionConfig) (*JoinByUserIdWorker, error) {
	// Create consumer for user ID chunks
	consumer := workerqueue.NewQueueConsumer(
		UserIdChunkQueue,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create user ID chunk consumer")
	}

	// Declare the user ID chunk queue using QueueMiddleware
	userChunkDeclarer := workerqueue.NewMessageMiddlewareQueue(
		UserIdChunkQueue,
		config,
	)
	if userChunkDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create user chunk queue declarer")
	}
	if err := userChunkDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		userChunkDeclarer.Close()
		return nil, fmt.Errorf("failed to declare user ID chunk queue: %v", err)
	}
	userChunkDeclarer.Close()

	// Create producer for query 4 results
	producer := workerqueue.NewMessageMiddlewareQueue(
		Query4ResultsQueue,
		config,
	)
	if producer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create query 4 results producer")
	}

	// Declare producer queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to declare query 4 results queue: %v", err)
	}

	return &JoinByUserIdWorker{
		consumer:     consumer,
		producer:     producer,
		config:       config,
		clientStates: make(map[string]*ClientState),
		stopChan:     make(chan struct{}),
	}, nil
}

// Start starts the join by user ID worker
func (jw *JoinByUserIdWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Join by User ID Worker: Starting...")

	// Start background join worker
	jw.startBackgroundJoinWorker()

	// Start consuming messages
	fmt.Println("Join by User ID Worker: Starting to listen for top users chunks...")
	return jw.consumer.StartConsuming(jw.createCallback())
}

// Close closes all connections
func (jw *JoinByUserIdWorker) Close() {
	fmt.Println("Join by User ID Worker: Shutting down...")

	// Stop background worker
	if jw.ticker != nil {
		jw.ticker.Stop()
	}
	if jw.stopChan != nil {
		close(jw.stopChan)
	}

	// Close connections
	if jw.consumer != nil {
		jw.consumer.Close()
	}
	if jw.producer != nil {
		jw.producer.Close()
	}

	fmt.Println("Join by User ID Worker: Shutdown complete")
}

// createCallback creates the message processing callback
func (jw *JoinByUserIdWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			if err := jw.processMessage(delivery); err != 0 {
				fmt.Printf("Join by User ID Worker: Error processing message: %v\n", err)
			}
		}
		done <- nil
	}
}

// processMessage processes a single top users chunk
func (jw *JoinByUserIdWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
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

	// Update client state (thread-safe)
	jw.stateMutex.Lock()
	state := jw.getOrCreateClientState(chunkMsg.ClientID)
	state.pendingUsers = append(state.pendingUsers, topUsers...)
	state.chunkMetadata = chunkMsg

	if chunkMsg.IsLastChunk {
		state.ready = true
		state.receivedAt = time.Now()
		fmt.Printf("Join by User ID Worker: Client %s marked ready with %d pending users\n",
			chunkMsg.ClientID, len(state.pendingUsers))
	}
	jw.stateMutex.Unlock()

	delivery.Ack(false)
	return 0
}

// getOrCreateClientState gets or creates a client state (must be called with lock held)
func (jw *JoinByUserIdWorker) getOrCreateClientState(clientID string) *ClientState {
	if jw.clientStates[clientID] == nil {
		jw.clientStates[clientID] = &ClientState{
			pendingUsers:  make([]TopUserRecord, 0),
			chunkMetadata: nil,
			receivedAt:    time.Time{},
			attemptCount:  0,
			chunkCounter:  1, // Start from 1
			ready:         false,
		}
	}
	return jw.clientStates[clientID]
}

// startBackgroundJoinWorker starts the background worker that processes ready clients
func (jw *JoinByUserIdWorker) startBackgroundJoinWorker() {
	jw.ticker = time.NewTicker(JoinTickerInterval)

	go func() {
		fmt.Println("Join by User ID Worker: Background join worker started")
		for {
			select {
			case <-jw.ticker.C:
				jw.processReadyClients()
			case <-jw.stopChan:
				fmt.Println("Join by User ID Worker: Background join worker stopped")
				return
			}
		}
	}()
}

// processReadyClients checks all clients and attempts joins for ready ones
func (jw *JoinByUserIdWorker) processReadyClients() {
	jw.stateMutex.Lock()
	defer jw.stateMutex.Unlock()

	for clientID, state := range jw.clientStates {
		if !state.ready {
			continue
		}

		// Wait initial period before first attempt
		if time.Since(state.receivedAt) < InitialWaitDuration {
			continue
		}

		// Try to join pending users
		found, notFound := jw.attemptJoinAll(state.pendingUsers, clientID)

		// Send found users immediately
		if len(found) > 0 {
			isLast := len(notFound) == 0
			if err := jw.sendJoinedChunk(clientID, state, found, isLast); err != 0 {
				fmt.Printf("Join by User ID Worker: Failed to send joined chunk for client %s: %v\n", clientID, err)
			} else {
				state.chunkCounter++
				fmt.Printf("Join by User ID Worker: Sent chunk with %d joined users for client %s (pending: %d)\n",
					len(found), clientID, len(notFound))
			}
		}

		// Update pending users
		state.pendingUsers = notFound
		state.attemptCount++

		// Cleanup if done or max attempts reached
		if len(notFound) == 0 {
			fmt.Printf("Join by User ID Worker: All users joined for client %s, cleaning up\n", clientID)
			jw.cleanupClientFiles(clientID)
			delete(jw.clientStates, clientID)
		} else if state.attemptCount >= MaxJoinAttempts {
			fmt.Printf("Join by User ID Worker: Max attempts reached for client %s, %d users not found (INNER JOIN - dropping)\n",
				clientID, len(notFound))
			// Send final EOS chunk if we've sent any data
			if state.chunkCounter > 1 {
				jw.sendEmptyEOS(clientID, state)
			}
			jw.cleanupClientFiles(clientID)
			delete(jw.clientStates, clientID)
		}
	}
}

// attemptJoinAll attempts to join all pending users, returns (found, notFound)
func (jw *JoinByUserIdWorker) attemptJoinAll(pendingUsers []TopUserRecord, clientID string) ([]TopUserRecord, []TopUserRecord) {
	var found []TopUserRecord
	var notFound []TopUserRecord

	for _, topUser := range pendingUsers {
		user, err := jw.lookupUserFromFile(topUser.UserID, clientID)
		if err != nil || user == nil {
			// User not found yet, keep in pending
			notFound = append(notFound, topUser)
		} else {
			// User found, add to results
			found = append(found, topUser)
		}
	}

	return found, notFound
}

// sendJoinedChunk sends a chunk with joined user data
func (jw *JoinByUserIdWorker) sendJoinedChunk(clientID string, state *ClientState, foundUsers []TopUserRecord, isLast bool) middleware.MessageMiddlewareError {
	// Build CSV data with joined users
	var csvBuilder strings.Builder
	csvBuilder.WriteString("user_id,store_id,purchase_count,rank,gender,birthdate,registered_at\n")

	for _, topUser := range foundUsers {
		user, err := jw.lookupUserFromFile(topUser.UserID, clientID)
		if err != nil || user == nil {
			// Should not happen since we already found them
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
	}

	csvData := csvBuilder.String()

	// Create chunk for output
	outputChunk := chunk.NewChunk(
		clientID,
		state.chunkMetadata.FileID,
		state.chunkMetadata.QueryType,
		state.chunkCounter,
		isLast,
		state.chunkMetadata.Step,
		len(csvData),
		state.chunkMetadata.TableID,
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

// sendEmptyEOS sends an empty EOS chunk
func (jw *JoinByUserIdWorker) sendEmptyEOS(clientID string, state *ClientState) middleware.MessageMiddlewareError {
	outputChunk := chunk.NewChunk(
		clientID,
		state.chunkMetadata.FileID,
		state.chunkMetadata.QueryType,
		state.chunkCounter,
		true, // IsLastChunk
		state.chunkMetadata.Step,
		0,
		state.chunkMetadata.TableID,
		"",
	)

	chunkMsg := chunk.NewChunkMessage(outputChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
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

// cleanupClientFiles deletes all partition files for a specific client
func (jw *JoinByUserIdWorker) cleanupClientFiles(clientID string) {
	// Delete all partition files for this client from the shared data directory
	pattern := filepath.Join("/shared-data", fmt.Sprintf("%s-users-partition-*.csv", clientID))
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
