package main

import (
	"container/heap"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	completioncleaner "github.com/tp-distribuidos-2c2025/shared/completion_cleaner"
	"github.com/tp-distribuidos-2c2025/shared/health_server"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
)

// UserRecord represents a user's purchase record
type UserRecord struct {
	UserID        string
	StoreID       string
	PurchaseCount int
	index         int // index in the heap
}

// MinHeap implements a min-heap for UserRecord based on PurchaseCount
type MinHeap []*UserRecord

func (h MinHeap) Len() int { return len(h) }

func (h MinHeap) Less(i, j int) bool {
	return h[i].PurchaseCount < h[j].PurchaseCount
}

func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *MinHeap) Push(x interface{}) {
	n := len(*h)
	record := x.(*UserRecord)
	record.index = n
	*h = append(*h, record)
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	record := old[n-1]
	old[n-1] = nil
	record.index = -1
	*h = old[0 : n-1]
	return record
}

// StoreTopUsers maintains top 3 users for a store
type StoreTopUsers struct {
	StoreID  string
	TopUsers MinHeap
	MaxSize  int
}

// NewStoreTopUsers creates a new StoreTopUsers with max size 3
func NewStoreTopUsers(storeID string) *StoreTopUsers {
	return &StoreTopUsers{
		StoreID:  storeID,
		TopUsers: make(MinHeap, 0, 3),
		MaxSize:  3,
	}
}

// Add adds or updates a user in the top-3 list
func (st *StoreTopUsers) Add(userID string, purchaseCount int) {
	// Check if user already exists
	for i, record := range st.TopUsers {
		if record.UserID == userID {
			// Update existing user
			st.TopUsers[i].PurchaseCount = purchaseCount
			heap.Fix(&st.TopUsers, i)
			return
		}
	}

	// If we haven't reached max size, just add
	if st.TopUsers.Len() < st.MaxSize {
		heap.Push(&st.TopUsers, &UserRecord{
			UserID:        userID,
			StoreID:       st.StoreID,
			PurchaseCount: purchaseCount,
		})
		return
	}

	// If this user has more purchases than the minimum, replace the minimum
	if purchaseCount > st.TopUsers[0].PurchaseCount {
		st.TopUsers[0].UserID = userID
		st.TopUsers[0].PurchaseCount = purchaseCount
		heap.Fix(&st.TopUsers, 0)
	}
}

// GetTopUsers returns the top users in descending order
func (st *StoreTopUsers) GetTopUsers() []*UserRecord {
	// Create a copy and sort descending
	result := make([]*UserRecord, len(st.TopUsers))
	copy(result, st.TopUsers)

	// Sort descending by purchase count
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].PurchaseCount < result[j].PurchaseCount {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}

// ClientState holds the state for a specific client
type ClientState struct {
	topUsersByStore map[string]*StoreTopUsers // key: store_id
	receivedChunks  map[int]bool              // Track which chunk numbers we've received (chunk number = partition number for Q4)
	numPartitions   int                       // Expected number of chunks (one per partition)
}

// TopUsersWorker processes user-store aggregations and selects top users per store
type TopUsersWorker struct {
	consumer         *workerqueue.QueueConsumer
	exchangeProducer *exchange.ExchangeMiddleware
	config           *middleware.ConnectionConfig
	clientStates     map[string]*ClientState // key: ClientID
	numPartitions    int                     // Total number of partitions
	numReaders       int                     // Number of readers (5)

	// Fault tolerance components
	messageManager *messagemanager.MessageManager
	stateManager   *TopUsersStateManager
}

// NewTopUsersWorker creates a new top users worker
func NewTopUsersWorker() *TopUsersWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Get NUM_PARTITIONS from environment (total partitions across all orchestrators)
	numPartitionsStr := os.Getenv("NUM_PARTITIONS")
	numPartitions := 100 // Default to 100 if not set
	if numPartitionsStr != "" {
		if n, err := strconv.Atoi(numPartitionsStr); err == nil && n > 0 {
			numPartitions = n
		}
	}
	log.Printf("Top Users Worker: Expecting %d chunks per client (one per partition)", numPartitions)

	// Number of readers (fixed at 5)
	numReaders := 5

	// Use builder to create all resources
	stateDir := "/app/worker-data"
	metadataDir := filepath.Join(stateDir, "metadata")
	processedChunksPath := filepath.Join(stateDir, "processed-chunks.txt")

	builder := worker_builder.NewWorkerBuilder("Top Users Worker").
		WithConfig(config).
		// Queue consumer
		WithQueueConsumer(queues.Query4TopUsersQueue, true).
		// Exchange producer (direct type for routing to readers)
		WithExchangeProducer(queues.UserIdJoinChunksExchange, []string{}, true,
			worker_builder.ExchangeDeclarationOptions{
				Type:       "direct",
				Durable:    false,
				AutoDelete: false,
				Internal:   false,
				NoWait:     false,
			}).
		// State management
		WithDirectory(stateDir, 0755).
		WithDirectory(metadataDir, 0755).
		WithMessageManager(processedChunksPath)

	// Validate builder
	if err := builder.Validate(); err != nil {
		log.Fatalf("Top Users Worker: Builder validation failed: %v", err)
	}

	// Extract consumer from builder
	consumer := builder.GetQueueConsumer(queues.Query4TopUsersQueue)
	if consumer == nil {
		log.Fatal("Top Users Worker: Failed to get consumer from builder")
	}

	// Extract exchange producer from builder
	exchangeProducer := builder.GetExchangeProducer(queues.UserIdJoinChunksExchange)
	if exchangeProducer == nil {
		log.Fatal("Top Users Worker: Failed to get exchange producer from builder")
	}

	// Extract MessageManager from builder
	messageManager := builder.GetResourceTracker().Get(
		worker_builder.ResourceTypeMessageManager,
		"message-manager",
	)
	if messageManager == nil {
		log.Fatal("Top Users Worker: Failed to get message manager from builder")
	}
	mm, ok := messageManager.(*messagemanager.MessageManager)
	if !ok {
		log.Fatal("Top Users Worker: Message manager has wrong type")
	}

	// Add CompletionCleaner with MessageManager as cleanup handler
	// Use WORKER_ID from environment (service name) for cleanup queue name
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		log.Fatal("Top Users Worker: WORKER_ID environment variable is required")
	}
	builder.WithCompletionCleaner(
		queues.ClientCompletionCleanupExchange,
		workerID,
		[]completioncleaner.CleanupHandler{mm},
	)

	// Initialize custom StateManager (worker-specific, not part of builder)
	stateManager := NewTopUsersStateManager(metadataDir, numPartitions)

	worker := &TopUsersWorker{
		consumer:         consumer,
		exchangeProducer: exchangeProducer,
		config:           config,
		clientStates:     make(map[string]*ClientState),
		numPartitions:    numPartitions,
		numReaders:       numReaders,
		messageManager:   mm,
		stateManager:     stateManager,
	}

	// Rebuild state from CSV metadata on startup
	log.Println("Top Users Worker: Rebuilding state from metadata...")
	if err := stateManager.RebuildState(worker.clientStates); err != nil {
		log.Printf("Top Users Worker: Warning - failed to rebuild state: %v", err)
	} else {
		log.Println("Top Users Worker: State rebuilt successfully")
	}

	return worker
}

// getOrCreateClientState gets or creates client state
func (tw *TopUsersWorker) getOrCreateClientState(clientID string) *ClientState {
	if tw.clientStates[clientID] == nil {
		tw.clientStates[clientID] = &ClientState{
			topUsersByStore: make(map[string]*StoreTopUsers),
			receivedChunks:  make(map[int]bool),
			numPartitions:   tw.numPartitions,
		}
	}
	return tw.clientStates[clientID]
}

// processMessage processes a single message from reduce worker
func (tw *TopUsersWorker) processMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	clientID := chunkMsg.ClientID
	chunkNumber := int(chunkMsg.ChunkNumber)
	msgID := chunkMsg.ID

	// Check for duplicate chunk
	if tw.messageManager.IsProcessed(clientID, msgID) {
		log.Printf("Top Users Worker: Chunk %s already processed, skipping", msgID)
		return 0 // Success - callback will ack
	}

	// Get or create client state
	clientState := tw.getOrCreateClientState(clientID)

	log.Printf("Top Users Worker: Received chunk %d (partition %d) for client %s (expecting %d total chunks)",
		chunkNumber, chunkNumber, clientID, clientState.numPartitions)

	// Process chunk through state manager (persists CSV data and updates state)
	if err := tw.stateManager.ProcessChunk(chunkMsg, clientState); err != nil {
		log.Printf("Top Users Worker: Failed to process chunk: %v", err)
		return middleware.MessageMiddlewareMessageError // Error - callback will nack
	}

	// Mark chunk as processed in MessageManager
	if err := tw.messageManager.MarkProcessed(clientID, msgID); err != nil {
		log.Printf("Top Users Worker: Warning - failed to mark chunk as processed: %v", err)
	}

	// Check if we've received all expected chunks (all partitions 0 through numPartitions-1)
	allChunksReceived := true
	for i := 0; i < clientState.numPartitions; i++ {
		if !clientState.receivedChunks[i] {
			allChunksReceived = false
			break
		}
	}

	if allChunksReceived {
		log.Printf("Top Users Worker: Received all %d chunks (partitions 0-%d) for client %s, sending top users...",
			clientState.numPartitions, clientState.numPartitions-1, clientID)

		if err := tw.sendTopUsers(clientID, clientState); err != 0 {
			log.Printf("Top Users Worker: Failed to send top users: %v", err)
			return err
		}

		// Mark client as ready (deletes CSV metadata file)
		if err := tw.stateManager.MarkClientReady(clientID); err != nil {
			log.Printf("Top Users Worker: Warning - failed to mark client ready: %v", err)
		}

		// Clear client state
		delete(tw.clientStates, clientID)
	} else {
		receivedCount := len(clientState.receivedChunks)
		log.Printf("Top Users Worker: Client %s has %d/%d chunks (partitions), waiting for more...",
			clientID, receivedCount, clientState.numPartitions)
	}

	return 0
}

// getUserPartition returns the partition number for a given user ID
func getUserPartition(userID string, numPartitions int) (int, error) {
	// Parse user ID (handle both int and float formats)
	userIDFloat, err := strconv.ParseFloat(userID, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid user ID %s: %w", userID, err)
	}
	userIDInt := int(userIDFloat)
	return userIDInt % numPartitions, nil
}

// sendTopUsers sends the final top users to the join workers, one chunk per reader
func (tw *TopUsersWorker) sendTopUsers(clientID string, clientState *ClientState) middleware.MessageMiddlewareError {
	// Group top users by reader (1-5)
	readerUsers := make(map[int][]*UserRecord) // readerID -> users

	// Build rank map for all users (for consistent output)
	rankMap := make(map[string]int) // userID -> rank
	for storeID, storeTop := range clientState.topUsersByStore {
		// Skip metadata marker entries (empty storeID)
		if storeID == "" {
			continue
		}
		topUsers := storeTop.GetTopUsers()
		for rank, user := range topUsers {
			// Skip marker entries with empty userID
			if user.UserID == "" {
				continue
			}
			rankMap[user.UserID] = rank + 1
		}
	}

	// Group users by reader based on their partition
	for storeID, storeTop := range clientState.topUsersByStore {
		// Skip metadata marker entries (empty storeID) - they are only for fault tolerance
		if storeID == "" {
			continue
		}
		topUsers := storeTop.GetTopUsers()
		for _, user := range topUsers {
			// Skip marker entries with empty userID
			if user.UserID == "" {
				continue
			}
			partition, err := getUserPartition(user.UserID, tw.numPartitions)
			if err != nil {
				log.Printf("Top Users Worker: Failed to get partition for user %s: %v", user.UserID, err)
				continue
			}
			// Determine which reader owns this partition
			readerID := (partition % tw.numReaders) + 1
			readerUsers[readerID] = append(readerUsers[readerID], user)
		}
	}

	log.Printf("Top Users Worker: Grouped top users into %d readers for client %s", len(readerUsers), clientID)

	// Send one chunk per reader
	for readerID := 1; readerID <= tw.numReaders; readerID++ {
		users := readerUsers[readerID]

		// Build CSV for this reader
		var csvBuilder strings.Builder
		csvBuilder.WriteString("user_id,store_id,purchase_count,rank\n")

		for _, user := range users {
			rank := rankMap[user.UserID]
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d,%d\n",
				user.UserID,
				user.StoreID,
				user.PurchaseCount,
				rank,
			))
		}

		csvData := csvBuilder.String()

		// Determine if this is the last chunk (last reader)
		isLastChunk := (readerID == tw.numReaders)
		isLastFromTable := isLastChunk

		// Create chunk for this reader
		outputChunk := chunk.NewChunk(
			clientID,
			"TP01",   // File ID for top users (Query 4) - doesn't end in number to avoid parsing as file count
			4,        // Query Type 4
			readerID, // Chunk Number = reader ID
			isLastChunk,
			isLastFromTable,
			len(csvData),
			1, // Table ID 1
			csvData,
		)

		// Serialize the chunk
		chunkMsg := chunk.NewChunkMessage(outputChunk)
		serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
		if err != nil {
			log.Printf("Top Users Worker: Failed to serialize chunk for reader %d: %v", readerID, err)
			continue
		}

		// Get routing key for this reader
		routingKey := queues.GetUserIdJoinRoutingKey(readerID)

		// Send to exchange with reader-specific routing key
		sendErr := tw.exchangeProducer.Send(serializedData, []string{routingKey})
		if sendErr != 0 {
			log.Printf("Top Users Worker: Failed to send chunk for reader %d: %v", readerID, sendErr)
			continue
		}

		log.Printf("Top Users Worker: Sent chunk for reader %d (%d users) (IsLastChunk=%t)",
			readerID, len(users), isLastChunk)
	}

	log.Printf("Top Users Worker: Successfully sent all reader chunks for client %s (%d stores, %d readers)",
		clientID, len(clientState.topUsersByStore), len(readerUsers))
	return 0
}

// Start starts the top users worker
func (tw *TopUsersWorker) Start() {
	log.Println("Starting Top Users Worker for Query 4...")

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {

			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				log.Printf("Top Users Worker: Failed to deserialize chunk: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}

			if err := tw.processMessage(chunkMsg); err != 0 {
				log.Printf("Failed to process message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}

	if err := tw.consumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming: %v", err)
	}

	// Keep the worker running
	select {}
}

// Close closes the worker
func (tw *TopUsersWorker) Close() {
	if tw.consumer != nil {
		tw.consumer.Close()
	}
	if tw.exchangeProducer != nil {
		tw.exchangeProducer.Close()
	}
	if tw.messageManager != nil {
		tw.messageManager.Close()
	}
}

func main() {
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8888"
	}
	healthSrv := health_server.NewHealthServer(healthPort)
	go healthSrv.Start()
	defer healthSrv.Stop()

	topUsersWorker := NewTopUsersWorker()
	defer topUsersWorker.Close()

	topUsersWorker.Start()
}
