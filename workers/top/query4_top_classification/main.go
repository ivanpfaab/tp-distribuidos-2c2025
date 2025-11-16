package main

import (
	"container/heap"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
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
	receivedChunks  map[int]bool               // Track which chunk numbers we've received (chunk number = worker ID)
	numWorkers      int                        // Expected number of chunks
}

// TopUsersWorker processes user-store aggregations and selects top users per store
type TopUsersWorker struct {
	consumer     *workerqueue.QueueConsumer
	producer     *workerqueue.QueueMiddleware
	config       *middleware.ConnectionConfig
	clientStates map[string]*ClientState // key: ClientID
	numWorkers   int                     // Number of workers (from environment)
}

// NewTopUsersWorker creates a new top users worker
func NewTopUsersWorker() *TopUsersWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Get NUM_WORKERS from environment
	numWorkersStr := os.Getenv("NUM_WORKERS")
	numWorkers := 3 // Default to 3 if not set
	if numWorkersStr != "" {
		if n, err := strconv.Atoi(numWorkersStr); err == nil && n > 0 {
			numWorkers = n
		}
	}
	log.Printf("Top Users Worker: Expecting %d chunks per client", numWorkers)

	// Create consumer for top users queue
	consumer := workerqueue.NewQueueConsumer(queues.Query4TopUsersQueue, config)
	if consumer == nil {
		log.Fatal("Failed to create consumer")
	}

	// Declare the input queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(queues.Query4TopUsersQueue, config)
	if queueDeclarer == nil {
		consumer.Close()
		log.Fatal("Failed to create queue declarer")
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		log.Fatalf("Failed to declare input queue '%s': %v", queues.Query4TopUsersQueue, err)
	}
	queueDeclarer.Close()

	// Create producer for output queue (to User join)
	producer := workerqueue.NewMessageMiddlewareQueue(queues.UserIdChunkQueue, config)
	if producer == nil {
		consumer.Close()
		log.Fatal("Failed to create producer")
	}

	// Declare the output queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		log.Fatalf("Failed to declare output queue '%s': %v", queues.UserIdChunkQueue, err)
	}

	return &TopUsersWorker{
		consumer:     consumer,
		producer:     producer,
		config:       config,
		clientStates: make(map[string]*ClientState),
		numWorkers:   numWorkers,
	}
}

// getOrCreateClientState gets or creates client state
func (tw *TopUsersWorker) getOrCreateClientState(clientID string) *ClientState {
	if tw.clientStates[clientID] == nil {
		tw.clientStates[clientID] = &ClientState{
			topUsersByStore: make(map[string]*StoreTopUsers),
			receivedChunks:  make(map[int]bool),
			numWorkers:      tw.numWorkers,
		}
	}
	return tw.clientStates[clientID]
}

// processMessage processes a single message from reduce worker
func (tw *TopUsersWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		log.Printf("Top Users Worker: Failed to deserialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	clientID := chunkMsg.ClientID
	chunkNumber := int(chunkMsg.ChunkNumber)
	clientState := tw.getOrCreateClientState(clientID)

	log.Printf("Top Users Worker: Received chunk %d for client %s (expecting chunks 1-%d)", chunkNumber, clientID, clientState.numWorkers)

	// Mark this chunk as received
	clientState.receivedChunks[chunkNumber] = true

	// Process the chunk data first (if it has data)
	if chunkMsg.ChunkSize > 0 && len(chunkMsg.ChunkData) > 0 {
		log.Printf("Top Users Worker: Processing data chunk %d for client %s", chunkNumber, clientID)
		if err := tw.processChunkData(chunkMsg, clientState); err != nil {
			log.Printf("Top Users Worker: Failed to process chunk data: %v", err)
			return middleware.MessageMiddlewareMessageError
		}
	}

	// Check if we've received all expected chunks (1 through numWorkers)
	allChunksReceived := true
	for i := 1; i <= clientState.numWorkers; i++ {
		if !clientState.receivedChunks[i] {
			allChunksReceived = false
			break
		}
	}

	if allChunksReceived {
		log.Printf("Top Users Worker: Received all %d chunks for client %s, sending top users...", clientState.numWorkers, clientID)

		if err := tw.sendTopUsers(clientID, clientState); err != 0 {
			log.Printf("Top Users Worker: Failed to send top users: %v", err)
			return err
		}

		// Clear client state
		delete(tw.clientStates, clientID)
	} else {
		receivedCount := len(clientState.receivedChunks)
		log.Printf("Top Users Worker: Client %s has %d/%d chunks, waiting for more...", clientID, receivedCount, clientState.numWorkers)
	}

	return 0
}

// processChunkData processes chunk data and updates top users per store
func (tw *TopUsersWorker) processChunkData(chunkMsg *chunk.Chunk, clientState *ClientState) error {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkMsg.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	// Skip header row
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) < 3 {
			continue
		}

		userID := record[0]
		storeID := record[1]
		purchaseCount, _ := strconv.Atoi(record[2])

		// Get or create store top users
		if clientState.topUsersByStore[storeID] == nil {
			clientState.topUsersByStore[storeID] = NewStoreTopUsers(storeID)
		}

		// Add/update user in top-3 for this store
		clientState.topUsersByStore[storeID].Add(userID, purchaseCount)
	}

	log.Printf("Top Users Worker: Processed chunk for client %s - Now tracking %d stores",
		chunkMsg.ClientID, len(clientState.topUsersByStore))

	return nil
}

// sendTopUsers sends the final top users to the join worker
func (tw *TopUsersWorker) sendTopUsers(clientID string, clientState *ClientState) middleware.MessageMiddlewareError {
	// Convert top users to CSV with clean schema
	var csvBuilder strings.Builder
	csvBuilder.WriteString("user_id,store_id,purchase_count,rank\n")

	for _, storeTop := range clientState.topUsersByStore {
		topUsers := storeTop.GetTopUsers()
		for rank, user := range topUsers {
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d,%d\n",
				user.UserID,
				user.StoreID,
				user.PurchaseCount,
				rank+1, // Rank starts from 1
			))
		}
	}

	csvData := csvBuilder.String()

	// Create chunk for output
	outputChunk := chunk.NewChunk(
		clientID,
		"TOP4", // File ID for top users
		4,      // Query Type 4
		1,      // Chunk Number
		true,   // Is Last Chunk
		true,   // Is Last File (final results)
		len(csvData),
		1, // Table ID 1
		csvData,
	)

	// Serialize and send
	chunkMsg := chunk.NewChunkMessage(outputChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		log.Printf("Top Users Worker: Failed to serialize output chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	sendErr := tw.producer.Send(serializedData)
	if sendErr != 0 {
		log.Printf("Top Users Worker: Failed to send output chunk: %v", sendErr)
		return sendErr
	}

	log.Printf("Top Users Worker: Successfully sent top users for client %s (%d stores)", clientID, len(clientState.topUsersByStore))
	return 0
}

// Start starts the top users worker
func (tw *TopUsersWorker) Start() {
	log.Println("Starting Top Users Worker for Query 4...")

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := tw.processMessage(delivery)
			if err != 0 {
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
	if tw.producer != nil {
		tw.producer.Close()
	}
}

func main() {
	topUsersWorker := NewTopUsersWorker()
	defer topUsersWorker.Close()

	topUsersWorker.Start()
}
