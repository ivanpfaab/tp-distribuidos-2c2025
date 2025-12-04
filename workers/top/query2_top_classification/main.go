package main

import (
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
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
)

// ItemRecord represents a single item aggregation
type ItemRecord struct {
	ItemID        string
	Year          int
	Month         int
	TotalQuantity int
	TotalSubtotal float64
}

// MonthTopItems stores top items for a specific month
type MonthTopItems struct {
	TopByQuantity *ItemRecord
	TopByRevenue  *ItemRecord
}

// ClientState holds the state for a specific client
type ClientState struct {
	topItemsByMonth map[string]*MonthTopItems // key: "YYYY-MM"
	receivedChunks  map[int]bool              // Track which chunk numbers we've received (chunk number = partition number)
	numPartitions   int                       // Expected number of chunks (one per partition)
}

// TopItemsWorker processes month-level aggregations and selects top items
type TopItemsWorker struct {
	consumer      *workerqueue.QueueConsumer
	producer      *workerqueue.QueueMiddleware
	config        *middleware.ConnectionConfig
	clientStates  map[string]*ClientState // key: ClientID
	numPartitions int                     // Total number of partitions (from environment)

	// Fault tolerance components
	messageManager *messagemanager.MessageManager
	stateManager   *TopItemsStateManager
}

// NewTopItemsWorker creates a new top items worker
func NewTopItemsWorker() *TopItemsWorker {
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
	log.Printf("Top Items Worker: Expecting %d chunks per client (one per partition)", numPartitions)

	// Use builder to create all resources
	stateDir := "/app/worker-data"
	metadataDir := filepath.Join(stateDir, "metadata")
	processedChunksPath := filepath.Join(stateDir, "processed-chunks.txt")

	builder := worker_builder.NewWorkerBuilder("Top Items Worker").
		WithConfig(config).
		// Queue consumer
		WithQueueConsumer(queues.Query2TopItemsQueue, true).
		// Queue producer
		WithQueueProducer(queues.ItemIdChunkQueue, true).
		// State management
		WithDirectory(stateDir, 0755).
		WithDirectory(metadataDir, 0755).
		WithMessageManager(processedChunksPath)

	// Validate builder
	if err := builder.Validate(); err != nil {
		log.Fatalf("Top Items Worker: Builder validation failed: %v", err)
	}

	// Extract consumer from builder
	consumer := builder.GetQueueConsumer(queues.Query2TopItemsQueue)
	if consumer == nil {
		log.Fatal("Top Items Worker: Failed to get consumer from builder")
	}

	// Extract producer from builder
	producer := builder.GetQueueProducer(queues.ItemIdChunkQueue)
	if producer == nil {
		log.Fatal("Top Items Worker: Failed to get producer from builder")
	}

	// Extract MessageManager from builder
	messageManager := builder.GetResourceTracker().Get(
		worker_builder.ResourceTypeMessageManager,
		"message-manager",
	)
	if messageManager == nil {
		log.Fatal("Top Items Worker: Failed to get message manager from builder")
	}
	mm, ok := messageManager.(*messagemanager.MessageManager)
	if !ok {
		log.Fatal("Top Items Worker: Message manager has wrong type")
	}

	// Add CompletionCleaner with MessageManager as cleanup handler
	// Use WORKER_ID from environment (service name) for cleanup queue name
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		log.Fatal("Top Items Worker: WORKER_ID environment variable is required")
	}
	builder.WithCompletionCleaner(
		queues.ClientCompletionCleanupExchange,
		workerID,
		[]completioncleaner.CleanupHandler{mm},
	)

	// Initialize custom StateManager (worker-specific, not part of builder)
	stateManager := NewTopItemsStateManager(metadataDir, numPartitions)

	worker := &TopItemsWorker{
		consumer:       consumer,
		producer:       producer,
		config:         config,
		clientStates:   make(map[string]*ClientState),
		numPartitions:  numPartitions,
		messageManager: mm,
		stateManager:   stateManager,
	}

	// Rebuild state from CSV metadata on startup
	log.Println("Top Items Worker: Rebuilding state from metadata...")
	if err := stateManager.RebuildState(worker.clientStates); err != nil {
		log.Printf("Top Items Worker: Warning - failed to rebuild state: %v", err)
	} else {
		log.Println("Top Items Worker: State rebuilt successfully")
	}

	return worker
}

// getOrCreateClientState gets or creates client state
func (tw *TopItemsWorker) getOrCreateClientState(clientID string) *ClientState {
	if tw.clientStates[clientID] == nil {
		tw.clientStates[clientID] = &ClientState{
			topItemsByMonth: make(map[string]*MonthTopItems),
			receivedChunks:  make(map[int]bool),
			numPartitions:   tw.numPartitions,
		}
	}
	return tw.clientStates[clientID]
}

// processMessage processes a single message from reduce workers
func (tw *TopItemsWorker) processMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	clientID := chunkMsg.ClientID
	chunkNumber := int(chunkMsg.ChunkNumber)
	msgID := chunkMsg.ID

	// Check for duplicate chunk
	if tw.messageManager.IsProcessed(clientID, msgID) {
		log.Printf("Top Items Worker: Chunk %s already processed, skipping", msgID)
		return 0 // Success - callback will ack
	}

	// Get or create client state
	clientState := tw.getOrCreateClientState(clientID)

	log.Printf("Top Items Worker: Received chunk %d (partition %d) for client %s (expecting %d total chunks)",
		chunkNumber, chunkNumber, clientID, clientState.numPartitions)

	// Process chunk through state manager (persists CSV data and updates state)
	if err := tw.stateManager.ProcessChunk(chunkMsg, clientState); err != nil {
		log.Printf("Top Items Worker: Failed to process chunk: %v", err)
		return middleware.MessageMiddlewareMessageError // Error - callback will nack
	}

	// Mark chunk as processed in MessageManager
	if err := tw.messageManager.MarkProcessed(clientID, msgID); err != nil {
		log.Printf("Top Items Worker: Warning - failed to mark chunk as processed: %v", err)
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
		log.Printf("Top Items Worker: Received all %d chunks (partitions 0-%d) for client %s, sending top items...",
			clientState.numPartitions, clientState.numPartitions-1, clientID)

		if err := tw.sendTopItems(clientID, clientState); err != 0 {
			log.Printf("Top Items Worker: Failed to send top items: %v", err)
			return err
		}

		// Mark client as ready (deletes CSV metadata file)
		if err := tw.stateManager.MarkClientReady(clientID); err != nil {
			log.Printf("Top Items Worker: Warning - failed to mark client ready: %v", err)
		}

		// Clear client state
		delete(tw.clientStates, clientID)
	} else {
		receivedCount := len(clientState.receivedChunks)
		log.Printf("Top Items Worker: Client %s has %d/%d chunks (partitions), waiting for more...", clientID, receivedCount, clientState.numPartitions)
	}

	return 0
}

// sendTopItems sends the final top items to the join worker
func (tw *TopItemsWorker) sendTopItems(clientID string, clientState *ClientState) middleware.MessageMiddlewareError {
	// Convert top items to CSV
	// Schema: year,month,item_id,quantity,subtotal,category
	// 'category' field indicates: 1=top by quantity, 2=top by revenue
	var csvBuilder strings.Builder
	csvBuilder.WriteString("year,month,item_id,quantity,subtotal,category\n")

	for _, monthTop := range clientState.topItemsByMonth {
		// Add top by quantity
		if monthTop.TopByQuantity != nil {
			csvBuilder.WriteString(fmt.Sprintf("%d,%d,%s,%d,%.2f,%d\n",
				monthTop.TopByQuantity.Year,
				monthTop.TopByQuantity.Month,
				monthTop.TopByQuantity.ItemID,
				monthTop.TopByQuantity.TotalQuantity,
				monthTop.TopByQuantity.TotalSubtotal,
				1, // category = 1 (top by quantity)
			))
		}

		// Add top by revenue (if different from top by quantity)
		if monthTop.TopByRevenue != nil {
			if monthTop.TopByQuantity == nil || monthTop.TopByRevenue.ItemID != monthTop.TopByQuantity.ItemID {
				csvBuilder.WriteString(fmt.Sprintf("%d,%d,%s,%d,%.2f,%d\n",
					monthTop.TopByRevenue.Year,
					monthTop.TopByRevenue.Month,
					monthTop.TopByRevenue.ItemID,
					monthTop.TopByRevenue.TotalQuantity,
					monthTop.TopByRevenue.TotalSubtotal,
					2, // category = 2 (top by revenue)
				))
			}
		}
	}

	csvData := csvBuilder.String()

	// Create chunk for output
	outputChunk := chunk.NewChunk(
		clientID,
		"TOP2", // File ID for top items
		2,      // Query Type 2
		1,      // Chunk Number
		true,   // Is Last Chunk
		true,   // Is Last File (final results)
		len(csvData),
		2, // Table ID 2
		csvData,
	)

	// Serialize and send
	chunkMsg := chunk.NewChunkMessage(outputChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		log.Printf("Top Items Worker: Failed to serialize output chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	sendErr := tw.producer.Send(serializedData)
	if sendErr != 0 {
		log.Printf("Top Items Worker: Failed to send output chunk: %v", sendErr)
		return sendErr
	}

	log.Printf("Top Items Worker: Successfully sent top items for client %s (%d months)", clientID, len(clientState.topItemsByMonth))
	return 0
}

// Start starts the top items worker
func (tw *TopItemsWorker) Start() {
	log.Println("Starting Top Items Worker for Query 2...")

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			log.Printf("Top Items Worker: Processing message from queue")

			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				log.Printf("Top Items Worker: Failed to deserialize chunk: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}

			if err := tw.processMessage(chunkMsg); err != 0 {
				log.Printf("Top Items Worker: Failed to process message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			log.Printf("Top Items Worker: Message processed successfully, acknowledging")
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
func (tw *TopItemsWorker) Close() {
	if tw.consumer != nil {
		tw.consumer.Close()
	}
	if tw.producer != nil {
		tw.producer.Close()
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

	topItemsWorker := NewTopItemsWorker()
	defer topItemsWorker.Close()

	topItemsWorker.Start()
}
