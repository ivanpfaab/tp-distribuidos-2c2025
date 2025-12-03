package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
	joinchunk "github.com/tp-distribuidos-2c2025/workers/join/shared/chunk"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/dictionary"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/handler"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/joiner"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/notifier"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/parser"
)

// ItemIdJoinWorker encapsulates the ItemID join worker state and dependencies
type ItemIdJoinWorker struct {
	dictionaryConsumer   *exchange.ExchangeConsumer
	chunkConsumer        *workerqueue.QueueConsumer
	completionConsumer   *exchange.ExchangeConsumer
	outputProducer       *workerqueue.QueueMiddleware
	orchestratorProducer *workerqueue.QueueMiddleware
	config               *middleware.ConnectionConfig
	workerID             string
	messageManager       *messagemanager.MessageManager

	// Shared components
	dictManager          *dictionary.Manager[*MenuItem]
	dictHandler          *handler.DictionaryHandler[*MenuItem]
	completionHandler    *handler.CompletionHandler[*MenuItem]
	chunkSender          *joinchunk.Sender
	orchestratorNotifier *notifier.OrchestratorNotifier
}

// NewItemIdJoinWorker creates a new ItemIdJoinWorker instance
func NewItemIdJoinWorker(config *middleware.ConnectionConfig) (*ItemIdJoinWorker, error) {
	instanceID := os.Getenv("WORKER_INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}

	instanceRoutingKey := fmt.Sprintf("%s-instance-%s", queues.ItemIdDictionaryRoutingKey, instanceID)
	fmt.Printf("ItemID Join Worker: Initializing with instance ID: %s, routing key: %s\n", instanceID, instanceRoutingKey)

	// Use builder to create all resources
	stateDir := "/app/worker-data"
	dictDir := filepath.Join(stateDir, "dictionaries")
	stateFilePath := filepath.Join(stateDir, "processed-ids.txt")

	builder := worker_builder.NewWorkerBuilder("ItemID Join Worker").
		WithConfig(config).
		// Exchange consumers (exchanges already declared by data-handler and orchestrator)
		WithExchangeConsumer(queues.ItemIdDictionaryExchange, []string{instanceRoutingKey}, false).
		WithExchangeConsumer(queues.ItemIdCompletionExchange, []string{queues.ItemIdCompletionRoutingKey}, false).
		// Queue consumer
		WithQueueConsumer(queues.ItemIdChunkQueue, true).
		// Queue producers
		WithQueueProducer(queues.Query2ResultsQueue, true).
		WithQueueProducer(queues.InMemoryJoinCompletionQueue, true).
		// State management
		WithDirectory(stateDir, 0755).
		WithDirectory(dictDir, 0755).
		WithMessageManager(stateFilePath).
		// DictionaryManager config (actual manager created separately)
		WithDictionaryManager(worker_builder.DictionaryManagerConfig{
			DictDir: dictDir,
			ParseFunc: func(csvData string, clientID string) (map[string]*MenuItem, error) {
				return parser.ParseMenuItems(csvData, clientID)
			},
			RebuildOnStart: true,
		})

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract resources from builder
	dictionaryConsumer := builder.GetExchangeConsumer(queues.ItemIdDictionaryExchange)
	completionConsumer := builder.GetExchangeConsumer(queues.ItemIdCompletionExchange)
	chunkConsumer := builder.GetQueueConsumer(queues.ItemIdChunkQueue)
	outputProducer := builder.GetQueueProducer(queues.Query2ResultsQueue)
	orchestratorProducer := builder.GetQueueProducer(queues.InMemoryJoinCompletionQueue)

	if dictionaryConsumer == nil || completionConsumer == nil || chunkConsumer == nil || outputProducer == nil || orchestratorProducer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get resources from builder"))
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

	workerID := fmt.Sprintf("itemid-worker-%s", instanceID)

	// Initialize DictionaryManager (worker-specific, created separately)
	dictManager := dictionary.NewManager[*MenuItem]()
	dictConfig := builder.GetDictionaryManagerConfig()

	parseFunc := func(csvData string, clientID string) (map[string]*MenuItem, error) {
		return parser.ParseMenuItems(csvData, clientID)
	}

	// Rebuild dictionaries on startup if configured
	if dictConfig != nil && dictConfig.RebuildOnStart {
		rebuiltCount, err := dictManager.RebuildAllDictionaries(dictConfig.DictDir, parseFunc)
		if err != nil {
			fmt.Printf("ItemID Join Worker: Warning - failed to rebuild dictionaries: %v\n", err)
		} else {
			fmt.Printf("ItemID Join Worker: Rebuilt %d dictionaries on startup\n", rebuiltCount)
		}
	}

	dictHandler := handler.NewDictionaryHandler(dictManager, parseFunc, dictDir, "ItemID Join Worker")
	completionHandler := handler.NewCompletionHandler(dictManager, dictDir, "ItemID Join Worker")
	chunkSender := joinchunk.NewSender(outputProducer)
	orchestratorNotifier := notifier.NewOrchestratorNotifier(orchestratorProducer, "itemid-join-worker")

	return &ItemIdJoinWorker{
		dictionaryConsumer:   dictionaryConsumer,
		chunkConsumer:        chunkConsumer,
		completionConsumer:   completionConsumer,
		outputProducer:       outputProducer,
		orchestratorProducer: orchestratorProducer,
		config:               config,
		workerID:             workerID,
		messageManager:       mm,
		dictManager:          dictManager,
		dictHandler:          dictHandler,
		completionHandler:    completionHandler,
		chunkSender:          chunkSender,
		orchestratorNotifier: orchestratorNotifier,
	}, nil
}

// Start starts the ItemID join worker
func (w *ItemIdJoinWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("ItemID Join Worker: Starting to listen for messages...")

	// Set queue names for persistent queues
	dictionaryQueueName := fmt.Sprintf("itemid-dictionary-%s-queue", w.workerID)
	w.dictionaryConsumer.SetQueueName(dictionaryQueueName)
	completionQueueName := fmt.Sprintf("itemid-completion-%s-queue", w.workerID)
	w.completionConsumer.SetQueueName(completionQueueName)

	// Start consuming from dictionary queue
	if err := w.dictionaryConsumer.StartConsuming(w.createDictionaryCallback()); err != 0 {
		fmt.Printf("Failed to start dictionary consumer: %v\n", err)
	}

	// Start consuming from chunk queue
	if err := w.chunkConsumer.StartConsuming(w.createChunkCallback()); err != 0 {
		fmt.Printf("Failed to start chunk consumer: %v\n", err)
	}

	// Start consuming completion signals
	if err := w.completionConsumer.StartConsuming(w.createCompletionCallback()); err != 0 {
		fmt.Printf("Failed to start completion consumer: %v\n", err)
	}

	return 0
}

// Close closes all connections
func (w *ItemIdJoinWorker) Close() {
	if w.messageManager != nil {
		w.messageManager.Close()
	}
	if w.dictionaryConsumer != nil {
		w.dictionaryConsumer.Close()
	}
	if w.chunkConsumer != nil {
		w.chunkConsumer.Close()
	}
	if w.completionConsumer != nil {
		w.completionConsumer.Close()
	}
	if w.outputProducer != nil {
		w.outputProducer.Close()
	}
	if w.orchestratorProducer != nil {
		w.orchestratorProducer.Close()
	}
}

// createDictionaryCallback creates the dictionary message processing callback
func (w *ItemIdJoinWorker) createDictionaryCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				fmt.Printf("ItemID Join Worker: Failed to deserialize dictionary chunk: %v\n", err)
				delivery.Nack(false, false)
				continue
			}

			// Check if chunk was already processed
			if w.messageManager.IsProcessed(chunkMsg.ClientID, chunkMsg.ID) {
				fmt.Printf("ItemID Join Worker: Dictionary chunk %s already processed, skipping\n", chunkMsg.ID)
				delivery.Ack(false)
				continue
			}

			if err := w.dictHandler.ProcessMessage(chunkMsg); err != 0 {
				delivery.Nack(false, false)
				done <- fmt.Errorf("failed to process dictionary message: %v", err)
				return
			}

			// Mark chunk as processed after successful processing
			if err := w.messageManager.MarkProcessed(chunkMsg.ClientID, chunkMsg.ID); err != nil {
				fmt.Printf("ItemID Join Worker: Failed to mark dictionary chunk as processed: %v\n", err)
			}

			delivery.Ack(false)
		}
		done <- nil
	}
}

// createChunkCallback creates the chunk message processing callback
func (w *ItemIdJoinWorker) createChunkCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {

			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				fmt.Printf("ItemID Join Worker: Failed to deserialize chunk: %v\n", err)
				delivery.Nack(false, false)
				continue
			}

			if !w.dictManager.IsReady(chunkMsg.ClientID) {
				fmt.Printf("ItemID Join Worker: Dictionary not ready for client %s, NACKing chunk for retry\n", chunkMsg.ClientID)
				delivery.Nack(false, true) // Requeue
				continue
			}

			if err := w.processChunkMessage(chunkMsg); err != 0 {
				fmt.Printf("ItemID Join Worker: Failed to process chunk message: %v\n", err)
				delivery.Nack(false, false) // Don't requeue on processing errors
				done <- fmt.Errorf("failed to process chunk message: %v", err)
				return
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// processChunkMessage processes chunk messages for joining
func (w *ItemIdJoinWorker) processChunkMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Printf("ItemID Join Worker: Received chunk message\n")

	// Check if chunk was already processed
	if w.messageManager.IsProcessed(chunkMsg.ClientID, chunkMsg.ID) {
		fmt.Printf("ItemID Join Worker: Chunk %s already processed, skipping\n", chunkMsg.ID)
		return 0 // Success - callback will ack
	}

	// Process the chunk
	if err := w.processChunk(chunkMsg); err != 0 {
		fmt.Printf("ItemID Join Worker: Failed to process chunk: %v\n", err)
		return middleware.MessageMiddlewareMessageError // Error - callback will nack
	}

	// Mark chunk as processed after successful processing
	if err := w.messageManager.MarkProcessed(chunkMsg.ClientID, chunkMsg.ID); err != nil {
		fmt.Printf("ItemID Join Worker: Failed to mark chunk as processed: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	return 0 // Success - callback will ack
}

// processChunk processes a single chunk for joining
func (w *ItemIdJoinWorker) processChunk(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Printf("ItemID Join Worker: Processing chunk - QueryType: %d, ClientID: %s, ChunkNumber: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.FileID)

	// Perform join
	joinedChunk, err := w.performJoin(chunkMsg)
	if err != nil {
		fmt.Printf("ItemID Join Worker: Failed to perform join: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send joined chunk
	if err := w.chunkSender.SendChunkObject(joinedChunk); err != 0 {
		fmt.Printf("ItemID Join Worker: Failed to send joined chunk: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send orchestrator notification
	if err := w.orchestratorNotifier.SendChunkNotification(chunkMsg); err != nil {
		fmt.Printf("ItemID Join Worker: Failed to send orchestrator notification: %v\n", err)
	}

	fmt.Printf("ItemID Join Worker: Successfully processed and sent joined chunk\n")
	return 0
}

// performJoin performs the actual join operation
func (w *ItemIdJoinWorker) performJoin(chunkMsg *chunk.Chunk) (*chunk.Chunk, error) {
	fmt.Printf("ItemID Join Worker: Performing join for QueryType: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.FileID)

	// Get client's menu items dictionary
	menuItems, exists := w.dictManager.GetClientDictionary(chunkMsg.ClientID)
	if !exists {
		menuItems = make(map[string]*MenuItem)
	}

	var joinedData string

	// Check if this is grouped data from GroupBy
	if parser.IsGroupedData(chunkMsg.ChunkData, "year", "category", "item_id") {
		fmt.Printf("ItemID Join Worker: Received grouped data, joining with menu items\n")

		groupedData, err := parser.ParseGroupedTransactionItems(chunkMsg.ChunkData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse grouped transaction items data: %w", err)
		}

		joinedData = joiner.BuildGroupedTransactionItemMenuJoin(groupedData, menuItems)
	} else {
		// Parse transaction items data
		transactionItemsData, err := parser.ParseTransactionItems(chunkMsg.ChunkData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse transaction items data: %w", err)
		}

		joinedData = joiner.BuildTransactionItemMenuJoin(transactionItemsData, menuItems)
	}

	// Create new chunk with joined data
	joinedChunk := chunk.NewChunk(
		chunkMsg.ClientID,
		chunkMsg.FileID,
		chunkMsg.QueryType,
		chunkMsg.ChunkNumber,
		chunkMsg.IsLastChunk,
		chunkMsg.IsLastFromTable,
		len(joinedData),
		chunkMsg.TableID,
		joinedData,
	)

	return joinedChunk, nil
}

// createCompletionCallback creates the completion signal processing callback
func (w *ItemIdJoinWorker) createCompletionCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {

			completionSignal, err := signals.DeserializeJoinCompletionSignal(delivery.Body)
			if err != nil {
				fmt.Printf("ItemID Join Worker: Failed to deserialize completion signal: %v\n", err)
				delivery.Ack(false)
				continue
			}

			if err := w.completionHandler.ProcessMessage(completionSignal); err != 0 {
				fmt.Printf("ItemID Join Worker: Error processing completion signal: %v\n", err)
				delivery.Nack(false, false)
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
