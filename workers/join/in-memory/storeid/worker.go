package main

import (
	"fmt"
	"os"
	"path/filepath"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	joinchunk "github.com/tp-distribuidos-2c2025/workers/join/shared/chunk"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/dictionary"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/handler"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/joiner"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/notifier"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/parser"
)

// StoreIdJoinWorker encapsulates the StoreID join worker state and dependencies
type StoreIdJoinWorker struct {
	dictionaryConsumer   *exchange.ExchangeConsumer
	chunkConsumer        *workerqueue.QueueConsumer
	completionConsumer   *exchange.ExchangeConsumer
	outputProducer       *workerqueue.QueueMiddleware
	orchestratorProducer *workerqueue.QueueMiddleware
	config               *StoreIdConfig
	workerID             string
	messageManager       *messagemanager.MessageManager

	// Shared components
	dictManager          *dictionary.Manager[*Store]
	dictHandler          *handler.DictionaryHandler[*Store]
	completionHandler    *handler.CompletionHandler[*Store]
	chunkSender          *joinchunk.Sender
	orchestratorNotifier *notifier.OrchestratorNotifier
}

// NewStoreIdJoinWorker creates a new StoreIdJoinWorker instance
func NewStoreIdJoinWorker(config *StoreIdConfig) (*StoreIdJoinWorker, error) {
	instanceID := os.Getenv("WORKER_INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}

	instanceRoutingKey := fmt.Sprintf("%s-instance-%s", queues.StoreIdDictionaryRoutingKey, instanceID)
	fmt.Printf("StoreID Join Worker: Initializing with instance ID: %s, routing key: %s\n", instanceID, instanceRoutingKey)

	// Create consumers and producers
	dictionaryConsumer := exchange.NewExchangeConsumer(
		queues.StoreIdDictionaryExchange,
		[]string{instanceRoutingKey},
		config.ConnectionConfig,
	)
	if dictionaryConsumer == nil {
		return nil, fmt.Errorf("failed to create dictionary consumer")
	}

	chunkConsumer := workerqueue.NewQueueConsumer(
		queues.StoreIdChunkQueue,
		config.ConnectionConfig,
	)
	if chunkConsumer == nil {
		dictionaryConsumer.Close()
		return nil, fmt.Errorf("failed to create chunk consumer")
	}

	outputProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.Query3ResultsQueue,
		config.ConnectionConfig,
	)
	if outputProducer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		return nil, fmt.Errorf("failed to create output producer")
	}

	completionConsumer := exchange.NewExchangeConsumer(
		queues.StoreIdCompletionExchange,
		[]string{queues.StoreIdCompletionRoutingKey},
		config.ConnectionConfig,
	)
	if completionConsumer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		return nil, fmt.Errorf("failed to create completion consumer")
	}

	orchestratorProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.InMemoryJoinCompletionQueue,
		config.ConnectionConfig,
	)
	if orchestratorProducer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		completionConsumer.Close()
		return nil, fmt.Errorf("failed to create orchestrator producer")
	}

	// Declare queues
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(queues.StoreIdChunkQueue, config.ConnectionConfig)
	if inputQueueDeclarer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		completionConsumer.Close()
		return nil, fmt.Errorf("failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		completionConsumer.Close()
		inputQueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare input queue: %v", err)
	}
	inputQueueDeclarer.Close()

	if err := outputProducer.DeclareQueue(false, false, false, false); err != 0 {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		completionConsumer.Close()
		return nil, fmt.Errorf("failed to declare output queue: %v", err)
	}

	workerID := fmt.Sprintf("storeid-worker-%s", instanceID)

	// Initialize MessageManager for fault tolerance
	stateDir := "/app/worker-data"
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		completionConsumer.Close()
		orchestratorProducer.Close()
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	stateFilePath := filepath.Join(stateDir, "processed-ids.txt")
	messageManager := messagemanager.NewMessageManager(stateFilePath)
	if err := messageManager.LoadProcessedIDs(); err != nil {
		fmt.Printf("StoreID Join Worker: Warning - failed to load processed chunks: %v (starting with empty state)\n", err)
	} else {
		count := messageManager.GetProcessedCount()
		fmt.Printf("StoreID Join Worker: Loaded %d processed chunks\n", count)
	}

	// Initialize shared components
	dictManager := dictionary.NewManager[*Store]()
	parseFunc := func(csvData string, clientID string) (map[string]*Store, error) {
		return parser.ParseStores(csvData, clientID)
	}
	dictHandler := handler.NewDictionaryHandler(dictManager, parseFunc, "StoreID Join Worker")
	completionHandler := handler.NewCompletionHandler(dictManager, "StoreID Join Worker")
	chunkSender := joinchunk.NewSender(outputProducer)
	orchestratorNotifier := notifier.NewOrchestratorNotifier(orchestratorProducer, "storeid-join-worker")

	return &StoreIdJoinWorker{
		dictionaryConsumer:   dictionaryConsumer,
		chunkConsumer:        chunkConsumer,
		completionConsumer:   completionConsumer,
		outputProducer:       outputProducer,
		orchestratorProducer: orchestratorProducer,
		config:               config,
		workerID:             workerID,
		messageManager:       messageManager,
		dictManager:          dictManager,
		dictHandler:          dictHandler,
		completionHandler:    completionHandler,
		chunkSender:          chunkSender,
		orchestratorNotifier: orchestratorNotifier,
	}, nil
}

// Start starts the StoreID join worker
func (w *StoreIdJoinWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("StoreID Join Worker: Starting to listen for messages...")

	// Set queue names for persistent queues
	dictionaryQueueName := fmt.Sprintf("storeid-dictionary-%s-queue", w.workerID)
	w.dictionaryConsumer.SetQueueName(dictionaryQueueName)
	completionQueueName := fmt.Sprintf("storeid-completion-%s-queue", w.workerID)
	w.completionConsumer.SetQueueName(completionQueueName)

	if err := w.dictionaryConsumer.StartConsuming(w.createDictionaryCallback()); err != 0 {
		fmt.Printf("Failed to start dictionary consumer: %v\n", err)
	}

	if err := w.chunkConsumer.StartConsuming(w.createChunkCallback()); err != 0 {
		fmt.Printf("Failed to start chunk consumer: %v\n", err)
	}

	if err := w.completionConsumer.StartConsuming(w.createCompletionCallback()); err != 0 {
		fmt.Printf("Failed to start completion consumer: %v\n", err)
	}

	return 0
}

// Close closes all connections
func (w *StoreIdJoinWorker) Close() {
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
func (w *StoreIdJoinWorker) createDictionaryCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			// Check if chunk was already processed
			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				fmt.Printf("StoreID Join Worker: Failed to deserialize dictionary chunk: %v\n", err)
				delivery.Nack(false, false)
				continue
			}

			if w.messageManager.IsProcessed(chunkMsg.ID) {
				fmt.Printf("StoreID Join Worker: Dictionary chunk %s already processed, skipping\n", chunkMsg.ID)
				delivery.Ack(false)
				continue
			}

			if err := w.dictHandler.ProcessMessage(delivery); err != 0 {
				done <- fmt.Errorf("failed to process dictionary message: %v", err)
				return
			}

			// Mark chunk as processed after successful processing
			if err := w.messageManager.MarkProcessed(chunkMsg.ID); err != nil {
				fmt.Printf("StoreID Join Worker: Failed to mark dictionary chunk as processed: %v\n", err)
			}
		}
	}
}

// createChunkCallback creates the chunk message processing callback
func (w *StoreIdJoinWorker) createChunkCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := w.processChunkMessage(delivery)
			if err != 0 {
				done <- fmt.Errorf("failed to process chunk message: %v", err)
				return
			}
		}
	}
}

// processChunkMessage processes chunk messages for joining
func (w *StoreIdJoinWorker) processChunkMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("StoreID Join Worker: Received chunk message\n")

	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("StoreID Join Worker: Failed to deserialize chunk: %v\n", err)
		delivery.Nack(false, false)
		return middleware.MessageMiddlewareMessageError
	}

	// Check if chunk was already processed
	if w.messageManager.IsProcessed(chunkMsg.ID) {
		fmt.Printf("StoreID Join Worker: Chunk %s already processed, skipping\n", chunkMsg.ID)
		delivery.Ack(false)
		return 0
	}

	// Check if dictionary is ready
	if !w.dictManager.IsReady(chunkMsg.ClientID) {
		fmt.Printf("StoreID Join Worker: Dictionary not ready for client %s, NACKing chunk for retry\n", chunkMsg.ClientID)
		delivery.Nack(false, true)
		return 0
	}

	// Process the chunk
	if err := w.processChunk(chunkMsg); err != 0 {
		fmt.Printf("StoreID Join Worker: Failed to process chunk: %v\n", err)
		delivery.Nack(false, false)
		return middleware.MessageMiddlewareMessageError
	}

	// Mark chunk as processed after successful processing
	if err := w.messageManager.MarkProcessed(chunkMsg.ID); err != nil {
		fmt.Printf("StoreID Join Worker: Failed to mark chunk as processed: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	delivery.Ack(false)
	return 0
}

// processChunk processes a single chunk for joining
func (w *StoreIdJoinWorker) processChunk(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Printf("StoreID Join Worker: Processing chunk - QueryType: %d, ClientID: %s, ChunkNumber: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.FileID)

	// Perform join
	joinedChunk, err := w.performJoin(chunkMsg)
	if err != nil {
		fmt.Printf("StoreID Join Worker: Failed to perform join: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send joined chunk
	if err := w.chunkSender.SendChunkObject(joinedChunk); err != 0 {
		fmt.Printf("StoreID Join Worker: Failed to send joined chunk: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send orchestrator notification
	if err := w.orchestratorNotifier.SendChunkNotification(chunkMsg); err != nil {
		fmt.Printf("StoreID Join Worker: Failed to send orchestrator notification: %v\n", err)
	}

	fmt.Printf("StoreID Join Worker: Successfully processed and sent joined chunk\n")
	return 0
}

// performJoin performs the actual join operation
func (w *StoreIdJoinWorker) performJoin(chunkMsg *chunk.Chunk) (*chunk.Chunk, error) {
	fmt.Printf("StoreID Join Worker: Performing join for QueryType: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.FileID)

	// Get client's stores dictionary
	stores, exists := w.dictManager.GetClientDictionary(chunkMsg.ClientID)
	if !exists {
		stores = make(map[string]*Store)
	}

	var joinedData string

	// Check if this is grouped data from GroupBy
	if parser.IsGroupedData(chunkMsg.ChunkData, "year", "count", "store_id") {
		fmt.Printf("StoreID Join Worker: Received grouped data, joining with stores\n")

		groupedData, err := parser.ParseGroupedTransactions(chunkMsg.ChunkData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse grouped transaction data: %w", err)
		}

		joinedData = joiner.BuildGroupedTransactionStoreJoin(groupedData, stores)
	} else {
		// Parse transaction data
		transactionData, err := parser.ParseTransactions(chunkMsg.ChunkData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse transaction data: %w", err)
		}

		joinedData = joiner.BuildTransactionStoreJoin(transactionData, stores)
	}

	// Create new chunk with joined data
	joinedChunk := chunk.NewChunk(
		chunkMsg.ClientID,
		"0001", // Default file ID for StoreID join
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
func (w *StoreIdJoinWorker) createCompletionCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := w.completionHandler.ProcessMessage(delivery)
			if err != 0 {
				done <- fmt.Errorf("failed to process completion signal: %v", err)
				return
			}
		}
	}
}
