package filter

import (
	"fmt"
	"os"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	completioncleaner "github.com/tp-distribuidos-2c2025/shared/completion_cleaner"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// FilterLogic defines the signature for filter logic that processes a chunk
type FilterLogic func(chunkMsg *chunk.Chunk) (chunk.Chunk, middleware.MessageMiddlewareError)

// RoutingRule defines where to route a chunk based on query type
type RoutingRule struct {
	QueryTypes []byte // Query types that match this rule
	Producer   *workerqueue.QueueMiddleware
}

// Config holds configuration for a filter worker
type Config struct {
	WorkerName       string
	InputQueue       string
	OutputProducers  map[string]*workerqueue.QueueMiddleware // Key: producer name (e.g., "timeFilter", "reply")
	RoutingRules     []RoutingRule                           // Rules for routing based on query type
	FilterLogic      FilterLogic
	StateFilePath    string
	ConnectionConfig *middleware.ConnectionConfig
}

// BaseFilterWorker is a generic filter worker that can be configured for different filter types
type BaseFilterWorker struct {
	config            *Config
	consumer          *workerqueue.QueueConsumer
	messageManager    *messagemanager.MessageManager
	completionCleaner *completioncleaner.CompletionCleaner
}

// NewBaseFilterWorker creates a new base filter worker with the given configuration
func NewBaseFilterWorker(config *Config) (*BaseFilterWorker, error) {
	// Create consumer
	consumer := workerqueue.NewQueueConsumer(config.InputQueue, config.ConnectionConfig)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create %s consumer", config.WorkerName)
	}

	// Declare input queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(config.InputQueue, config.ConnectionConfig)
	if queueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer for %s", config.WorkerName)
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare %s queue: %v", config.WorkerName, err)
	}
	queueDeclarer.Close()

	// Declare all output queues
	for name, producer := range config.OutputProducers {
		if producer == nil {
			consumer.Close()
			return nil, fmt.Errorf("output producer '%s' is nil for %s", name, config.WorkerName)
		}
		if err := producer.DeclareQueue(false, false, false, false); err != 0 {
			consumer.Close()
			// Close all producers
			for _, p := range config.OutputProducers {
				if p != nil {
					p.Close()
				}
			}
			return nil, fmt.Errorf("failed to declare output queue '%s' for %s: %v", name, config.WorkerName, err)
		}
	}

	// Initialize MessageManager for fault tolerance
	messageManager := messagemanager.NewMessageManager(config.StateFilePath)
	if err := messageManager.LoadProcessedIDs(); err != nil {
		fmt.Printf("%s: Warning - failed to load processed IDs: %v (starting with empty state)\n", config.WorkerName, err)
	} else {
		count := messageManager.GetProcessedCount()
		fmt.Printf("%s: Loaded %d processed IDs\n", config.WorkerName, count)
	}

	// Create CompletionCleaner with MessageManager as cleanup handler
	// Use WORKER_ID from environment (service name) for cleanup queue name
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		consumer.Close()
		for _, p := range config.OutputProducers {
			if p != nil {
				p.Close()
			}
		}
		return nil, fmt.Errorf("WORKER_ID environment variable is required")
	}
	completionCleaner, err := completioncleaner.NewCompletionCleaner(
		queues.ClientCompletionCleanupExchange,
		workerID,
		[]completioncleaner.CleanupHandler{messageManager},
		config.ConnectionConfig,
	)
	if err != nil {
		consumer.Close()
		for _, p := range config.OutputProducers {
			if p != nil {
				p.Close()
			}
		}
		return nil, fmt.Errorf("failed to create completion cleaner for %s: %w", config.WorkerName, err)
	}

	// Start CompletionCleaner automatically
	if err := completionCleaner.Start(); err != 0 {
		completionCleaner.Close()
		consumer.Close()
		for _, p := range config.OutputProducers {
			if p != nil {
				p.Close()
			}
		}
		return nil, fmt.Errorf("failed to start completion cleaner for %s: %v", config.WorkerName, err)
	}

	return &BaseFilterWorker{
		config:            config,
		consumer:          consumer,
		messageManager:    messageManager,
		completionCleaner: completionCleaner,
	}, nil
}

// Start starts the filter worker
func (bfw *BaseFilterWorker) Start() middleware.MessageMiddlewareError {
	fmt.Printf("%s: Starting to listen for messages...\n", bfw.config.WorkerName)
	err := bfw.consumer.StartConsuming(bfw.createCallback())
	if err != 0 {
		fmt.Printf("%s: ERROR - StartConsuming failed with error: %v\n", bfw.config.WorkerName, err)
		return err
	}
	fmt.Printf("%s: Successfully registered as consumer\n", bfw.config.WorkerName)
	return 0
}

// Close closes all connections
func (bfw *BaseFilterWorker) Close() {
	if bfw.completionCleaner != nil {
		bfw.completionCleaner.Close()
	}
	if bfw.messageManager != nil {
		bfw.messageManager.Close()
	}
	if bfw.consumer != nil {
		bfw.consumer.Close()
	}
	// Close all output producers
	for _, producer := range bfw.config.OutputProducers {
		if producer != nil {
			producer.Close()
		}
	}
}

// createCallback creates the message processing callback
func (bfw *BaseFilterWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Printf("%s: Callback started, waiting for messages...\n", bfw.config.WorkerName)
		messageCount := 0
		for delivery := range *consumeChannel {
			messageCount++
			fmt.Printf("%s: Received message #%d\n", bfw.config.WorkerName, messageCount)

			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				fmt.Printf("%s: Failed to deserialize chunk message: %v\n", bfw.config.WorkerName, err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}

			if err := bfw.processMessage(chunkMsg); err != 0 {
				fmt.Printf("%s: Failed to process message: %v\n", bfw.config.WorkerName, err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		fmt.Printf("%s: Consume channel closed after processing %d messages\n", bfw.config.WorkerName, messageCount)
		done <- nil
	}
}

// processMessage processes a single message
func (bfw *BaseFilterWorker) processMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Check if already processed
	if bfw.messageManager.IsProcessed(chunkMsg.ClientID, chunkMsg.ID) {
		fmt.Printf("%s: Chunk ID %s already processed, skipping\n", bfw.config.WorkerName, chunkMsg.ID)
		return 0 // Return 0 to ACK (handled by createCallback)
	}

	// Process the chunk using the configured filter logic
	fmt.Printf("%s: Processing chunk - QueryType: %d, ClientID: %s, ChunkNumber: %d\n",
		bfw.config.WorkerName, chunkMsg.QueryType, chunkMsg.ClientID, chunkMsg.ChunkNumber)

	responseChunk, msgErr := bfw.config.FilterLogic(chunkMsg)
	if msgErr != 0 {
		fmt.Printf("%s: Failed to apply filter logic: %v\n", bfw.config.WorkerName, msgErr)
		return msgErr
	}

	// Route the message based on query type
	return bfw.routeMessage(&responseChunk, chunkMsg.ID)
}

// routeMessage routes the processed chunk to the appropriate next stage based on routing rules
func (bfw *BaseFilterWorker) routeMessage(chunkMsg *chunk.Chunk, chunkID string) middleware.MessageMiddlewareError {
	// Find matching routing rule
	var targetProducer *workerqueue.QueueMiddleware
	for _, rule := range bfw.config.RoutingRules {
		for _, queryType := range rule.QueryTypes {
			if chunkMsg.QueryType == queryType {
				targetProducer = rule.Producer
				break
			}
		}
		if targetProducer != nil {
			break
		}
	}

	if targetProducer == nil {
		fmt.Printf("%s: No routing rule found for QueryType: %d\n", bfw.config.WorkerName, chunkMsg.QueryType)
		return middleware.MessageMiddlewareMessageError
	}

	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	replyData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("%s: Failed to serialize reply message: %v\n", bfw.config.WorkerName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to target producer
	sendErr := targetProducer.Send(replyData)
	if sendErr != 0 {
		fmt.Printf("%s: Failed to send to target producer: %v\n", bfw.config.WorkerName, sendErr)
		return sendErr
	}

	fmt.Printf("%s: Successfully routed chunk for ClientID: %s, ChunkNumber: %d, QueryType: %d\n",
		bfw.config.WorkerName, chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.QueryType)

	// Mark as processed (must be after successful send)
	if err := bfw.messageManager.MarkProcessed(chunkMsg.ClientID, chunkID); err != nil {
		fmt.Printf("%s: Failed to mark chunk as processed: %v\n", bfw.config.WorkerName, err)
		return middleware.MessageMiddlewareMessageError // Will NACK and requeue
	}

	return 0
}
