package main

import (
	"fmt"
	"os"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	completioncleaner "github.com/tp-distribuidos-2c2025/shared/completion_cleaner"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
)

// QueryGateway encapsulates the query gateway state and dependencies
type QueryGateway struct {
	consumer              *workerqueue.QueueConsumer
	query2GroupByProducer *workerqueue.QueueMiddleware // Query 2 - MapReduce
	query3GroupByProducer *workerqueue.QueueMiddleware // Query 3 - MapReduce
	query4GroupByProducer *workerqueue.QueueMiddleware // Query 4 - MapReduce
	query1ResultsProducer *workerqueue.QueueMiddleware
	config                *middleware.ConnectionConfig
	messageManager        *messagemanager.MessageManager
}

// NewQueryGateway creates a new QueryGateway instance
func NewQueryGateway(config *middleware.ConnectionConfig) (*QueryGateway, error) {
	// Use builder to create all resources
	builder := worker_builder.NewWorkerBuilder("Query Gateway").
		WithConfig(config).
		// Queue consumer
		WithQueueConsumer(queues.ReplyFilterBusQueue, true).
		// Queue producers for GroupBy workers
		WithQueueProducer(queues.Query2GroupByQueue, true).
		WithQueueProducer(queues.Query3GroupByQueue, true).
		WithQueueProducer(queues.Query4GroupByQueue, true).
		// Queue producer for Query 1 results
		WithQueueProducer(queues.Query1ResultsQueue, true).
		// State management
		WithStandardStateDirectory("/app/worker-data").
		WithMessageManager("/app/worker-data/processed-ids.txt")

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract consumer from builder
	consumer := builder.GetQueueConsumer(queues.ReplyFilterBusQueue)
	if consumer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get consumer from builder"))
	}

	// Extract producers from builder
	query2GroupByProducer := builder.GetQueueProducer(queues.Query2GroupByQueue)
	query3GroupByProducer := builder.GetQueueProducer(queues.Query3GroupByQueue)
	query4GroupByProducer := builder.GetQueueProducer(queues.Query4GroupByQueue)
	query1ResultsProducer := builder.GetQueueProducer(queues.Query1ResultsQueue)

	if query2GroupByProducer == nil || query3GroupByProducer == nil || query4GroupByProducer == nil || query1ResultsProducer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get producers from builder"))
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

	// Add CompletionCleaner with MessageManager as cleanup handler
	// Use WORKER_ID from environment (service name) for cleanup queue name
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return nil, fmt.Errorf("WORKER_ID environment variable is required")
	}
	builder.WithCompletionCleaner(
		queues.ClientCompletionCleanupExchange,
		workerID,
		[]completioncleaner.CleanupHandler{mm},
	)

	return &QueryGateway{
		consumer:              consumer,
		query2GroupByProducer: query2GroupByProducer,
		query3GroupByProducer: query3GroupByProducer,
		query4GroupByProducer: query4GroupByProducer,
		query1ResultsProducer: query1ResultsProducer,
		config:                config,
		messageManager:        mm,
	}, nil
}

// Start starts the query gateway
func (qg *QueryGateway) Start() middleware.MessageMiddlewareError {
	fmt.Println("Query Gateway: Starting to listen for messages from reply-filter-bus...")
	return qg.consumer.StartConsuming(qg.createCallback())
}

// Close closes all connections
func (qg *QueryGateway) Close() {
	if qg.messageManager != nil {
		qg.messageManager.Close()
	}
	if qg.consumer != nil {
		qg.consumer.Close()
	}
	if qg.query2GroupByProducer != nil {
		qg.query2GroupByProducer.Close()
	}
	if qg.query3GroupByProducer != nil {
		qg.query3GroupByProducer.Close()
	}
	if qg.query4GroupByProducer != nil {
		qg.query4GroupByProducer.Close()
	}
	if qg.query1ResultsProducer != nil {
		qg.query1ResultsProducer.Close()
	}
}

// createCallback creates the message processing callback
func (qg *QueryGateway) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Query Gateway: Starting to listen for messages...")
		for delivery := range *consumeChannel {
			// Deserialize the chunk message in the callback (middleware layer)
			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				fmt.Printf("Query Gateway: Failed to deserialize chunk message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}

			// Process with deserialized chunk
			if err := qg.processMessage(chunkMsg); err != 0 {
				fmt.Printf("Query Gateway: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
