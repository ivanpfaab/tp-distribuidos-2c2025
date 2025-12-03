package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
)

// PartitionerWorker handles partitioning of chunks based on query type
type PartitionerWorker struct {
	config         *PartitionerConfig
	consumer       *workerqueue.QueueConsumer
	processor      *PartitionerProcessor
	messageManager *messagemanager.MessageManager
}

// NewPartitionerWorker creates a new partitioner worker instance
func NewPartitionerWorker(config *PartitionerConfig) (*PartitionerWorker, error) {
	// Use builder to create queue consumer and MessageManager
	builder := worker_builder.NewWorkerBuilder("Partitioner Worker").
		WithConfig(config.ConnectionConfig).
		WithQueueConsumer(config.QueueName, true). // auto-declare
		WithStandardStateDirectory("/app/worker-data").
		WithMessageManager("/app/worker-data/processed-ids.txt")

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract consumer from builder
	consumer := builder.GetQueueConsumer(config.QueueName)
	if consumer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get consumer from builder"))
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

	// Create processor with partitioning configuration
	processor, err := NewPartitionerProcessor(config.QueryType, config.NumPartitions, config.NumWorkers, config.ConnectionConfig)
	if err != nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to create processor: %v", err))
	}

	return &PartitionerWorker{
		config:         config,
		consumer:       consumer,
		processor:      processor,
		messageManager: mm,
	}, nil
}

// Start begins processing messages from the queue
func (w *PartitionerWorker) Start() middleware.MessageMiddlewareError {
	testing_utils.LogInfo("Partitioner Worker", "Starting partitioner for query type %d with %d partitions",
		w.config.QueryType, w.config.NumPartitions)

	// Start consuming messages
	return w.consumer.StartConsuming(w.createCallback())
}

// createCallback creates the message processing callback
func (w *PartitionerWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		testing_utils.LogInfo("Partitioner Worker", "Callback started, waiting for messages...")
		messageCount := 0
		for delivery := range *consumeChannel {
			messageCount++
			testing_utils.LogInfo("Partitioner Worker", "Received message #%d", messageCount)

			// Deserialize the message in the callback (middleware layer)
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				testing_utils.LogWarn("Partitioner Worker", "Failed to deserialize message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}

			chunkMessage, _ := message.(*chunk.Chunk)

			// Process with deserialized chunk
			if err := w.processMessage(chunkMessage); err != nil {
				testing_utils.LogError("Partitioner Worker", "Failed to process message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		testing_utils.LogInfo("Partitioner Worker", "Consume channel closed after processing %d messages", messageCount)
		done <- nil
	}
}

// processMessage processes a single message
func (w *PartitionerWorker) processMessage(chunkMessage *chunk.Chunk) error {

	// Check if already processed
	if w.messageManager.IsProcessed(chunkMessage.ClientID, chunkMessage.ID) {
		testing_utils.LogInfo("Partitioner Worker", "Chunk ID %s already processed, skipping", chunkMessage.ID)
		return nil
	}

	// Process the chunk with partitioning
	if err := w.processor.ProcessChunk(chunkMessage); err != nil {
		return err
	}

	// Mark as processed (must be after successful processing)
	if err := w.messageManager.MarkProcessed(chunkMessage.ClientID, chunkMessage.ID); err != nil {
		testing_utils.LogError("Partitioner Worker", "Failed to mark chunk as processed: %v", err)
		return fmt.Errorf("failed to mark chunk as processed: %v", err)
	}

	return nil
}

// Close closes the partitioner worker
func (w *PartitionerWorker) Close() {
	if w.messageManager != nil {
		w.messageManager.Close()
	}
	if w.consumer != nil {
		w.consumer.Close()
	}
}
