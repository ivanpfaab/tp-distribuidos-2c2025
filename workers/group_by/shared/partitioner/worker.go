package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
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
	// Create queue consumer
	consumer := workerqueue.NewQueueConsumer(config.QueueName, config.ConnectionConfig)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create queue consumer")
	}

	// Declare the input queue (following the pattern from time-filter/worker.go)
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(config.QueueName, config.ConnectionConfig)
	if queueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare input queue: %v", err)
	}
	queueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create processor with partitioning configuration
	processor, err := NewPartitionerProcessor(config.QueryType, config.NumPartitions, config.NumWorkers, config.ConnectionConfig)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create processor: %v", err)
	}

	// Initialize MessageManager for fault tolerance
	messageManager := messagemanager.NewMessageManager("/app/worker-data/processed-ids.txt")
	if err := messageManager.LoadProcessedIDs(); err != nil {
		testing_utils.LogWarn("Partitioner Worker", "Failed to load processed IDs: %v (starting with empty state)", err)
	} else {
		count := messageManager.GetProcessedCount()
		testing_utils.LogInfo("Partitioner Worker", "Loaded %d processed IDs", count)
	}

	return &PartitionerWorker{
		config:         config,
		consumer:       consumer,
		processor:      processor,
		messageManager: messageManager,
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

			// Process the message
			if err := w.processMessage(delivery.Body); err != nil {
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
func (w *PartitionerWorker) processMessage(messageBody []byte) error {
	// Deserialize the message
	message, err := deserializer.Deserialize(messageBody)
	if err != nil {
		testing_utils.LogWarn("Partitioner Worker", "Failed to deserialize message: %v", err)
		return err
	}

	// Check if it's a chunk message
	chunkMessage, ok := message.(*chunk.Chunk)
	if !ok {
		testing_utils.LogWarn("Partitioner Worker", "Received non-chunk message, skipping")
		return nil
	}

	// Check if already processed
	if w.messageManager.IsProcessed(chunkMessage.ID) {
		testing_utils.LogInfo("Partitioner Worker", "Chunk ID %s already processed, skipping", chunkMessage.ID)
		return nil
	}

	// Process the chunk with partitioning
	if err := w.processor.ProcessChunk(chunkMessage); err != nil {
		return err
	}

	// Mark as processed (must be after successful processing)
	if err := w.messageManager.MarkProcessed(chunkMessage.ID); err != nil {
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
