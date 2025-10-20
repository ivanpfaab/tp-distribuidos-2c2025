package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

// GroupByWorker handles group by operations for partitioned chunks
type GroupByWorker struct {
	config                  *WorkerConfig
	consumer                *exchange.ExchangeConsumer
	orchestratorProducer    *workerqueue.QueueMiddleware
	workerIDStr             string
}

// NewGroupByWorker creates a new group by worker instance
func NewGroupByWorker(config *WorkerConfig) (*GroupByWorker, error) {
	// Create exchange consumer for the routing keys this worker handles
	consumer := exchange.NewExchangeConsumer(config.ExchangeName, config.RoutingKeys, config.ConnectionConfig)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create exchange consumer")
	}

	// Declare the topic exchange
	exchangeDeclarer := exchange.NewMessageMiddlewareExchange(config.ExchangeName, []string{}, config.ConnectionConfig)
	if exchangeDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create exchange declarer")
	}
	if err := exchangeDeclarer.DeclareExchange("topic", false, false, false, false); err != 0 {
		consumer.Close()
		exchangeDeclarer.Close()
		return nil, fmt.Errorf("failed to declare topic exchange: %v", err)
	}
	exchangeDeclarer.Close()

	// Create producer for orchestrator chunk notifications
	orchestratorProducer := workerqueue.NewMessageMiddlewareQueue(config.OrchestratorQueue, config.ConnectionConfig)
	if orchestratorProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create orchestrator producer")
	}

	// Declare orchestrator queue
	if err := orchestratorProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		orchestratorProducer.Close()
		return nil, fmt.Errorf("failed to declare orchestrator queue: %v", err)
	}

	// Generate worker ID string
	workerIDStr := fmt.Sprintf("query%d-groupby-worker-%d", config.QueryType, config.WorkerID)

	return &GroupByWorker{
		config:               config,
		consumer:             consumer,
		orchestratorProducer: orchestratorProducer,
		workerIDStr:          workerIDStr,
	}, nil
}

// Start begins processing messages from the exchange
func (w *GroupByWorker) Start() middleware.MessageMiddlewareError {
	testing_utils.LogInfo("GroupBy Worker", "Starting worker for query %d, listening to routing keys: %v",
		w.config.QueryType, w.config.RoutingKeys)

	// Start consuming messages
	return w.consumer.StartConsuming(w.createCallback())
}

// createCallback creates the message processing callback
func (w *GroupByWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		testing_utils.LogInfo("GroupBy Worker", "Callback started, waiting for messages...")
		messageCount := 0
		for delivery := range *consumeChannel {
			messageCount++
			testing_utils.LogInfo("GroupBy Worker", "Received message #%d", messageCount)

			// Process the message
			if err := w.processMessage(delivery.Body); err != nil {
				testing_utils.LogError("GroupBy Worker", "Failed to process message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		testing_utils.LogInfo("GroupBy Worker", "Consume channel closed after processing %d messages", messageCount)
		done <- nil
	}
}

// processMessage processes a single message
func (w *GroupByWorker) processMessage(messageBody []byte) error {
	// Deserialize the message
	message, err := deserializer.Deserialize(messageBody)
	if err != nil {
		testing_utils.LogWarn("GroupBy Worker", "Failed to deserialize message: %v", err)
		return err
	}

	// Check if it's a chunk message
	chunkMessage, ok := message.(*chunk.Chunk)
	if !ok {
		testing_utils.LogWarn("GroupBy Worker", "Received non-chunk message, skipping")
		return nil
	}

	// Process the chunk (dummy group by for now)
	return w.processChunk(chunkMessage)
}

// processChunk processes a single chunk with dummy group by logic
func (w *GroupByWorker) processChunk(chunkMessage *chunk.Chunk) error {
	testing_utils.LogInfo("GroupBy Worker", "Processing chunk %d from client %s, file %s (IsLastChunk=%t, IsLastFromTable=%t)",
		chunkMessage.ChunkNumber, chunkMessage.ClientID, chunkMessage.FileID,
		chunkMessage.IsLastChunk, chunkMessage.IsLastFromTable)

	// ===== DUMMY GROUP BY LOGIC =====
	// For now, we just log that we received the chunk
	// In the future, this will perform actual group by aggregation
	testing_utils.LogInfo("GroupBy Worker", "Dummy group by processing for chunk %d (TableID=%d, ChunkSize=%d)",
		chunkMessage.ChunkNumber, chunkMessage.TableID, chunkMessage.ChunkSize)

	// ===== SEND CHUNK NOTIFICATION TO ORCHESTRATOR =====
	return w.sendChunkNotification(chunkMessage)
}

// sendChunkNotification sends a chunk notification to the orchestrator
func (w *GroupByWorker) sendChunkNotification(chunkMessage *chunk.Chunk) error {
	// Create chunk notification
	notification := signals.NewChunkNotification(
		chunkMessage.ClientID,
		chunkMessage.FileID,
		w.workerIDStr,
		chunkMessage.TableID,
		chunkMessage.ChunkNumber,
		chunkMessage.IsLastChunk,
		chunkMessage.IsLastFromTable,
	)

	// Serialize notification
	serializedNotification, err := signals.SerializeChunkNotification(notification)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk notification: %v", err)
	}

	// Send to orchestrator
	if sendErr := w.orchestratorProducer.Send(serializedNotification); sendErr != 0 {
		return fmt.Errorf("failed to send chunk notification to orchestrator: %v", sendErr)
	}

	testing_utils.LogInfo("GroupBy Worker", "Sent chunk notification to orchestrator for chunk %d (ClientID=%s, FileID=%s)",
		chunkMessage.ChunkNumber, chunkMessage.ClientID, chunkMessage.FileID)

	return nil
}

// Close closes the worker
func (w *GroupByWorker) Close() {
	if w.consumer != nil {
		w.consumer.Close()
	}
	if w.orchestratorProducer != nil {
		w.orchestratorProducer.Close()
	}
}
