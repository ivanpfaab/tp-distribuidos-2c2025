package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	completioncleaner "github.com/tp-distribuidos-2c2025/shared/completion_cleaner"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	"github.com/tp-distribuidos-2c2025/shared/testing"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// InMemoryJoinOrchestrator handles completion tracking and resource cleanup for in-memory joins
type InMemoryJoinOrchestrator struct {
	consumer          *workerqueue.QueueConsumer
	itemIdProducer    *exchange.ExchangeMiddleware
	storeIdProducer   *exchange.ExchangeMiddleware
	completionTracker *shared.CompletionTracker
	workerID          string

	// Fault tolerance components
	messageManager *messagemanager.MessageManager
	stateManager   *StateManager
}

// NewInMemoryJoinOrchestrator creates a new InMemoryJoinOrchestrator instance
func NewInMemoryJoinOrchestrator(config *middleware.ConnectionConfig) (*InMemoryJoinOrchestrator, error) {
	// Use builder to create all resources
	metadataDir := "/app/orchestrator-data/metadata"
	processedNotificationsPath := "/app/orchestrator-data/processed-notifications.txt"

	builder := worker_builder.NewWorkerBuilder("In-Memory Join Orchestrator").
		WithConfig(config).
		// Queue consumer
		WithQueueConsumer(queues.InMemoryJoinCompletionQueue, true).
		// Exchange producers (fanout exchanges for broadcasting to all join workers)
		WithExchangeProducer(queues.ItemIdCompletionExchange, []string{queues.ItemIdCompletionRoutingKey}, true, worker_builder.ExchangeDeclarationOptions{
			Type: "fanout",
		}).
		WithExchangeProducer(queues.StoreIdCompletionExchange, []string{queues.StoreIdCompletionRoutingKey}, true, worker_builder.ExchangeDeclarationOptions{
			Type: "fanout",
		}).
		// State management
		WithDirectory(metadataDir, 0755).
		WithMessageManager(processedNotificationsPath)

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract resources from builder
	consumer := builder.GetQueueConsumer(queues.InMemoryJoinCompletionQueue)
	itemIdProducer := builder.GetExchangeProducer(queues.ItemIdCompletionExchange)
	storeIdProducer := builder.GetExchangeProducer(queues.StoreIdCompletionExchange)

	if consumer == nil || itemIdProducer == nil || storeIdProducer == nil {
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

	// Add CompletionCleaner with MessageManager as cleanup handler
	// Use WORKER_ID from environment (service name) for cleanup queue name
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return nil, builder.CleanupOnError(fmt.Errorf("WORKER_ID environment variable is required"))
	}
	builder.WithCompletionCleaner(
		queues.ClientCompletionCleanupExchange,
		workerID,
		[]completioncleaner.CleanupHandler{mm},
	)

	// Create state manager first (completion tracker will be set after creation)
	stateManager := NewStateManager(metadataDir, nil)

	// Create orchestrator instance first (will be used in callback)
	orchestrator := &InMemoryJoinOrchestrator{
		consumer:        consumer,
		itemIdProducer:  itemIdProducer,
		storeIdProducer: storeIdProducer,
		workerID:        workerID,
		messageManager:  mm,
		stateManager:    stateManager,
	}

	// Create completion tracker with callback for StoreID completion
	completionTracker := shared.NewCompletionTracker("in-memory-join-orchestrator", func(clientID string, clientStatus *shared.ClientStatus) {
		testing.LogInfo("In-Memory Join Orchestrator", "StoreID client %s completed", clientID)

		// Send completion signal for StoreID (query type 3)
		orchestrator.sendCompletionSignal(clientID, 3, orchestrator.storeIdProducer)

		// Delete CSV metadata file for completed client
		if err := stateManager.DeleteClientMetadata(clientID); err != nil {
			testing.LogWarn("In-Memory Join Orchestrator", "Failed to delete metadata file for client %s: %v", clientID, err)
		}
	})

	// Set completion tracker in orchestrator and state manager
	orchestrator.completionTracker = completionTracker
	stateManager.completionTracker = completionTracker

	// Rebuild state from CSV metadata on startup
	if err := orchestrator.stateManager.RebuildState(); err != nil {
		testing.LogWarn("In-Memory Join Orchestrator", "Failed to rebuild state from CSV: %v", err)
	}

	return orchestrator, nil
}

// Start starts the in-memory join orchestrator
func (imo *InMemoryJoinOrchestrator) Start() middleware.MessageMiddlewareError {
	testing.LogInfo("In-Memory Join Orchestrator", "Starting...")
	return imo.consumer.StartConsuming(imo.createCallback())
}

// Close closes all connections
func (imo *InMemoryJoinOrchestrator) Close() {
	testing.LogInfo("In-Memory Join Orchestrator", "Shutting down...")

	if imo.consumer != nil {
		imo.consumer.Close()
	}
	if imo.itemIdProducer != nil {
		imo.itemIdProducer.Close()
	}
	if imo.storeIdProducer != nil {
		imo.storeIdProducer.Close()
	}

	if imo.messageManager != nil {
		imo.messageManager.Close()
	}

	testing.LogInfo("In-Memory Join Orchestrator", "Shutdown complete")
}

// createCallback creates the message processing callback
func (imo *InMemoryJoinOrchestrator) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			// Deserialize the message using the deserializer
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				testing.LogError("In-Memory Join Orchestrator", "Failed to deserialize message: %v", err)
				delivery.Ack(false)
				continue
			}

			// Handle chunk notification from join workers
			msg, ok := message.(*signals.ChunkNotification)
			if !ok {
				testing.LogWarn("In-Memory Join Orchestrator", "Received unknown message type: %T", message)
				delivery.Ack(false)
				continue
			}

			// Process with deserialized notification
			if err := imo.processChunkNotification(msg); err != 0 {
				testing.LogError("In-Memory Join Orchestrator", "Error processing chunk notification: %v", err)
				// Determine if we should nack or ack based on error type
				if err == middleware.MessageMiddlewareMessageError {
					delivery.Nack(false, true)
				} else {
					delivery.Ack(false)
				}
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// processChunkNotification processes a chunk completion notification
func (imo *InMemoryJoinOrchestrator) processChunkNotification(msg *signals.ChunkNotification) middleware.MessageMiddlewareError {
	testing.LogInfo("In-Memory Join Orchestrator", "Received chunk notification - ClientID: %s, FileID: %s, MapWorkerID: %s",
		msg.ClientID, msg.FileID, msg.MapWorkerID)

	// Check for duplicate notification
	if imo.messageManager.IsProcessed(msg.ClientID, msg.ID) {
		return 0
	}

	// Handle ItemID worker notifications immediately (no file tracking needed)
	if strings.Contains(msg.MapWorkerID, "itemid-join-worker") {
		if err := imo.messageManager.MarkProcessed(msg.ClientID, msg.ID); err != nil {
			testing.LogError("In-Memory Join Orchestrator", "Failed to mark notification as processed: %v", err)
			return middleware.MessageMiddlewareMessageError
		}

		imo.sendCompletionSignal(msg.ClientID, 2, imo.itemIdProducer)
		return 0
	}

	// Handle StoreID worker notifications with CompletionTracker and CSV persistence
	if err := imo.completionTracker.ProcessChunkNotification(msg); err != nil {
		testing.LogError("In-Memory Join Orchestrator", "Failed to process chunk notification: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	if err := imo.stateManager.AppendNotification(msg); err != nil {
		testing.LogWarn("In-Memory Join Orchestrator", "Failed to append notification to CSV: %v", err)
	}

	if err := imo.messageManager.MarkProcessed(msg.ClientID, msg.ID); err != nil {
		testing.LogWarn("In-Memory Join Orchestrator", "Failed to mark notification as processed: %v", err)
	}

	return 0
}

// sendCompletionSignal sends a completion signal to the appropriate workers
func (imo *InMemoryJoinOrchestrator) sendCompletionSignal(clientID string, queryType int, producer *exchange.ExchangeMiddleware) {
	var resourceType string
	var routingKey string

	switch queryType {
	case 2:
		resourceType = "itemid"
		routingKey = queues.ItemIdCompletionRoutingKey
	case 3:
		resourceType = "storeid"
		routingKey = queues.StoreIdCompletionRoutingKey
	default:
		testing.LogWarn("In-Memory Join Orchestrator", "Unknown query type %d for client %s", queryType, clientID)
		return
	}

	completionSignal := signals.NewJoinCompletionSignal(clientID, resourceType, imo.workerID)
	messageData, err := signals.SerializeJoinCompletionSignal(completionSignal)
	if err != nil {
		testing.LogError("In-Memory Join Orchestrator", "Failed to serialize completion signal: %v", err)
		return
	}

	if err := producer.Send(messageData, []string{routingKey}); err != 0 {
		testing.LogError("In-Memory Join Orchestrator", "Failed to send completion signal for client %s: %v", clientID, err)
	} else {
		testing.LogInfo("In-Memory Join Orchestrator", "Sent completion signal for client %s", clientID)
	}
}
