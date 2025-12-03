package main

import (
	"fmt"
	"log"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// InFileJoinOrchestrator manages the completion tracking and signaling for in-file joins
type InFileJoinOrchestrator struct {
	// Consumer for chunk notifications from user partition writers
	consumer *workerqueue.QueueConsumer

	// Exchange producer for signaling user join workers
	completionProducer *exchange.ExchangeMiddleware

	// Configuration
	config *middleware.ConnectionConfig

	// Completion tracker
	completionTracker *shared.CompletionTracker

	// Fault tolerance components
	messageManager *messagemanager.MessageManager
	stateManager   *StateManager
}

// NewInFileJoinOrchestrator creates a new in-file join orchestrator
func NewInFileJoinOrchestrator(config *middleware.ConnectionConfig) (*InFileJoinOrchestrator, error) {
	// Use builder to create all resources
	metadataDir := "/app/orchestrator-data/metadata"
	processedNotificationsPath := "/app/orchestrator-data/processed-notifications.txt"

	builder := worker_builder.NewWorkerBuilder("In-File Join Orchestrator").
		WithConfig(config).
		// Queue consumer
		WithQueueConsumer(queues.UserPartitionCompletionQueue, true).
		// Exchange producer (fanout exchange for broadcasting to all user join workers)
		WithExchangeProducer(queues.UserIdCompletionExchange, []string{queues.UserIdCompletionRoutingKey}, true, worker_builder.ExchangeDeclarationOptions{
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
	consumer := builder.GetQueueConsumer(queues.UserPartitionCompletionQueue)
	completionProducer := builder.GetExchangeProducer(queues.UserIdCompletionExchange)

	if consumer == nil || completionProducer == nil {
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

	// Create state manager first (completion tracker will be set after creation)
	stateManager := NewStateManager(metadataDir, nil)

	// Create completion tracker with callback that uses state manager
	completionTracker := shared.NewCompletionTracker(
		"InFileJoinOrchestrator",
		func(clientID string, clientStatus *shared.ClientStatus) {
			// Add a small delay to ensure all files are fully written to disk
			log.Printf("All files completed for client %s, waiting 200ms to ensure file sync...", clientID)
			time.Sleep(200 * time.Millisecond)

			// Send completion signal to all user join workers
			if err := sendCompletionSignal(completionProducer, clientID); err != nil {
				log.Printf("Failed to send completion signal for client %s: %v", clientID, err)
			} else {
				log.Printf("Sent completion signal for client %s to all user join workers", clientID)
			}

			// Delete CSV metadata file for completed client
			if err := stateManager.DeleteClientMetadata(clientID); err != nil {
				log.Printf("Warning: failed to delete metadata file for client %s: %v", clientID, err)
			} else {
				log.Printf("Deleted metadata file for completed client %s", clientID)
			}
		},
	)

	// Set completion tracker in state manager
	stateManager.completionTracker = completionTracker

	orchestrator := &InFileJoinOrchestrator{
		consumer:           consumer,
		completionProducer: completionProducer,
		config:             config,
		completionTracker:  completionTracker,
		messageManager:     mm,
		stateManager:       stateManager,
	}

	// Rebuild state from CSV metadata on startup
	if err := orchestrator.stateManager.RebuildState(); err != nil {
		log.Printf("Warning: failed to rebuild state from CSV: %v", err)
	}

	return orchestrator, nil
}

// Start starts the orchestrator
func (o *InFileJoinOrchestrator) Start() {
	log.Println("In-File Join Orchestrator: Starting...")

	// Start consuming chunk notifications
	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			// Deserialize the chunk notification
			message, err := signals.DeserializeChunkNotification(delivery.Body)
			if err != nil {
				log.Printf("Failed to deserialize chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Check for duplicate notification
			if o.messageManager.IsProcessed(message.ClientID, message.ID) {
				log.Printf("Notification %s already processed, skipping", message.ID)
				delivery.Ack(false)
				continue
			}

			// Process the chunk notification
			if err := o.completionTracker.ProcessChunkNotification(message); err != nil {
				log.Printf("Failed to process chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Append notification to CSV for state rebuild
			if err := o.stateManager.AppendNotification(message); err != nil {
				log.Printf("Warning: failed to append notification to CSV: %v", err)
			}

			// Mark as processed in MessageManager
			if err := o.messageManager.MarkProcessed(message.ClientID, message.ID); err != nil {
				log.Printf("Warning: failed to mark notification as processed: %v", err)
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	if err := o.consumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming: %v", err)
	}
}

// sendCompletionSignal sends a completion signal to all user join workers
func sendCompletionSignal(producer *exchange.ExchangeMiddleware, clientID string) error {
	// Create completion signal
	signal := signals.NewJoinCompletionSignal(
		clientID,
		"user-files",
		"in-file-join-orchestrator",
	)

	// Serialize the signal
	messageData, err := signals.SerializeJoinCompletionSignal(signal)
	if err != nil {
		return fmt.Errorf("failed to serialize completion signal: %w", err)
	}

	// Send to exchange (fanout will broadcast to all user join workers)
	if err := producer.Send(messageData, []string{queues.UserIdCompletionRoutingKey}); err != 0 {
		return fmt.Errorf("failed to send completion signal: %v", err)
	}
	return nil
}

// Close closes all connections
func (o *InFileJoinOrchestrator) Close() {
	if o.consumer != nil {
		o.consumer.Close()
	}
	if o.completionProducer != nil {
		o.completionProducer.Close()
	}
	if o.messageManager != nil {
		o.messageManager.Close()
	}
}
