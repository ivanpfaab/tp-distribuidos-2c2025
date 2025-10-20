package main

import (
	"fmt"
	"log"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
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
}

// NewInFileJoinOrchestrator creates a new in-file join orchestrator
func NewInFileJoinOrchestrator(config *middleware.ConnectionConfig) (*InFileJoinOrchestrator, error) {
	// Create consumer for chunk notifications from user partition writers
	consumer := workerqueue.NewQueueConsumer(
		queues.UserPartitionCompletionQueue, // Queue for receiving completion notifications
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create consumer")
	}

	// Declare the completion queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		queues.UserPartitionCompletionQueue,
		config,
	)
	if queueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer for user partition completion queue")
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare user partition completion queue: %v", err)
	}
	queueDeclarer.Close()

	// Create exchange producer for signaling user join workers
	completionProducer := exchange.NewMessageMiddlewareExchange(
		queues.UserIdCompletionExchange,
		[]string{queues.UserIdCompletionRoutingKey}, // Use fanout exchange to broadcast to all user join workers
		config,
	)
	if completionProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create completion exchange producer")
	}

	// Declare the exchange
	if err := completionProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		consumer.Close()
		completionProducer.Close()
		return nil, fmt.Errorf("failed to declare completion exchange: %v", err)
	}

	// Create completion tracker with callback
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
		},
	)

	return &InFileJoinOrchestrator{
		consumer:           consumer,
		completionProducer: completionProducer,
		config:             config,
		completionTracker:  completionTracker,
	}, nil
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

			// Process the chunk notification
			if err := o.completionTracker.ProcessChunkNotification(message); err != nil {
				log.Printf("Failed to process chunk notification: %v", err)
				delivery.Ack(false)
				continue
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
}
