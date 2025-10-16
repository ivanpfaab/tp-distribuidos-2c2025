package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// ChunkNotification is now defined in the protocol/signals package

// TerminationSignal represents a signal to terminate processing for a specific client
type TerminationSignal struct {
	QueryType int
	ClientID  string
	Message   string
}

// OrchestratorCommunicator handles communication with the orchestrator
type OrchestratorCommunicator struct {
	chunkNotificationProducer *workerqueue.QueueMiddleware
	terminationConsumer       *exchange.ExchangeConsumer
	mapWorkerID               string
	config                    *middleware.ConnectionConfig
}

// NewOrchestratorCommunicator creates a new orchestrator communicator
func NewOrchestratorCommunicator(mapWorkerID string, config *middleware.ConnectionConfig) *OrchestratorCommunicator {
	// Create producer for chunk notifications
	chunkNotificationProducer := workerqueue.NewMessageMiddlewareQueue(queues.Query3OrchestratorChunksQueue, config)
	if chunkNotificationProducer == nil {
		log.Fatalf("Failed to create chunk notification producer")
	}

	// Declare the chunk notification queue
	if err := chunkNotificationProducer.DeclareQueue(false, false, false, false); err != 0 {
		chunkNotificationProducer.Close()
		log.Fatalf("Failed to declare chunk notification queue: %v", err)
	}

	// Create consumer for termination signals (fanout exchange)
	// For fanout exchanges, we need to use a special approach since ExchangeConsumer expects routing keys
	terminationConsumer := exchange.NewExchangeConsumer(queues.Query3MapTerminationExchange, []string{""}, config)
	if terminationConsumer == nil {
		chunkNotificationProducer.Close()
		log.Fatalf("Failed to create termination signal consumer")
	}

	// Declare the termination exchange (fanout). TODO CHECK IF WE SHOULD USE []string{""} OR []string{}
	exchangeDeclarer := exchange.NewMessageMiddlewareExchange(queues.Query3MapTerminationExchange, []string{}, config)
	if exchangeDeclarer == nil {
		chunkNotificationProducer.Close()
		terminationConsumer.Close()
		log.Fatalf("Failed to create exchange declarer for exchange: %s", queues.Query3MapTerminationExchange)
	}
	if err := exchangeDeclarer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		chunkNotificationProducer.Close()
		terminationConsumer.Close()
		exchangeDeclarer.Close()
		log.Fatalf("Failed to declare termination exchange %s: %v", queues.Query3MapTerminationExchange, err)
	}
	exchangeDeclarer.Close() // Close the declarer as we don't need it anymore\

	return &OrchestratorCommunicator{
		chunkNotificationProducer: chunkNotificationProducer,
		terminationConsumer:       terminationConsumer,
		mapWorkerID:               mapWorkerID,
		config:                    config,
	}
}

// NotifyChunkProcessed sends a notification to the orchestrator about a processed chunk
func (oc *OrchestratorCommunicator) NotifyChunkProcessed(chunk *chunk.Chunk) error {
	notification := signals.NewChunkNotification(
		chunk.ClientID,
		chunk.FileID,
		oc.mapWorkerID,
		chunk.TableID,
		chunk.ChunkNumber,
		chunk.IsLastChunk,
	)

	// Serialize notification using protocol
	notificationData, err := signals.SerializeChunkNotification(notification)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk notification: %v", err)
	}

	// Send notification
	if err := oc.chunkNotificationProducer.Send(notificationData); err != 0 {
		return fmt.Errorf("failed to send chunk notification: error code %v", err)
	}

	log.Printf("Notified orchestrator about chunk %d from file %s (Table %d)",
		chunk.ChunkNumber, chunk.FileID, chunk.TableID)

	return nil
}

// StartTerminationListener starts listening for termination signals
func (oc *OrchestratorCommunicator) StartTerminationListener(terminationCallback func(*TerminationSignal)) {
	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("Map worker %s started listening for termination signals", oc.mapWorkerID)

		for delivery := range *consumeChannel {
			log.Printf("Map worker %s received termination signal: %d bytes", oc.mapWorkerID, len(delivery.Body))

			// Deserialize termination signal
			var signal TerminationSignal
			if err := json.Unmarshal(delivery.Body, &signal); err != nil {
				log.Printf("Failed to deserialize termination signal: %v", err)
				delivery.Ack(false)
				continue
			}

			log.Printf("Map worker %s received termination signal for Query %d, Client %s: %s",
				oc.mapWorkerID, signal.QueryType, signal.ClientID, signal.Message)

			// Call the termination callback with the signal
			terminationCallback(&signal)

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	if err := oc.terminationConsumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming termination signals: %v", err)
	}
}

// Close closes the orchestrator communicator
func (oc *OrchestratorCommunicator) Close() {
	if oc.chunkNotificationProducer != nil {
		oc.chunkNotificationProducer.Close()
	}
	if oc.terminationConsumer != nil {
		oc.terminationConsumer.Close()
	}
}
