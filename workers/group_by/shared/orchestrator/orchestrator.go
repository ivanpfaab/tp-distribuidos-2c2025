package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// TerminationSignal represents a signal to terminate processing
type TerminationSignal struct {
	QueryType int
	ClientID  string
	Message   string
}

// GroupByOrchestrator manages the coordination of map-reduce operations
type GroupByOrchestrator struct {
	config              *OrchestratorConfig
	completionTracker   *shared.CompletionTracker
	chunkConsumer       *workerqueue.QueueConsumer
	terminationProducer *exchange.ExchangeMiddleware
	completionProducer  *workerqueue.QueueMiddleware // For sending completion chunks to next step
}

// NewGroupByOrchestrator creates a new group by orchestrator
func NewGroupByOrchestrator(queryType int) *GroupByOrchestrator {
	log.Printf("NewGroupByOrchestrator: Query Type %d", queryType)
	config := NewOrchestratorConfig(queryType)

	orchestrator := &GroupByOrchestrator{
		config: config,
	}

	// Create completion tracker with callback to send termination signals
	trackerName := fmt.Sprintf("Query%d-Orchestrator", queryType)
	orchestrator.completionTracker = shared.NewCompletionTracker(trackerName, orchestrator.onClientCompleted)

	orchestrator.initializeQueues()
	return orchestrator
}

// initializeQueues sets up all necessary queues and exchanges
func (gbo *GroupByOrchestrator) initializeQueues() {
	// Create consumer for chunk notifications from map workers
	queueName := getChunkNotificationQueueName(gbo.config.QueryType)
	gbo.chunkConsumer = workerqueue.NewQueueConsumer(queueName, gbo.config.RabbitMQConfig)
	if gbo.chunkConsumer == nil {
		log.Fatalf("Failed to create chunk notification consumer for queue: %s", queueName)
	}

	// Declare the chunk notification queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(queueName, gbo.config.RabbitMQConfig)
	if queueDeclarer == nil {
		gbo.chunkConsumer.Close()
		log.Fatalf("Failed to create queue declarer for queue: %s", queueName)
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		gbo.chunkConsumer.Close()
		queueDeclarer.Close()
		log.Fatalf("Failed to declare chunk notification queue %s: %v", queueName, err)
	}
	queueDeclarer.Close()

	// Create fanout exchange producer for termination signals to map workers
	terminationExchangeName := getMapWorkerTerminationExchangeName(gbo.config.QueryType)
	gbo.terminationProducer = exchange.NewMessageMiddlewareExchange(terminationExchangeName, []string{}, gbo.config.RabbitMQConfig)
	if gbo.terminationProducer == nil {
		gbo.chunkConsumer.Close()
		log.Fatalf("Failed to create termination exchange producer")
	}

	// Declare the termination exchange
	if err := gbo.terminationProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		gbo.chunkConsumer.Close()
		gbo.terminationProducer.Close()
		log.Fatalf("Failed to declare termination exchange: %v", err)
	}

	// Create completion chunk producer for next step
	completionQueueName := getCompletionQueueName(gbo.config.QueryType)
	gbo.completionProducer = workerqueue.NewMessageMiddlewareQueue(completionQueueName, gbo.config.RabbitMQConfig)
	if gbo.completionProducer == nil {
		gbo.chunkConsumer.Close()
		gbo.terminationProducer.Close()
		log.Fatalf("Failed to create completion chunk producer for queue: %s", completionQueueName)
	}

	log.Printf("Orchestrator initialized for Query %d (expected files will be inferred dynamically)",
		gbo.config.QueryType)
	log.Printf("Completion chunks will be sent to queue: %s", completionQueueName)
}

// onClientCompleted is called when all files for a client are completed
func (gbo *GroupByOrchestrator) onClientCompleted(clientID string, clientStatus *shared.ClientStatus) {
	log.Printf("Client %s: All %d files completed! Sending completion chunk to next step...",
		clientID, clientStatus.TotalExpectedFiles)

	// Send completion chunk to next step
	if err := gbo.sendCompletionChunk(clientID); err != nil {
		log.Printf("Failed to send completion chunk for client %s: %v", clientID, err)
		return
	}

	// Clear client state from completion tracker
	gbo.completionTracker.ClearClientState(clientID)

	log.Printf("Client %s: Completion chunk sent and state cleared", clientID)
}

// sendTerminationSignals sends termination signals to all map and reduce workers
func (gbo *GroupByOrchestrator) sendTerminationSignals(clientID string) {
	terminationSignal := TerminationSignal{
		QueryType: gbo.config.QueryType,
		ClientID:  clientID,
		Message:   fmt.Sprintf("All data processing completed for client %s", clientID),
	}

	// Serialize termination signal
	signalData, err := json.Marshal(terminationSignal)
	if err != nil {
		log.Printf("Failed to serialize termination signal: %v", err)
		return
	}

	// Send to map workers
	log.Printf("Sending termination signal to map workers for Query %d", gbo.config.QueryType)
	if err := gbo.terminationProducer.Send(signalData, []string{}); err != 0 {
		log.Printf("Failed to send termination signal to map workers: %v", err)
	}

	log.Printf("Termination signals sent successfully for Query %d", gbo.config.QueryType)
}

// sendCompletionChunk sends an empty completion chunk to the next processing step
func (gbo *GroupByOrchestrator) sendCompletionChunk(clientID string) error {
	// Create empty completion chunk with chunkNumber = -1, isLastChunk = True
	// We use empty values for FileID since this is a completion signal
	completionChunk := chunk.NewChunk(
		clientID,            // ClientID
		"DONE",              // FileID - using "DONE" as a marker
		byte(gbo.config.QueryType), // QueryType
		-1,                  // ChunkNumber = -1 to indicate completion
		true,                // IsLastChunk = True
		true,                // IsLastFromTable = True
		0,                   // Step
		0,                   // ChunkSize = 0 (empty chunk)
		0,                   // TableID
		"",                  // ChunkData = empty
	)

	// Create chunk message
	completionMessage := chunk.NewChunkMessage(completionChunk)

	// Serialize the chunk
	serializedChunk, err := chunk.SerializeChunkMessage(completionMessage)
	if err != nil {
		return fmt.Errorf("failed to serialize completion chunk: %v", err)
	}

	// Send to the appropriate queue based on query type
	if sendErr := gbo.completionProducer.Send(serializedChunk); sendErr != 0 {
		return fmt.Errorf("failed to send completion chunk: error code %v", sendErr)
	}

	completionQueueName := getCompletionQueueName(gbo.config.QueryType)
	log.Printf("Sent completion chunk for client %s to queue %s", clientID, completionQueueName)

	return nil
}

// Start starts the orchestrator
func (gbo *GroupByOrchestrator) Start() {
	log.Printf("Starting Group By Orchestrator for Query %d...", gbo.config.QueryType)

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("Orchestrator started consuming chunk notifications")

		for delivery := range *consumeChannel {
			log.Printf("Received chunk notification: %d bytes", len(delivery.Body))

			// Deserialize chunk notification using protocol
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				log.Printf("Failed to deserialize chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			notification, ok := message.(*signals.ChunkNotification)
			if !ok {
				log.Printf("Received message is not a ChunkNotification: %T", message)
				delivery.Ack(false)
				continue
			}

			// Process the notification using the completion tracker
			if err := gbo.completionTracker.ProcessChunkNotification(notification); err != nil {
				log.Printf("Failed to process chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	queueName := getChunkNotificationQueueName(gbo.config.QueryType)
	if err := gbo.chunkConsumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming chunk notifications: %v", err)
	}

	log.Printf("Successfully started consuming from queue: %s", queueName)
}

// Close closes the orchestrator
func (gbo *GroupByOrchestrator) Close() {
	if gbo.chunkConsumer != nil {
		gbo.chunkConsumer.Close()
	}
	if gbo.terminationProducer != nil {
		gbo.terminationProducer.Close()
	}
	if gbo.completionProducer != nil {
		gbo.completionProducer.Close()
	}
}

// getChunkNotificationQueueName returns the queue name for chunk notifications
func getChunkNotificationQueueName(queryType int) string {
	return fmt.Sprintf("query%d-orchestrator-chunks", queryType)
}

// getMapWorkerTerminationExchangeName returns the exchange name for map worker termination
func getMapWorkerTerminationExchangeName(queryType int) string {
	return fmt.Sprintf("query%d-map-termination", queryType)
}

// getCompletionQueueName returns the queue name for completion chunks based on query type
func getCompletionQueueName(queryType int) string {
	switch queryType {
	case 2:
		return queues.Query2GroupByResultsQueue
	case 3:
		return queues.Query3GroupByResultsQueue
	case 4:
		return queues.Query4GroupByResultsQueue
	default:
		log.Fatalf("Unknown query type: %d", queryType)
		return ""
	}
}
