package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// InMemoryJoinOrchestrator handles completion tracking and resource cleanup for in-memory joins
type InMemoryJoinOrchestrator struct {
	consumer          *workerqueue.QueueConsumer
	itemIdProducer    *exchange.ExchangeMiddleware
	storeIdProducer   *exchange.ExchangeMiddleware
	completionTracker *shared.CompletionTracker
	config            *middleware.ConnectionConfig
	workerID          string
}

// NewInMemoryJoinOrchestrator creates a new InMemoryJoinOrchestrator instance
func NewInMemoryJoinOrchestrator(config *middleware.ConnectionConfig) (*InMemoryJoinOrchestrator, error) {
	// Create consumer for completion notifications
	consumer := workerqueue.NewQueueConsumer(
		queues.InMemoryJoinCompletionQueue,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create completion consumer")
	}

	// Declare the completion queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		queues.InMemoryJoinCompletionQueue,
		config,
	)
	if queueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare completion queue: %v", err)
	}
	queueDeclarer.Close()

	// Create ItemID completion signal producer
	itemIdProducer := exchange.NewMessageMiddlewareExchange(
		queues.ItemIdCompletionExchange,
		[]string{queues.ItemIdCompletionRoutingKey},
		config,
	)
	if itemIdProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create ItemID completion producer")
	}

	// Create StoreID completion signal producer
	storeIdProducer := exchange.NewMessageMiddlewareExchange(
		queues.StoreIdCompletionExchange,
		[]string{queues.StoreIdCompletionRoutingKey},
		config,
	)
	if storeIdProducer == nil {
		consumer.Close()
		itemIdProducer.Close()
		return nil, fmt.Errorf("failed to create StoreID completion producer")
	}

	// Declare exchanges
	if err := itemIdProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		return nil, fmt.Errorf("failed to declare ItemID completion exchange: %v", err)
	}

	if err := storeIdProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		return nil, fmt.Errorf("failed to declare StoreID completion exchange: %v", err)
	}

	// Create completion tracker
	completionTracker := shared.NewCompletionTracker("in-memory-join-orchestrator", func(clientID string, clientStatus *shared.ClientStatus) {
		fmt.Printf("In-Memory Join Orchestrator: Client %s completed\n", clientID)
	})

	// Generate worker ID
	workerID := "in-memory-join-orchestrator"

	return &InMemoryJoinOrchestrator{
		consumer:          consumer,
		itemIdProducer:    itemIdProducer,
		storeIdProducer:   storeIdProducer,
		completionTracker: completionTracker,
		config:            config,
		workerID:          workerID,
	}, nil
}

// Start starts the in-memory join orchestrator
func (imo *InMemoryJoinOrchestrator) Start() middleware.MessageMiddlewareError {
	fmt.Println("In-Memory Join Orchestrator: Starting...")

	// Start consuming completion notifications
	fmt.Println("In-Memory Join Orchestrator: Starting to listen for completion notifications...")
	return imo.consumer.StartConsuming(imo.createCallback())
}

// Close closes all connections
func (imo *InMemoryJoinOrchestrator) Close() {
	fmt.Println("In-Memory Join Orchestrator: Shutting down...")

	if imo.consumer != nil {
		imo.consumer.Close()
	}
	if imo.itemIdProducer != nil {
		imo.itemIdProducer.Close()
	}
	if imo.storeIdProducer != nil {
		imo.storeIdProducer.Close()
	}

	fmt.Println("In-Memory Join Orchestrator: Shutdown complete")
}

// createCallback creates the message processing callback
func (imo *InMemoryJoinOrchestrator) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			if err := imo.processChunkNotification(delivery); err != 0 {
				fmt.Printf("In-Memory Join Orchestrator: Error processing chunk notification: %v\n", err)
			}
		}
		done <- nil
	}
}

// processChunkNotification processes a chunk completion notification
func (imo *InMemoryJoinOrchestrator) processChunkNotification(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the message using the deserializer
	message, err := deserializer.Deserialize(delivery.Body)
	if err != nil {
		fmt.Printf("In-Memory Join Orchestrator: Failed to deserialize message: %v\n", err)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	// Handle different message types
	switch msg := message.(type) {
	case *signals.ChunkNotification:
		// Handle chunk notification from join workers
		fmt.Printf("In-Memory Join Orchestrator: Received chunk notification - ClientID: %s, FileID: %s, ChunkNumber: %d, IsLastChunk: %t\n",
			msg.ClientID, msg.FileID, msg.ChunkNumber, msg.IsLastChunk)

		// Process chunk notification using completion tracker
		err = imo.completionTracker.ProcessChunkNotification(msg)
		if err != nil {
			fmt.Printf("In-Memory Join Orchestrator: Failed to process chunk notification: %v\n", err)
			delivery.Ack(false)
			return middleware.MessageMiddlewareMessageError
		}

		// Check if client is completed
		if imo.completionTracker.IsClientCompleted(msg.ClientID) {
			// Infer query type from file ID patterns
			queryType := imo.inferQueryTypeFromFileID(msg.FileID)
			imo.onClientCompletion(msg.ClientID, queryType)
		}

	case *chunk.Chunk:
		// Handle chunk message (fallback for direct chunk messages)
		fmt.Printf("In-Memory Join Orchestrator: Received chunk - ClientID: %s, QueryType: %d, IsLastChunk: %t\n",
			msg.ClientID, msg.QueryType, msg.IsLastChunk)

		// Create chunk notification
		notification := &signals.ChunkNotification{
			ClientID:        msg.ClientID,
			FileID:          msg.FileID,
			TableID:         int(msg.TableID),
			ChunkNumber:     int(msg.ChunkNumber),
			IsLastChunk:     msg.IsLastChunk,
			IsLastFromTable: msg.IsLastFromTable,
			MapWorkerID:     imo.workerID,
		}

		// Process chunk notification using completion tracker
		err = imo.completionTracker.ProcessChunkNotification(notification)
		if err != nil {
			fmt.Printf("In-Memory Join Orchestrator: Failed to process chunk notification: %v\n", err)
			delivery.Ack(false)
			return middleware.MessageMiddlewareMessageError
		}

		// Check if client is completed
		if imo.completionTracker.IsClientCompleted(msg.ClientID) {
			imo.onClientCompletion(msg.ClientID, int(msg.QueryType))
		}

	case *signals.JoinCompletionSignal:
		// Handle completion signal from join workers
		fmt.Printf("In-Memory Join Orchestrator: Received completion signal for client %s\n", msg.ClientID)

		// For now, just acknowledge - the completion tracker should handle this
		// In the future, we might want to track completion signals differently

	default:
		fmt.Printf("In-Memory Join Orchestrator: Received unknown message type: %T\n", message)
	}

	delivery.Ack(false)
	return 0
}

// onClientCompletion handles when a client's query is completed
func (imo *InMemoryJoinOrchestrator) onClientCompletion(clientID string, queryType int) {
	fmt.Printf("In-Memory Join Orchestrator: Client %s completed query type %d\n", clientID, queryType)

	// Determine which exchange to use based on query type
	var producer *exchange.ExchangeMiddleware
	var exchangeName string
	var routingKey string

	switch queryType {
	case 2: // ItemID join
		producer = imo.itemIdProducer
		exchangeName = queues.ItemIdCompletionExchange
		routingKey = queues.ItemIdCompletionRoutingKey
	case 3: // StoreID join
		producer = imo.storeIdProducer
		exchangeName = queues.StoreIdCompletionExchange
		routingKey = queues.StoreIdCompletionRoutingKey
	default:
		fmt.Printf("In-Memory Join Orchestrator: Unknown query type %d for client %s\n", queryType, clientID)
		return
	}

	// Send completion signal
	imo.sendCompletionSignal(clientID, queryType, producer, exchangeName, routingKey)
}

// sendCompletionSignal sends a completion signal to the appropriate workers
func (imo *InMemoryJoinOrchestrator) sendCompletionSignal(clientID string, queryType int, producer *exchange.ExchangeMiddleware, exchangeName, routingKey string) {
	completionSignal := signals.NewJoinCompletionSignal(clientID, getResourceType(queryType), imo.workerID)

	messageData, err := signals.SerializeJoinCompletionSignal(completionSignal)
	if err != nil {
		fmt.Printf("In-Memory Join Orchestrator: Failed to serialize completion signal: %v\n", err)
		return
	}

	if err := producer.Send(messageData, []string{routingKey}); err != 0 {
		fmt.Printf("In-Memory Join Orchestrator: Failed to send completion signal to %s: %v\n", exchangeName, err)
	} else {
		fmt.Printf("In-Memory Join Orchestrator: Sent completion signal for client %s to %s\n", clientID, exchangeName)
	}
}

// getResourceType returns the resource type string based on query type
func getResourceType(queryType int) string {
	switch queryType {
	case 2:
		return "itemid"
	case 3:
		return "storeid"
	default:
		return "unknown"
	}
}

// inferQueryTypeFromFileID infers the query type from file ID patterns
func (imo *InMemoryJoinOrchestrator) inferQueryTypeFromFileID(fileID string) int {
	// File ID patterns:
	// Query 2 (ItemID): Files start with "I" (e.g., "I001", "I002")
	// Query 3 (StoreID): Files start with "S" (e.g., "S001", "S002")
	// Query 4 (UserID): Files start with "U" (e.g., "U001", "U002")

	if len(fileID) > 0 {
		switch fileID[0] {
		case 'I':
			return 2 // ItemID join
		case 'S':
			return 3 // StoreID join
		case 'U':
			return 4 // UserID join
		}
	}

	return 0 // Unknown
}
