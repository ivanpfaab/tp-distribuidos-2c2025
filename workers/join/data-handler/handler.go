package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// logWithTimestamp prints a formatted message with timestamp
func logWithTimestamp(format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf("[%s] %s\n", timestamp, fmt.Sprintf(format, args...))
}

// JoinDataHandler encapsulates the join data handler state and dependencies
type JoinDataHandler struct {
	consumer           *workerqueue.QueueConsumer
	itemIdProducer     *exchange.ExchangeMiddleware
	storeIdProducer    *exchange.ExchangeMiddleware
	userIdProducer     *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig
	itemIdWorkerCount  int // Number of ItemID worker instances to broadcast to
	storeIdWorkerCount int // Number of StoreID worker instances to broadcast to
}

// NewJoinDataHandler creates a new JoinDataHandler instance
func NewJoinDataHandler(config *middleware.ConnectionConfig) (*JoinDataHandler, error) {
	// Get the number of ItemID workers from environment (defaults to 1)
	itemIdWorkerCountStr := os.Getenv("ITEMID_WORKER_COUNT")
	itemIdWorkerCount := 1
	if itemIdWorkerCountStr != "" {
		if count, err := strconv.Atoi(itemIdWorkerCountStr); err == nil && count > 0 {
			itemIdWorkerCount = count
		}
	}

	// Get the number of StoreID workers from environment (defaults to 1)
	storeIdWorkerCountStr := os.Getenv("STOREID_WORKER_COUNT")
	storeIdWorkerCount := 1
	if storeIdWorkerCountStr != "" {
		if count, err := strconv.Atoi(storeIdWorkerCountStr); err == nil && count > 0 {
			storeIdWorkerCount = count
		}
	}

	logWithTimestamp("Join Data Handler: Initializing with %d ItemID worker instance(s) and %d StoreID worker instance(s)",
		itemIdWorkerCount, storeIdWorkerCount)

	// Create consumer for fixed join data (consume from queue, not exchange)
	consumer := workerqueue.NewQueueConsumer(
		FixedJoinDataQueue,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create fixed join data consumer")
	}

	// Declare the queue before consuming
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(FixedJoinDataQueue, config)
	if queueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}

	// Declare the queue
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare fixed join data queue: %v", err)
	}
	queueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create producers for dictionaries
	// ItemID uses exchange for broadcasting to all workers
	itemIdProducer := exchange.NewMessageMiddlewareExchange(
		JoinItemIdDictionaryExchange,
		[]string{JoinItemIdDictionaryRoutingKey},
		config,
	)
	if itemIdProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create item ID dictionary producer")
	}

	// StoreID uses exchange for broadcasting to all workers
	storeIdProducer := exchange.NewMessageMiddlewareExchange(
		JoinStoreIdDictionaryExchange,
		[]string{JoinStoreIdDictionaryRoutingKey},
		config,
	)
	if storeIdProducer == nil {
		consumer.Close()
		itemIdProducer.Close()
		return nil, fmt.Errorf("failed to create store ID dictionary producer")
	}

	userIdProducer := workerqueue.NewMessageMiddlewareQueue(
		JoinUserIdDictionaryQueue,
		config,
	)
	if userIdProducer == nil {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		return nil, fmt.Errorf("failed to create user ID dictionary producer")
	}

	// Declare exchanges and queues
	// ItemID: Declare exchange as direct (durable)
	if err := itemIdProducer.DeclareExchange("direct", false, false, false, false); err != 0 {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		userIdProducer.Close()
		return nil, fmt.Errorf("failed to declare item ID dictionary exchange: %v", err)
	}

	// StoreID: Declare exchange as direct (durable)
	if err := storeIdProducer.DeclareExchange("direct", false, false, false, false); err != 0 {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		userIdProducer.Close()
		return nil, fmt.Errorf("failed to declare store ID dictionary exchange: %v", err)
	}

	if err := userIdProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		userIdProducer.Close()
		return nil, fmt.Errorf("failed to declare user ID dictionary queue: %v", err)
	}

	return &JoinDataHandler{
		consumer:           consumer,
		itemIdProducer:     itemIdProducer,
		storeIdProducer:    storeIdProducer,
		userIdProducer:     userIdProducer,
		config:             config,
		itemIdWorkerCount:  itemIdWorkerCount,
		storeIdWorkerCount: storeIdWorkerCount,
	}, nil
}

// Start starts the join data handler
func (jdh *JoinDataHandler) Start() middleware.MessageMiddlewareError {
	logWithTimestamp("Join Data Handler: Starting to listen for fixed join data...")
	return jdh.consumer.StartConsuming(jdh.createCallback())
}

// Close closes all connections
func (jdh *JoinDataHandler) Close() {
	if jdh.consumer != nil {
		jdh.consumer.Close()
	}
	if jdh.itemIdProducer != nil {
		jdh.itemIdProducer.Close()
	}
	if jdh.storeIdProducer != nil {
		jdh.storeIdProducer.Close()
	}
	if jdh.userIdProducer != nil {
		jdh.userIdProducer.Close()
	}
}

// createCallback creates the message processing callback
func (jdh *JoinDataHandler) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := jdh.processMessage(delivery)
			if err != 0 {
				done <- fmt.Errorf("failed to process message: %v", err)
				return
			}
		}
	}
}

// processMessage processes a single message and routes it to the appropriate dictionary queue/exchange
func (jdh *JoinDataHandler) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		logWithTimestamp("Join Data Handler: Failed to deserialize chunk message: %v", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	// Route based on FileID
	sendErr := jdh.routeAndSendByFileId(chunkMsg.FileID, delivery.Body)
	if sendErr != 0 {
		logWithTimestamp("Join Data Handler: Failed to route chunk with FileID %s: %v", chunkMsg.FileID, sendErr)
		delivery.Nack(false, true) // Reject and requeue
		return sendErr
	}

	logWithTimestamp("Join Data Handler: Successfully routed chunk with FileID %s", chunkMsg.FileID)
	delivery.Ack(false) // Acknowledge the original message
	return 0
}

// routeAndSendByFileId routes and sends message based on FileID
func (jdh *JoinDataHandler) routeAndSendByFileId(fileId string, messageData []byte) middleware.MessageMiddlewareError {
	fileIdUpper := strings.ToUpper(fileId)
	logWithTimestamp("Join Data Handler: Received message with FileID %s", fileId)

	if strings.Contains(fileIdUpper, "MN") {
		logWithTimestamp("Join Data Handler: Received FileID %s, routing to %d ItemID worker instance(s)",
			fileId, jdh.itemIdWorkerCount)
		// ItemID: Send to exchange (broadcast to all worker instances)
		// Generate routing keys for all ItemID worker instances
		routingKeys := make([]string, jdh.itemIdWorkerCount)
		for i := 0; i < jdh.itemIdWorkerCount; i++ {
			instanceID := i + 1 // 1-indexed
			routingKeys[i] = fmt.Sprintf("%s-instance-%d", JoinItemIdDictionaryRoutingKey, instanceID)
		}

		logWithTimestamp("Join Data Handler: Broadcasting FileID %s to %d ItemID worker instance(s) with routing keys: %v",
			fileId, jdh.itemIdWorkerCount, routingKeys)
		return jdh.itemIdProducer.Send(messageData, routingKeys)
	} else if strings.Contains(fileIdUpper, "ST") {
		logWithTimestamp("Join Data Handler: Received FileID %s, routing to %d StoreID worker instance(s)",
			fileId, jdh.storeIdWorkerCount)
		// StoreID: Send to exchange (broadcast to all worker instances)
		// Generate routing keys for all StoreID worker instances
		routingKeys := make([]string, jdh.storeIdWorkerCount)
		for i := 0; i < jdh.storeIdWorkerCount; i++ {
			instanceID := i + 1 // 1-indexed
			routingKeys[i] = fmt.Sprintf("%s-instance-%d", JoinStoreIdDictionaryRoutingKey, instanceID)
		}

		logWithTimestamp("Join Data Handler: Broadcasting FileID %s to %d StoreID worker instance(s) with routing keys: %v",
			fileId, jdh.storeIdWorkerCount, routingKeys)
		return jdh.storeIdProducer.Send(messageData, routingKeys)
	} else if strings.Contains(fileIdUpper, "US") {
		logWithTimestamp("Join Data Handler: Received FileID %s, routing to UserID dictionary queue", fileId)
		// UserID: Send to queue (single worker)
		logWithTimestamp("Join Data Handler: Routing FileID %s to UserID dictionary queue", fileId)
		return jdh.userIdProducer.Send(messageData)
	}

	logWithTimestamp("Join Data Handler: Unknown FileID: %s, ignoring message", fileId)
	return 0 // Success - just ignore unknown file types
}
