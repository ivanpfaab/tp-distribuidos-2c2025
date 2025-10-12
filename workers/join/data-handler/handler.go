package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// JoinDataHandler encapsulates the join data handler state and dependencies
type JoinDataHandler struct {
	consumer         *exchange.ExchangeConsumer
	itemIdProducer   *exchange.ExchangeMiddleware
	storeIdProducer  *workerqueue.QueueMiddleware
	userIdProducer   *workerqueue.QueueMiddleware
	config           *middleware.ConnectionConfig
	itemIdWorkerCount int // Number of ItemID worker instances to broadcast to
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
	fmt.Printf("Join Data Handler: Initializing with %d ItemID worker instance(s)\n", itemIdWorkerCount)
	
	// Create consumer for fixed join data
	consumer := exchange.NewExchangeConsumer(
		FixedJoinDataExchange,
		[]string{FixedJoinDataRoutingKey},
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create fixed join data consumer")
	}

	// Declare the exchange before consuming
	exchangeProducer := exchange.NewMessageMiddlewareExchange(
		FixedJoinDataExchange,
		[]string{FixedJoinDataRoutingKey},
		config,
	)
	if exchangeProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create exchange producer for declaration")
	}

	// Declare the exchange (creating it twice is not harmful in RabbitMQ)
	if err := exchangeProducer.DeclareExchange("direct", false, false, false, false); err != 0 {
		consumer.Close()
		exchangeProducer.Close()
		return nil, fmt.Errorf("failed to declare fixed join data exchange: %v", err)
	}
	exchangeProducer.Close() // Close the producer as we don't need it anymore

	// Create producers for dictionaries
	// ItemID uses exchange for broadcasting to all workers
	itemIdProducer := exchange.NewMessageMiddlewareExchange(
		JoinItemIdDictionaryExchange,
		[]string{JoinItemIdDictionaryRoutingKey},
		config,
	)
	if itemIdProducer == nil {
		consumer.Close()
		exchangeProducer.Close()
		return nil, fmt.Errorf("failed to create item ID dictionary producer")
	}

	storeIdProducer := workerqueue.NewMessageMiddlewareQueue(
		JoinStoreIdDictionaryQueue,
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
	// ItemID: Declare exchange as fanout (durable)
	if err := itemIdProducer.DeclareExchange("fanout", true, false, false, false); err != 0 {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		userIdProducer.Close()
		return nil, fmt.Errorf("failed to declare item ID dictionary exchange: %v", err)
	}

	if err := storeIdProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		userIdProducer.Close()
		return nil, fmt.Errorf("failed to declare store ID dictionary queue: %v", err)
	}

	if err := userIdProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		userIdProducer.Close()
		return nil, fmt.Errorf("failed to declare user ID dictionary queue: %v", err)
	}

	return &JoinDataHandler{
		consumer:         consumer,
		itemIdProducer:   itemIdProducer,
		storeIdProducer:  storeIdProducer,
		userIdProducer:   userIdProducer,
		config:           config,
		itemIdWorkerCount: itemIdWorkerCount,
	}, nil
}

// Start starts the join data handler
func (jdh *JoinDataHandler) Start() middleware.MessageMiddlewareError {
	fmt.Println("Join Data Handler: Starting to listen for fixed join data...")
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
	fmt.Printf("Join Data Handler: Received message: %s\n", string(delivery.Body))

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Join Data Handler: Failed to deserialize chunk message: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	// Route based on FileID
	sendErr := jdh.routeAndSendByFileId(chunkMsg.FileID, delivery.Body)
	if sendErr != 0 {
		fmt.Printf("Join Data Handler: Failed to route chunk with FileID %s: %v\n", chunkMsg.FileID, sendErr)
		delivery.Nack(false, true) // Reject and requeue
		return sendErr
	}

	fmt.Printf("Join Data Handler: Successfully routed chunk with FileID %s\n", chunkMsg.FileID)
	delivery.Ack(false) // Acknowledge the original message
	return 0
}

// routeAndSendByFileId routes and sends message based on FileID
func (jdh *JoinDataHandler) routeAndSendByFileId(fileId string, messageData []byte) middleware.MessageMiddlewareError {
	fileIdUpper := strings.ToUpper(fileId)

	if strings.Contains(fileIdUpper, "MN") {
		// ItemID: Send to exchange (broadcast to all worker instances)
		// Generate routing keys for all ItemID worker instances
		routingKeys := make([]string, jdh.itemIdWorkerCount)
		for i := 0; i < jdh.itemIdWorkerCount; i++ {
			instanceID := i + 1 // 1-indexed
			routingKeys[i] = fmt.Sprintf("%s-instance-%d", JoinItemIdDictionaryRoutingKey, instanceID)
		}
		
		fmt.Printf("Join Data Handler: Broadcasting FileID %s to %d ItemID worker instance(s) with routing keys: %v\n", 
			fileId, jdh.itemIdWorkerCount, routingKeys)
		return jdh.itemIdProducer.Send(messageData, routingKeys)
	} else if strings.Contains(fileIdUpper, "ST") {
		// StoreID: Send to queue (single worker)
		fmt.Printf("Join Data Handler: Routing FileID %s to StoreID dictionary queue\n", fileId)
		return jdh.storeIdProducer.Send(messageData)
	} else if strings.Contains(fileIdUpper, "US") {
		// UserID: Send to queue (single worker)
		fmt.Printf("Join Data Handler: Routing FileID %s to UserID dictionary queue\n", fileId)
		return jdh.userIdProducer.Send(messageData)
	}

	fmt.Printf("Join Data Handler: Unknown FileID: %s, ignoring message\n", fileId)
	return 0 // Success - just ignore unknown file types
}
