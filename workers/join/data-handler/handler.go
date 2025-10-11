package main

import (
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// JoinDataHandler encapsulates the join data handler state and dependencies
type JoinDataHandler struct {
	consumer        *exchange.ExchangeConsumer
	itemIdProducer  *workerqueue.QueueMiddleware
	storeIdProducer *workerqueue.QueueMiddleware
	userIdProducer  *workerqueue.QueueMiddleware
	config          *middleware.ConnectionConfig
}

// NewJoinDataHandler creates a new JoinDataHandler instance
func NewJoinDataHandler(config *middleware.ConnectionConfig) (*JoinDataHandler, error) {
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

	// Create producers for each dictionary queue
	itemIdProducer := workerqueue.NewMessageMiddlewareQueue(
		JoinItemIdDictionaryQueue,
		config,
	)
	if itemIdProducer == nil {
		consumer.Close()
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

	// Declare all queues
	if err := itemIdProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdProducer.Close()
		storeIdProducer.Close()
		userIdProducer.Close()
		return nil, fmt.Errorf("failed to declare item ID dictionary queue: %v", err)
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
		consumer:        consumer,
		itemIdProducer:  itemIdProducer,
		storeIdProducer: storeIdProducer,
		userIdProducer:  userIdProducer,
		config:          config,
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

// processMessage processes a single message and routes it to the appropriate dictionary queue
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
	targetQueue := jdh.routeByFileId(chunkMsg.FileID)
	if targetQueue == nil {
		fmt.Printf("Join Data Handler: Unknown FileID: %s, ignoring message\n", chunkMsg.FileID)
		delivery.Ack(false) // Acknowledge to remove from queue
		return 0
	}

	// Forward the chunk to the appropriate dictionary queue
	if err := targetQueue.Send(delivery.Body); err != 0 {
		fmt.Printf("Join Data Handler: Failed to send chunk to dictionary queue: %v\n", err)
		delivery.Nack(false, true) // Reject and requeue
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Join Data Handler: Successfully routed chunk with FileID %s to dictionary queue\n", chunkMsg.FileID)
	delivery.Ack(false) // Acknowledge the original message
	return 0
}

// routeByFileId determines which dictionary queue to use based on FileID
func (jdh *JoinDataHandler) routeByFileId(fileId string) *workerqueue.QueueMiddleware {
	fileIdUpper := strings.ToUpper(fileId)

	if strings.Contains(fileIdUpper, "MN") {
		fmt.Printf("Join Data Handler: Routing FileID %s to ItemID dictionary\n", fileId)
		return jdh.itemIdProducer
	} else if strings.Contains(fileIdUpper, "ST") {
		fmt.Printf("Join Data Handler: Routing FileID %s to StoreID dictionary\n", fileId)
		return jdh.storeIdProducer
	} else if strings.Contains(fileIdUpper, "US") {
		fmt.Printf("Join Data Handler: Routing FileID %s to UserID dictionary\n", fileId)
		return jdh.userIdProducer
	}

	return nil
}
