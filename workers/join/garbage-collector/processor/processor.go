package processor

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/workers/join/garbage-collector/config"
)

// JoinGarbageCollector handles cleanup coordination across all workers
type JoinGarbageCollector struct {
	config                 *config.Config
	completionConsumer     *workerqueue.QueueConsumer
	storeidCleanupProducer *exchange.ExchangeMiddleware
	itemidCleanupProducer  *exchange.ExchangeMiddleware
	useridCleanupProducer  *exchange.ExchangeMiddleware
}

// NewJoinGarbageCollector creates a new garbage collector instance
func NewJoinGarbageCollector(cfg *config.Config) (*JoinGarbageCollector, error) {
	// Create completion consumer
	completionConsumer := workerqueue.NewQueueConsumer(
		cfg.CompletionQueue,
		cfg.ConnectionConfig,
	)
	if completionConsumer == nil {
		return nil, fmt.Errorf("failed to create completion consumer")
	}

	// Declare the completion queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(cfg.CompletionQueue, cfg.ConnectionConfig)
	if queueDeclarer == nil {
		completionConsumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}

	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		completionConsumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare completion queue: %v", err)
	}
	queueDeclarer.Close()

	// Create cleanup exchange producers
	storeidCleanupProducer := exchange.NewMessageMiddlewareExchange(
		cfg.StoreIDExchange,
		[]string{}, // Empty for fanout
		cfg.ConnectionConfig,
	)
	if storeidCleanupProducer == nil {
		completionConsumer.Close()
		return nil, fmt.Errorf("failed to create storeid cleanup producer")
	}

	itemidCleanupProducer := exchange.NewMessageMiddlewareExchange(
		cfg.ItemIDExchange,
		[]string{}, // Empty for fanout
		cfg.ConnectionConfig,
	)
	if itemidCleanupProducer == nil {
		completionConsumer.Close()
		storeidCleanupProducer.Close()
		return nil, fmt.Errorf("failed to create itemid cleanup producer")
	}

	useridCleanupProducer := exchange.NewMessageMiddlewareExchange(
		cfg.UserIDExchange,
		[]string{}, // Empty for fanout
		cfg.ConnectionConfig,
	)
	if useridCleanupProducer == nil {
		completionConsumer.Close()
		storeidCleanupProducer.Close()
		itemidCleanupProducer.Close()
		return nil, fmt.Errorf("failed to create userid cleanup producer")
	}

	// Declare cleanup exchanges as fanout, non-durable
	if err := storeidCleanupProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		completionConsumer.Close()
		storeidCleanupProducer.Close()
		itemidCleanupProducer.Close()
		useridCleanupProducer.Close()
		return nil, fmt.Errorf("failed to declare storeid cleanup exchange: %v", err)
	}

	if err := itemidCleanupProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		completionConsumer.Close()
		storeidCleanupProducer.Close()
		itemidCleanupProducer.Close()
		useridCleanupProducer.Close()
		return nil, fmt.Errorf("failed to declare itemid cleanup exchange: %v", err)
	}

	if err := useridCleanupProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		completionConsumer.Close()
		storeidCleanupProducer.Close()
		itemidCleanupProducer.Close()
		useridCleanupProducer.Close()
		return nil, fmt.Errorf("failed to declare userid cleanup exchange: %v", err)
	}

	return &JoinGarbageCollector{
		config:                 cfg,
		completionConsumer:     completionConsumer,
		storeidCleanupProducer: storeidCleanupProducer,
		itemidCleanupProducer:  itemidCleanupProducer,
		useridCleanupProducer:  useridCleanupProducer,
	}, nil
}

// Start starts the garbage collector
func (gc *JoinGarbageCollector) Start() middleware.MessageMiddlewareError {
	log.Println("Join Garbage Collector: Starting to listen for completion notifications...")
	return gc.completionConsumer.StartConsuming(gc.createCallback())
}

// Close closes all connections
func (gc *JoinGarbageCollector) Close() {
	if gc.completionConsumer != nil {
		gc.completionConsumer.Close()
	}
	if gc.storeidCleanupProducer != nil {
		gc.storeidCleanupProducer.Close()
	}
	if gc.itemidCleanupProducer != nil {
		gc.itemidCleanupProducer.Close()
	}
	if gc.useridCleanupProducer != nil {
		gc.useridCleanupProducer.Close()
	}
}

// createCallback creates the message processing callback
func (gc *JoinGarbageCollector) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := gc.processCompletionMessage(delivery)
			if err != nil {
				done <- fmt.Errorf("failed to process completion message: %v", err)
				return
			}
		}
	}
}

// processCompletionMessage processes a completion signal
func (gc *JoinGarbageCollector) processCompletionMessage(delivery amqp.Delivery) error {
	// Deserialize the message using the deserializer
	message, err := deserializer.Deserialize(delivery.Body)
	if err != nil {
		log.Printf("Join Garbage Collector: Failed to deserialize message: %v", err)
		delivery.Nack(false, false) // Reject the message
		return err
	}

	// Check if it's a completion signal
	completionSignal, ok := message.(*signals.JoinCompletionSignal)
	if !ok {
		log.Printf("Join Garbage Collector: Received non-completion message type")
		delivery.Nack(false, false) // Reject the message
		return fmt.Errorf("expected JoinCompletionSignal, got %T", message)
	}

	log.Printf("Join Garbage Collector: Received completion signal for client %s, resource type %s",
		completionSignal.ClientID, completionSignal.ResourceType)

	// Route based on resource type
	switch completionSignal.ResourceType {
	case "stores":
		gc.sendStoreIDCleanup(completionSignal.ClientID)
	case "items":
		gc.sendItemIDCleanup(completionSignal.ClientID)
	case "users":
		gc.sendUserIDCleanup(completionSignal.ClientID)
	default:
		log.Printf("Join Garbage Collector: Unknown resource type: %s", completionSignal.ResourceType)
	}

	delivery.Ack(false) // Acknowledge the message
	return nil
}

// sendStoreIDCleanup sends cleanup signal to all StoreID workers
func (gc *JoinGarbageCollector) sendStoreIDCleanup(clientID string) {
	cleanupSignal := signals.NewJoinCleanupSignal(clientID)

	messageData, err := signals.SerializeJoinCleanupSignal(cleanupSignal)
	if err != nil {
		log.Printf("Join Garbage Collector: Failed to serialize storeid cleanup signal: %v", err)
		return
	}

	// Send fanout message (empty routeKeys = fanout)
	if err := gc.storeidCleanupProducer.Send(messageData, []string{}); err != 0 {
		log.Printf("Join Garbage Collector: Failed to send storeid cleanup signal: %v", err)
	} else {
		log.Printf("Join Garbage Collector: Sent storeid cleanup signal for client %s", clientID)
	}
}

// sendItemIDCleanup sends cleanup signal to all ItemID workers
func (gc *JoinGarbageCollector) sendItemIDCleanup(clientID string) {
	cleanupSignal := signals.NewJoinCleanupSignal(clientID)

	messageData, err := signals.SerializeJoinCleanupSignal(cleanupSignal)
	if err != nil {
		log.Printf("Join Garbage Collector: Failed to serialize itemid cleanup signal: %v", err)
		return
	}

	// Send fanout message (empty routeKeys = fanout)
	if err := gc.itemidCleanupProducer.Send(messageData, []string{}); err != 0 {
		log.Printf("Join Garbage Collector: Failed to send itemid cleanup signal: %v", err)
	} else {
		log.Printf("Join Garbage Collector: Sent itemid cleanup signal for client %s", clientID)
	}
}

// sendUserIDCleanup sends stop writing and cleanup signal to all UserID writers
func (gc *JoinGarbageCollector) sendUserIDCleanup(clientID string) {
	cleanupSignal := signals.NewJoinCleanupSignal(clientID)

	messageData, err := signals.SerializeJoinCleanupSignal(cleanupSignal)
	if err != nil {
		log.Printf("Join Garbage Collector: Failed to serialize userid cleanup signal: %v", err)
		return
	}

	// Send fanout message (empty routeKeys = fanout)
	if err := gc.useridCleanupProducer.Send(messageData, []string{}); err != 0 {
		log.Printf("Join Garbage Collector: Failed to send userid cleanup signal: %v", err)
	} else {
		log.Printf("Join Garbage Collector: Sent userid cleanup signal for client %s", clientID)
	}
}
