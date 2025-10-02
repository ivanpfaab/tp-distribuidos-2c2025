package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// GroupByWorkerService encapsulates the group by worker service with distributed architecture
type GroupByWorkerService struct {
	consumer      *exchange.ExchangeConsumer
	replyProducer *workerqueue.QueueMiddleware
	config        *middleware.ConnectionConfig
	orchestrator  *GroupByOrchestrator
	numWorkers    int
}

// NewGroupByWorkerService creates a new GroupByWorkerService instance
func NewGroupByWorkerService(config *middleware.ConnectionConfig, numWorkers int) (*GroupByWorkerService, error) {
	// Create group by consumer
	consumer := exchange.NewExchangeConsumer(
		GroupByExchangeName,
		[]string{GroupByRoutingKey},
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create group by consumer")
	}

	// Create reply producer
	replyProducer := workerqueue.NewMessageMiddlewareQueue(
		ReplyQueueName,
		config,
	)
	if replyProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create reply producer")
	}

	// Declare the reply queue
	if err := replyProducer.DeclareQueue(true, false, false, false); err != 0 {
		consumer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to declare reply queue: %v", err)
	}

	// Create the distributed orchestrator
	orchestrator := NewGroupByOrchestrator(numWorkers)

	return &GroupByWorkerService{
		consumer:      consumer,
		replyProducer: replyProducer,
		config:        config,
		orchestrator:  orchestrator,
		numWorkers:    numWorkers,
	}, nil
}

// Start starts the group by worker service
func (gbws *GroupByWorkerService) Start() middleware.MessageMiddlewareError {
	fmt.Printf("GroupBy Worker Service: Starting with %d workers\n", gbws.numWorkers)

	// Start the distributed orchestrator
	gbws.orchestrator.Start()

	// Start listening for messages from RabbitMQ
	fmt.Println("GroupBy Worker Service: Starting to listen for messages...")
	return gbws.consumer.StartConsuming(gbws.createCallback())
}

// Close closes all connections
func (gbws *GroupByWorkerService) Close() {
	// Stop the orchestrator
	gbws.orchestrator.Stop()

	// Close RabbitMQ connections
	if gbws.consumer != nil {
		gbws.consumer.Close()
	}
	if gbws.replyProducer != nil {
		gbws.replyProducer.Close()
	}
}

// createCallback creates the message processing callback
func (gbws *GroupByWorkerService) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("GroupBy Worker Service: Starting to listen for messages...")
		for delivery := range *consumeChannel {
			if err := gbws.processMessage(delivery); err != 0 {
				fmt.Printf("GroupBy Worker Service: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
