package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// GroupByWorker encapsulates the group by worker state and dependencies
type GroupByWorker struct {
	consumer      *exchange.ExchangeConsumer
	replyProducer *workerqueue.QueueMiddleware
	config        *middleware.ConnectionConfig
}

// NewGroupByWorker creates a new GroupByWorker instance
func NewGroupByWorker(config *middleware.ConnectionConfig) (*GroupByWorker, error) {
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

	return &GroupByWorker{
		consumer:      consumer,
		replyProducer: replyProducer,
		config:        config,
	}, nil
}

// Start starts the group by worker
func (gbw *GroupByWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("GroupBy Worker: Starting to listen for messages...")
	return gbw.consumer.StartConsuming(gbw.createCallback())
}

// Close closes all connections
func (gbw *GroupByWorker) Close() {
	if gbw.consumer != nil {
		gbw.consumer.Close()
	}
	if gbw.replyProducer != nil {
		gbw.replyProducer.Close()
	}
}

// createCallback creates the message processing callback
func (gbw *GroupByWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("GroupBy Worker: Starting to listen for messages...")
		for delivery := range *consumeChannel {
			if err := gbw.processMessage(delivery); err != 0 {
				fmt.Printf("GroupBy Worker: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
