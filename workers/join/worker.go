package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// JoinWorker encapsulates the join worker state and dependencies
type JoinWorker struct {
	consumer      *exchange.ExchangeConsumer
	replyProducer *workerqueue.QueueMiddleware
	config        *middleware.ConnectionConfig
}

// NewJoinWorker creates a new JoinWorker instance
func NewJoinWorker(config *middleware.ConnectionConfig) (*JoinWorker, error) {
	// Create join consumer
	consumer := exchange.NewExchangeConsumer(
		JoinExchangeName,
		[]string{JoinRoutingKey},
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create join consumer")
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
	if err := replyProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to declare reply queue: %v", err)
	}

	return &JoinWorker{
		consumer:      consumer,
		replyProducer: replyProducer,
		config:        config,
	}, nil
}

// Start starts the join worker
func (jw *JoinWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Join Worker: Starting to listen for messages...")
	return jw.consumer.StartConsuming(jw.createCallback())
}

// Close closes all connections
func (jw *JoinWorker) Close() {
	if jw.consumer != nil {
		jw.consumer.Close()
	}
	if jw.replyProducer != nil {
		jw.replyProducer.Close()
	}
}

// createCallback creates the message processing callback
func (jw *JoinWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Join Worker: Starting to listen for messages...")
		for delivery := range *consumeChannel {
			if err := jw.processMessage(delivery); err != 0 {
				fmt.Printf("Join Worker: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
