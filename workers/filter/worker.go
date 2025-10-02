package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// FilterWorker encapsulates the filter worker state and dependencies
type FilterWorker struct {
	consumer      *exchange.ExchangeConsumer
	replyProducer *workerqueue.QueueMiddleware
	config        *middleware.ConnectionConfig
}

// NewFilterWorker creates a new FilterWorker instance
func NewFilterWorker(config *middleware.ConnectionConfig) (*FilterWorker, error) {
	// Create filter consumer
	consumer := exchange.NewExchangeConsumer(
		FilterExchangeName,
		[]string{FilterRoutingKey},
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create filter consumer")
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

	return &FilterWorker{
		consumer:      consumer,
		replyProducer: replyProducer,
		config:        config,
	}, nil
}

// Start starts the filter worker
func (fw *FilterWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Filter Worker: Starting to listen for messages...")
	return fw.consumer.StartConsuming(fw.createCallback())
}

// Close closes all connections
func (fw *FilterWorker) Close() {
	if fw.consumer != nil {
		fw.consumer.Close()
	}
	if fw.replyProducer != nil {
		fw.replyProducer.Close()
	}
}

// createCallback creates the message processing callback
func (fw *FilterWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Filter Worker: Starting to listen for messages...")
		for delivery := range *consumeChannel {
			if err := fw.processMessage(delivery); err != 0 {
				fmt.Printf("Filter Worker: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
