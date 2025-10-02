package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// AggregatorWorker encapsulates the aggregator worker state and dependencies
type AggregatorWorker struct {
	consumer      *exchange.ExchangeConsumer
	replyProducer *workerqueue.QueueMiddleware
	config        *middleware.ConnectionConfig
}

// NewAggregatorWorker creates a new AggregatorWorker instance
func NewAggregatorWorker(config *middleware.ConnectionConfig) (*AggregatorWorker, error) {
	// Create aggregator consumer
	consumer := exchange.NewExchangeConsumer(
		AggregatorExchangeName,
		[]string{AggregatorRoutingKey},
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create aggregator consumer")
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

	return &AggregatorWorker{
		consumer:      consumer,
		replyProducer: replyProducer,
		config:        config,
	}, nil
}

// Start starts the aggregator worker
func (aw *AggregatorWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Aggregator Worker: Starting to listen for messages...")
	return aw.consumer.StartConsuming(aw.createCallback())
}

// Close closes all connections
func (aw *AggregatorWorker) Close() {
	if aw.consumer != nil {
		aw.consumer.Close()
	}
	if aw.replyProducer != nil {
		aw.replyProducer.Close()
	}
}

// createCallback creates the message processing callback
func (aw *AggregatorWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Aggregator Worker: Starting to listen for messages...")
		for delivery := range *consumeChannel {
			if err := aw.processMessage(delivery); err != 0 {
				fmt.Printf("Aggregator Worker: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
