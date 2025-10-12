package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// AmountFilterWorker encapsulates the amount filter worker state and dependencies
type AmountFilterWorker struct {
	consumer      *workerqueue.QueueConsumer
	replyProducer *workerqueue.QueueMiddleware
	config        *middleware.ConnectionConfig
}

// NewAmountFilterWorker creates a new AmountFilterWorker instance
func NewAmountFilterWorker(config *middleware.ConnectionConfig) (*AmountFilterWorker, error) {
	// Create amount filter consumer
	consumer := workerqueue.NewQueueConsumer(
		AmountFilterQueueName,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create amount filter consumer")
	}

	// Declare the amount filter queue using QueueMiddleware
	amountFilterConsumer := workerqueue.NewMessageMiddlewareQueue(
		AmountFilterQueueName,
		config,
	)
	if amountFilterConsumer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}
	if err := amountFilterConsumer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		amountFilterConsumer.Close()
		return nil, fmt.Errorf("failed to declare amount filter queue: %v", err)
	}
	amountFilterConsumer.Close() // Close the declarer as we don't need it anymore

	// Create reply producer
	replyProducer := workerqueue.NewMessageMiddlewareQueue(
		ReplyFilterBusQueueName,
		config,
	)
	if replyProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create reply producer")
	}

	// Declare producer queue

	if err := replyProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to declare reply queue: %v", err)
	}

	return &AmountFilterWorker{
		consumer:      consumer,
		replyProducer: replyProducer,
		config:        config,
	}, nil
}

// Start starts the amount filter worker
func (afw *AmountFilterWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Amount Filter Worker: Starting to listen for messages...")
	err := afw.consumer.StartConsuming(afw.createCallback())
	if err != 0 {
		fmt.Printf("Amount Filter Worker: ERROR - StartConsuming failed with error: %v\n", err)
		return err
	}
	fmt.Println("Amount Filter Worker: Successfully registered as consumer")
	return 0
}

// Close closes all connections
func (afw *AmountFilterWorker) Close() {
	if afw.consumer != nil {
		afw.consumer.Close()
	}
	if afw.replyProducer != nil {
		afw.replyProducer.Close()
	}
}

// createCallback creates the message processing callback
func (afw *AmountFilterWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Amount Filter Worker: Callback started, waiting for messages...")
		messageCount := 0
		for delivery := range *consumeChannel {
			messageCount++
			fmt.Printf("Amount Filter Worker: Received message #%d\n", messageCount)
			if err := afw.processMessage(delivery); err != 0 {
				fmt.Printf("Amount Filter Worker: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		fmt.Printf("Amount Filter Worker: Consume channel closed after processing %d messages\n", messageCount)
		done <- nil
	}
}
