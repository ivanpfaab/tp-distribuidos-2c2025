package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// YearFilterWorker encapsulates the year filter worker state and dependencies
type YearFilterWorker struct {
	consumer           *workerqueue.QueueConsumer
	timeFilterProducer *workerqueue.QueueMiddleware
	replyProducer      *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig
}

// NewYearFilterWorker creates a new YearFilterWorker instance
func NewYearFilterWorker(config *middleware.ConnectionConfig) (*YearFilterWorker, error) {
	// Create year filter consumer
	consumer := workerqueue.NewQueueConsumer(
		YearFilterQueueName,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create year filter consumer")
	}

	// Declare the year filter queue using QueueMiddleware
	yearFilterConsumer := workerqueue.NewMessageMiddlewareQueue(
		YearFilterQueueName,
		config,
	)
	if yearFilterConsumer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}
	if err := yearFilterConsumer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		yearFilterConsumer.Close()
		return nil, fmt.Errorf("failed to declare year filter queue: %v", err)
	}
	yearFilterConsumer.Close() // Close the declarer as we don't need it anymore

	// Create time filter producer
	timeFilterProducer := workerqueue.NewMessageMiddlewareQueue(
		TimeFilterQueueName,
		config,
	)
	if timeFilterProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create time filter producer")
	}

	// Create reply producer
	replyProducer := workerqueue.NewMessageMiddlewareQueue(
		ReplyFilterBusQueueName,
		config,
	)
	if replyProducer == nil {
		consumer.Close()
		timeFilterProducer.Close()
		return nil, fmt.Errorf("failed to create reply producer")
	}

	// Declare producer queues

	if err := timeFilterProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		timeFilterProducer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to declare time filter queue: %v", err)
	}

	if err := replyProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		timeFilterProducer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to declare reply queue: %v", err)
	}

	return &YearFilterWorker{
		consumer:           consumer,
		timeFilterProducer: timeFilterProducer,
		replyProducer:      replyProducer,
		config:             config,
	}, nil
}

// Start starts the year filter worker
func (yfw *YearFilterWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Year Filter Worker: Starting to listen for messages...")
	err := yfw.consumer.StartConsuming(yfw.createCallback())
	if err != 0 {
		fmt.Printf("Year Filter Worker: ERROR - StartConsuming failed with error: %v\n", err)
		return err
	}
	fmt.Println("Year Filter Worker: Successfully registered as consumer")
	return 0
}

// Close closes all connections
func (yfw *YearFilterWorker) Close() {
	if yfw.consumer != nil {
		yfw.consumer.Close()
	}
	if yfw.timeFilterProducer != nil {
		yfw.timeFilterProducer.Close()
	}
	if yfw.replyProducer != nil {
		yfw.replyProducer.Close()
	}
}

// createCallback creates the message processing callback
func (yfw *YearFilterWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Year Filter Worker: Callback started, waiting for messages...")
		messageCount := 0
		for delivery := range *consumeChannel {
			messageCount++
			fmt.Printf("Year Filter Worker: Received message #%d\n", messageCount)
			if err := yfw.processMessage(delivery); err != 0 {
				fmt.Printf("Year Filter Worker: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		fmt.Printf("Year Filter Worker: Consume channel closed after processing %d messages\n", messageCount)
		done <- nil
	}
}
