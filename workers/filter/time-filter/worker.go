package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// TimeFilterWorker encapsulates the time filter worker state and dependencies
type TimeFilterWorker struct {
	consumer             *workerqueue.QueueConsumer
	amountFilterProducer *workerqueue.QueueMiddleware
	replyProducer        *workerqueue.QueueMiddleware
	config               *middleware.ConnectionConfig
}

// NewTimeFilterWorker creates a new TimeFilterWorker instance
func NewTimeFilterWorker(config *middleware.ConnectionConfig) (*TimeFilterWorker, error) {
	// Create time filter consumer
	consumer := workerqueue.NewQueueConsumer(
		queues.TimeFilterQueue,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create time filter consumer")
	}

	// Declare the time filter queue using QueueMiddleware
	timeFilterConsumer := workerqueue.NewMessageMiddlewareQueue(
		queues.TimeFilterQueue,
		config,
	)
	if timeFilterConsumer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}
	if err := timeFilterConsumer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		timeFilterConsumer.Close()
		return nil, fmt.Errorf("failed to declare time filter queue: %v", err)
	}
	timeFilterConsumer.Close() // Close the declarer as we don't need it anymore

	// Create amount filter producer
	amountFilterProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.AmountFilterQueue,
		config,
	)
	if amountFilterProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create amount filter producer")
	}

	// Create reply producer
	replyProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.ReplyFilterBusQueue,
		config,
	)
	if replyProducer == nil {
		consumer.Close()
		amountFilterProducer.Close()
		return nil, fmt.Errorf("failed to create reply producer")
	}

	// Declare producer queues

	if err := amountFilterProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		amountFilterProducer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to declare amount filter queue: %v", err)
	}

	if err := replyProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		amountFilterProducer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to declare reply queue: %v", err)
	}

	return &TimeFilterWorker{
		consumer:             consumer,
		amountFilterProducer: amountFilterProducer,
		replyProducer:        replyProducer,
		config:               config,
	}, nil
}

// Start starts the time filter worker
func (tfw *TimeFilterWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Time Filter Worker: Starting to listen for messages...")
	err := tfw.consumer.StartConsuming(tfw.createCallback())
	if err != 0 {
		fmt.Printf("Time Filter Worker: ERROR - StartConsuming failed with error: %v\n", err)
		return err
	}
	fmt.Println("Time Filter Worker: Successfully registered as consumer")
	return 0
}

// Close closes all connections
func (tfw *TimeFilterWorker) Close() {
	if tfw.consumer != nil {
		tfw.consumer.Close()
	}
	if tfw.amountFilterProducer != nil {
		tfw.amountFilterProducer.Close()
	}
	if tfw.replyProducer != nil {
		tfw.replyProducer.Close()
	}
}

// createCallback creates the message processing callback
func (tfw *TimeFilterWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Time Filter Worker: Callback started, waiting for messages...")
		messageCount := 0
		for delivery := range *consumeChannel {
			messageCount++
			fmt.Printf("Time Filter Worker: Received message #%d\n", messageCount)
			if err := tfw.processMessage(delivery); err != 0 {
				fmt.Printf("Time Filter Worker: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		fmt.Printf("Time Filter Worker: Consume channel closed after processing %d messages\n", messageCount)
		done <- nil
	}
}
