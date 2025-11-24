package main

import (
	"fmt"

	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// AmountFilterWorker encapsulates the amount filter worker state and dependencies
type AmountFilterWorker struct {
	consumer      *workerqueue.QueueConsumer
	replyProducer *workerqueue.QueueMiddleware
	config        *middleware.ConnectionConfig
	messageManager *messagemanager.MessageManager
}

// NewAmountFilterWorker creates a new AmountFilterWorker instance
func NewAmountFilterWorker(config *middleware.ConnectionConfig) (*AmountFilterWorker, error) {
	// Create amount filter consumer
	consumer := workerqueue.NewQueueConsumer(
		queues.AmountFilterQueue,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create amount filter consumer")
	}

	// Declare the amount filter queue using QueueMiddleware
	amountFilterConsumer := workerqueue.NewMessageMiddlewareQueue(
		queues.AmountFilterQueue,
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
		queues.ReplyFilterBusQueue,
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

	// Initialize MessageManager for fault tolerance
	messageManager := messagemanager.NewMessageManager("/app/worker-data/processed-ids.txt")
	if err := messageManager.LoadProcessedIDs(); err != nil {
		fmt.Printf("Amount Filter Worker: Warning - failed to load processed IDs: %v (starting with empty state)\n", err)
	} else {
		count := messageManager.GetProcessedCount()
		fmt.Printf("Amount Filter Worker: Loaded %d processed IDs\n", count)
	}

	return &AmountFilterWorker{
		consumer:       consumer,
		replyProducer:  replyProducer,
		config:         config,
		messageManager: messageManager,
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
	if afw.messageManager != nil {
		afw.messageManager.Close()
	}
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
