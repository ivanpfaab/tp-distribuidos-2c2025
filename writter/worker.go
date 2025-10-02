package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// DataWriterWorker encapsulates the data writer worker state and dependencies
type DataWriterWorker struct {
	consumer *workerqueue.QueueConsumer
	config   *middleware.ConnectionConfig
}

// NewDataWriterWorker creates a new DataWriterWorker instance
func NewDataWriterWorker(config *middleware.ConnectionConfig) (*DataWriterWorker, error) {
	// Create data writer consumer
	consumer := workerqueue.NewQueueConsumer(
		DataWriterQueueName,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create data writer consumer")
	}

	return &DataWriterWorker{
		consumer: consumer,
		config:   config,
	}, nil
}

// DeclareQueue declares the data writer queue on RabbitMQ
func (dww *DataWriterWorker) DeclareQueue() middleware.MessageMiddlewareError {
	// Create a temporary producer to declare the queue
	queueProducer := workerqueue.NewMessageMiddlewareQueue(
		DataWriterQueueName,
		dww.config,
	)
	if queueProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}
	defer queueProducer.Close()

	// Declare the queue (durable, not auto-delete, not exclusive, wait for confirmation)
	if err := queueProducer.DeclareQueue(true, false, false, false); err != 0 {
		return err
	}

	fmt.Printf("Data Writer: Queue '%s' declared successfully\n", DataWriterQueueName)
	return 0
}

// Start starts the data writer worker
func (dww *DataWriterWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Data Writer: Starting to listen for chunks...")
	return dww.consumer.StartConsuming(dww.createCallback())
}

// Close closes all connections
func (dww *DataWriterWorker) Close() {
	if dww.consumer != nil {
		dww.consumer.Close()
	}
}

// createCallback creates the message processing callback
func (dww *DataWriterWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Data Writer: Starting to listen for chunks...")
		for delivery := range *consumeChannel {
			if err := dww.processMessage(delivery); err != 0 {
				fmt.Printf("Data Writer: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
