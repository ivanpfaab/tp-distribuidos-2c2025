package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// DataWriterWorker encapsulates the data writer worker state and dependencies
type DataWriterWorker struct {
	consumer     *workerqueue.QueueConsumer
	joinProducer *exchange.ExchangeMiddleware
	config       *middleware.ConnectionConfig
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

	// Create join producer for sending reference data to join worker
	joinProducer := exchange.NewMessageMiddlewareExchange(
		"join-exchange",
		[]string{"join"},
		config,
	)
	if joinProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create join producer")
	}

	return &DataWriterWorker{
		consumer:     consumer,
		joinProducer: joinProducer,
		config:       config,
	}, nil
}

// DeclareQueue declares the data writer queue and join exchange on RabbitMQ
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

	// Declare the queue (non-durable, not auto-delete, not exclusive, wait for confirmation)
	if err := queueProducer.DeclareQueue(false, false, false, false); err != 0 {
		return err
	}

	// Declare the join exchange
	if err := dww.joinProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	fmt.Printf("Data Writer: Queue '%s' and join exchange declared successfully\n", DataWriterQueueName)
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
	if dww.joinProducer != nil {
		dww.joinProducer.Close()
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

// SendReferenceDataToJoinWorker sends reference data to the join worker
func (dww *DataWriterWorker) SendReferenceDataToJoinWorker(fileID string, data []byte) middleware.MessageMiddlewareError {
	fmt.Printf("Data Writer: Sending reference data for FileID: %s to join worker\n", fileID)
	fmt.Printf("Data Writer: Message content: %s\n", string(data))

	// Send the reference data to the join worker
	if err := dww.joinProducer.Send(data, []string{"join"}); err != 0 {
		fmt.Printf("Data Writer: Failed to send reference data to join worker: %v\n", err)
		return err
	}

	fmt.Printf("Data Writer: Successfully sent reference data for FileID: %s to join worker\n", fileID)
	return 0
}
