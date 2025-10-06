package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// QueryGateway encapsulates the query gateway state and dependencies
type QueryGateway struct {
	consumer *workerqueue.QueueConsumer
	config   *middleware.ConnectionConfig
}

// NewQueryGateway creates a new QueryGateway instance
func NewQueryGateway(config *middleware.ConnectionConfig) (*QueryGateway, error) {
	// Create reply-filter-bus consumer
	consumer := workerqueue.NewQueueConsumer(
		ReplyFilterBusQueueName,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create query gateway consumer")
	}

	// Declare the reply-filter-bus queue using QueueMiddleware
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		ReplyFilterBusQueueName,
		config,
	)
	if queueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer")
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare reply-filter-bus queue: %v", err)
	}
	queueDeclarer.Close() // Close the declarer as we don't need it anymore

	return &QueryGateway{
		consumer: consumer,
		config:   config,
	}, nil
}

// Start starts the query gateway
func (qg *QueryGateway) Start() middleware.MessageMiddlewareError {
	fmt.Println("Query Gateway: Starting to listen for messages from reply-filter-bus...")
	return qg.consumer.StartConsuming(qg.createCallback())
}

// Close closes all connections
func (qg *QueryGateway) Close() {
	if qg.consumer != nil {
		qg.consumer.Close()
	}
}

// createCallback creates the message processing callback
func (qg *QueryGateway) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Query Gateway: Starting to listen for messages...")
		for delivery := range *consumeChannel {
			if err := qg.processMessage(delivery); err != 0 {
				fmt.Printf("Query Gateway: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
