package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// QueryGateway encapsulates the query gateway state and dependencies
type QueryGateway struct {
	consumer              *workerqueue.QueueConsumer
	itemIdJoinProducer    *workerqueue.QueueMiddleware
	storeIdJoinProducer   *workerqueue.QueueMiddleware
	query1ResultsProducer *workerqueue.QueueMiddleware
	query4ResultsProducer *workerqueue.QueueMiddleware
	config                *middleware.ConnectionConfig
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

	// Initialize queue producer for sending chunks to ItemID join worker
	itemIdJoinProducer := workerqueue.NewMessageMiddlewareQueue(
		ItemIdJoinChunkQueue,
		config,
	)
	if itemIdJoinProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create ItemID join producer")
	}

	// Declare the ItemID join producer queue
	if err := itemIdJoinProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdJoinProducer.Close()
		return nil, fmt.Errorf("failed to declare ItemID join queue: %v", err)
	}

	// Initialize queue producer for sending chunks to StoreID join worker
	storeIdJoinProducer := workerqueue.NewMessageMiddlewareQueue(
		StoreIdJoinChunkQueue,
		config,
	)
	if storeIdJoinProducer == nil {
		consumer.Close()
		itemIdJoinProducer.Close()
		return nil, fmt.Errorf("failed to create StoreID join producer")
	}

	// Declare the StoreID join producer queue
	if err := storeIdJoinProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdJoinProducer.Close()
		storeIdJoinProducer.Close()
		return nil, fmt.Errorf("failed to declare StoreID join queue: %v", err)
	}

	// Initialize queue producer for Query 1 results
	query1ResultsProducer := workerqueue.NewMessageMiddlewareQueue(
		Query1ResultsQueue,
		config,
	)
	if query1ResultsProducer == nil {
		consumer.Close()
		itemIdJoinProducer.Close()
		storeIdJoinProducer.Close()
		return nil, fmt.Errorf("failed to create Query1 results producer")
	}

	// Declare the Query1 results queue
	if err := query1ResultsProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdJoinProducer.Close()
		storeIdJoinProducer.Close()
		query1ResultsProducer.Close()
		return nil, fmt.Errorf("failed to declare Query1 results queue: %v", err)
	}

	// Initialize queue producer for Query 4 results
	query4ResultsProducer := workerqueue.NewMessageMiddlewareQueue(
		Query4ResultsQueue,
		config,
	)
	if query4ResultsProducer == nil {
		consumer.Close()
		itemIdJoinProducer.Close()
		storeIdJoinProducer.Close()
		query1ResultsProducer.Close()
		return nil, fmt.Errorf("failed to create Query4 results producer")
	}

	// Declare the Query4 results queue
	if err := query4ResultsProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdJoinProducer.Close()
		storeIdJoinProducer.Close()
		query1ResultsProducer.Close()
		query4ResultsProducer.Close()
		return nil, fmt.Errorf("failed to declare Query4 results queue: %v", err)
	}

	return &QueryGateway{
		consumer:              consumer,
		itemIdJoinProducer:    itemIdJoinProducer,
		storeIdJoinProducer:   storeIdJoinProducer,
		query1ResultsProducer: query1ResultsProducer,
		query4ResultsProducer: query4ResultsProducer,
		config:                config,
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
	if qg.itemIdJoinProducer != nil {
		qg.itemIdJoinProducer.Close()
	}
	if qg.storeIdJoinProducer != nil {
		qg.storeIdJoinProducer.Close()
	}
	if qg.query1ResultsProducer != nil {
		qg.query1ResultsProducer.Close()
	}
	if qg.query4ResultsProducer != nil {
		qg.query4ResultsProducer.Close()
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
