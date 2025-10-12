package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// QueryGateway encapsulates the query gateway state and dependencies
type QueryGateway struct {
	consumer                 *workerqueue.QueueConsumer
	itemIdGroupByProducer    *workerqueue.QueueMiddleware // Query 2 -> MapReduce
	storeIdGroupByProducer   *workerqueue.QueueMiddleware // Query 3/4 -> Dummy GroupBy
	query1ResultsProducer    *workerqueue.QueueMiddleware
	config                   *middleware.ConnectionConfig
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

	// Initialize queue producer for sending chunks to ItemID GroupBy (Query 2 Map Worker)
	itemIdGroupByProducer := workerqueue.NewMessageMiddlewareQueue(
		ItemIdGroupByChunkQueue,
		config,
	)
	if itemIdGroupByProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create ItemID GroupBy producer")
	}

	// Declare the ItemID GroupBy producer queue
	if err := itemIdGroupByProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdGroupByProducer.Close()
		return nil, fmt.Errorf("failed to declare ItemID GroupBy queue: %v", err)
	}

	// Initialize queue producer for sending chunks to StoreID GroupBy (Query 3/4 Dummy Worker)
	storeIdGroupByProducer := workerqueue.NewMessageMiddlewareQueue(
		StoreIdGroupByChunkQueue,
		config,
	)
	if storeIdGroupByProducer == nil {
		consumer.Close()
		itemIdGroupByProducer.Close()
		return nil, fmt.Errorf("failed to create StoreID GroupBy producer")
	}

	// Declare the StoreID GroupBy producer queue
	if err := storeIdGroupByProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdGroupByProducer.Close()
		storeIdGroupByProducer.Close()
		return nil, fmt.Errorf("failed to declare StoreID GroupBy queue: %v", err)
	}

	// Initialize queue producer for Query 1 results
	query1ResultsProducer := workerqueue.NewMessageMiddlewareQueue(
		Query1ResultsQueue,
		config,
	)
	if query1ResultsProducer == nil {
		consumer.Close()
		itemIdGroupByProducer.Close()
		storeIdGroupByProducer.Close()
		return nil, fmt.Errorf("failed to create Query1 results producer")
	}

	// Declare the Query1 results queue
	if err := query1ResultsProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		itemIdGroupByProducer.Close()
		storeIdGroupByProducer.Close()
		query1ResultsProducer.Close()
		return nil, fmt.Errorf("failed to declare Query1 results queue: %v", err)
	}

	return &QueryGateway{
		consumer:               consumer,
		itemIdGroupByProducer:  itemIdGroupByProducer,
		storeIdGroupByProducer: storeIdGroupByProducer,
		query1ResultsProducer:  query1ResultsProducer,
		config:                 config,
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
	if qg.itemIdGroupByProducer != nil {
		qg.itemIdGroupByProducer.Close()
	}
	if qg.storeIdGroupByProducer != nil {
		qg.storeIdGroupByProducer.Close()
	}
	if qg.query1ResultsProducer != nil {
		qg.query1ResultsProducer.Close()
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
