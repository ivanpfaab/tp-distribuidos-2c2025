package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// QueryGateway encapsulates the query gateway state and dependencies
type QueryGateway struct {
	consumer              *workerqueue.QueueConsumer
	query2GroupByProducer *workerqueue.QueueMiddleware // Query 2 - MapReduce
	query3GroupByProducer *workerqueue.QueueMiddleware // Query 3 - MapReduce
	query4GroupByProducer *workerqueue.QueueMiddleware // Query 4 - MapReduce
	query1ResultsProducer *workerqueue.QueueMiddleware
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

	// Initialize queue producer for sending chunks to Query 2 GroupBy (MapReduce Worker)
	query2GroupByProducer := workerqueue.NewMessageMiddlewareQueue(
		ItemIdGroupByChunkQueue, // query2-map-queue
		config,
	)
	if query2GroupByProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create Query 2 GroupBy producer")
	}

	// Declare the Query 2 GroupBy producer queue
	if err := query2GroupByProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		query2GroupByProducer.Close()
		return nil, fmt.Errorf("failed to declare Query 2 GroupBy queue: %v", err)
	}

	// Initialize queue producer for sending chunks to Query 3 GroupBy (MapReduce Worker)
	query3GroupByProducer := workerqueue.NewMessageMiddlewareQueue(
		StoreIdGroupByChunkQueue, // query3-map-queue
		config,
	)
	if query3GroupByProducer == nil {
		consumer.Close()
		query2GroupByProducer.Close()
		return nil, fmt.Errorf("failed to create Query 3 GroupBy producer")
	}

	// Declare the Query 3 GroupBy producer queue
	if err := query3GroupByProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		query2GroupByProducer.Close()
		query3GroupByProducer.Close()
		return nil, fmt.Errorf("failed to declare Query 3 GroupBy queue: %v", err)
	}

	// Initialize queue producer for sending chunks to Query 4 GroupBy (MapReduce Worker)
	query4GroupByProducer := workerqueue.NewMessageMiddlewareQueue(
		Query4MapQueue, // query4-map-queue
		config,
	)
	if query4GroupByProducer == nil {
		consumer.Close()
		query2GroupByProducer.Close()
		query3GroupByProducer.Close()
		return nil, fmt.Errorf("failed to create Query 4 GroupBy producer")
	}

	// Declare the Query 4 GroupBy producer queue
	if err := query4GroupByProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		query2GroupByProducer.Close()
		query3GroupByProducer.Close()
		query4GroupByProducer.Close()
		return nil, fmt.Errorf("failed to declare Query 4 GroupBy queue: %v", err)
	}

	// Initialize queue producer for Query 1 results
	query1ResultsProducer := workerqueue.NewMessageMiddlewareQueue(
		Query1ResultsQueue,
		config,
	)
	if query1ResultsProducer == nil {
		consumer.Close()
		query2GroupByProducer.Close()
		query3GroupByProducer.Close()
		query4GroupByProducer.Close()
		return nil, fmt.Errorf("failed to create Query 1 results producer")
	}

	// Declare the Query 1 results queue
	if err := query1ResultsProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		query2GroupByProducer.Close()
		query3GroupByProducer.Close()
		query4GroupByProducer.Close()
		query1ResultsProducer.Close()
		return nil, fmt.Errorf("failed to declare Query 1 results queue: %v", err)
	}

	return &QueryGateway{
		consumer:              consumer,
		query2GroupByProducer: query2GroupByProducer,
		query3GroupByProducer: query3GroupByProducer,
		query4GroupByProducer: query4GroupByProducer,
		query1ResultsProducer: query1ResultsProducer,
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
	if qg.query2GroupByProducer != nil {
		qg.query2GroupByProducer.Close()
	}
	if qg.query3GroupByProducer != nil {
		qg.query3GroupByProducer.Close()
	}
	if qg.query4GroupByProducer != nil {
		qg.query4GroupByProducer.Close()
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
