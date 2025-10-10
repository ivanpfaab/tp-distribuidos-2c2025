package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// StreamingWorker encapsulates the streaming worker state and dependencies
type StreamingWorker struct {
	consumer       *exchange.ExchangeConsumer
	query1Consumer *workerqueue.QueueConsumer
	query2Consumer *workerqueue.QueueConsumer
	query3Consumer *workerqueue.QueueConsumer
	query4Consumer *workerqueue.QueueConsumer
	config         *middleware.ConnectionConfig
}

// NewStreamingWorker creates a new StreamingWorker instance
func NewStreamingWorker(config *middleware.ConnectionConfig) (*StreamingWorker, error) {
	// Create streaming consumer
	consumer := exchange.NewExchangeConsumer(
		StreamingExchangeName,
		[]string{StreamingRoutingKey},
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create streaming consumer")
	}

	// Declare Query1 results queue
	query1QueueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		Query1ResultsQueue,
		config,
	)
	if query1QueueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create Query1 queue declarer")
	}
	if err := query1QueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		query1QueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare Query1 results queue: %v", err)
	}
	query1QueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create Query1 results consumer
	query1Consumer := workerqueue.NewQueueConsumer(
		Query1ResultsQueue,
		config,
	)
	if query1Consumer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create Query1 results consumer")
	}

	// Declare Query2 results queue
	query2QueueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		Query2ResultsQueue,
		config,
	)
	if query2QueueDeclarer == nil {
		consumer.Close()
		query1Consumer.Close()
		return nil, fmt.Errorf("failed to create Query2 queue declarer")
	}
	if err := query2QueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		query1Consumer.Close()
		query2QueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare Query2 results queue: %v", err)
	}
	query2QueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create Query2 results consumer
	query2Consumer := workerqueue.NewQueueConsumer(
		Query2ResultsQueue,
		config,
	)
	if query2Consumer == nil {
		consumer.Close()
		query1Consumer.Close()
		return nil, fmt.Errorf("failed to create Query2 results consumer")
	}

	// Declare Query3 results queue
	query3QueueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		Query3ResultsQueue,
		config,
	)
	if query3QueueDeclarer == nil {
		consumer.Close()
		query1Consumer.Close()
		query2Consumer.Close()
		return nil, fmt.Errorf("failed to create Query3 queue declarer")
	}
	if err := query3QueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		query1Consumer.Close()
		query2Consumer.Close()
		query3QueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare Query3 results queue: %v", err)
	}
	query3QueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create Query3 results consumer
	query3Consumer := workerqueue.NewQueueConsumer(
		Query3ResultsQueue,
		config,
	)
	if query3Consumer == nil {
		consumer.Close()
		query1Consumer.Close()
		query2Consumer.Close()
		return nil, fmt.Errorf("failed to create Query3 results consumer")
	}

	// Declare Query4 results queue
	query4QueueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		Query4ResultsQueue,
		config,
	)
	if query4QueueDeclarer == nil {
		consumer.Close()
		query1Consumer.Close()
		query2Consumer.Close()
		query3Consumer.Close()
		return nil, fmt.Errorf("failed to create Query4 queue declarer")
	}
	if err := query4QueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		query1Consumer.Close()
		query2Consumer.Close()
		query3Consumer.Close()
		query4QueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare Query4 results queue: %v", err)
	}
	query4QueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create Query4 results consumer
	query4Consumer := workerqueue.NewQueueConsumer(
		Query4ResultsQueue,
		config,
	)
	if query4Consumer == nil {
		consumer.Close()
		query1Consumer.Close()
		query2Consumer.Close()
		query3Consumer.Close()
		return nil, fmt.Errorf("failed to create Query4 results consumer")
	}

	return &StreamingWorker{
		consumer:       consumer,
		query1Consumer: query1Consumer,
		query2Consumer: query2Consumer,
		query3Consumer: query3Consumer,
		query4Consumer: query4Consumer,
		config:         config,
	}, nil
}

// Start starts the streaming worker
func (sw *StreamingWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Streaming Worker: Starting to listen for messages...")

	if err := sw.consumer.StartConsuming(sw.createCallback()); err != 0 {
		fmt.Printf("Failed to start streaming consumer: %v\n", err)
	}

	if err := sw.query1Consumer.StartConsuming(sw.createQuery1Callback()); err != 0 {
		fmt.Printf("Failed to start Query1 results consumer: %v\n", err)
	}


	if err := sw.query2Consumer.StartConsuming(sw.createQuery2Callback()); err != 0 {
		fmt.Printf("Failed to start Query2 results consumer: %v\n", err)
	}

	if err := sw.query3Consumer.StartConsuming(sw.createQuery3Callback()); err != 0 {
		fmt.Printf("Failed to start Query3 results consumer: %v\n", err)
	}

	if err := sw.query4Consumer.StartConsuming(sw.createQuery4Callback()); err != 0 {
		fmt.Printf("Failed to start Query4 results consumer: %v\n", err)
	}

	return 0
}

// Close closes all connections
func (sw *StreamingWorker) Close() {
	if sw.consumer != nil {
		sw.consumer.Close()
	}
	if sw.query1Consumer != nil {
		sw.query1Consumer.Close()
	}
	if sw.query2Consumer != nil {
		sw.query2Consumer.Close()
	}
	if sw.query3Consumer != nil {
		sw.query3Consumer.Close()
	}
	if sw.query4Consumer != nil {
		sw.query4Consumer.Close()
	}
}

// createCallback creates the message processing callback
func (sw *StreamingWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Streaming Worker: Starting to listen for messages...")
		for delivery := range *consumeChannel {
			if err := sw.processMessage(delivery); err != 0 {
				fmt.Printf("Streaming Worker: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// createQuery1Callback creates the message processing callback for Query1 results
func (sw *StreamingWorker) createQuery1Callback() middleware.OnMessageCallback {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Streaming Worker: Starting to listen for Query1 results...")
		for delivery := range *consumeChannel {
			if err := sw.processMessage(delivery); err != 0 {
				fmt.Printf("Streaming Worker: Failed to process Query1 message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// createQuery2Callback creates the message processing callback for Query2 results
func (sw *StreamingWorker) createQuery2Callback() middleware.OnMessageCallback {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Streaming Worker: Starting to listen for Query2 results...")
		for delivery := range *consumeChannel {
			if err := sw.processMessage(delivery); err != 0 {
				fmt.Printf("Streaming Worker: Failed to process Query2 message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// createQuery3Callback creates the message processing callback for Query3 results
func (sw *StreamingWorker) createQuery3Callback() middleware.OnMessageCallback {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Streaming Worker: Starting to listen for Query3 results...")
		for delivery := range *consumeChannel {
			if err := sw.processMessage(delivery); err != 0 {
				fmt.Printf("Streaming Worker: Failed to process Query3 message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// createQuery4Callback creates the message processing callback for Query4 results
func (sw *StreamingWorker) createQuery4Callback() middleware.OnMessageCallback {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("Streaming Worker: Starting to listen for Query4 results...")
		for delivery := range *consumeChannel {
			if err := sw.processMessage(delivery); err != 0 {
				fmt.Printf("Streaming Worker: Failed to process Query4 message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
