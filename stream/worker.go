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
	query2Consumer *workerqueue.QueueConsumer
	query3Consumer *workerqueue.QueueConsumer
	config         *middleware.ConnectionConfig
	printedSchemas map[string]bool // Track which queries have had their schema printed
	// For final filtering that requires all chunks
	collectedChunks map[string][]string // clientID -> []chunkData
	chunkCounts     map[string]int      // clientID -> total chunks expected
	receivedCounts  map[string]int      // clientID -> chunks received
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

	// Create Query2 results consumer
	query2Consumer := workerqueue.NewQueueConsumer(
		Query2ResultsQueue,
		config,
	)
	if query2Consumer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create Query2 results consumer")
	}

	// Create Query3 results consumer
	query3Consumer := workerqueue.NewQueueConsumer(
		Query3ResultsQueue,
		config,
	)
	if query3Consumer == nil {
		consumer.Close()
		query2Consumer.Close()
		return nil, fmt.Errorf("failed to create Query3 results consumer")
	}

	return &StreamingWorker{
		consumer:        consumer,
		query2Consumer:  query2Consumer,
		query3Consumer:  query3Consumer,
		config:          config,
		printedSchemas:  make(map[string]bool),
		collectedChunks: make(map[string][]string),
		chunkCounts:     make(map[string]int),
		receivedCounts:  make(map[string]int),
	}, nil
}

// Start starts the streaming worker
func (sw *StreamingWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Streaming Worker: Starting to listen for messages...")

	// Start consuming from streaming exchange
	go func() {
		if err := sw.consumer.StartConsuming(sw.createCallback()); err != 0 {
			fmt.Printf("Failed to start streaming consumer: %v\n", err)
		}
	}()

	// Start consuming from Query2 results queue
	go func() {
		if err := sw.query2Consumer.StartConsuming(sw.createQuery2Callback()); err != 0 {
			fmt.Printf("Failed to start Query2 results consumer: %v\n", err)
		}
	}()

	// Start consuming from Query3 results queue
	go func() {
		if err := sw.query3Consumer.StartConsuming(sw.createQuery3Callback()); err != 0 {
			fmt.Printf("Failed to start Query3 results consumer: %v\n", err)
		}
	}()

	return 0
}

// Close closes all connections
func (sw *StreamingWorker) Close() {
	if sw.consumer != nil {
		sw.consumer.Close()
	}
	if sw.query2Consumer != nil {
		sw.query2Consumer.Close()
	}
	if sw.query3Consumer != nil {
		sw.query3Consumer.Close()
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
