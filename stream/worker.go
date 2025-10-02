package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
)

// StreamingWorker encapsulates the streaming worker state and dependencies
type StreamingWorker struct {
	consumer       *exchange.ExchangeConsumer
	config         *middleware.ConnectionConfig
	printedSchemas map[string]bool // Track which queries have had their schema printed
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

	return &StreamingWorker{
		consumer:       consumer,
		config:         config,
		printedSchemas: make(map[string]bool),
	}, nil
}

// Start starts the streaming worker
func (sw *StreamingWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Streaming Worker: Starting to listen for messages...")
	return sw.consumer.StartConsuming(sw.createCallback())
}

// Close closes all connections
func (sw *StreamingWorker) Close() {
	if sw.consumer != nil {
		sw.consumer.Close()
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
