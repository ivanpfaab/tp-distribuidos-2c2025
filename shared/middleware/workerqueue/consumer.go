package workerqueue

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"fmt"
)

// StartConsuming implements the consumer startup logic for MessageMiddlewareQueue.
func (q *MessageMiddlewareQueue) StartConsuming(
	m *MessageMiddlewareQueue,
	onMessageCallback onMessageCallback,
) MessageMiddlewareError {
	if m.channel == nil {
		return MessageMiddlewareDisconnectedError
	}
	
	// Start consuming from the queue
	deliveries, err := (*m.channel).Consume(
		m.queueName,
		"",    // consumer tag (empty for auto-generated)
		false, // auto-ack (we'll handle acknowledgments manually)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		fmt.Printf("Queue '%s' Consumer: Failed to start consuming: %v\n", m.queueName, err)
		return MessageMiddlewareMessageError
	}
	
	// Set the consume channel
	m.consumeChannel = (*ConsumeChannel)(&deliveries)

	// Start the processing loop in a goroutine
	go func() {
		done := make(chan error, 1) 
		fmt.Printf("Queue '%s' Consumer: Starting consumer.\n", m.queueName)

		// Call the onMessageCallback with the consume channel
		onMessageCallback(m.consumeChannel, done)
	}()

	return 0
}

// StopConsuming implements the consumer shutdown logic.
func (q *MessageMiddlewareQueue) StopConsuming(
	m *MessageMiddlewareQueue,
) MessageMiddlewareError {
	if m.channel == nil {
		return MessageMiddlewareDisconnectedError
	}

	if m.consumeChannel == nil {
		fmt.Printf("Queue '%s' Consumer: Not consuming, StopConsuming has no effect.\n", m.queueName)
		return 0 
	}
	
	// Cancel the consumer
	err := (*m.channel).Cancel("", false) // Empty string cancels all consumers on this channel
	if err != nil {
		fmt.Printf("Queue '%s' Consumer: Failed to cancel consumer: %v\n", m.queueName, err)
		return MessageMiddlewareMessageError
	}

	// Close the consume channel
	if m.consumeChannel != nil {
		close(*m.consumeChannel)
	}
	m.consumeChannel = nil 
	fmt.Printf("Queue '%s' Consumer: Halted.\n", m.queueName)

	return 0
}

// Close disconnects the channel.
func (q *MessageMiddlewareQueue) Close(
	m *MessageMiddlewareQueue,
) MessageMiddlewareError {
	if m.channel == nil {
		return 0 // Already closed
	}

	// Stop consuming first if we're consuming
	if m.consumeChannel != nil {
		stopErr := q.StopConsuming(m)
		if stopErr != 0 {
			fmt.Printf("Queue '%s': Error stopping consumption during close: %v\n", m.queueName, stopErr)
		}
	}

	// Close the AMQP channel
	err := (*m.channel).Close()
	if err != nil {
		fmt.Printf("Queue '%s': Close error: %v\n", m.queueName, err)
		return MessageMiddlewareCloseError
	}

	m.channel = nil 
	fmt.Printf("Queue '%s': Channel closed.\n", m.queueName)

	return 0
}
