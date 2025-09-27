package workerqueue

import (
	"fmt"
	"tp-distribuidos-2c2025/shared/middleware"
)

// QueueConsumer wraps the middleware.MessageMiddlewareQueue with consumer methods
type QueueConsumer struct {
	*middleware.MessageMiddlewareQueue
}

// NewQueueConsumer creates a new QueueConsumer instance
func NewQueueConsumer(
	queueName string,
	config *middleware.ConnectionConfig,
) *QueueConsumer {
	// Create channel
	channel, err := middleware.CreateMiddlewareChannel(config)
	if err != nil {
		fmt.Printf("Queue '%s' Consumer: Failed to create channel: %v\n", queueName, err)
		return nil
	}

	return &QueueConsumer{
		MessageMiddlewareQueue: &middleware.MessageMiddlewareQueue{
			QueueName: queueName,
			Channel:   channel,
		},
	}
}

// StartConsuming implements the consumer startup logic for MessageMiddlewareQueue.
func (m *QueueConsumer) StartConsuming(
	onMessageCallback middleware.OnMessageCallback,
) middleware.MessageMiddlewareError {
	if m.Channel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}
	
	// Start consuming from the queue
	deliveries, err := (*m.Channel).Consume(
		m.QueueName,
		"",    // consumer tag (empty for auto-generated)
		false, // auto-ack (we'll handle acknowledgments manually)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		fmt.Printf("Queue '%s' Consumer: Failed to start consuming: %v\n", m.QueueName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Set the consume channel
	m.ConsumeChannel = &deliveries

	// Start the processing loop in a goroutine
	go func() {
		done := make(chan error, 1)
		fmt.Printf("Queue '%s' Consumer: Starting consumer.\n", m.QueueName)

		// Call the onMessageCallback with the consume channel
		onMessageCallback(m.ConsumeChannel, done)
	}()

	return 0
}

// StopConsuming implements the consumer shutdown logic.
func (m *QueueConsumer) StopConsuming() middleware.MessageMiddlewareError {
	if m.Channel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}
	
	if m.ConsumeChannel == nil {
		fmt.Printf("Queue '%s' Consumer: Not consuming, StopConsuming has no effect.\n", m.QueueName)
		return 0 
	}
	
	// Cancel the consumer
	err := (*m.Channel).Cancel("", false) // Empty string cancels all consumers on this channel
	if err != nil {
		fmt.Printf("Queue '%s' Consumer: Failed to cancel consumer: %v\n", m.QueueName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Clear the consume channel reference
	m.ConsumeChannel = nil 
	fmt.Printf("Queue '%s' Consumer: Halted.\n", m.QueueName)

	return 0
}

// Close disconnects the channel.
func (m *QueueConsumer) Close() middleware.MessageMiddlewareError {
	if m.Channel == nil {
		return 0 // Already closed
	}

	// Stop consuming first if we're consuming
	if m.ConsumeChannel != nil {
		stopErr := m.StopConsuming()
		if stopErr != 0 {
			fmt.Printf("Queue '%s': Error stopping consumption during close: %v\n", m.QueueName, stopErr)
		}
	}

	// Close the AMQP channel
	err := (*m.Channel).Close()
	if err != nil {
		fmt.Printf("Queue '%s': Close error: %v\n", m.QueueName, err)
		return middleware.MessageMiddlewareCloseError
	}

	m.Channel = nil 
	fmt.Printf("Queue '%s': Channel closed.\n", m.QueueName)

	return 0
}
