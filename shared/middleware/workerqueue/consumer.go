package workerqueue

import (
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
		middleware.LogError("Queue Consumer", "Failed to create channel for queue '%s': %v", queueName, err)
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
	if m.MessageMiddlewareQueue.Channel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Start consuming from the queue
	deliveries, err := (*m.MessageMiddlewareQueue.Channel).Consume(
		m.MessageMiddlewareQueue.QueueName,
		"",    // consumer tag (empty for auto-generated)
		false, // auto-ack (we'll handle acknowledgments manually)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		middleware.LogError("Queue Consumer", "Failed to start consuming for queue '%s': %v", m.MessageMiddlewareQueue.QueueName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Set the consume channel
	m.MessageMiddlewareQueue.ConsumeChannel = &deliveries

	// Start the processing loop in a goroutine
	go func() {
		done := make(chan error, 1)
		middleware.LogDebug("Queue Consumer", "Starting consumer for queue '%s'", m.MessageMiddlewareQueue.QueueName)

		// Call the onMessageCallback with the consume channel
		onMessageCallback(m.MessageMiddlewareQueue.ConsumeChannel, done)
	}()

	return 0
}

// StopConsuming implements the consumer shutdown logic.
func (m *QueueConsumer) StopConsuming() middleware.MessageMiddlewareError {
	if m.MessageMiddlewareQueue.Channel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	if m.MessageMiddlewareQueue.ConsumeChannel == nil {
		middleware.LogDebug("Queue Consumer", "Not consuming for queue '%s', StopConsuming has no effect", m.MessageMiddlewareQueue.QueueName)
		return 0
	}

	// Cancel the consumer
	err := (*m.MessageMiddlewareQueue.Channel).Cancel("", false) // Empty string cancels all consumers on this channel
	if err != nil {
		middleware.LogError("Queue Consumer", "Failed to cancel consumer for queue '%s': %v", m.MessageMiddlewareQueue.QueueName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Clear the consume channel reference
	m.MessageMiddlewareQueue.ConsumeChannel = nil
	middleware.LogDebug("Queue Consumer", "Consumer halted for queue '%s'", m.MessageMiddlewareQueue.QueueName)

	return 0
}

// Close disconnects the channel.
func (m *QueueConsumer) Close() middleware.MessageMiddlewareError {
	if m.MessageMiddlewareQueue.Channel == nil {
		return 0 // Already closed
	}

	// Stop consuming first if we're consuming
	if m.MessageMiddlewareQueue.ConsumeChannel != nil {
		stopErr := m.StopConsuming()
		if stopErr != 0 {
			middleware.LogError("Queue Consumer", "Error stopping consumption during close for queue '%s': %v", m.MessageMiddlewareQueue.QueueName, stopErr)
		}
	}

	// Close the AMQP channel
	err := (*m.MessageMiddlewareQueue.Channel).Close()
	if err != nil {
		middleware.LogError("Queue Consumer", "Close error for queue '%s': %v", m.MessageMiddlewareQueue.QueueName, err)
		return middleware.MessageMiddlewareCloseError
	}

	m.MessageMiddlewareQueue.Channel = nil
	middleware.LogDebug("Queue Consumer", "Channel closed for queue '%s'", m.MessageMiddlewareQueue.QueueName)

	return 0
}
