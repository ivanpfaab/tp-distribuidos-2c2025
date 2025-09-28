package workerqueue

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"tp-distribuidos-2c2025/shared/middleware"
)

// QueueMiddleware wraps the middleware.MessageMiddlewareQueue with additional methods
type QueueMiddleware struct {
	*middleware.MessageMiddlewareQueue
}

// NewMessageMiddlewareQueue creates a new QueueMiddleware instance
func NewMessageMiddlewareQueue(queueName string, config *middleware.ConnectionConfig) *QueueMiddleware {
	// Create channel
	channel, err := middleware.CreateMiddlewareChannel(config)
	if err != nil {
		middleware.LogError("Queue Producer", "Failed to create channel for queue '%s': %v", queueName, err)
		return nil
	}

	return &QueueMiddleware{
		MessageMiddlewareQueue: &middleware.MessageMiddlewareQueue{
			QueueName: queueName,
			Channel:   channel,
		},
	}
}

// DeclareQueue declares the queue on the RabbitMQ server.
// Parameters:
//   - durable: If true, the queue will survive server restarts
//   - autoDelete: If true, the queue will be deleted when no longer used
//   - exclusive: If true, the queue can only be used by one connection
//   - noWait: If true, don't wait for a server response
func (m *QueueMiddleware) DeclareQueue(
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool,
) middleware.MessageMiddlewareError {
	if m.MessageMiddlewareQueue.Channel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}
	
	_, err := (*m.MessageMiddlewareQueue.Channel).QueueDeclare(
		m.MessageMiddlewareQueue.QueueName,
		durable,
		autoDelete,
		exclusive,
		noWait,
		nil, // arguments
	)
	if err != nil {
		middleware.LogError("Queue Producer", "Failed to declare queue '%s': %v", m.MessageMiddlewareQueue.QueueName, err)
		return middleware.MessageMiddlewareMessageError
	}
	
	middleware.LogDebug("Queue Producer", "Queue '%s' declared (durable: %t)", m.MessageMiddlewareQueue.QueueName, durable)
	return 0
}


// Send implements the producer logic for MessageMiddlewareQueue.
func (m *QueueMiddleware) Send(
	message []byte,
) middleware.MessageMiddlewareError {
	if m.MessageMiddlewareQueue.Channel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	err := (*m.MessageMiddlewareQueue.Channel).Publish(
		"",          // exchange (empty for default queue)
		m.MessageMiddlewareQueue.QueueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		},
	)

	if err != nil {
		middleware.LogError("Queue Producer", "Send error for queue '%s': %v", m.MessageMiddlewareQueue.QueueName, err)
		return middleware.MessageMiddlewareMessageError
	}
	middleware.LogDebug("Queue Producer", "Message sent to queue '%s'", m.MessageMiddlewareQueue.QueueName)

	return 0
}

// Delete forces the remote deletion of the queue.
func (m *QueueMiddleware) Delete() middleware.MessageMiddlewareError {
	if m.MessageMiddlewareQueue.Channel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Delete the queue
	_, err := (*m.MessageMiddlewareQueue.Channel).QueueDelete(
		m.MessageMiddlewareQueue.QueueName,
		false, // ifUnused - set to false to force deletion even if in use
		false, // ifEmpty - set to false to force deletion even if not empty
		false, // noWait
	)

	if err != nil {
		middleware.LogError("Queue Producer", "Delete error for queue '%s': %v", m.MessageMiddlewareQueue.QueueName, err)
		return middleware.MessageMiddlewareDeleteError
	}
	
	middleware.LogDebug("Queue Producer", "Queue '%s' deleted", m.MessageMiddlewareQueue.QueueName)

	return 0
}

// Close disconnects the channel.
func (m *QueueMiddleware) Close() middleware.MessageMiddlewareError {
	if m.MessageMiddlewareQueue.Channel == nil {
		return 0 // Already closed
	}

	// Close the AMQP channel
	err := (*m.MessageMiddlewareQueue.Channel).Close()
	if err != nil {
		middleware.LogError("Queue Producer", "Close error for queue '%s': %v", m.MessageMiddlewareQueue.QueueName, err)
		return middleware.MessageMiddlewareCloseError
	}

	m.MessageMiddlewareQueue.Channel = nil 
	middleware.LogDebug("Queue Producer", "Channel closed for queue '%s'", m.MessageMiddlewareQueue.QueueName)

	return 0
}