package workerqueue

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"fmt"
)

// DeclareQueue declares the queue on the RabbitMQ server.
// Parameters:
//   - durable: If true, the queue will survive server restarts
//   - autoDelete: If true, the queue will be deleted when no longer used
//   - exclusive: If true, the queue can only be used by one connection
//   - noWait: If true, don't wait for a server response
func (q *MessageMiddlewareQueue) DeclareQueue(
	m *MessageMiddlewareQueue,
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool
) MessageMiddlewareError {
	if m.channel == nil {
		return MessageMiddlewareDisconnectedError
	}
	
	_, err := (*m.channel).QueueDeclare(
		m.queueName,
		durable,
		autoDelete,
		exclusive,
		noWait
	)
	if err != nil {
		fmt.Printf("Queue '%s' Producer: Failed to declare queue: %v\n", m.queueName, err)
		return MessageMiddlewareMessageError
	}
	
	fmt.Printf("Queue '%s' Producer: Successfully declared (durable: %t).\n", m.queueName, durable)
	return 0
}


// Send implements the producer logic for MessageMiddlewareQueue.
func (q *MessageMiddlewareQueue) Send(
	m *MessageMiddlewareQueue,
	message []byte,
) MessageMiddlewareError {
	if m.channel == nil {
		return MessageMiddlewareDisconnectedError
	}

	err := (*m.channel).Publish(
		"",          // exchange (empty for default queue)
		m.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		},
	)

	if err != nil {
		fmt.Printf("Queue '%s' Producer: Send error: %v\n", m.queueName, err)
		return MessageMiddlewareMessageError
	}
	fmt.Printf("Queue '%s' Producer: Message sent.\n", m.queueName)

	return 0
}

// Delete forces the remote deletion of the queue.
func (q *MessageMiddlewareQueue) Delete(
	m *MessageMiddlewareQueue,
) MessageMiddlewareError {
	if m.channel == nil {
		return MessageMiddlewareDisconnectedError
	}

	// Delete the queue
	_, err := (*m.channel).QueueDelete(
		m.queueName,
		false, // ifUnused - set to false to force deletion even if in use
		false, // ifEmpty - set to false to force deletion even if not empty
		false, // noWait
	)

	if err != nil {
		fmt.Printf("Queue '%s': Delete error: %v\n", m.queueName, err)
		return MessageMiddlewareDeleteError
	}
	
	fmt.Printf("Queue '%s': Remote queue deleted.\n", m.queueName)

	return 0
}