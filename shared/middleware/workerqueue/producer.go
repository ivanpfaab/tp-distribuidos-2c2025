package workerqueue

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// WorkerQueueProducer implements MessageMiddleware for work queue communication
type WorkerQueueProducer struct {
	queueName   string
	amqpChannel *amqp.Channel
	connection  *amqp.Connection
}

// NewWorkerQueueProducer creates a new work queue producer
func NewWorkerQueueProducer(rabbitMQURL, queueName string) (*WorkerQueueProducer, error) {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare the queue as durable
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &WorkerQueueProducer{
		queueName:   queueName,
		amqpChannel: ch,
		connection:  conn,
	}, nil
}

// StartConsuming is not applicable for producers
func (wqp *WorkerQueueProducer) StartConsuming(m *WorkerQueueProducer, onMessageCallback func(<-chan amqp.Delivery, chan error)) MessageMiddlewareError {
	return MessageMiddlewareMessageError
}

// StopConsuming is not applicable for producers
func (wqp *WorkerQueueProducer) StopConsuming(m *WorkerQueueProducer) MessageMiddlewareError {
	return MessageMiddlewareMessageError
}

// Send publishes a message to the work queue
func (wqp *WorkerQueueProducer) Send(m *WorkerQueueProducer, message []byte) MessageMiddlewareError {
	if wqp.amqpChannel == nil || wqp.connection == nil {
		return MessageMiddlewareDisconnectedError
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := wqp.amqpChannel.PublishWithContext(
		ctx,
		"",           // exchange (empty for default exchange)
		wqp.queueName, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // Make message persistent
			ContentType:  "text/plain",
			Body:         message,
		},
	)

	if err != nil {
		return MessageMiddlewareMessageError
	}

	return 0 // No error
}

// Close closes the connection and channel
func (wqp *WorkerQueueProducer) Close(m *WorkerQueueProducer) MessageMiddlewareError {
	var err error
	if wqp.amqpChannel != nil {
		err = wqp.amqpChannel.Close()
		if err != nil {
			return MessageMiddlewareCloseError
		}
	}
	if wqp.connection != nil {
		err = wqp.connection.Close()
		if err != nil {
			return MessageMiddlewareCloseError
		}
	}
	return 0 // No error
}

// Delete deletes the queue
func (wqp *WorkerQueueProducer) Delete(m *WorkerQueueProducer) MessageMiddlewareError {
	if wqp.amqpChannel == nil {
		return MessageMiddlewareDeleteError
	}

	_, err := wqp.amqpChannel.QueueDelete(
		wqp.queueName, // name
		false,         // if-unused
		false,         // if-empty
		false,         // no-wait
	)
	if err != nil {
		return MessageMiddlewareDeleteError
	}

	return 0 // No error
}

// MessageMiddlewareError represents different types of middleware errors
type MessageMiddlewareError int

const (
	MessageMiddlewareMessageError MessageMiddlewareError = iota + 1
	MessageMiddlewareDisconnectedError
	MessageMiddlewareCloseError
	MessageMiddlewareDeleteError
)
