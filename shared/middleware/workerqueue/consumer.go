package workerqueue

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// WorkerQueueConsumer implements MessageMiddleware for work queue communication
type WorkerQueueConsumer struct {
	queueName      string
	amqpChannel    *amqp.Channel
	connection     *amqp.Connection
	consumeChannel <-chan amqp.Delivery
}

// NewWorkerQueueConsumer creates a new work queue consumer
func NewWorkerQueueConsumer(rabbitMQURL, queueName string) (*WorkerQueueConsumer, error) {
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

	return &WorkerQueueConsumer{
		queueName:   queueName,
		amqpChannel: ch,
		connection:  conn,
	}, nil
}

// StartConsuming begins consuming messages from the work queue
func (wqc *WorkerQueueConsumer) StartConsuming(m *WorkerQueueConsumer, onMessageCallback func(<-chan amqp.Delivery, chan error)) MessageMiddlewareError {
	if wqc.amqpChannel == nil || wqc.connection == nil {
		return MessageMiddlewareDisconnectedError
	}

	// Start consuming
	msgs, err := wqc.amqpChannel.Consume(
		wqc.queueName, // queue
		"",            // consumer
		false,         // auto-ack (we'll ack manually)
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		return MessageMiddlewareMessageError
	}

	wqc.consumeChannel = msgs

	// Start goroutine to handle messages
	go func() {
		done := make(chan error, 1)
		for {
			select {
			case msg, ok := <-msgs:
				if !ok {
					// Channel closed, connection lost
					done <- fmt.Errorf("connection lost")
					return
				}
				onMessageCallback(msgs, done)
				msg.Ack(false) // Acknowledge the message
			case err := <-done:
				if err != nil {
					// Handle error
					return
				}
			}
		}
	}()

	return 0 // No error
}

// StopConsuming stops consuming messages from the work queue
func (wqc *WorkerQueueConsumer) StopConsuming(m *WorkerQueueConsumer) MessageMiddlewareError {
	if wqc.amqpChannel == nil {
		return MessageMiddlewareDisconnectedError
	}

	err := wqc.amqpChannel.Cancel("", false)
	if err != nil {
		return MessageMiddlewareMessageError
	}

	return 0 // No error
}

// Send is not applicable for consumers
func (wqc *WorkerQueueConsumer) Send(m *WorkerQueueConsumer, message []byte) MessageMiddlewareError {
	return MessageMiddlewareMessageError
}

// Close closes the connection and channel
func (wqc *WorkerQueueConsumer) Close(m *WorkerQueueConsumer) MessageMiddlewareError {
	var err error
	if wqc.amqpChannel != nil {
		err = wqc.amqpChannel.Close()
		if err != nil {
			return MessageMiddlewareCloseError
		}
	}
	if wqc.connection != nil {
		err = wqc.connection.Close()
		if err != nil {
			return MessageMiddlewareCloseError
		}
	}
	return 0 // No error
}

// Delete deletes the queue
func (wqc *WorkerQueueConsumer) Delete(m *WorkerQueueConsumer) MessageMiddlewareError {
	if wqc.amqpChannel == nil {
		return MessageMiddlewareDeleteError
	}

	_, err := wqc.amqpChannel.QueueDelete(
		wqc.queueName, // name
		false,         // if-unused
		false,         // if-empty
		false,         // no-wait
	)
	if err != nil {
		return MessageMiddlewareDeleteError
	}

	return 0 // No error
}
