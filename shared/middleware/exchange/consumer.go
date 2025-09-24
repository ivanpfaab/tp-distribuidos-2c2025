package exchange

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ExchangeConsumer implements MessageMiddleware for exchange-based communication
type ExchangeConsumer struct {
	exchangeName   string
	exchangeType   string
	routeKeys      []string
	amqpChannel    *amqp.Channel
	connection     *amqp.Connection
	consumeChannel <-chan amqp.Delivery
	queueName      string
}

// NewExchangeConsumer creates a new exchange consumer
func NewExchangeConsumer(rabbitMQURL, exchangeName, exchangeType string, routeKeys []string) (*ExchangeConsumer, error) {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare the exchange
	err = ch.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Create a temporary queue
	q, err := ch.QueueDeclare(
		"",    // name (empty means server will generate a unique name)
		true,  // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	// Bind the queue to the exchange
	routingKey := ""
	if exchangeType == "direct" || exchangeType == "topic" {
		// For direct and topic exchanges, use the first route key if available
		if len(routeKeys) > 0 {
			routingKey = routeKeys[0]
		}
	}

	err = ch.QueueBind(
		q.Name,       // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to bind queue: %w", err)
	}

	return &ExchangeConsumer{
		exchangeName: exchangeName,
		exchangeType: exchangeType,
		routeKeys:    routeKeys,
		amqpChannel:  ch,
		connection:   conn,
		queueName:    q.Name,
	}, nil
}

// StartConsuming begins consuming messages from the exchange
func (ec *ExchangeConsumer) StartConsuming(m *ExchangeConsumer, onMessageCallback func(<-chan amqp.Delivery, chan error)) MessageMiddlewareError {
	if ec.amqpChannel == nil || ec.connection == nil {
		return MessageMiddlewareDisconnectedError
	}

	// Set QoS to process one message at a time
	err := ec.amqpChannel.Qos(1, 0, false)
	if err != nil {
		return MessageMiddlewareMessageError
	}

	// Start consuming
	msgs, err := ec.amqpChannel.Consume(
		ec.queueName, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return MessageMiddlewareMessageError
	}

	ec.consumeChannel = msgs

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

// StopConsuming stops consuming messages from the exchange
func (ec *ExchangeConsumer) StopConsuming(m *ExchangeConsumer) MessageMiddlewareError {
	if ec.amqpChannel == nil {
		return MessageMiddlewareDisconnectedError
	}

	err := ec.amqpChannel.Cancel("", false)
	if err != nil {
		return MessageMiddlewareMessageError
	}

	return 0 // No error
}

// Send is not applicable for consumers
func (ec *ExchangeConsumer) Send(m *ExchangeConsumer, message []byte) MessageMiddlewareError {
	return MessageMiddlewareMessageError
}

// Close closes the connection and channel
func (ec *ExchangeConsumer) Close(m *ExchangeConsumer) MessageMiddlewareError {
	var err error
	if ec.amqpChannel != nil {
		err = ec.amqpChannel.Close()
		if err != nil {
			return MessageMiddlewareCloseError
		}
	}
	if ec.connection != nil {
		err = ec.connection.Close()
		if err != nil {
			return MessageMiddlewareCloseError
		}
	}
	return 0 // No error
}

// Delete deletes the exchange
func (ec *ExchangeConsumer) Delete(m *ExchangeConsumer) MessageMiddlewareError {
	if ec.amqpChannel == nil {
		return MessageMiddlewareDeleteError
	}

	err := ec.amqpChannel.ExchangeDelete(
		ec.exchangeName, // name
		false,           // if-unused
		false,           // no-wait
	)
	if err != nil {
		return MessageMiddlewareDeleteError
	}

	return 0 // No error
}
