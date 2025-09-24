package exchange

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Valid exchange types
const (
	ExchangeTypeFanout  = "fanout"
	ExchangeTypeDirect  = "direct"
	ExchangeTypeTopic   = "topic"
	ExchangeTypeDefault = ""
)

// ExchangeProducer implements MessageMiddleware for exchange-based communication
type ExchangeProducer struct {
	exchangeName string
	exchangeType string
	routeKeys    []string
	amqpChannel  *amqp.Channel
	connection   *amqp.Connection
}

// NewExchangeProducer creates a new exchange producer
func NewExchangeProducer(rabbitMQURL, exchangeName, exchangeType string, routeKeys []string) (*ExchangeProducer, error) {
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

	return &ExchangeProducer{
		exchangeName: exchangeName,
		exchangeType: exchangeType,
		routeKeys:    routeKeys,
		amqpChannel:  ch,
		connection:   conn,
	}, nil
}

// StartConsuming is not applicable for producers
func (ep *ExchangeProducer) StartConsuming(m *ExchangeProducer, onMessageCallback func(<-chan amqp.Delivery, chan error)) MessageMiddlewareError {
	return MessageMiddlewareMessageError
}

// StopConsuming is not applicable for producers
func (ep *ExchangeProducer) StopConsuming(m *ExchangeProducer) MessageMiddlewareError {
	return MessageMiddlewareMessageError
}

// Send publishes a message to the exchange
func (ep *ExchangeProducer) Send(m *ExchangeProducer, message []byte) MessageMiddlewareError {
	if ep.amqpChannel == nil || ep.connection == nil {
		return MessageMiddlewareDisconnectedError
	}

	// Determine routing key based on exchange type
	routingKey := ""
	if ep.exchangeType == ExchangeTypeDirect || ep.exchangeType == ExchangeTypeTopic {
		// For direct and topic exchanges, use the first route key if available
		if len(ep.routeKeys) > 0 {
			routingKey = ep.routeKeys[0]
		}
	}

	err := ep.amqpChannel.Publish(
		ep.exchangeName, // exchange
		routingKey,      // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		},
	)

	if err != nil {
		return MessageMiddlewareMessageError
	}

	return 0 // No error
}

// Close closes the connection and channel
func (ep *ExchangeProducer) Close(m *ExchangeProducer) MessageMiddlewareError {
	var err error
	if ep.amqpChannel != nil {
		err = ep.amqpChannel.Close()
		if err != nil {
			return MessageMiddlewareCloseError
		}
	}
	if ep.connection != nil {
		err = ep.connection.Close()
		if err != nil {
			return MessageMiddlewareCloseError
		}
	}
	return 0 // No error
}

// Delete deletes the exchange
func (ep *ExchangeProducer) Delete(m *ExchangeProducer) MessageMiddlewareError {
	if ep.amqpChannel == nil {
		return MessageMiddlewareDeleteError
	}

	err := ep.amqpChannel.ExchangeDelete(
		ep.exchangeName, // name
		false,           // if-unused
		false,           // no-wait
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
