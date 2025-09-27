package exchange

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"fmt"
)

// DeclareExchange declares the exchange on the RabbitMQ server.
// Parameters:
//   - exchangeType: Type of exchange ("direct", "topic", "fanout", "headers")
//   - durable: If true, the exchange will survive server restarts
//   - autoDelete: If true, the exchange will be deleted when no longer used
//   - internal: If true, the exchange cannot be used directly by publishers
//   - noWait: If true, don't wait for a server response
func (e *MessageMiddlewareExchange) DeclareExchange(
	m *MessageMiddlewareExchange,
	exchangeType string,
	durable bool,
	autoDelete bool,
	internal bool,
	noWait bool,
) MessageMiddlewareError {
	if m.amqpChannel == nil {
		return MessageMiddlewareDisconnectedError
	}
	
	err := (*m.amqpChannel).ExchangeDeclare(
		m.exchangeName,
		exchangeType,
		durable,
		autoDelete,
		internal,
		noWait,
	)
	if err != nil {
		fmt.Printf("Exchange '%s' Producer: Failed to declare exchange: %v\n", m.exchangeName, err)
		return MessageMiddlewareMessageError
	}
	
	fmt.Printf("Exchange '%s' Producer: Successfully declared (type: %s, durable: %t).\n", m.exchangeName, exchangeType, durable)
	return 0
}


// Send implements the producer logic for MessageMiddlewareExchange.
func (e *MessageMiddlewareExchange) Send(
	m *MessageMiddlewareExchange,
	message []byte,
) MessageMiddlewareError {
	if m.amqpChannel == nil {
		return MessageMiddlewareDisconnectedError
	}

	// Send to each routing key
	for _, routeKey := range m.routeKeys {
		err := (*m.amqpChannel).Publish(
			m.exchangeName, // The target exchange
			routeKey,       // The routing key
			false,          // mandatory
			false,          // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        message,
			},
		)
		if err != nil {
			fmt.Printf("Exchange '%s' Producer: Send error for key '%s': %v\n", m.exchangeName, routeKey, err)
			return MessageMiddlewareMessageError
		}
		fmt.Printf("Exchange '%s' Producer: Message sent with key '%s'.\n", m.exchangeName, routeKey)
	}

	return 0
}

// Delete forces the remote deletion of the exchange.
func (e *MessageMiddlewareExchange) Delete(
	m *MessageMiddlewareExchange,
) MessageMiddlewareError {
	if m.amqpChannel == nil {
		return MessageMiddlewareDisconnectedError
	}

	// Delete the exchange
	err := (*m.amqpChannel).ExchangeDelete(
		m.exchangeName,
		false, // ifUnused - set to false to force deletion even if in use
		false, // noWait
	)

	if err != nil {
		fmt.Printf("Exchange '%s': Delete error: %v\n", m.exchangeName, err)
		return MessageMiddlewareDeleteError
	}
	
	fmt.Printf("Exchange '%s': Remote exchange deleted.\n", m.exchangeName)

	return 0
}