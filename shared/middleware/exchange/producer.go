package exchange

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"fmt"
	"tp-distribuidos-2c2025/shared/middleware"
)

// ExchangeMiddleware wraps the middleware.MessageMiddlewareExchange with additional methods
type ExchangeMiddleware struct {
	*middleware.MessageMiddlewareExchange
}

// NewMessageMiddlewareExchange creates a new ExchangeMiddleware instance
func NewMessageMiddlewareExchange(exchangeName string, routeKeys []string, config *middleware.ConnectionConfig) *ExchangeMiddleware {
	// Create channel
	channel, err := middleware.CreateMiddlewareChannel(config)
	if err != nil {
		fmt.Printf("Exchange '%s' Producer: Failed to create channel: %v\n", exchangeName, err)
		return nil
	}

	return &ExchangeMiddleware{
		MessageMiddlewareExchange: &middleware.MessageMiddlewareExchange{
			ExchangeName: exchangeName,
			RouteKeys:    routeKeys,
			AmqpChannel:  channel,
		},
	}
}

// DeclareExchange declares the exchange on the RabbitMQ server.
// Parameters:
//   - exchangeType: Type of exchange ("direct", "topic", "fanout", "headers")
//   - durable: If true, the exchange will survive server restarts
//   - autoDelete: If true, the exchange will be deleted when no longer used
//   - internal: If true, the exchange cannot be used directly by publishers
//   - noWait: If true, don't wait for a server response
func (m *ExchangeMiddleware) DeclareExchange(
	exchangeType string,
	durable bool,
	autoDelete bool,
	internal bool,
	noWait bool,
) middleware.MessageMiddlewareError {
	if m.AmqpChannel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	err := (*m.AmqpChannel).ExchangeDeclare(
		m.ExchangeName,
		exchangeType,
		durable,
		autoDelete,
		internal,
		noWait,
		nil, // arguments
	)
	if err != nil {
		fmt.Printf("Exchange '%s' Producer: Failed to declare exchange: %v\n", m.ExchangeName, err)
		return middleware.MessageMiddlewareMessageError
	}
	
	fmt.Printf("Exchange '%s' Producer: Successfully declared (type: %s, durable: %t).\n", m.ExchangeName, exchangeType, durable)
	return 0
}


// Send implements the producer logic for MessageMiddlewareExchange.
func (m *ExchangeMiddleware) Send(
	message []byte,
) middleware.MessageMiddlewareError {
	if m.AmqpChannel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Send to each routing key
	for _, routeKey := range m.RouteKeys {
		err := (*m.AmqpChannel).Publish(
			m.ExchangeName, // The target exchange
			routeKey,       // The routing key
			false,          // mandatory
			false,          // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        message,
			},
		)
		if err != nil {
			fmt.Printf("Exchange '%s' Producer: Send error for key '%s': %v\n", m.ExchangeName, routeKey, err)
			return middleware.MessageMiddlewareMessageError
		}
		fmt.Printf("Exchange '%s' Producer: Message sent with key '%s'.\n", m.ExchangeName, routeKey)
	}

	return 0
}

// Delete forces the remote deletion of the exchange.
func (m *ExchangeMiddleware) Delete() middleware.MessageMiddlewareError {
	if m.AmqpChannel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Delete the exchange
	err := (*m.AmqpChannel).ExchangeDelete(
		m.ExchangeName,
		false, // ifUnused - set to false to force deletion even if in use
		false, // noWait
	)

	if err != nil {
		fmt.Printf("Exchange '%s': Delete error: %v\n", m.ExchangeName, err)
		return middleware.MessageMiddlewareDeleteError
	}
	
	fmt.Printf("Exchange '%s': Remote exchange deleted.\n", m.ExchangeName)

	return 0
}

// Close disconnects the channel.
func (m *ExchangeMiddleware) Close() middleware.MessageMiddlewareError {
	if m.AmqpChannel == nil {
		return 0 // Already closed
	}

	// Close the AMQP channel
	err := (*m.AmqpChannel).Close()
	if err != nil {
		fmt.Printf("Exchange '%s': Close error: %v\n", m.ExchangeName, err)
		return middleware.MessageMiddlewareCloseError
	}

	m.AmqpChannel = nil 
	fmt.Printf("Exchange '%s': Channel closed.\n", m.ExchangeName)

	return 0
}