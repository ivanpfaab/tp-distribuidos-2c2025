package exchange

import (
	"fmt"
	"tp-distribuidos-2c2025/shared/middleware"
)

// ExchangeConsumer wraps the middleware.MessageMiddlewareExchange with consumer methods
type ExchangeConsumer struct {
	*middleware.MessageMiddlewareExchange
}

// NewExchangeConsumer creates a new ExchangeConsumer instance
func NewExchangeConsumer(
	exchangeName string,
	routeKeys []string,
	config *middleware.ConnectionConfig,
) *ExchangeConsumer {
	// Create channel
	channel, err := middleware.CreateMiddlewareChannel(config)
	if err != nil {
		fmt.Printf("Exchange '%s' Consumer: Failed to create channel: %v\n", exchangeName, err)
		return nil
	}

	return &ExchangeConsumer{
		MessageMiddlewareExchange: &middleware.MessageMiddlewareExchange{
			ExchangeName: exchangeName,
			RouteKeys:    routeKeys,
			AmqpChannel:  channel,
		},
	}
}

// StartConsuming implements the consumer startup logic for MessageMiddlewareExchange.
func (m *ExchangeConsumer) StartConsuming(
	onMessageCallback middleware.OnMessageCallback,
) middleware.MessageMiddlewareError {
	if m.AmqpChannel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}
	
	// Create a temporary queue for this consumer
	queue, err := (*m.AmqpChannel).QueueDeclare(
		"",    // name (empty for auto-generated)
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		fmt.Printf("Exchange '%s' Consumer: Failed to declare queue: %v\n", m.ExchangeName, err)
		return middleware.MessageMiddlewareMessageError
	}
	
	// Bind the queue to the exchange with all routing keys
	for _, routingKey := range m.RouteKeys {
		err = (*m.AmqpChannel).QueueBind(
			queue.Name,
			routingKey,
			m.ExchangeName,
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			fmt.Printf("Exchange '%s' Consumer: Failed to bind queue with key '%s': %v\n", m.ExchangeName, routingKey, err)
			return middleware.MessageMiddlewareMessageError
		}
	}
	
	// Start consuming from the queue
	deliveries, err := (*m.AmqpChannel).Consume(
		queue.Name,
		"",    // consumer tag (empty for auto-generated)
		false, // auto-ack --> the callback will handle the acknowledgments manually
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		fmt.Printf("Exchange '%s' Consumer: Failed to start consuming: %v\n", m.ExchangeName, err)
		return middleware.MessageMiddlewareMessageError
	}
	
	// Set the consume channel
	m.ConsumeChannel = &deliveries
	
	// Start the processing loop in a goroutine
	go func() {
		done := make(chan error, 1)
		fmt.Printf("Exchange '%s' Consumer: Starting consumer on queue '%s'.\n", m.ExchangeName, queue.Name)
		
		// Call the onMessageCallback with the consume channel
		onMessageCallback(m.ConsumeChannel, done)
	}()

	return 0
}


// StopConsuming implements the consumer shutdown logic.
func (m *ExchangeConsumer) StopConsuming() middleware.MessageMiddlewareError {
	if m.AmqpChannel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}
	
	if m.ConsumeChannel == nil {
		fmt.Printf("Exchange '%s' Consumer: Not consuming, StopConsuming has no effect.\n", m.ExchangeName)
		return 0
	}

	// Cancel the consumer
	err := (*m.AmqpChannel).Cancel("", false) // Empty string cancels all consumers on this channel
	if err != nil {
		fmt.Printf("Exchange '%s' Consumer: Failed to cancel consumer: %v\n", m.ExchangeName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Clear the consume channel reference
	m.ConsumeChannel = nil
	
	fmt.Printf("Exchange '%s' Consumer: Halted.\n", m.ExchangeName)

	return 0
}

// Close disconnects the channel.
func (m *ExchangeConsumer) Close() middleware.MessageMiddlewareError {
	if m.AmqpChannel == nil {
		return 0 // Already closed
	}

	// Stop consuming first if we're consuming
	if m.ConsumeChannel != nil {
		stopErr := m.StopConsuming()
		if stopErr != 0 {
			fmt.Printf("Exchange '%s': Error stopping consumption during close: %v\n", m.ExchangeName, stopErr)
		}
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
