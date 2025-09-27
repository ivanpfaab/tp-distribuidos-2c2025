package exchange

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

// StartConsuming implements the consumer startup logic for MessageMiddlewareExchange.
func (e *MessageMiddlewareExchange) StartConsuming(
	m *MessageMiddlewareExchange,
	onMessageCallback onMessageCallback,
) MessageMiddlewareError {
	if m.amqpChannel == nil {
		return MessageMiddlewareDisconnectedError
	}
	
	// Create a temporary queue for this consumer
	queue, err := (*m.amqpChannel).QueueDeclare(
		"",    // name (empty for auto-generated)
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		fmt.Printf("Exchange '%s' Consumer: Failed to declare queue: %v\n", m.exchangeName, err)
		return MessageMiddlewareMessageError
	}
	
	// Bind the queue to the exchange with all routing keys
	for _, routingKey := range m.routeKeys {
		err = (*m.amqpChannel).QueueBind(
			queue.Name,
			routingKey,
			m.exchangeName,
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			fmt.Printf("Exchange '%s' Consumer: Failed to bind queue with key '%s': %v\n", m.exchangeName, routingKey, err)
			return MessageMiddlewareMessageError
		}
	}
	
	// Start consuming from the queue
	deliveries, err := (*m.amqpChannel).Consume(
		queue.Name,
		"",    // consumer tag (empty for auto-generated)
		false, // auto-ack --> the callback will handle the acknowledgments manually
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		fmt.Printf("Exchange '%s' Consumer: Failed to start consuming: %v\n", m.exchangeName, err)
		return MessageMiddlewareMessageError
	}
	
	// Set the consume channel
	m.consumeChannel = (*ConsumeChannel)(&deliveries)
	
	// Start the processing loop in a goroutine
	go func() {
		done := make(chan error, 1)
		fmt.Printf("Exchange '%s' Consumer: Starting consumer on queue '%s'.\n", m.exchangeName, queue.Name)
		
		// Call the onMessageCallback with the consume channel
		onMessageCallback(m.consumeChannel, done)
	}()

	return 0
}


// StopConsuming implements the consumer shutdown logic.
func (e *MessageMiddlewareExchange) StopConsuming(
	m *MessageMiddlewareExchange,
) MessageMiddlewareError {
	if m.amqpChannel == nil {
		return MessageMiddlewareDisconnectedError
	}

	if m.consumeChannel == nil {
		fmt.Printf("Exchange '%s' Consumer: Not consuming, StopConsuming has no effect.\n", m.exchangeName)
		return 0
	}

	// Cancel the consumer
	err := (*m.amqpChannel).Cancel("", false) // Empty string cancels all consumers on this channel
	if err != nil {
		fmt.Printf("Exchange '%s' Consumer: Failed to cancel consumer: %v\n", m.exchangeName, err)
		return MessageMiddlewareMessageError
	}

	// Close the consume channel
	if m.consumeChannel != nil {
		close(*m.consumeChannel)
	}
	m.consumeChannel = nil
	
	fmt.Printf("Exchange '%s' Consumer: Halted.\n", m.exchangeName)

	return 0
}

// Close disconnects the channel.
func (e *MessageMiddlewareExchange) Close(
	m *MessageMiddlewareExchange,
) MessageMiddlewareError {
	if m.amqpChannel == nil {
		return 0 // Already closed
	}

	// Stop consuming first if we're consuming
	if m.consumeChannel != nil {
		stopErr := e.StopConsuming(m)
		if stopErr != 0 {
			fmt.Printf("Exchange '%s': Error stopping consumption during close: %v\n", m.exchangeName, stopErr)
		}
	}

	// Close the AMQP channel
	err := (*m.amqpChannel).Close()
	if err != nil {
		fmt.Printf("Exchange '%s': Close error: %v\n", m.exchangeName, err)
		return MessageMiddlewareCloseError
	}

	m.amqpChannel = nil 
	fmt.Printf("Exchange '%s': Channel closed.\n", m.exchangeName)

	return 0
}
