package exchange

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
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
		testing_utils.LogError("Exchange Consumer", "Failed to create channel for '%s': %v", exchangeName, err)
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

	queueName := ""
	testing_utils.LogDebug("Exchange Consumer", "Starting consumer for exchange '%s' with route keys '%v'", m.ExchangeName, m.RouteKeys)
	if len(m.RouteKeys) == 1 {
		queueName = fmt.Sprintf("queue-%s", m.RouteKeys[0])
	} else {
		queueName = ""
	}

	// Create a temporary queue for this consumer
	queue, err := (*m.AmqpChannel).QueueDeclare(
		queueName, // name (empty for auto-generated)
		false,     // durable
		true,      // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		testing_utils.LogError("Exchange Consumer", "Failed to declare queue for exchange '%s': %v", m.ExchangeName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// TODO: Handle multiple keys
	// Bind the queue to the exchange with all routing keys
	testing_utils.LogDebug("Exchange Consumer", "Binding queue '%s' to exchange '%s' with key '%s'", queue.Name, m.ExchangeName, m.RouteKeys[0])

	testing_utils.LogDebug("Exchange Consumer", "Consumer with '%v' keys ", len(m.RouteKeys))
	//for _, routingKey := range m.RouteKeys {
	err = (*m.AmqpChannel).QueueBind(
		queueName,
		m.RouteKeys[0],
		m.ExchangeName,
		false, // no-wait
		nil,   // arguments
	)
	//	if err != nil {
	// 		testing_utils.LogError("Exchange Consumer", "Failed to bind queue for exchange '%s' with key '%s': %v", m.ExchangeName, routingKey, err)
	// 		return middleware.MessageMiddlewareMessageError
	// 	}
	// }

	// Start consuming from the queue
	deliveries, err := (*m.AmqpChannel).Consume(
		queue.Name,
		"",    // consumer tag (empty for auto-generated)
		false, // auto-ack --> the callback will handle the acknowledgments manually
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		testing_utils.LogError("Exchange Consumer", "Failed to start consuming for exchange '%s': %v", m.ExchangeName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Set the consume channel
	m.ConsumeChannel = &deliveries

	// Start the processing loop in a goroutine
	go func() {
		done := make(chan error, 1)
		testing_utils.LogDebug("Exchange Consumer", "Starting consumer for exchange '%s' on queue '%s'", m.ExchangeName, queue.Name)

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
		testing_utils.LogDebug("Exchange Consumer", "Not consuming for exchange '%s', StopConsuming has no effect", m.ExchangeName)
		return 0
	}

	// Cancel the consumer
	err := (*m.AmqpChannel).Cancel("", false) // Empty string cancels all consumers on this channel
	if err != nil {
		testing_utils.LogError("Exchange Consumer", "Failed to cancel consumer for exchange '%s': %v", m.ExchangeName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Clear the consume channel reference
	m.ConsumeChannel = nil

	testing_utils.LogDebug("Exchange Consumer", "Consumer halted for exchange '%s'", m.ExchangeName)

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
			testing_utils.LogError("Exchange Consumer", "Error stopping consumption during close for exchange '%s': %v", m.ExchangeName, stopErr)
		}
	}

	// Close the AMQP channel
	err := (*m.AmqpChannel).Close()
	if err != nil {
		testing_utils.LogError("Exchange Consumer", "Close error for exchange '%s': %v", m.ExchangeName, err)
		return middleware.MessageMiddlewareCloseError
	}

	m.AmqpChannel = nil
	testing_utils.LogDebug("Exchange Consumer", "Channel closed for exchange '%s'", m.ExchangeName)

	return 0
}
