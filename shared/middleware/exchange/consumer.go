package exchange

import (
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

// ExchangeConsumer wraps the middleware.MessageMiddlewareExchange with consumer methods
type ExchangeConsumer struct {
	*middleware.MessageMiddlewareExchange
	queueName string // Optional named queue (empty = temporary auto-generated)
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
		queueName: "", // Default to temporary queue
	}
}

// SetQueueName sets the queue name for persistent queue (call before StartConsuming)
// If not set or empty, creates a temporary auto-generated queue
func (m *ExchangeConsumer) SetQueueName(queueName string) {
	m.queueName = queueName
}

func (m *ExchangeConsumer) StartConsuming(
	onMessageCallback middleware.OnMessageCallback,
) middleware.MessageMiddlewareError {
	if m.AmqpChannel == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	testing_utils.LogDebug("Exchange Consumer",
		"Starting consumer for exchange '%s' with route keys '%v'",
		m.ExchangeName, m.RouteKeys)

	// 1. Create a queue for this consumer
	// If queueName is empty, RabbitMQ auto-generates a temporary one
	queue, err := (*m.AmqpChannel).QueueDeclare(
		m.queueName,       	// name (empty for auto-generated)
		false, 				// durable (true if named, false if temporary)
		false, 				// delete when unused (true if temporary, false if named)
		false,             	// exclusive (only this consumer can use it)
		false,             	// no-wait
		nil,              	// arguments (empty)
	)
	if err != nil {
		testing_utils.LogError("Exchange Consumer",
			"Failed to declare queue for exchange '%s': %v",
			m.ExchangeName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// 2. Bind the queue
	// If it's a fanout exchange, routing key doesn’t matter — use ""
	routingKeys := m.RouteKeys
	if len(routingKeys) == 0 {
		routingKeys = []string{""}
	}

	for _, routingKey := range routingKeys {
		err := (*m.AmqpChannel).QueueBind(
			queue.Name,
			routingKey,
			m.ExchangeName,
			false, // no-wait
			nil,   // args
		)
		if err != nil {
			testing_utils.LogError("Exchange Consumer",
				"Failed to bind queue '%s' to exchange '%s' with key '%s': %v",
				queue.Name, m.ExchangeName, routingKey, err)
			return middleware.MessageMiddlewareMessageError
		}
	}

	// 3. Start consuming
	deliveries, err := (*m.AmqpChannel).Consume(
		queue.Name,
		"",    // consumer tag (auto-generated)
		false, // auto-ack (false means you’ll manually ack)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		testing_utils.LogError("Exchange Consumer",
			"Failed to start consuming for exchange '%s': %v",
			m.ExchangeName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// 4. Save channel and start callback
	m.ConsumeChannel = &deliveries

	go func() {
		done := make(chan error, 1)
		testing_utils.LogDebug("Exchange Consumer",
			"Consuming from exchange '%s' on queue '%s'",
			m.ExchangeName, queue.Name)
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
