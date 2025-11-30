package worker_builder

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// QueueDeclarationOptions holds options for queue declaration
type QueueDeclarationOptions struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
}

// DefaultQueueOptions returns default queue declaration options
func DefaultQueueOptions() QueueDeclarationOptions {
	return QueueDeclarationOptions{
		Durable:    false,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
	}
}

// ExchangeDeclarationOptions holds options for exchange declaration
type ExchangeDeclarationOptions struct {
	Type       string // "direct", "topic", "fanout", "headers"
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
}

// DefaultExchangeOptions returns default exchange declaration options
func DefaultExchangeOptions(exchangeType string) ExchangeDeclarationOptions {
	return ExchangeDeclarationOptions{
		Type:       exchangeType,
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
	}
}

// WithQueueConsumer adds a queue consumer with optional auto-declaration
func (wb *WorkerBuilder) WithQueueConsumer(queueName string, autoDeclare bool, opts ...QueueDeclarationOptions) *WorkerBuilder {
	if wb.config == nil {
		wb.addError(fmt.Errorf("config must be set before adding queue consumer"))
		return wb
	}

	// Create consumer
	consumer := workerqueue.NewQueueConsumer(queueName, wb.config)
	if consumer == nil {
		wb.addError(fmt.Errorf("failed to create queue consumer for '%s'", queueName))
		return wb
	}

	// Register for cleanup
	index := wb.resourceTracker.GetLastIndex()
	wb.resourceTracker.Register(
		ResourceTypeQueueConsumer,
		queueName,
		consumer,
		func() error {
			if err := consumer.Close(); err != 0 {
				return fmt.Errorf("failed to close queue consumer: %v", err)
			}
			return nil
		},
	)

	// Auto-declare if requested
	if autoDeclare {
		opts := getQueueOptions(opts...)
		queueDeclarer := workerqueue.NewMessageMiddlewareQueue(queueName, wb.config)
		if queueDeclarer == nil {
			wb.addError(fmt.Errorf("failed to create queue declarer for '%s'", queueName))
			return wb
		}

		if err := queueDeclarer.DeclareQueue(opts.Durable, opts.AutoDelete, opts.Exclusive, opts.NoWait); err != 0 {
			// Cleanup consumer on error
			wb.CleanupFromIndex(index)
			wb.addError(fmt.Errorf("failed to declare queue '%s': %v", queueName, err))
			queueDeclarer.Close()
			return wb
		}
		queueDeclarer.Close()
	}

	return wb
}

// WithQueueProducer adds a queue producer with optional auto-declaration
func (wb *WorkerBuilder) WithQueueProducer(queueName string, autoDeclare bool, opts ...QueueDeclarationOptions) *WorkerBuilder {
	if wb.config == nil {
		wb.addError(fmt.Errorf("config must be set before adding queue producer"))
		return wb
	}

	// Create producer
	producer := workerqueue.NewMessageMiddlewareQueue(queueName, wb.config)
	if producer == nil {
		wb.addError(fmt.Errorf("failed to create queue producer for '%s'", queueName))
		return wb
	}

	// Register for cleanup
	index := wb.resourceTracker.GetLastIndex()
	wb.resourceTracker.Register(
		ResourceTypeQueueProducer,
		queueName,
		producer,
		func() error {
			if err := producer.Close(); err != 0 {
				return fmt.Errorf("failed to close queue producer: %v", err)
			}
			return nil
		},
	)

	// Auto-declare if requested
	if autoDeclare {
		opts := getQueueOptions(opts...)
		if err := producer.DeclareQueue(opts.Durable, opts.AutoDelete, opts.Exclusive, opts.NoWait); err != 0 {
			// Cleanup producer on error
			wb.CleanupFromIndex(index)
			wb.addError(fmt.Errorf("failed to declare queue '%s': %v", queueName, err))
			return wb
		}
	}

	return wb
}

// WithExchangeConsumer adds an exchange consumer with optional auto-declaration
// routingKeys: If empty, uses empty string (for fanout exchanges)
func (wb *WorkerBuilder) WithExchangeConsumer(
	exchangeName string,
	routingKeys []string,
	autoDeclare bool,
	exchangeOpts ...ExchangeDeclarationOptions,
) *WorkerBuilder {
	if wb.config == nil {
		wb.addError(fmt.Errorf("config must be set before adding exchange consumer"))
		return wb
	}

	// Create consumer
	consumer := exchange.NewExchangeConsumer(exchangeName, routingKeys, wb.config)
	if consumer == nil {
		wb.addError(fmt.Errorf("failed to create exchange consumer for '%s'", exchangeName))
		return wb
	}

	// Register for cleanup
	index := wb.resourceTracker.GetLastIndex()
	wb.resourceTracker.Register(
		ResourceTypeExchangeConsumer,
		exchangeName,
		consumer,
		func() error {
			if err := consumer.Close(); err != 0 {
				return fmt.Errorf("failed to close exchange consumer: %v", err)
			}
			return nil
		},
	)

	// Auto-declare if requested
	if autoDeclare {
		opts := getExchangeOptions(exchangeOpts...)
		exchangeDeclarer := exchange.NewMessageMiddlewareExchange(exchangeName, routingKeys, wb.config)
		if exchangeDeclarer == nil {
			wb.addError(fmt.Errorf("failed to create exchange declarer for '%s'", exchangeName))
			return wb
		}

		if err := exchangeDeclarer.DeclareExchange(opts.Type, opts.Durable, opts.AutoDelete, opts.Internal, opts.NoWait); err != 0 {
			// Cleanup consumer on error
			wb.CleanupFromIndex(index)
			wb.addError(fmt.Errorf("failed to declare exchange '%s': %v", exchangeName, err))
			exchangeDeclarer.Close()
			return wb
		}
		exchangeDeclarer.Close()
	}

	return wb
}

// WithExchangeProducer adds an exchange producer with optional auto-declaration
// routingKeys: Default routing keys for this producer (can be overridden when sending)
func (wb *WorkerBuilder) WithExchangeProducer(
	exchangeName string,
	routingKeys []string,
	autoDeclare bool,
	exchangeOpts ...ExchangeDeclarationOptions,
) *WorkerBuilder {
	if wb.config == nil {
		wb.addError(fmt.Errorf("config must be set before adding exchange producer"))
		return wb
	}

	// Create producer
	producer := exchange.NewMessageMiddlewareExchange(exchangeName, routingKeys, wb.config)
	if producer == nil {
		wb.addError(fmt.Errorf("failed to create exchange producer for '%s'", exchangeName))
		return wb
	}

	// Register for cleanup
	index := wb.resourceTracker.GetLastIndex()
	wb.resourceTracker.Register(
		ResourceTypeExchangeProducer,
		exchangeName,
		producer,
		func() error {
			if err := producer.Close(); err != 0 {
				return fmt.Errorf("failed to close exchange producer: %v", err)
			}
			return nil
		},
	)

	// Auto-declare if requested
	if autoDeclare {
		opts := getExchangeOptions(exchangeOpts...)
		if err := producer.DeclareExchange(opts.Type, opts.Durable, opts.AutoDelete, opts.Internal, opts.NoWait); err != 0 {
			// Cleanup producer on error
			wb.CleanupFromIndex(index)
			wb.addError(fmt.Errorf("failed to declare exchange '%s': %v", exchangeName, err))
			return wb
		}
	}

	return wb
}

// GetQueueConsumer retrieves a queue consumer by name
func (wb *WorkerBuilder) GetQueueConsumer(queueName string) *workerqueue.QueueConsumer {
	resource := wb.resourceTracker.Get(ResourceTypeQueueConsumer, queueName)
	if resource == nil {
		return nil
	}
	consumer, ok := resource.(*workerqueue.QueueConsumer)
	if !ok {
		return nil
	}
	return consumer
}

// GetQueueProducer retrieves a queue producer by name
func (wb *WorkerBuilder) GetQueueProducer(queueName string) *workerqueue.QueueMiddleware {
	resource := wb.resourceTracker.Get(ResourceTypeQueueProducer, queueName)
	if resource == nil {
		return nil
	}
	producer, ok := resource.(*workerqueue.QueueMiddleware)
	if !ok {
		return nil
	}
	return producer
}

// GetExchangeConsumer retrieves an exchange consumer by exchange name
func (wb *WorkerBuilder) GetExchangeConsumer(exchangeName string) *exchange.ExchangeConsumer {
	resource := wb.resourceTracker.Get(ResourceTypeExchangeConsumer, exchangeName)
	if resource == nil {
		return nil
	}
	consumer, ok := resource.(*exchange.ExchangeConsumer)
	if !ok {
		return nil
	}
	return consumer
}

// GetExchangeProducer retrieves an exchange producer by exchange name
func (wb *WorkerBuilder) GetExchangeProducer(exchangeName string) *exchange.ExchangeMiddleware {
	resource := wb.resourceTracker.Get(ResourceTypeExchangeProducer, exchangeName)
	if resource == nil {
		return nil
	}
	producer, ok := resource.(*exchange.ExchangeMiddleware)
	if !ok {
		return nil
	}
	return producer
}

// Helper functions

func getQueueOptions(opts ...QueueDeclarationOptions) QueueDeclarationOptions {
	if len(opts) > 0 {
		return opts[0]
	}
	return DefaultQueueOptions()
}

func getExchangeOptions(opts ...ExchangeDeclarationOptions) ExchangeDeclarationOptions {
	if len(opts) > 0 {
		return opts[0]
	}
	// Default to "direct" if no type specified
	return DefaultExchangeOptions("direct")
}
