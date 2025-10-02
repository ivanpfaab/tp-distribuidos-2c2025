package consumers

import (
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// Consumers manages all queue consumers for the query orchestrator
type Consumers struct {
	DataHandlerConsumer *workerqueue.QueueConsumer
	ReplyConsumer       *workerqueue.QueueConsumer
}

// NewConsumers creates a new Consumers instance
func NewConsumers() *Consumers {
	return &Consumers{}
}

// Initialize initializes all queue consumers
func (c *Consumers) Initialize(config *middleware.ConnectionConfig) middleware.MessageMiddlewareError {
	// Initialize Data Handler consumer
	c.DataHandlerConsumer = workerqueue.NewQueueConsumer(
		"step0-data-queue",
		config,
	)
	if c.DataHandlerConsumer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Initialize Reply consumer
	c.ReplyConsumer = workerqueue.NewQueueConsumer(
		"orchestrator-reply-queue",
		config,
	)
	if c.ReplyConsumer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	return 0
}

// DeclareQueues declares all queues on RabbitMQ
func (c *Consumers) DeclareQueues(config *middleware.ConnectionConfig) middleware.MessageMiddlewareError {
	// Declare data handler queue
	if err := c.declareDataHandlerQueue(config); err != 0 {
		return err
	}

	// Declare reply queue
	if err := c.declareReplyQueue(config); err != 0 {
		return err
	}

	return 0
}

// declareDataHandlerQueue declares the data handler queue on RabbitMQ
func (c *Consumers) declareDataHandlerQueue(config *middleware.ConnectionConfig) middleware.MessageMiddlewareError {
	// Create a temporary producer to declare the queue
	queueConsumer := workerqueue.NewMessageMiddlewareQueue(
		"step0-data-queue",
		config,
	)
	if queueConsumer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}
	defer queueConsumer.Close()

	// Declare the queue (same parameters as data handler)
	if err := queueConsumer.DeclareQueue(false, false, false, false); err != 0 {
		return err
	}

	return 0
}

// declareReplyQueue declares the reply queue on RabbitMQ
func (c *Consumers) declareReplyQueue(config *middleware.ConnectionConfig) middleware.MessageMiddlewareError {
	// Create a temporary producer to declare the queue
	queueConsumer := workerqueue.NewMessageMiddlewareQueue(
		"orchestrator-reply-queue",
		config,
	)
	if queueConsumer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}
	defer queueConsumer.Close()

	// Declare the queue (same parameters as data handler)
	if err := queueConsumer.DeclareQueue(false, false, false, false); err != 0 {
		return err
	}

	return 0
}
