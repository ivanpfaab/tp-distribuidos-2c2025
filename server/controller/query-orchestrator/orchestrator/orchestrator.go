package orchestrator

import (
	"github.com/tp-distribuidos-2c2025/server/controller/query-orchestrator/config"
	"github.com/tp-distribuidos-2c2025/server/controller/query-orchestrator/consumers"
	"github.com/tp-distribuidos-2c2025/server/controller/query-orchestrator/handlers"
	"github.com/tp-distribuidos-2c2025/server/controller/query-orchestrator/producers"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// QueryOrchestrator manages the query orchestration process
type QueryOrchestrator struct {
	producers      *producers.Producers
	consumers      *consumers.Consumers
	messageHandler *handlers.MessageHandler
	config         *config.Config
}

// NewQueryOrchestrator creates a new Query Orchestrator instance
func NewQueryOrchestrator(cfg *config.Config) *QueryOrchestrator {
	producers := producers.NewProducers()
	consumers := consumers.NewConsumers()
	messageHandler := handlers.NewMessageHandler(producers)

	return &QueryOrchestrator{
		producers:      producers,
		consumers:      consumers,
		messageHandler: messageHandler,
		config:         cfg,
	}
}

// Initialize sets up all the exchange producers and consumers
func (qo *QueryOrchestrator) Initialize() middleware.MessageMiddlewareError {
	middlewareConfig := qo.config.ToMiddlewareConfig()

	// Initialize producers
	if err := qo.producers.Initialize(middlewareConfig); err != 0 {
		return err
	}

	// Initialize consumers
	if err := qo.consumers.Initialize(middlewareConfig); err != 0 {
		return err
	}

	// Declare all exchanges
	if err := qo.producers.DeclareExchanges(); err != 0 {
		return err
	}

	// Declare all queues
	if err := qo.consumers.DeclareQueues(middlewareConfig); err != 0 {
		return err
	}

	return 0
}

// StartConsuming starts consuming messages from both the data handler queue and reply queue in parallel
func (qo *QueryOrchestrator) StartConsuming() middleware.MessageMiddlewareError {
	// Start data handler consumer
	if err := qo.consumers.DataHandlerConsumer.StartConsuming(qo.messageHandler.QueryOrchestratorCallback); err != 0 {
		return err
	}

	// Start reply consumer
	if err := qo.consumers.ReplyConsumer.StartConsuming(qo.messageHandler.QueryOrchestratorCallback); err != 0 {
		return err
	}

	return 0
}

// Close closes all connections
func (qo *QueryOrchestrator) Close() middleware.MessageMiddlewareError {
	var lastErr middleware.MessageMiddlewareError

	// Close producers
	if err := qo.producers.Close(); err != 0 {
		lastErr = err
	}

	return lastErr
}
