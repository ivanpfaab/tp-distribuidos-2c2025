package main

import (
	"fmt"
	"tp-distribuidos-2c2025/protocol/chunk"
	"tp-distribuidos-2c2025/shared/middleware"
	"tp-distribuidos-2c2025/shared/middleware/exchange"
)

type QueryOrchestrator struct {
	// Exchange producers for each target node
	filterProducer     *exchange.ExchangeMiddleware
	aggregatorProducer *exchange.ExchangeMiddleware
	joinProducer       *exchange.ExchangeMiddleware
	groupByProducer    *exchange.ExchangeMiddleware

	// Configuration
	config *middleware.ConnectionConfig
}

// NewQueryOrchestrator creates a new Query Orchestrator instance
func NewQueryOrchestrator(config *middleware.ConnectionConfig) *QueryOrchestrator {
	return &QueryOrchestrator{
		config: config,
	}
}

// Initialize sets up all the exchange producers
func (qo *QueryOrchestrator) Initialize() middleware.MessageMiddlewareError {
	// Initialize Filter producer
	qo.filterProducer = exchange.NewMessageMiddlewareExchange(
		"filter-exchange",
		[]string{"filter"},
		qo.config,
	)
	if qo.filterProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Initialize Aggregator producer
	qo.aggregatorProducer = exchange.NewMessageMiddlewareExchange(
		"aggregator-exchange",
		[]string{"aggregator"},
		qo.config,
	)
	if qo.aggregatorProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Initialize Join producer
	qo.joinProducer = exchange.NewMessageMiddlewareExchange(
		"join-exchange",
		[]string{"join"},
		qo.config,
	)
	if qo.joinProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Initialize Group By producer
	qo.groupByProducer = exchange.NewMessageMiddlewareExchange(
		"groupby-exchange",
		[]string{"groupby"},
		qo.config,
	)
	if qo.groupByProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Declare all exchanges
	if err := qo.declareExchanges(); err != 0 {
		return err
	}

	return 0
}

// declareExchanges declares all the exchanges on RabbitMQ
func (qo *QueryOrchestrator) declareExchanges() middleware.MessageMiddlewareError {
	// Declare Filter exchange
	if err := qo.filterProducer.DeclareExchange("fanout", true, false, false, false); err != 0 {
		return err
	}

	// Declare Aggregator exchange
	if err := qo.aggregatorProducer.DeclareExchange("fanout", true, false, false, false); err != 0 {
		return err
	}

	// Declare Join exchange
	if err := qo.joinProducer.DeclareExchange("fanout", true, false, false, false); err != 0 {
		return err
	}

	// Declare Group By exchange
	if err := qo.groupByProducer.DeclareExchange("fanout", true, false, false, false); err != 0 {
		return err
	}

	return 0
}

// ProcessChunk routes a chunk message to the appropriate node based on QueryType and Step
func (qo *QueryOrchestrator) ProcessChunk(chunkMsg *chunk.ChunkMessage) middleware.MessageMiddlewareError {
	// Determine target based on QueryType and Step
	target := qo.determineTarget(chunkMsg.QueryType, chunkMsg.Step)

	fmt.Println("Target: ", target)

	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		return middleware.MessageMiddlewareMessageError
	}
	fmt.Println("MessageData: ", messageData)

	// Route to appropriate producer
	switch target {
	case "filter":
		return qo.filterProducer.Send(messageData, []string{"filter"})
	case "aggregator":
		return qo.aggregatorProducer.Send(messageData, []string{"aggregator"})
	case "join":
		return qo.joinProducer.Send(messageData, []string{"join"})
	case "groupby":
		return qo.groupByProducer.Send(messageData, []string{"groupby"})
	default:
		return middleware.MessageMiddlewareMessageError
	}
}

// determineTarget returns the target node based on QueryType and Step
func (qo *QueryOrchestrator) determineTarget(queryType uint8, step int) string {
	// Routing logic based on QueryType and Step combinations

	fmt.Println("QueryType: ", queryType)
	fmt.Println("Step: ", step)

	switch queryType {
	case 1:
		switch step {
		case 1:
			return "filter"
		case 2:
			return "aggregator"
		case 3:
			return "join"
		case 4:
			return "groupby"
		}
		// Add more QueryType cases as needed
		// case 2:
		//     switch step {
		//     case 1:
		//         return "some-other-node"
		//     }
	}

	// Default case - could be an error or default routing
	return "unknown"
}

// Close closes all connections
func (qo *QueryOrchestrator) Close() middleware.MessageMiddlewareError {
	var lastErr middleware.MessageMiddlewareError

	if qo.filterProducer != nil {
		if err := qo.filterProducer.Close(); err != 0 {
			lastErr = err
		}
	}

	if qo.aggregatorProducer != nil {
		if err := qo.aggregatorProducer.Close(); err != 0 {
			lastErr = err
		}
	}

	if qo.joinProducer != nil {
		if err := qo.joinProducer.Close(); err != 0 {
			lastErr = err
		}
	}

	if qo.groupByProducer != nil {
		if err := qo.groupByProducer.Close(); err != 0 {
			lastErr = err
		}
	}

	return lastErr
}
