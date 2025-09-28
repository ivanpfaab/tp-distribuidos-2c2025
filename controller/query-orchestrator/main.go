package main

import (
	"fmt"
	"os"
	"strconv"
	"tp-distribuidos-2c2025/protocol/chunk"
	"tp-distribuidos-2c2025/shared/middleware"
	"tp-distribuidos-2c2025/shared/middleware/exchange"
)

func main() {
	// Get configuration from environment variables or use defaults
	host := getEnv("RABBITMQ_HOST", "localhost")
	port := getEnv("RABBITMQ_PORT", "5672")
	portInt, err := strconv.Atoi(port)
	if err != nil {
		fmt.Printf("Invalid port: %v\n", err)
		return
	}
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	fmt.Printf("Connecting to RabbitMQ at %s:%s with user %s\n", host, port, username)

	// Create connection configuration
	config := &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
	}

	// Create Query Orchestrator
	orchestrator := NewQueryOrchestrator(config)

	// Initialize the orchestrator (creates exchanges and producers)
	if err := orchestrator.Initialize(); err != 0 {
		fmt.Printf("Failed to initialize orchestrator: %v\n", err)
		return
	}
	defer orchestrator.Close()

	fmt.Println("Query Orchestrator initialized successfully!")
	fmt.Println("Exchanges created: filter-exchange, aggregator-exchange, join-exchange, groupby-exchange")

	fmt.Println("Query Orchestrator is ready to process chunks!")
	fmt.Println("Start the worker nodes to begin processing:")
	fmt.Println("  - Filter worker: go run workers/filter/main.go")
	fmt.Println("  - Aggregator worker: go run workers/aggregator/main.go")
	fmt.Println("  - Join worker: go run workers/join/main.go")
	fmt.Println("  - GroupBy worker: go run workers/group_by/main.go")

	// Keep the orchestrator running
	select {}
}

// QueryOrchestrator struct and methods
type QueryOrchestrator struct {
	// Exchange producers for each target node
	filterProducer     *exchange.ExchangeMiddleware
	aggregatorProducer *exchange.ExchangeMiddleware
	joinProducer       *exchange.ExchangeMiddleware
	groupByProducer    *exchange.ExchangeMiddleware

	// Target router for determining where to send chunks
	targetRouter *TargetRouter

	// Configuration
	config *middleware.ConnectionConfig
}

// NewQueryOrchestrator creates a new Query Orchestrator instance
func NewQueryOrchestrator(config *middleware.ConnectionConfig) *QueryOrchestrator {
	return &QueryOrchestrator{
		config:       config,
		targetRouter: NewTargetRouter(),
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
	if err := qo.filterProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	// Declare Aggregator exchange
	if err := qo.aggregatorProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	// Declare Join exchange
	if err := qo.joinProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	// Declare Group By exchange
	if err := qo.groupByProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	return 0
}

// ProcessChunk routes a chunk message to the appropriate node based on QueryType and Step
func (qo *QueryOrchestrator) ProcessChunk(chunkMsg *chunk.ChunkMessage) middleware.MessageMiddlewareError {
	// Determine target based on QueryType and Step
	target := qo.targetRouter.DetermineTarget(chunkMsg.QueryType, chunkMsg.Step)

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

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
