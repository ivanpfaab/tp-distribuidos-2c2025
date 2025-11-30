package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
	filterbase "github.com/tp-distribuidos-2c2025/workers/filter"
)

// AmountFilterWorker wraps the base filter worker with amount filter specific configuration
type AmountFilterWorker struct {
	*filterbase.BaseFilterWorker
}

// NewAmountFilterWorker creates a new AmountFilterWorker instance
func NewAmountFilterWorker(config *middleware.ConnectionConfig) (*AmountFilterWorker, error) {
	// Use builder to create queue producer
	builder := worker_builder.NewWorkerBuilder("Amount Filter Worker").
		WithConfig(config).
		WithQueueProducer(queues.ReplyFilterBusQueue, true) // auto-declare

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract producer from builder
	replyProducer := builder.GetQueueProducer(queues.ReplyFilterBusQueue)
	if replyProducer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get producer from builder"))
	}

	// Configure routing rules - amount filter always routes to reply bus
	routingRules := []filterbase.RoutingRule{
		{
			QueryTypes: []byte{chunk.QueryType1, chunk.QueryType2, chunk.QueryType3, chunk.QueryType4},
			Producer:   replyProducer,
		},
	}

	// Create base filter worker configuration
	filterConfig := &filterbase.Config{
		WorkerName:       "Amount Filter Worker",
		InputQueue:       queues.AmountFilterQueue,
		OutputProducers:  map[string]*workerqueue.QueueMiddleware{"reply": replyProducer},
		RoutingRules:     routingRules,
		FilterLogic:      AmountFilterLogic,
		StateFilePath:    "/app/worker-data/processed-ids.txt",
		ConnectionConfig: config,
	}

	// Create base worker (still creates consumer and MessageManager internally)
	baseWorker, err := filterbase.NewBaseFilterWorker(filterConfig)
	if err != nil {
		// Builder will handle cleanup of producer on error
		return nil, builder.CleanupOnError(fmt.Errorf("failed to create base filter worker: %w", err))
	}

	// Note: Producer is now managed by the worker's lifecycle, not the builder
	// The builder's cleanup is only called on error during initialization
	return &AmountFilterWorker{
		BaseFilterWorker: baseWorker,
	}, nil
}
