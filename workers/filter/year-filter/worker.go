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

// YearFilterWorker wraps the base filter worker with year filter specific configuration
type YearFilterWorker struct {
	*filterbase.BaseFilterWorker
}

// NewYearFilterWorker creates a new YearFilterWorker instance
func NewYearFilterWorker(config *middleware.ConnectionConfig) (*YearFilterWorker, error) {
	// Use builder to create queue producers
	builder := worker_builder.NewWorkerBuilder("Year Filter Worker").
		WithConfig(config).
		WithQueueProducer(queues.TimeFilterQueue, true). // auto-declare
		WithQueueProducer(queues.ReplyFilterBusQueue, true)

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract producers from builder
	timeFilterProducer := builder.GetQueueProducer(queues.TimeFilterQueue)
	replyProducer := builder.GetQueueProducer(queues.ReplyFilterBusQueue)

	if timeFilterProducer == nil || replyProducer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get producers from builder"))
	}

	// Configure routing rules
	routingRules := []filterbase.RoutingRule{
		{
			QueryTypes: []byte{chunk.QueryType1, chunk.QueryType3},
			Producer:   timeFilterProducer,
		},
		{
			QueryTypes: []byte{chunk.QueryType2, chunk.QueryType4},
			Producer:   replyProducer,
		},
	}

	// Create base filter worker configuration
	filterConfig := &filterbase.Config{
		WorkerName:       "Year Filter Worker",
		InputQueue:       queues.YearFilterQueue,
		OutputProducers:  map[string]*workerqueue.QueueMiddleware{"timeFilter": timeFilterProducer, "reply": replyProducer},
		RoutingRules:     routingRules,
		FilterLogic:      YearFilterLogic,
		StateFilePath:    "/app/worker-data/processed-ids.txt",
		ConnectionConfig: config,
	}

	// Create base worker (still creates consumer and MessageManager internally)
	baseWorker, err := filterbase.NewBaseFilterWorker(filterConfig)
	if err != nil {
		// Builder will handle cleanup of producers on error
		return nil, builder.CleanupOnError(fmt.Errorf("failed to create base filter worker: %w", err))
	}

	// Note: Producers are now managed by the worker's lifecycle, not the builder
	// The builder's cleanup is only called on error during initialization
	return &YearFilterWorker{
		BaseFilterWorker: baseWorker,
	}, nil
}
