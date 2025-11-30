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

// TimeFilterWorker wraps the base filter worker with time filter specific configuration
type TimeFilterWorker struct {
	*filterbase.BaseFilterWorker
}

// NewTimeFilterWorker creates a new TimeFilterWorker instance
func NewTimeFilterWorker(config *middleware.ConnectionConfig) (*TimeFilterWorker, error) {
	// Use builder to create queue producers
	builder := worker_builder.NewWorkerBuilder("Time Filter Worker").
		WithConfig(config).
		WithQueueProducer(queues.AmountFilterQueue, true).  // auto-declare
		WithQueueProducer(queues.ReplyFilterBusQueue, true)

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract producers from builder
	amountFilterProducer := builder.GetQueueProducer(queues.AmountFilterQueue)
	replyProducer := builder.GetQueueProducer(queues.ReplyFilterBusQueue)

	if amountFilterProducer == nil || replyProducer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get producers from builder"))
	}

	// Configure routing rules
	routingRules := []filterbase.RoutingRule{
		{
			QueryTypes: []byte{chunk.QueryType1},
			Producer:   amountFilterProducer,
		},
		{
			QueryTypes: []byte{chunk.QueryType3},
			Producer:   replyProducer,
		},
	}

	// Create base filter worker configuration
	filterConfig := &filterbase.Config{
		WorkerName:       "Time Filter Worker",
		InputQueue:       queues.TimeFilterQueue,
		OutputProducers:  map[string]*workerqueue.QueueMiddleware{"amountFilter": amountFilterProducer, "reply": replyProducer},
		RoutingRules:     routingRules,
		FilterLogic:      TimeFilterLogic,
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
	return &TimeFilterWorker{
		BaseFilterWorker: baseWorker,
	}, nil
}
