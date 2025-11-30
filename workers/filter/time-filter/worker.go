package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	filterbase "github.com/tp-distribuidos-2c2025/workers/filter"
)

// TimeFilterWorker wraps the base filter worker with time filter specific configuration
type TimeFilterWorker struct {
	*filterbase.BaseFilterWorker
}

// NewTimeFilterWorker creates a new TimeFilterWorker instance
func NewTimeFilterWorker(config *middleware.ConnectionConfig) (*TimeFilterWorker, error) {
	// Create amount filter producer
	amountFilterProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.AmountFilterQueue,
		config,
	)
	if amountFilterProducer == nil {
		return nil, fmt.Errorf("failed to create amount filter producer")
	}

	// Create reply producer
	replyProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.ReplyFilterBusQueue,
		config,
	)
	if replyProducer == nil {
		amountFilterProducer.Close()
		return nil, fmt.Errorf("failed to create reply producer")
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

	// Create base worker
	baseWorker, err := filterbase.NewBaseFilterWorker(filterConfig)
	if err != nil {
		amountFilterProducer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to create base filter worker: %w", err)
	}

	return &TimeFilterWorker{
		BaseFilterWorker: baseWorker,
	}, nil
}
