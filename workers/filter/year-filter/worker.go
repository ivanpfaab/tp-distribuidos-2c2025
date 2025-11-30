package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	filterbase "github.com/tp-distribuidos-2c2025/workers/filter"
)

// YearFilterWorker wraps the base filter worker with year filter specific configuration
type YearFilterWorker struct {
	*filterbase.BaseFilterWorker
}

// NewYearFilterWorker creates a new YearFilterWorker instance
func NewYearFilterWorker(config *middleware.ConnectionConfig) (*YearFilterWorker, error) {
	// Create time filter producer
	timeFilterProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.TimeFilterQueue,
		config,
	)
	if timeFilterProducer == nil {
		return nil, fmt.Errorf("failed to create time filter producer")
	}

	// Create reply producer
	replyProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.ReplyFilterBusQueue,
		config,
	)
	if replyProducer == nil {
		timeFilterProducer.Close()
		return nil, fmt.Errorf("failed to create reply producer")
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

	// Create base worker
	baseWorker, err := filterbase.NewBaseFilterWorker(filterConfig)
	if err != nil {
		timeFilterProducer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to create base filter worker: %w", err)
	}

	return &YearFilterWorker{
		BaseFilterWorker: baseWorker,
	}, nil
}
