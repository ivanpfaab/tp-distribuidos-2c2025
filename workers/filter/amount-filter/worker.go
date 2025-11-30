package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	filterbase "github.com/tp-distribuidos-2c2025/workers/filter"
)

// AmountFilterWorker wraps the base filter worker with amount filter specific configuration
type AmountFilterWorker struct {
	*filterbase.BaseFilterWorker
}

// NewAmountFilterWorker creates a new AmountFilterWorker instance
func NewAmountFilterWorker(config *middleware.ConnectionConfig) (*AmountFilterWorker, error) {
	// Create reply producer
	replyProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.ReplyFilterBusQueue,
		config,
	)
	if replyProducer == nil {
		return nil, fmt.Errorf("failed to create reply producer")
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

	// Create base worker
	baseWorker, err := filterbase.NewBaseFilterWorker(filterConfig)
	if err != nil {
		replyProducer.Close()
		return nil, fmt.Errorf("failed to create base filter worker: %w", err)
	}

	return &AmountFilterWorker{
		BaseFilterWorker: baseWorker,
	}, nil
}
