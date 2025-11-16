package main

import (
	"fmt"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// Query type constants
const (
	QueryType1 = 1
	QueryType2 = 2
	QueryType3 = 3
	QueryType4 = 4
)

// ResultsDispatcherWorker encapsulates the results dispatcher worker state and dependencies
type ResultsDispatcherWorker struct {
	queryConsumers        []*workerqueue.QueueConsumer // Indexed by queryType-1 (0=Query1, 1=Query2, etc.)
	clientResultsProducer *workerqueue.QueueMiddleware
	config                *middleware.ConnectionConfig

	// Query completion tracking
	query1Tracker         *shared.CompletionTracker
	clientQueryCompletion map[string]*ClientQueryStatus
	completionMutex       sync.RWMutex
}

// ClientQueryStatus tracks completion status for all queries per client
type ClientQueryStatus struct {
	ClientID            string
	Query1Completed     bool
	Query2Completed     bool
	Query3Completed     bool
	Query4Completed     bool
	AllQueriesCompleted bool
}

// createQueryConsumer creates and returns a queue consumer for a specific query type
// It handles queue declaration and consumer creation, with proper error handling and cleanup
func createQueryConsumer(queueName string, config *middleware.ConnectionConfig, queryType int, existingConsumers []*workerqueue.QueueConsumer) (*workerqueue.QueueConsumer, error) {
	// Declare queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(queueName, config)
	if queueDeclarer == nil {
		return nil, fmt.Errorf("failed to create Query%d queue declarer", queryType)
	}

	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		queueDeclarer.Close()
		// Close all existing consumers on error
		for _, consumer := range existingConsumers {
			if consumer != nil {
				consumer.Close()
			}
		}
		return nil, fmt.Errorf("failed to declare Query%d results queue: %v", queryType, err)
	}
	queueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create consumer
	consumer := workerqueue.NewQueueConsumer(queueName, config)
	if consumer == nil {
		// Close all existing consumers on error
		for _, consumer := range existingConsumers {
			if consumer != nil {
				consumer.Close()
			}
		}
		return nil, fmt.Errorf("failed to create Query%d results consumer", queryType)
	}

	return consumer, nil
}

// NewResultsDispatcherWorker creates a new ResultsDispatcherWorker instance
func NewResultsDispatcherWorker(config *middleware.ConnectionConfig) (*ResultsDispatcherWorker, error) {
	// Queue names for each query type
	queueNames := []string{
		Query1ResultsQueue,
		Query2ResultsQueue,
		Query3ResultsQueue,
		Query4ResultsQueue,
	}

	// Create consumers for all query types
	queryConsumers := make([]*workerqueue.QueueConsumer, len(queueNames))
	for i, queueName := range queueNames {
		consumer, err := createQueryConsumer(queueName, config, i+1, queryConsumers[:i])
		if err != nil {
			return nil, err
		}
		queryConsumers[i] = consumer
	}

	// Create client results producer
	clientResultsProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.ClientResultsQueue,
		config,
	)

	if clientResultsProducer == nil {
		// Close all consumers on error
		for _, consumer := range queryConsumers {
			if consumer != nil {
				consumer.Close()
			}
		}
		return nil, fmt.Errorf("failed to create client results producer")
	}

	if err := clientResultsProducer.DeclareQueue(false, false, false, false); err != 0 {
		// Close all consumers on error
		for _, consumer := range queryConsumers {
			if consumer != nil {
				consumer.Close()
			}
		}
		clientResultsProducer.Close()
		return nil, fmt.Errorf("failed to declare client results queue: %v", err)
	}

	// Create results dispatcher worker instance
	rd := &ResultsDispatcherWorker{
		queryConsumers:        queryConsumers,
		clientResultsProducer: clientResultsProducer,
		config:                config,
		clientQueryCompletion: make(map[string]*ClientQueryStatus),
	}

	// Initialize completion tracker (only for Query1)
	rd.query1Tracker = shared.NewCompletionTracker("Query1", rd.onQuery1Completed)

	return rd, nil
}

// Start starts the results dispatcher worker
func (rd *ResultsDispatcherWorker) Start() middleware.MessageMiddlewareError {
	testing_utils.LogInfo("Results Dispatcher", "Starting to listen for messages...")

	// Start consuming from all query result queues
	for i, consumer := range rd.queryConsumers {
		queryType := i + 1 // Query types are 1-indexed
		if err := consumer.StartConsuming(rd.createQueryCallback(queryType)); err != 0 {
			testing_utils.LogError("Results Dispatcher", "Failed to start Query%d results consumer: %v", queryType, err)
		}
	}

	return 0
}

// Close closes all connections
func (rd *ResultsDispatcherWorker) Close() {
	// Close all query consumers
	for _, consumer := range rd.queryConsumers {
		if consumer != nil {
			consumer.Close()
		}
	}

	// Close client results producer
	if rd.clientResultsProducer != nil {
		rd.clientResultsProducer.Close()
	}
}

// onQuery1Completed is called when Query1 completes for a client
func (rd *ResultsDispatcherWorker) onQuery1Completed(clientID string, clientStatus *shared.ClientStatus) {
	rd.updateQueryCompletion(clientID, QueryType1)
}

// updateQueryCompletion updates the completion status for Query1
func (rd *ResultsDispatcherWorker) updateQueryCompletion(clientID string, queryType int) {
	rd.completionMutex.Lock()
	defer rd.completionMutex.Unlock()

	// Initialize client query status if not exists
	if rd.clientQueryCompletion[clientID] == nil {
		rd.clientQueryCompletion[clientID] = &ClientQueryStatus{
			ClientID:            clientID,
			Query1Completed:     false,
			Query2Completed:     false,
			Query3Completed:     false,
			Query4Completed:     false,
			AllQueriesCompleted: false,
		}
	}

	clientQueryStatus := rd.clientQueryCompletion[clientID]

	// Mark Query1 as completed
	if queryType == QueryType1 {
		clientQueryStatus.Query1Completed = true
		testing_utils.LogInfo("Results Dispatcher", "✅ Query1 completed for client %s", clientID)
	}

	// Check if all queries are completed
	if clientQueryStatus.Query1Completed && clientQueryStatus.Query2Completed &&
		clientQueryStatus.Query3Completed && clientQueryStatus.Query4Completed {

		if !clientQueryStatus.AllQueriesCompleted {
			clientQueryStatus.AllQueriesCompleted = true
			rd.sendSystemCompleteMessage(clientID)
		}
	}
}

// sendSystemCompleteMessage sends a system complete signal to the client
func (rd *ResultsDispatcherWorker) sendSystemCompleteMessage(clientID string) {
	testing_utils.LogInfo("Results Dispatcher", "🎉 All queries completed for client %s! Sending system complete signal.", clientID)

	// Create completion signal
	completionMessage := fmt.Sprintf("All queries completed for client %s", clientID)
	completionSignal := signals.NewClientCompletionSignal(clientID, completionMessage)

	// Serialize the signal
	serializedSignal, err := signals.SerializeClientCompletionSignal(completionSignal)
	if err != nil {
		testing_utils.LogError("Results Dispatcher", "Failed to serialize completion signal for client %s: %v", clientID, err)
		return
	}

	// Send to client results queue
	if err := rd.clientResultsProducer.Send(serializedSignal); err != 0 {
		testing_utils.LogError("Results Dispatcher", "Failed to send completion signal for client %s: %v", clientID, err)
	}
}

// createQueryCallback creates the message processing callback for a specific query type
func (rd *ResultsDispatcherWorker) createQueryCallback(queryType int) middleware.OnMessageCallback {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		testing_utils.LogInfo("Results Dispatcher", "Starting to listen for Query%d results...", queryType)
		for delivery := range *consumeChannel {
			if err := rd.processMessage(delivery, queryType); err != 0 {
				testing_utils.LogError("Results Dispatcher", "Failed to process Query%d message: %v", queryType, err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
