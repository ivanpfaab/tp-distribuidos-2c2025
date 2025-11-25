package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	statefulworker "github.com/tp-distribuidos-2c2025/shared/stateful_worker"
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

	// Query completion tracking (all queries use CompletionTracker)
	query1Tracker         *shared.CompletionTracker
	query2Tracker         *shared.CompletionTracker
	query3Tracker         *shared.CompletionTracker
	query4Tracker         *shared.CompletionTracker
	clientQueryCompletion map[string]*ClientQueryStatus
	completionMutex       sync.RWMutex

	// Fault tolerance
	messageManager        *messagemanager.MessageManager
	statefulWorkerManager *statefulworker.StatefulWorkerManager
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

	// Initialize completion trackers for all queries
	rd.query1Tracker = shared.NewCompletionTracker("Query1", rd.onQuery1Completed)
	rd.query2Tracker = shared.NewCompletionTracker("Query2", rd.onQuery2Completed)
	rd.query3Tracker = shared.NewCompletionTracker("Query3", rd.onQuery3Completed)
	rd.query4Tracker = shared.NewCompletionTracker("Query4", rd.onQuery4Completed)

	// Initialize fault tolerance components
	if err := rd.initializeFaultTolerance(); err != nil {
		// Close all consumers on error
		for _, consumer := range queryConsumers {
			if consumer != nil {
				consumer.Close()
			}
		}
		clientResultsProducer.Close()
		return nil, fmt.Errorf("failed to initialize fault tolerance: %w", err)
	}

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

	// Close fault tolerance components
	if rd.messageManager != nil {
		rd.messageManager.Close()
	}
}

// onQuery1Completed is called when Query1 completes for a client
func (rd *ResultsDispatcherWorker) onQuery1Completed(clientID string, clientStatus *shared.ClientStatus) {
	rd.updateQueryCompletion(clientID, QueryType1)
}

// onQuery2Completed is called when Query2 completes for a client
func (rd *ResultsDispatcherWorker) onQuery2Completed(clientID string, clientStatus *shared.ClientStatus) {
	rd.updateQueryCompletion(clientID, QueryType2)
}

// onQuery3Completed is called when Query3 completes for a client
func (rd *ResultsDispatcherWorker) onQuery3Completed(clientID string, clientStatus *shared.ClientStatus) {
	rd.updateQueryCompletion(clientID, QueryType3)
}

// onQuery4Completed is called when Query4 completes for a client
func (rd *ResultsDispatcherWorker) onQuery4Completed(clientID string, clientStatus *shared.ClientStatus) {
	rd.updateQueryCompletion(clientID, QueryType4)
}

// updateQueryCompletion updates the completion status for queries
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

	status := rd.clientQueryCompletion[clientID]

	// Mark the specific query as completed
	switch queryType {
	case QueryType1:
		status.Query1Completed = true
	case QueryType2:
		status.Query2Completed = true
	case QueryType3:
		status.Query3Completed = true
	case QueryType4:
		status.Query4Completed = true
	}
	testing_utils.LogInfo("Results Dispatcher", "✅ Query%d completed for client %s", queryType, clientID)

	// Check if all queries are completed
	if status.Query1Completed && status.Query2Completed && status.Query3Completed && status.Query4Completed {
		if !status.AllQueriesCompleted {
			status.AllQueriesCompleted = true
			rd.sendSystemCompleteMessage(clientID)
			rd.cleanupClientState(clientID)
		}
	}
}

// cleanupClientState cleans up state when all queries are completed
func (rd *ResultsDispatcherWorker) cleanupClientState(clientID string) {
	if err := rd.statefulWorkerManager.MarkClientReady(clientID); err != nil {
		testing_utils.LogError("Results Dispatcher", "Failed to mark client ready: %v", err)
	}
	rd.query1Tracker.ClearClientState(clientID)
	rd.query2Tracker.ClearClientState(clientID)
	rd.query3Tracker.ClearClientState(clientID)
	rd.query4Tracker.ClearClientState(clientID)
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

// initializeFaultTolerance initializes MessageManager and StatefulWorkerManager
func (rd *ResultsDispatcherWorker) initializeFaultTolerance() error {
	// Ensure worker data directory exists
	stateDir := "/app/worker-data"
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return fmt.Errorf("failed to create state directory: %w", err)
	}

	// Initialize MessageManager for duplicate detection
	processedChunksPath := filepath.Join(stateDir, "processed-chunks.txt")
	rd.messageManager = messagemanager.NewMessageManager(processedChunksPath)
	if err := rd.messageManager.LoadProcessedIDs(); err != nil {
		testing_utils.LogInfo("Results Dispatcher", "Warning - failed to load processed chunks: %v (starting with empty state)", err)
	} else {
		count := rd.messageManager.GetProcessedCount()
		testing_utils.LogInfo("Results Dispatcher", "Loaded %d processed chunks", count)
	}

	// Initialize StatefulWorkerManager
	metadataDir := filepath.Join(stateDir, "metadata")
	csvColumns := []string{"msg_id", "client_id", "query_type", "file_id", "table_id", "chunk_number", "is_last_chunk", "is_last_from_table"}
	rd.statefulWorkerManager = statefulworker.NewStatefulWorkerManager(
		metadataDir,
		buildDispatcherStatus(rd), // Pass worker reference so UpdateState can access CompletionTrackers
		extractDispatcherMetadataRow,
		csvColumns,
	)

	// Rebuild state from persisted metadata
	// This will call UpdateState for each CSV row, which replays to CompletionTrackers
	testing_utils.LogInfo("Results Dispatcher", "Rebuilding state from metadata...")
	if err := rd.statefulWorkerManager.RebuildState(); err != nil {
		testing_utils.LogInfo("Results Dispatcher", "Warning - failed to rebuild state: %v", err)
	} else {
		testing_utils.LogInfo("Results Dispatcher", "State rebuilt successfully")
	}

	// Sync clientQueryCompletion with CompletionTracker state after rebuild
	rd.syncClientQueryCompletionFromTrackers()

	return nil
}

// syncClientQueryCompletionFromTrackers syncs clientQueryCompletion map with CompletionTracker state
func (rd *ResultsDispatcherWorker) syncClientQueryCompletionFromTrackers() {
	rd.completionMutex.Lock()
	defer rd.completionMutex.Unlock()

	// Get all client IDs from all trackers
	allClientIDs := make(map[string]bool)
	for _, clientID := range rd.query1Tracker.GetAllClientIDs() {
		allClientIDs[clientID] = true
	}
	for _, clientID := range rd.query2Tracker.GetAllClientIDs() {
		allClientIDs[clientID] = true
	}
	for _, clientID := range rd.query3Tracker.GetAllClientIDs() {
		allClientIDs[clientID] = true
	}
	for _, clientID := range rd.query4Tracker.GetAllClientIDs() {
		allClientIDs[clientID] = true
	}

	// Initialize or update ClientQueryStatus for each client based on CompletionTracker state
	for clientID := range allClientIDs {
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

		status := rd.clientQueryCompletion[clientID]
		status.Query1Completed = rd.query1Tracker.IsClientCompleted(clientID)
		status.Query2Completed = rd.query2Tracker.IsClientCompleted(clientID)
		status.Query3Completed = rd.query3Tracker.IsClientCompleted(clientID)
		status.Query4Completed = rd.query4Tracker.IsClientCompleted(clientID)
		status.AllQueriesCompleted = status.Query1Completed && status.Query2Completed &&
			status.Query3Completed && status.Query4Completed
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
