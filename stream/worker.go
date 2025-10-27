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

// StreamingWorker encapsulates the streaming worker state and dependencies
type StreamingWorker struct {
	query1Consumer        *workerqueue.QueueConsumer
	query2Consumer        *workerqueue.QueueConsumer
	query3Consumer        *workerqueue.QueueConsumer
	query4Consumer        *workerqueue.QueueConsumer
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

// NewStreamingWorker creates a new StreamingWorker instance
func NewStreamingWorker(config *middleware.ConnectionConfig) (*StreamingWorker, error) {
	// Declare Query1 results queue
	query1QueueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		Query1ResultsQueue,
		config,
	)
	if query1QueueDeclarer == nil {
		return nil, fmt.Errorf("failed to create Query1 queue declarer")
	}
	if err := query1QueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		query1QueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare Query1 results queue: %v", err)
	}
	query1QueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create Query1 results consumer
	query1Consumer := workerqueue.NewQueueConsumer(
		Query1ResultsQueue,
		config,
	)
	if query1Consumer == nil {
		return nil, fmt.Errorf("failed to create Query1 results consumer")
	}

	// Declare Query2 results queue
	query2QueueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		Query2ResultsQueue,
		config,
	)
	if query2QueueDeclarer == nil {
		query1Consumer.Close()
		return nil, fmt.Errorf("failed to create Query2 queue declarer")
	}
	if err := query2QueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		query1Consumer.Close()
		query2QueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare Query2 results queue: %v", err)
	}
	query2QueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create Query2 results consumer
	query2Consumer := workerqueue.NewQueueConsumer(
		Query2ResultsQueue,
		config,
	)
	if query2Consumer == nil {
		query1Consumer.Close()
		return nil, fmt.Errorf("failed to create Query2 results consumer")
	}

	// Declare Query3 results queue
	query3QueueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		Query3ResultsQueue,
		config,
	)
	if query3QueueDeclarer == nil {
		query1Consumer.Close()
		query2Consumer.Close()
		return nil, fmt.Errorf("failed to create Query3 queue declarer")
	}
	if err := query3QueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		query1Consumer.Close()
		query2Consumer.Close()
		query3QueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare Query3 results queue: %v", err)
	}
	query3QueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create Query3 results consumer
	query3Consumer := workerqueue.NewQueueConsumer(
		Query3ResultsQueue,
		config,
	)
	if query3Consumer == nil {
		query1Consumer.Close()
		query2Consumer.Close()
		return nil, fmt.Errorf("failed to create Query3 results consumer")
	}

	// Declare Query4 results queue
	query4QueueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		Query4ResultsQueue,
		config,
	)
	if query4QueueDeclarer == nil {
		query1Consumer.Close()
		query2Consumer.Close()
		query3Consumer.Close()
		return nil, fmt.Errorf("failed to create Query4 queue declarer")
	}
	if err := query4QueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		query1Consumer.Close()
		query2Consumer.Close()
		query3Consumer.Close()
		query4QueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare Query4 results queue: %v", err)
	}
	query4QueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create Query4 results consumer
	query4Consumer := workerqueue.NewQueueConsumer(
		Query4ResultsQueue,
		config,
	)
	if query4Consumer == nil {
		query1Consumer.Close()
		query2Consumer.Close()
		query3Consumer.Close()
		return nil, fmt.Errorf("failed to create Query4 results consumer")
	}

	// Create client results producer
	clientResultsProducer := workerqueue.NewMessageMiddlewareQueue(
		queues.ClientResultsQueue,
		config,
	)

	if err := clientResultsProducer.DeclareQueue(false, false, false, false); err != 0 {
		query1Consumer.Close()
		query2Consumer.Close()
		query3Consumer.Close()
		query4Consumer.Close()
		clientResultsProducer.Close()
		return nil, fmt.Errorf("failed to declare client results queue: %v", err)
	}

	// Create streaming worker instance
	sw := &StreamingWorker{
		query1Consumer:        query1Consumer,
		query2Consumer:        query2Consumer,
		query3Consumer:        query3Consumer,
		query4Consumer:        query4Consumer,
		clientResultsProducer: clientResultsProducer,
		config:                config,
		clientQueryCompletion: make(map[string]*ClientQueryStatus),
	}

	// Initialize completion tracker (only for Query1)
	sw.query1Tracker = shared.NewCompletionTracker("Query1", sw.onQuery1Completed)

	return sw, nil
}

// Start starts the streaming worker
func (sw *StreamingWorker) Start() middleware.MessageMiddlewareError {
	testing_utils.LogInfo("Streaming Worker", "Starting to listen for messages...")

	if err := sw.query1Consumer.StartConsuming(sw.createQuery1Callback()); err != 0 {
		testing_utils.LogError("Streaming Worker", "Failed to start Query1 results consumer: %v", err)
	}

	if err := sw.query2Consumer.StartConsuming(sw.createQuery2Callback()); err != 0 {
		testing_utils.LogError("Streaming Worker", "Failed to start Query2 results consumer: %v", err)
	}

	if err := sw.query3Consumer.StartConsuming(sw.createQuery3Callback()); err != 0 {
		testing_utils.LogError("Streaming Worker", "Failed to start Query3 results consumer: %v", err)
	}

	if err := sw.query4Consumer.StartConsuming(sw.createQuery4Callback()); err != 0 {
		testing_utils.LogError("Streaming Worker", "Failed to start Query4 results consumer: %v", err)
	}

	return 0
}

// Close closes all connections
func (sw *StreamingWorker) Close() {
	if sw.query1Consumer != nil {
		sw.query1Consumer.Close()
	}
	if sw.query2Consumer != nil {
		sw.query2Consumer.Close()
	}
	if sw.query3Consumer != nil {
		sw.query3Consumer.Close()
	}
	if sw.query4Consumer != nil {
		sw.query4Consumer.Close()
	}
	if sw.clientResultsProducer != nil {
		sw.clientResultsProducer.Close()
	}
}

// onQuery1Completed is called when Query1 completes for a client
func (sw *StreamingWorker) onQuery1Completed(clientID string, clientStatus *shared.ClientStatus) {
	sw.updateQueryCompletion(clientID, 1)
}

// updateQueryCompletion updates the completion status for Query1
func (sw *StreamingWorker) updateQueryCompletion(clientID string, queryType int) {
	sw.completionMutex.Lock()
	defer sw.completionMutex.Unlock()

	// Initialize client query status if not exists
	if sw.clientQueryCompletion[clientID] == nil {
		sw.clientQueryCompletion[clientID] = &ClientQueryStatus{
			ClientID:            clientID,
			Query1Completed:     false,
			Query2Completed:     false,
			Query3Completed:     false,
			Query4Completed:     false,
			AllQueriesCompleted: false,
		}
	}

	clientQueryStatus := sw.clientQueryCompletion[clientID]

	// Mark Query1 as completed
	if queryType == 1 {
		clientQueryStatus.Query1Completed = true
		testing_utils.LogInfo("Streaming Worker", "✅ Query1 completed for client %s", clientID)
	}

	// Check if all queries are completed
	if clientQueryStatus.Query1Completed && clientQueryStatus.Query2Completed &&
		clientQueryStatus.Query3Completed && clientQueryStatus.Query4Completed {

		if !clientQueryStatus.AllQueriesCompleted {
			clientQueryStatus.AllQueriesCompleted = true
			sw.sendSystemCompleteMessage(clientID)
		}
	}
}

// sendSystemCompleteMessage sends a system complete signal to the client
func (sw *StreamingWorker) sendSystemCompleteMessage(clientID string) {
	testing_utils.LogInfo("Streaming Worker", "🎉 All queries completed for client %s! Sending system complete signal.", clientID)

	// Create completion signal
	completionMessage := fmt.Sprintf("All queries completed for client %s", clientID)
	completionSignal := signals.NewClientCompletionSignal(clientID, completionMessage)

	// Serialize the signal
	serializedSignal, err := signals.SerializeClientCompletionSignal(completionSignal)
	if err != nil {
		testing_utils.LogError("Streaming Worker", "Failed to serialize completion signal for client %s: %v", clientID, err)
		return
	}

	// Send to client results queue
	if err := sw.clientResultsProducer.Send(serializedSignal); err != 0 {
		testing_utils.LogError("Streaming Worker", "Failed to send completion signal for client %s: %v", clientID, err)
	}
}

// createQuery1Callback creates the message processing callback for Query1 results
func (sw *StreamingWorker) createQuery1Callback() middleware.OnMessageCallback {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		testing_utils.LogInfo("Streaming Worker", "Starting to listen for Query1 results...")
		for delivery := range *consumeChannel {
			if err := sw.processMessage(delivery, 1); err != 0 {
				testing_utils.LogError("Streaming Worker", "Failed to process Query1 message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// createQuery2Callback creates the message processing callback for Query2 results
func (sw *StreamingWorker) createQuery2Callback() middleware.OnMessageCallback {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		testing_utils.LogInfo("Streaming Worker", "Starting to listen for Query2 results...")
		for delivery := range *consumeChannel {
			if err := sw.processMessage(delivery, 2); err != 0 {
				testing_utils.LogError("Streaming Worker", "Failed to process Query2 message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// createQuery3Callback creates the message processing callback for Query3 results
func (sw *StreamingWorker) createQuery3Callback() middleware.OnMessageCallback {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		testing_utils.LogInfo("Streaming Worker", "Starting to listen for Query3 results...")
		for delivery := range *consumeChannel {
			if err := sw.processMessage(delivery, 3); err != 0 {
				testing_utils.LogError("Streaming Worker", "Failed to process Query3 message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// createQuery4Callback creates the message processing callback for Query4 results
func (sw *StreamingWorker) createQuery4Callback() middleware.OnMessageCallback {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		testing_utils.LogInfo("Streaming Worker", "Starting to listen for Query4 results...")
		for delivery := range *consumeChannel {
			if err := sw.processMessage(delivery, 4); err != 0 {
				testing_utils.LogError("Streaming Worker", "Failed to process Query4 message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}
