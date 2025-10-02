package main

import (
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// GroupByOrchestrator manages the distributed group by operation and RabbitMQ integration
type GroupByOrchestrator struct {
	// RabbitMQ components
	consumer      *exchange.ExchangeConsumer
	replyProducer *workerqueue.QueueMiddleware
	config        *middleware.ConnectionConfig

	// Distributed processing components
	chunkQueue      chan *chunk.Chunk
	workers         []*GroupByWorker
	partialReducers []*GroupByPartialReducer
	reducer         *GroupByReducer
	numWorkers      int
	workerChannels  []chan *chunk.Chunk
	partialChannels []chan *chunk.Chunk
	reducerChannel  chan *chunk.Chunk
	done            chan bool
	wg              sync.WaitGroup

	// Query-specific coordination
	queryCompletion map[string]int // key: "queryType_clientID", value: completed workers count
	queryMutex      sync.RWMutex   // protects queryCompletion map
}

// NewGroupByOrchestrator creates a new orchestrator with RabbitMQ integration
func NewGroupByOrchestrator(config *middleware.ConnectionConfig, numWorkers int) (*GroupByOrchestrator, error) {
	// Create group by consumer
	consumer := exchange.NewExchangeConsumer(
		GroupByExchangeName,
		[]string{GroupByRoutingKey},
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create group by consumer")
	}

	// Create reply producer
	replyProducer := workerqueue.NewMessageMiddlewareQueue(
		ReplyQueueName,
		config,
	)
	if replyProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create reply producer")
	}

	// Declare the reply queue
	if err := replyProducer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		replyProducer.Close()
		return nil, fmt.Errorf("failed to declare reply queue: %v", err)
	}

	// Create channels for internal communication
	chunkQueue := make(chan *chunk.Chunk, numWorkers*2)
	workerChannels := make([]chan *chunk.Chunk, numWorkers)
	partialChannels := make([]chan *chunk.Chunk, numWorkers)
	reducerChannel := make(chan *chunk.Chunk, numWorkers)

	for i := 0; i < numWorkers; i++ {
		workerChannels[i] = make(chan *chunk.Chunk)
		partialChannels[i] = make(chan *chunk.Chunk)
	}

	orchestrator := &GroupByOrchestrator{
		// RabbitMQ components
		consumer:      consumer,
		replyProducer: replyProducer,
		config:        config,

		// Distributed processing components
		chunkQueue:      chunkQueue,
		numWorkers:      numWorkers,
		workerChannels:  workerChannels,
		partialChannels: partialChannels,
		reducerChannel:  reducerChannel,
		done:            make(chan bool),

		// Query-specific coordination
		queryCompletion: make(map[string]int),
	}

	// Create workers
	orchestrator.workers = make([]*GroupByWorker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		orchestrator.workers[i] = NewGroupByWorker(i, chunkQueue, workerChannels[i])
	}

	// Create partial reducers
	orchestrator.partialReducers = make([]*GroupByPartialReducer, numWorkers)
	for i := 0; i < numWorkers; i++ {
		orchestrator.partialReducers[i] = NewGroupByPartialReducer(i, workerChannels[i], partialChannels[i])
	}

	// Create final reducer
	orchestrator.reducer = NewGroupByReducer(partialChannels, reducerChannel)

	return orchestrator, nil
}

// Start starts the orchestrator and all its components
func (gbo *GroupByOrchestrator) Start() middleware.MessageMiddlewareError {
	fmt.Printf("GroupBy Orchestrator: Starting with %d workers\n", gbo.numWorkers)

	// Start internal workers
	for i, worker := range gbo.workers {
		gbo.wg.Add(1)
		go func(w *GroupByWorker, workerID int) {
			defer gbo.wg.Done()
			w.Start()
		}(worker, i)
	}

	// Start partial reducers
	for i, partialReducer := range gbo.partialReducers {
		gbo.wg.Add(1)
		go func(pr *GroupByPartialReducer, reducerID int) {
			defer gbo.wg.Done()
			pr.Start()
		}(partialReducer, i)
	}

	// Start final reducer
	gbo.wg.Add(1)
	go func() {
		defer gbo.wg.Done()
		gbo.reducer.Start()
	}()

	// Start listening for messages from RabbitMQ (if consumer is available)
	if gbo.consumer != nil {
		fmt.Println("GroupBy Orchestrator: Starting to listen for messages...")
		return gbo.consumer.StartConsuming(gbo.createCallback())
	}

	// For testing without RabbitMQ, just return success
	fmt.Println("GroupBy Orchestrator: Running in test mode (no RabbitMQ)")
	return 0
}

// Close closes all connections
func (gbo *GroupByOrchestrator) Close() {
	fmt.Println("GroupBy Orchestrator: Stopping all components...")

	// Stop the distributed processing
	gbo.Stop()

	// Close RabbitMQ connections
	if gbo.consumer != nil {
		gbo.consumer.Close()
	}
	if gbo.replyProducer != nil {
		gbo.replyProducer.Close()
	}
}

// Stop stops all distributed processing components
func (gbo *GroupByOrchestrator) Stop() {
	// Close chunk queue to signal workers to stop
	// Using select to avoid panic if channel is already closed
	select {
	case <-gbo.chunkQueue:
		// Channel already closed
	default:
		close(gbo.chunkQueue)
	}

	// Wait for all components to finish
	gbo.wg.Wait()

	fmt.Println("GroupBy Orchestrator: All components stopped")
}

// createCallback creates the message processing callback
func (gbo *GroupByOrchestrator) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		fmt.Println("GroupBy Orchestrator: Starting to listen for messages...")
		for delivery := range *consumeChannel {
			if err := gbo.processMessage(delivery); err != 0 {
				fmt.Printf("GroupBy Orchestrator: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// processMessage processes a single message
func (gbo *GroupByOrchestrator) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("GroupBy Orchestrator: Received message from RabbitMQ - Size: %d bytes\n", len(delivery.Body))

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("GroupBy Orchestrator: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Process the chunk using the distributed system
	fmt.Printf("GroupBy Orchestrator: Processing chunk - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\n",
		chunkMsg.ClientID, chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ChunkNumber, len(chunkMsg.ChunkData), chunkMsg.IsLastChunk)
	fmt.Printf("GroupBy Orchestrator: CHUNK DATA:\n%s\n", chunkMsg.ChunkData)

	// Send chunk to the distributed processing
	gbo.ProcessChunk(chunkMsg)

	// Listen for the final result
	go gbo.waitForResult(chunkMsg)

	return 0
}

// waitForResult waits for the final result from the distributed processing
func (gbo *GroupByOrchestrator) waitForResult(originalChunk *chunk.Chunk) {
	fmt.Printf("GroupBy Orchestrator: Waiting for result for ClientID: %s, QueryType: %d, ChunkNumber: %d\n",
		originalChunk.ClientID, originalChunk.QueryType, originalChunk.ChunkNumber)

	// Listen for results from the reducer
	for resultChunk := range gbo.reducerChannel {
		// Check if this result matches our original chunk
		if resultChunk.ClientID == originalChunk.ClientID && resultChunk.QueryType == originalChunk.QueryType {
			fmt.Printf("GroupBy Orchestrator: Received final result - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\n",
				resultChunk.ClientID, resultChunk.QueryType, resultChunk.Step, resultChunk.ChunkNumber, len(resultChunk.ChunkData), resultChunk.IsLastChunk)
			fmt.Printf("GroupBy Orchestrator: FINAL RESULT DATA:\n%s\n", resultChunk.ChunkData)

			// Only send reply if the result has actual data (Size > 0)
			// This prevents sending empty results that come from completion signals
			if len(resultChunk.ChunkData) > 0 {
				fmt.Printf("GroupBy Orchestrator: Sending reply with actual data (Size: %d)\n", len(resultChunk.ChunkData))
				gbo.sendReply(resultChunk)
				break
			} else {
				fmt.Printf("GroupBy Orchestrator: Ignoring empty result, waiting for data...\n")
			}
		}
	}
}

// sendReply sends a processed chunk as a reply back to the orchestrator
func (gbo *GroupByOrchestrator) sendReply(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message for reply
	replyData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("GroupBy Orchestrator: Failed to serialize reply message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send the reply to the orchestrator reply queue
	if err := gbo.replyProducer.Send(replyData); err != 0 {
		fmt.Printf("GroupBy Orchestrator: Failed to send reply to orchestrator: %v\n", err)
		return err
	}

	fmt.Printf("GroupBy Orchestrator: Reply sent successfully - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\n",
		chunkMsg.ClientID, chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ChunkNumber, len(chunkMsg.ChunkData), chunkMsg.IsLastChunk)
	return 0
}

// ProcessChunk processes a single chunk through the distributed group_by system
func (gbo *GroupByOrchestrator) ProcessChunk(chunk *chunk.Chunk) {
	fmt.Printf("\033[35m[ORCHESTRATOR] RECEIVED CHUNK - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\033[0m\n",
		chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData), chunk.IsLastChunk)
	fmt.Printf("\033[35m[ORCHESTRATOR] CHUNK DATA:\n%s\033[0m\n", chunk.ChunkData)

	// Send chunk to the shared queue for workers to process
	select {
	case gbo.chunkQueue <- chunk:
		fmt.Printf("\033[35m[ORCHESTRATOR] QUEUED CHUNK %d for %d workers\033[0m\n", chunk.ChunkNumber, gbo.numWorkers)
	default:
		fmt.Printf("\033[35m[ORCHESTRATOR] WARNING - chunk queue is full, chunk %d may be delayed\033[0m\n", chunk.ChunkNumber)
		gbo.chunkQueue <- chunk // Block until space is available
		fmt.Printf("\033[35m[ORCHESTRATOR] QUEUED CHUNK %d (after delay)\033[0m\n", chunk.ChunkNumber)
	}

	// If this is the last chunk for this query, send completion signals to all workers
	if chunk.IsLastChunk {
		gbo.sendQueryCompletionSignals(chunk)
	}
}

// sendQueryCompletionSignals sends completion signals to all workers for a specific query
func (gbo *GroupByOrchestrator) sendQueryCompletionSignals(chunkMsg *chunk.Chunk) {
	fmt.Printf("\033[35m[ORCHESTRATOR] SENDING COMPLETION SIGNALS - QueryType: %d, ClientID: %s\033[0m\n",
		chunkMsg.QueryType, chunkMsg.ClientID)

	// Create completion signal chunk (ChunkNumber = -1 indicates completion)
	completionChunk := chunk.NewChunk(
		chunkMsg.ClientID,
		chunkMsg.FileID,
		chunkMsg.QueryType,
		-1,   // Special value indicating query completion
		true, // IsLastChunk
		chunkMsg.Step,
		0, // ChunkSize
		chunkMsg.TableID,
		"", // ChunkData
	)

	// Send completion signal to all workers
	for i := 0; i < gbo.numWorkers; i++ {
		select {
		case gbo.chunkQueue <- completionChunk:
			fmt.Printf("\033[35m[ORCHESTRATOR] SENT COMPLETION SIGNAL to worker %d\033[0m\n", i)
		default:
			fmt.Printf("\033[35m[ORCHESTRATOR] WARNING - Could not send completion signal to worker %d\033[0m\n", i)
		}
	}
}

// FinishProcessing signals that all chunks have been sent and workers should finish
func (gbo *GroupByOrchestrator) FinishProcessing() {
	fmt.Println("\033[35m[ORCHESTRATOR] All chunks sent, closing chunk queue\033[0m")
	// Using select to avoid panic if channel is already closed
	select {
	case <-gbo.chunkQueue:
		// Channel already closed
	default:
		close(gbo.chunkQueue)
	}
}

// ResetForNextQuery resets the orchestrator for the next query
func (gbo *GroupByOrchestrator) ResetForNextQuery() {
	fmt.Println("\033[35m[ORCHESTRATOR] Resetting for next query\033[0m")

	// Create a new chunk queue for the next query
	gbo.chunkQueue = make(chan *chunk.Chunk, gbo.numWorkers*2)

	// Restart all components
	gbo.Start()
}

// GetResultChannel returns the channel where final results are sent
func (gbo *GroupByOrchestrator) GetResultChannel() <-chan *chunk.Chunk {
	return gbo.reducerChannel
}
