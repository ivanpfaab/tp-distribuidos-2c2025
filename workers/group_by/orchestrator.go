package main

import (
	"fmt"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// GroupByOrchestrator manages the distributed group by operation
type GroupByOrchestrator struct {
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
}

// NewGroupByOrchestrator creates a new orchestrator with the specified number of workers
func NewGroupByOrchestrator(numWorkers int) *GroupByOrchestrator {
	chunkQueue := make(chan *chunk.Chunk, 100)            // Buffered channel for chunks
	reducerChannel := make(chan *chunk.Chunk, numWorkers) // Buffered channel for partial results

	// Create worker and partial reducer channels
	workerChannels := make([]chan *chunk.Chunk, numWorkers)
	partialChannels := make([]chan *chunk.Chunk, numWorkers)

	for i := 0; i < numWorkers; i++ {
		workerChannels[i] = make(chan *chunk.Chunk)  // Unbuffered channel for worker-partial reducer communication
		partialChannels[i] = make(chan *chunk.Chunk) // Unbuffered channel for partial reducer-reducer communication
	}

	orchestrator := &GroupByOrchestrator{
		chunkQueue:      chunkQueue,
		numWorkers:      numWorkers,
		workerChannels:  workerChannels,
		partialChannels: partialChannels,
		reducerChannel:  reducerChannel,
		done:            make(chan bool),
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

	return orchestrator
}

// Start starts all components of the group by operation
func (gbo *GroupByOrchestrator) Start() {
	fmt.Printf("GroupBy Orchestrator: Starting with %d workers\n", gbo.numWorkers)

	// Start workers
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

	fmt.Println("GroupBy Orchestrator: All components started")
}

// ProcessChunk adds a chunk to the processing queue
func (gbo *GroupByOrchestrator) ProcessChunk(chunk *chunk.Chunk) {
	fmt.Printf("\033[35m[ORCHESTRATOR] RECEIVED CHUNK - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\033[0m\n",
		chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData))

	// Send chunk to the shared queue for workers to process
	select {
	case gbo.chunkQueue <- chunk:
		fmt.Printf("\033[35m[ORCHESTRATOR] QUEUED CHUNK %d for %d workers\033[0m\n", chunk.ChunkNumber, gbo.numWorkers)
	default:
		fmt.Printf("\033[35m[ORCHESTRATOR] WARNING - chunk queue is full, chunk %d may be delayed\033[0m\n", chunk.ChunkNumber)
		gbo.chunkQueue <- chunk // Block until space is available
		fmt.Printf("\033[35m[ORCHESTRATOR] QUEUED CHUNK %d (after delay)\033[0m\n", chunk.ChunkNumber)
	}
}

// FinishProcessing signals that all chunks have been sent and workers should finish
func (gbo *GroupByOrchestrator) FinishProcessing() {
	fmt.Println("\033[35m[ORCHESTRATOR] All chunks sent, closing chunk queue\033[0m")
	close(gbo.chunkQueue)
}

// Stop stops all components gracefully
func (gbo *GroupByOrchestrator) Stop() {
	fmt.Println("GroupBy Orchestrator: Stopping all components...")

	// Wait for all components to finish
	gbo.wg.Wait()

	fmt.Println("GroupBy Orchestrator: All components stopped")
}

// GetResultChannel returns the channel where final results are sent
func (gbo *GroupByOrchestrator) GetResultChannel() <-chan *chunk.Chunk {
	return gbo.reducerChannel
}
