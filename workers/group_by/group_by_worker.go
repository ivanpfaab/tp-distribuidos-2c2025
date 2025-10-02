package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// GroupByWorker represents a worker within the distributed group_by system
type GroupByWorker struct {
	workerID       int
	chunkQueue     <-chan *chunk.Chunk
	partialChannel chan<- *chunk.Chunk
	running        bool
	queryProcessor *QueryProcessor
}

// NewGroupByWorker creates a new group by worker
func NewGroupByWorker(workerID int, chunkQueue <-chan *chunk.Chunk, partialChannel chan<- *chunk.Chunk) *GroupByWorker {
	return &GroupByWorker{
		workerID:       workerID,
		chunkQueue:     chunkQueue,
		partialChannel: partialChannel,
		running:        false,
		queryProcessor: NewQueryProcessor(),
	}
}

// Start starts the worker to process chunks
func (gbw *GroupByWorker) Start() {
	gbw.running = true
	fmt.Printf("\033[37m[WORKER %d] Started and waiting for chunks\033[0m\n", gbw.workerID)

	for gbw.running {
		chunk, ok := <-gbw.chunkQueue
		if !ok {
			// Channel closed, worker should stop
			fmt.Printf("\033[37m[WORKER %d] Chunk queue closed, stopping\033[0m\n", gbw.workerID)
			gbw.running = false
			break
		}

		fmt.Printf("\033[37m[WORKER %d] RECEIVED CHUNK - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\033[0m\n",
			gbw.workerID, chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData), chunk.IsLastChunk)
		fmt.Printf("\033[37m[WORKER %d] CHUNK DATA:\n%s\033[0m\n", gbw.workerID, chunk.ChunkData)

		// Process the chunk
		result := gbw.processChunk(chunk)
		if result != nil {
			// Send result to partial reducer
			gbw.partialChannel <- result
			fmt.Printf("\033[37m[WORKER %d] SENT RESULT - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\033[0m\n",
				gbw.workerID, result.ClientID, result.QueryType, result.Step, result.ChunkNumber, len(result.ChunkData), result.IsLastChunk)
			fmt.Printf("\033[37m[WORKER %d] RESULT DATA:\n%s\033[0m\n", gbw.workerID, result.ChunkData)
		}
	}

	// Signal partial reducer that this worker is done
	close(gbw.partialChannel)
	fmt.Printf("\033[37m[WORKER %d] Stopped\033[0m\n", gbw.workerID)
}

// processChunk applies the group by operation to a chunk
func (gbw *GroupByWorker) processChunk(chunkData *chunk.Chunk) *chunk.Chunk {
	fmt.Printf("\033[37m[WORKER %d] PROCESSING - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d\033[0m\n",
		gbw.workerID, chunkData.QueryType, chunkData.Step, chunkData.ClientID, chunkData.ChunkNumber)

	// Process the query using the query processor (RAW DATA)
	results := gbw.queryProcessor.ProcessQuery(chunkData, RawData)

	// Convert results to CSV format
	resultData := gbw.queryProcessor.ResultsToCSV(results, int(chunkData.QueryType))

	// Get the number of groups for logging
	numGroups := getResultLength(results, int(chunkData.QueryType))

	// Create result chunk
	resultChunk := &chunk.Chunk{
		ClientID:    chunkData.ClientID,
		QueryType:   chunkData.QueryType,
		ChunkSize:   len(resultData),
		ChunkNumber: chunkData.ChunkNumber,
		IsLastChunk: chunkData.IsLastChunk,
		Step:        chunkData.Step, // Keep same step for consistency
		ChunkData:   resultData,
	}

	fmt.Printf("\033[37m[WORKER %d] COMPLETED - ChunkNumber: %d, Groups: %d, Size: %d bytes\033[0m\n",
		gbw.workerID, chunkData.ChunkNumber, numGroups, len(resultData))

	return resultChunk
}

// Stop stops the worker
func (gbw *GroupByWorker) Stop() {
	gbw.running = false
}

// getResultLength returns the length of results based on query type
func getResultLength(results interface{}, queryType int) int {
	switch queryType {
	case 2:
		return len(results.([]QueryType2Result))
	case 3:
		return len(results.([]QueryType3Result))
	case 4:
		return len(results.([]QueryType4Result))
	default:
		return 0
	}
}
