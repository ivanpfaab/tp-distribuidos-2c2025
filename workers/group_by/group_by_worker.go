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
	// Track per-query completion
	queryCompletion map[string]bool // key: "queryType_clientID"
}

// NewGroupByWorker creates a new group by worker
func NewGroupByWorker(workerID int, chunkQueue <-chan *chunk.Chunk, partialChannel chan<- *chunk.Chunk) *GroupByWorker {
	return &GroupByWorker{
		workerID:        workerID,
		chunkQueue:      chunkQueue,
		partialChannel:  partialChannel,
		running:         false,
		queryProcessor:  NewQueryProcessor(),
		queryCompletion: make(map[string]bool),
	}
}

// Start starts the worker to process chunks
func (gbw *GroupByWorker) Start() {
	gbw.running = true
	fmt.Printf("\033[37m[WORKER %d] Started and waiting for chunks\033[0m\n", gbw.workerID)

	for gbw.running {
		chunkData, ok := <-gbw.chunkQueue
		if !ok {
			// Channel closed, worker should stop
			fmt.Printf("\033[37m[WORKER %d] Chunk queue closed, stopping\033[0m\n", gbw.workerID)
			gbw.running = false
			break
		}

		fmt.Printf("\033[37m[WORKER %d] RECEIVED CHUNK - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\033[0m\n",
			gbw.workerID, chunkData.ClientID, chunkData.QueryType, chunkData.Step, chunkData.ChunkNumber, len(chunkData.ChunkData), chunkData.IsLastChunk)
		fmt.Printf("\033[37m[WORKER %d] CHUNK DATA:\n%s\033[0m\n", gbw.workerID, chunkData.ChunkData)

		// Process the chunk
		result := gbw.processChunk(chunkData)
		if result != nil {
			// Send result to partial reducer
			gbw.partialChannel <- result
			fmt.Printf("\033[37m[WORKER %d] SENT RESULT - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\033[0m\n",
				gbw.workerID, result.ClientID, result.QueryType, result.Step, result.ChunkNumber, len(result.ChunkData), result.IsLastChunk)
			fmt.Printf("\033[37m[WORKER %d] RESULT DATA:\n%s\033[0m\n", gbw.workerID, result.ChunkData)

			// Check if this is the last chunk for this specific query
			if result.IsLastChunk {
				queryKey := fmt.Sprintf("%d_%s", result.QueryType, result.ClientID)
				gbw.queryCompletion[queryKey] = true
				fmt.Printf("\033[37m[WORKER %d] QUERY COMPLETED - QueryKey: %s\033[0m\n", gbw.workerID, queryKey)

				// Send a special completion signal to partial reducer for this query
				completionChunk := &chunk.Chunk{
					ClientID:    result.ClientID,
					QueryType:   result.QueryType,
					TableID:     0,
					ChunkSize:   0,
					ChunkNumber: -1, // Special marker for completion
					IsLastChunk: true,
					Step:        result.Step,
					ChunkData:   "", // Empty data for completion signal
				}
				gbw.partialChannel <- completionChunk
				fmt.Printf("\033[37m[WORKER %d] SENT QUERY COMPLETION SIGNAL - QueryKey: %s\033[0m\n", gbw.workerID, queryKey)
			}
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
