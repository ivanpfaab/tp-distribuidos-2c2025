package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// GroupByWorker represents a worker within the distributed group_by system
type GroupByWorker struct {
	workerID       int
	chunkQueue     <-chan *chunk.Chunk
	partialChannel chan<- *chunk.Chunk
	running        bool
}

// NewGroupByWorker creates a new group by worker
func NewGroupByWorker(workerID int, chunkQueue <-chan *chunk.Chunk, partialChannel chan<- *chunk.Chunk) *GroupByWorker {
	return &GroupByWorker{
		workerID:       workerID,
		chunkQueue:     chunkQueue,
		partialChannel: partialChannel,
		running:        false,
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

		fmt.Printf("\033[37m[WORKER %d] RECEIVED CHUNK - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\033[0m\n",
			gbw.workerID, chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData))

		// Process the chunk
		result := gbw.processChunk(chunk)
		if result != nil {
			// Send result to partial reducer
			gbw.partialChannel <- result
			fmt.Printf("\033[37m[WORKER %d] SENT RESULT - ChunkNumber: %d, ResultSize: %d\033[0m\n",
				gbw.workerID, chunk.ChunkNumber, len(result.ChunkData))
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

	// Dummy group by operation: take first 10 elements
	lines := strings.Split(chunkData.ChunkData, "\n")
	var resultLines []string
	count := 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" && count < 10 {
			resultLines = append(resultLines, line)
			count++
		}
	}

	// Create result data
	resultData := strings.Join(resultLines, "\n")

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

	fmt.Printf("\033[37m[WORKER %d] COMPLETED - ChunkNumber: %d, Original: %d lines, Result: %d lines, Size: %d bytes\033[0m\n",
		gbw.workerID, chunkData.ChunkNumber, len(lines), len(resultLines), len(resultData))

	return resultChunk
}

// Stop stops the worker
func (gbw *GroupByWorker) Stop() {
	gbw.running = false
}
